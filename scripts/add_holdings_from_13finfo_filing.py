import os
import re
import json
import argparse
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


TABLE_13FINFO = "public.expected_13finfo_holdings"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) stock-analyzer/1.0"
TIMEOUT = 30
INSERT_CHUNK = 5000
BASE = "https://13f.info"


def make_session():
    s = requests.Session()
    retries = Retry(
        total=8,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": UA})
    return s


def clean_text(x):
    if x is None:
        return None
    t = str(x).strip()
    if t == "" or t.lower() == "nan":
        return None
    return t


def to_decimal(x):
    t = clean_text(x)
    if t is None:
        return None
    t = t.replace(",", "").replace("$", "").strip()
    t = t.replace("%", "").strip()
    try:
        return Decimal(t)
    except (InvalidOperation, ValueError):
        return None


def parse_filing_url(filing_url: str):
    """
    Example:
    https://13f.info/13f/000091957414001804-kingdon-capital-management-l-l-c-q4-2013
    -> filing_id='000091957414001804', cik='919574', quarter='Q4 2013'
    """
    path = urlparse(filing_url).path.strip("/")
    if not path.startswith("13f/"):
        raise ValueError("URL must look like https://13f.info/13f/<...>")

    slug = path.split("13f/", 1)[1]
    m = re.search(
        r"^(?P<fid>\d{18})-.*-(?P<q>q[1-4])-(?P<y>\d{4})(?:-new-holdings)?$",
        slug,
        re.I,
    )
    if not m:
        raise ValueError("Could not parse filing_id/quarter/year from filing URL")

    filing_id = m.group("fid")
    q = m.group("q").upper()
    y = m.group("y")

    cik10 = filing_id[:10]
    cik = cik10.lstrip("0") or "0"
    quarter = f"{q} {y}"
    return filing_id, cik, quarter


def normalize_holding_row(d):
    """
    Map JSON keys to DB columns.
    JSON key names can vary; we handle common ones.
    Also handles list input (new API format) by mapping by index.
    """
    if isinstance(d, list):
        # 0: sym, 1: issuer, 2: class, 3: cusip, 4: value, 5: pct, 6: shares, 7: principal, 8: option
        keys = ["sym", "issuer_name", "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]
        d_map = {}
        for i, k in enumerate(keys):
            if i < len(d):
                d_map[k] = d[i]
        d = d_map

    def get_any(*names):
        for n in names:
            for k, v in d.items():
                if k.lower() == n.lower():
                    return v
        return None

    sym = clean_text(get_any("sym", "ticker", "symbol"))
    issuer_name = clean_text(get_any("issuer_name", "issuer", "name", "issuerName"))
    cls = clean_text(get_any("class", "cl", "title_of_class", "titleOfClass"))
    cusip = clean_text(get_any("cusip"))
    value_000 = to_decimal(get_any("value_000", "value", "value000", "value_000s"))
    pct = to_decimal(get_any("pct", "percent", "percentage"))
    shares = to_decimal(get_any("shares", "share", "sshPrnamt", "sshprnamt"))
    principal = clean_text(get_any("principal", "share_type", "shareType"))
    option_type = clean_text(get_any("option_type", "put_call", "putCall", "optionType"))

    if cusip is None or issuer_name is None:
        return None

    return {
        "sym": sym,
        "issuer_name": issuer_name,
        "class": cls,
        "cusip": cusip,
        "value_000": value_000,
        "pct": pct,
        "shares": shares,
        "principal": principal,
        "option_type": option_type,
    }


def extract_holdings_from_json(payload: dict):
    """
    13f.info endpoint returns {"data": ...}
    Holdings are nested inside payload["data"].
    We find the largest list[dict] that looks like holdings:
      - dict contains 'cusip'
    """
    root = payload.get("data", payload)

    best = []
    stack = [root]

    def is_holding_dict(d):
        if not isinstance(d, dict):
            return False
        keys = {k.lower() for k in d.keys()}
        return "cusip" in keys

    while stack:
        x = stack.pop()

        if isinstance(x, dict):
            for vv in x.values():
                stack.append(vv)

        elif isinstance(x, list):
            if not x:
                continue

            is_list_of_dicts = all(isinstance(i, dict) for i in x)
            is_list_of_lists = all(isinstance(i, list) for i in x)

            if is_list_of_dicts:
                # must contain cusip in most rows
                holding_like = sum(1 for i in x if is_holding_dict(i))
                if holding_like >= max(5, int(0.5 * len(x))):
                    if len(x) > len(best):
                        best = x
            
            elif is_list_of_lists:
                # New format: list of lists
                # We assume a list of lists that is substantial is our table
                if len(x) > len(best):
                    best = x

            for i in x:
                # Only recurse if the item is a container (list or dict), 
                # but valid rows (lists) might be recursed into and ignored, which is fine.
                if isinstance(i, (dict, list)):
                    stack.append(i)

    return best


def main():
    # Load environment variables
    load_dotenv()
    load_dotenv(".env.local")
    
    # Also attempt to load from the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, ".env"))

    ap = argparse.ArgumentParser()
    ap.add_argument("--filing-url", required=True)
    ap.add_argument("--mode", choices=["replace", "append"], default="replace")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    session = make_session()

    filing_id, cik, quarter = parse_filing_url(args.filing_url)
    manager_url = f"{BASE}/manager/{filing_id[:10]}"

    api_url = f"{BASE}/data/13f/{filing_id}"
    r = session.get(api_url, timeout=TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"13f.info API failed: {r.status_code} {api_url}")

    payload = r.json()
    raw_holdings = extract_holdings_from_json(payload)  # now searches inside payload["data"]

    holdings = []
    for d in raw_holdings:
        row = normalize_holding_row(d)
        if row:
            holdings.append(row)

    print("======================================================================")
    print("Scrape holdings from 13f.info JSON endpoint -> DB")
    print(f"Filing URL : {args.filing_url}")
    print(f"API URL    : {api_url}")
    print(f"CIK        : {cik}")
    print(f"Quarter    : {quarter}")
    print(f"Rows parsed: {len(holdings)}")
    print(f"Mode       : {args.mode}")
    print(f"Dry run    : {args.dry_run}")
    print("======================================================================")

    if args.dry_run:
        for row in holdings[:10]:
            print(row)
        return

    if len(holdings) == 0:
        print("⚠️ Parsed 0 holdings. Nothing to insert.")
        return

    save_holdings_to_db(holdings, db_url, args.mode, args.filing_url, manager_url, cik, quarter)


def save_holdings_to_db(holdings, db_url, mode, filing_url, manager_url, cik, quarter):
    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            if mode == "replace":
                cur.execute(
                    f"DELETE FROM {TABLE_13FINFO} WHERE filing_url = %s",
                    (filing_url,),
                )

        total = 0
        batch = []

        def flush():
            nonlocal total, batch
            if not batch:
                return
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {TABLE_13FINFO}
                    (
                      manager_url,
                      cik,
                      quarter,
                      filing_url,
                      sym,
                      issuer_name,
                      class,
                      cusip,
                      value_000,
                      pct,
                      shares,
                      principal,
                      option_type
                    )
                    VALUES %s
                    """,
                    batch,
                    page_size=len(batch),
                )
            conn.commit()
            total += len(batch)
            batch = []

        for h in holdings:
            batch.append(
                (
                    manager_url,     # manager_url (best-effort)
                    cik,
                    quarter,
                    filing_url,
                    h["sym"],
                    h["issuer_name"],
                    h["class"],
                    h["cusip"],
                    h["value_000"],
                    h["pct"],
                    h["shares"],
                    h["principal"],
                    h["option_type"],
                )
            )
            if len(batch) >= INSERT_CHUNK:
                flush()

        flush()
        print(f"✅ Inserted rows: {total}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
