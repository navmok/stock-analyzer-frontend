"""
13f.info Holdings Scraper using JSON API

Uses the discovered API endpoint: https://13f.info/data/13f/{filing_id}

Usage:
  python download_13finfo_holdings_api.py --quarter "Q3 2025" --max-workers 8

Test:
  python download_13finfo_holdings_api.py --quarter "Q3 2025" --limit 10 --max-workers 4
"""
import argparse
import os
import re
import time
import random
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from concurrent.futures import ThreadPoolExecutor, as_completed

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import execute_values


BASE = "https://13f.info"
UA = os.getenv("SCRAPER_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) 13f-holdings-scraper/3.0")

TABLE = "public.expected_13finfo_holdings"

DEFAULT_OUT_CSV = "13finfo_holdings_checkpoint.csv"
DEFAULT_DONE_TXT = "13finfo_holdings_done_managers.txt"
DEFAULT_FAIL_TXT = "13finfo_holdings_failed_managers.txt"

# Polite jitter
MIN_JITTER = 0.05
MAX_JITTER = 0.2


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})

    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=50,
        pool_maxsize=50,
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = build_session()


def jitter():
    time.sleep(random.uniform(MIN_JITTER, MAX_JITTER))


def norm_quarter(q: str) -> str:
    q = q.strip()
    if not re.match(r"^Q[1-4]\s+\d{4}$", q):
        raise ValueError('Quarter must look like "Q3 2025"')
    return q


def get_soup(url: str, timeout=30) -> BeautifulSoup:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")


def parse_cik_from_manager_url(manager_url: str) -> str | None:
    m = re.search(r"/manager/(\d{10})-", manager_url)
    return m.group(1) if m else None


def append_lines(path: str, lines: list[str]):
    if not lines:
        return
    with open(path, "a", encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip("\n") + "\n")


def load_done_set(done_path: str) -> set[str]:
    if not os.path.exists(done_path):
        return set()
    done = set()
    with open(done_path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if u:
                done.add(u)
    return done


def append_checkpoint_csv(path: str, df: pd.DataFrame):
    if df is None or df.empty:
        return
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", header=write_header, index=False)


def collect_manager_urls() -> list[str]:
    """Collect all manager URLs from 13f.info"""
    urls = []
    seen = set()

    next_url = f"{BASE}/managers"
    
    while next_url:
        try:
            soup = get_soup(next_url)
        except Exception as e:
            print(f"Error fetching {next_url}: {e}")
            break

        for a in soup.select('a[href^="/manager/"]'):
            href = a.get("href", "")
            full = urljoin(BASE, href)
            if "/manager/" in full and full not in seen:
                seen.add(full)
                urls.append(full)

        # Find next page
        nxt = soup.find("a", string=re.compile(r"^\s*Next\s*$", re.I))
        if nxt and nxt.get("href"):
            next_url = urljoin(BASE, nxt["href"])
        else:
            link_next = soup.find("a", attrs={"rel": "next"})
            next_url = urljoin(BASE, link_next["href"]) if link_next and link_next.get("href") else None

        time.sleep(0.2)
        if next_url in seen:
            break

    return urls


def find_filing_for_quarter(manager_url: str, quarter: str) -> tuple[str | None, str | None]:
    """
    Find filing ID and URL for the given quarter.
    Returns (filing_id, filing_url) or (None, None)
    """
    try:
        soup = get_soup(manager_url)
    except Exception:
        return None, None
    
    quarter_pat = re.compile(rf"\b{re.escape(quarter)}\b", re.I)

    # Look for /13f/ links in rows containing the quarter
    for row in soup.find_all(["tr", "div", "li"]):
        txt = row.get_text(" ", strip=True)
        if txt and quarter_pat.search(txt):
            a = row.find("a", href=re.compile(r"^/13f/"))
            if a and a.get("href"):
                href = a["href"]
                filing_url = urljoin(BASE, href)
                # Extract filing ID from href: /13f/{filing_id}-name-quarter
                m = re.search(r"/13f/(\d+)-", href)
                if m:
                    filing_id = m.group(1)
                    return filing_id, filing_url
    
    return None, None


def fetch_holdings_from_api(filing_id: str) -> list[dict] | None:
    """
    Fetch holdings data from 13f.info JSON API.
    
    API endpoint: https://13f.info/data/13f/{filing_id}
    
    Returns list of holdings dicts or None on error.
    """
    api_url = f"{BASE}/data/13f/{filing_id}"
    
    try:
        r = SESSION.get(api_url, timeout=30)
        if r.status_code != 200:
            return None
        
        data = r.json()
        
        # Data format: {"data": [[sym, issuer, class, cusip, value, pct, shares, principal, option], ...]}
        rows = data.get("data", [])
        
        if not rows:
            return None
        
        # Column order from the API
        columns = ["sym", "issuer_name", "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]
        
        holdings = []
        for row in rows:
            if len(row) >= 9:
                holding = {
                    "sym": row[0],
                    "issuer_name": row[1],
                    "class": row[2],
                    "cusip": row[3],
                    "value_000": row[4],
                    "pct": row[5],
                    "shares": row[6],
                    "principal": row[7],
                    "option_type": row[8],
                }
                holdings.append(holding)
        
        return holdings
        
    except Exception as e:
        return None


def holdings_to_dataframe(holdings: list[dict], manager_url: str, cik: str, quarter: str, filing_url: str) -> pd.DataFrame:
    """Convert holdings list to normalized DataFrame"""
    if not holdings:
        return pd.DataFrame()
    
    df = pd.DataFrame(holdings)
    
    # Add metadata columns
    df["manager_url"] = manager_url
    df["cik"] = cik
    df["quarter"] = quarter
    df["filing_url"] = filing_url
    
    # Ensure all expected columns exist
    for col in ["sym", "issuer_name", "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]:
        if col not in df.columns:
            df[col] = None
    
    # Clean numeric fields
    for c in ["value_000", "pct", "shares"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    # Reorder columns
    col_order = ["manager_url", "cik", "quarter", "filing_url", "sym", "issuer_name", "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]
    df = df[col_order]
    
    return df


def insert_chunk(cur, df: pd.DataFrame):
    """Insert DataFrame chunk into database"""
    cols = [
        "manager_url", "cik", "quarter", "filing_url",
        "sym", "issuer_name", "class", "cusip",
        "value_000", "pct", "shares", "principal", "option_type"
    ]

    for c in cols:
        if c not in df.columns:
            df[c] = None

    df2 = df[cols].copy()

    for c in ["value_000", "pct", "shares"]:
        df2[c] = pd.to_numeric(df2[c], errors="coerce")

    rows = list(df2.itertuples(index=False, name=None))
    if not rows:
        return 0

    execute_values(
        cur,
        f"""
        INSERT INTO {TABLE}
        (manager_url, cik, quarter, filing_url,
         sym, issuer_name, class, cusip,
         value_000, pct, shares, principal, option_type)
        VALUES %s
        """,
        rows,
        page_size=5000,
    )
    return len(rows)


def load_db_from_checkpoint_csv(csv_path: str, chunksize: int = 200_000):
    """Load CSV into database in chunks"""
    db_url = os.environ["DATABASE_URL"]

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {TABLE};")

                total_inserted = 0
                for chunk in pd.read_csv(csv_path, chunksize=chunksize):
                    n = insert_chunk(cur, chunk)
                    total_inserted += n
                    print(f"✅ Inserted chunk: {n} rows (total={total_inserted})")

                cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
                count = cur.fetchone()[0]
                print(f"✅ Final row count in {TABLE}: {count}")
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarter", required=True, help='e.g. "Q3 2025"')
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--limit", type=int, default=0, help="test: only first N managers (0=all)")
    ap.add_argument("--out-csv", default=DEFAULT_OUT_CSV)
    ap.add_argument("--done-file", default=DEFAULT_DONE_TXT)
    ap.add_argument("--fail-file", default=DEFAULT_FAIL_TXT)
    ap.add_argument("--checkpoint-every", type=int, default=200)
    ap.add_argument("--db-chunksize", type=int, default=200_000)
    args = ap.parse_args()

    quarter = norm_quarter(args.quarter)

    out_csv = args.out_csv
    done_file = args.done_file
    fail_file = args.fail_file
    max_workers = max(1, args.max_workers)

    # Resume: done managers are tracked in done_file
    done_set = load_done_set(done_file)
    print(f"🔁 Resume mode: {len(done_set)} managers already done (from {done_file})")

    print("Collecting manager URLs...")
    manager_urls = collect_manager_urls()
    print(f"Found {len(manager_urls)} managers")
    
    if args.limit and args.limit > 0:
        manager_urls = manager_urls[: args.limit]
        print(f"⚠️ TEST MODE: limiting to first {args.limit} managers")

    remaining = [u for u in manager_urls if u not in done_set]
    print(f"▶️ Remaining managers: {len(remaining)} (of {len(manager_urls)} total)")

    if not remaining:
        if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
            print(f"📥 Loading DB from checkpoint CSV: {out_csv}")
            load_db_from_checkpoint_csv(out_csv, chunksize=args.db_chunksize)
        else:
            print(f"✅ No remaining managers and no checkpoint CSV found")
        return

    scraped_rows_buffer = []
    done_to_append = []
    fail_to_append = []

    def worker(manager_url: str):
        jitter()
        cik = parse_cik_from_manager_url(manager_url)

        # Find filing for this quarter
        filing_id, filing_url = find_filing_for_quarter(manager_url, quarter)
        if not filing_id:
            return manager_url, "no_filing", None, None, None

        # Fetch holdings from JSON API
        holdings = fetch_holdings_from_api(filing_id)
        if not holdings:
            return manager_url, "no_data", filing_url, None, None

        # Convert to DataFrame
        df = holdings_to_dataframe(holdings, manager_url, cik, quarter, filing_url)
        return manager_url, "ok", filing_url, df, len(holdings)

    total = len(remaining)
    done = 0
    ok_count = 0
    skip_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(worker, u) for u in remaining]

        for fut in as_completed(futs):
            done += 1
            try:
                manager_url, status, filing_url, df, num_holdings = fut.result()
            except Exception as e:
                fail_count += 1
                print(f"[{done}/{total}] FAIL -> {e}")
                fail_to_append.append(f"{remaining[done-1]}\t{repr(e)}")
                continue

            if status == "no_filing":
                skip_count += 1
                print(f"[{done}/{total}] SKIP {manager_url} -> no filing for {quarter}")
                done_to_append.append(manager_url)
            elif status == "no_data":
                fail_count += 1
                print(f"[{done}/{total}] WARN {manager_url} -> API returned no data | filing_url={filing_url}")
                fail_to_append.append(f"{manager_url}\tno_data")
            else:
                ok_count += 1
                print(f"[{done}/{total}] OK {manager_url} -> {num_holdings} holdings")
                scraped_rows_buffer.append(df)
                done_to_append.append(manager_url)

            # Checkpoint every N managers
            if done % args.checkpoint_every == 0 or done == total:
                if scraped_rows_buffer:
                    batch_df = pd.concat(scraped_rows_buffer, ignore_index=True)
                    append_checkpoint_csv(out_csv, batch_df)
                    print(f"💾 Checkpoint: {out_csv} (+{len(batch_df)} rows)")
                    scraped_rows_buffer = []

                if done_to_append:
                    append_lines(done_file, done_to_append)
                    done_set.update(done_to_append)
                    done_to_append = []

                if fail_to_append:
                    append_lines(fail_file, fail_to_append)
                    fail_to_append = []

    print(f"\n✅ Scrape complete. ok={ok_count} skip={skip_count} fail={fail_count}")
    
    if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
        with open(out_csv, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f)
        if line_count >= 2:
            print(f"📥 Loading DB from checkpoint CSV: {out_csv}")
            load_db_from_checkpoint_csv(out_csv, chunksize=args.db_chunksize)
        else:
            print("⚠️ Checkpoint CSV has header only (0 rows). Skipping DB load.")
    else:
        print("⚠️ No checkpoint CSV created. Skipping DB load.")


if __name__ == "__main__":
    main()