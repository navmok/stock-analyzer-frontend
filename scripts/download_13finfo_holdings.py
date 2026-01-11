# download_13finfo_holdings_fast.py
#
# Full optimized 13f.info holdings scraper:
# - Fast HTML parsing (BeautifulSoup, NO pd.read_html)
# - Retry + connection pooling
# - Append-only checkpoint CSV (no full rewrites)
# - Separate done-managers file for fast resume
# - DB load from CSV in CHUNKS (no full CSV in memory)
#
# Usage:
#   cd C:\Users\mokkapatin\Downloads\stock-analyzer-frontend\scripts
#   python download_13finfo_holdings_fast.py --quarter "Q3 2025" --max-workers 8
#
# Test:
#   python download_13finfo_holdings_fast.py --quarter "Q3 2025" --limit 10 --max-workers 4
#
# Requirements:
#   pip install requests beautifulsoup4 pandas psycopg2-binary python-dotenv urllib3
#
# .env:
#   DATABASE_URL=postgresql://postgres:YOURPASS@localhost:5432/stock_analyzer
#   (If password has #, use %23)

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
UA = os.getenv("SCRAPER_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) 13f-holdings-scraper/2.0")

TABLE = "public.expected_13finfo_holdings"

DEFAULT_OUT_CSV = "13finfo_holdings_checkpoint.csv"
DEFAULT_DONE_TXT = "13finfo_holdings_done_managers.txt"
DEFAULT_FAIL_TXT = "13finfo_holdings_failed_managers.txt"

# Polite jitter (per request)
MIN_JITTER = 0.03
MAX_JITTER = 0.15


# -----------------------------
# HTTP session with retry/pooling
# -----------------------------
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


# -----------------------------
# Utilities
# -----------------------------
def norm_quarter(q: str) -> str:
    q = q.strip()
    if not re.match(r"^Q[1-4]\s+\d{4}$", q):
        raise ValueError('Quarter must look like "Q3 2025"')
    return q


def get_text(el) -> str:
    return el.get_text(" ", strip=True) if el else ""


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


# -----------------------------
# Collect manager URLs (paged)
# -----------------------------
def collect_manager_urls() -> list[str]:
    urls = []
    seen = set()

    next_url = None
    try_urls = [f"{BASE}/managers", f"{BASE}/manager"]
    for u in try_urls:
        try:
            r = SESSION.get(u, timeout=20)
            if r.status_code == 200:
                next_url = u
                break
        except Exception:
            pass

    if not next_url:
        raise RuntimeError("Could not find manager index page (/managers or /manager)")
    
    while next_url:
        soup = get_soup(next_url)

        for a in soup.select('a[href^="/manager/"]'):
            href = a.get("href", "")
            full = urljoin(BASE, href)
            if "/manager/" in full and full not in seen:
                seen.add(full)
                urls.append(full)

        # next page
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


# -----------------------------
# Find filing link for quarter
# -----------------------------
def find_filing_link_for_quarter(manager_url: str, quarter: str) -> str | None:
    soup = get_soup(manager_url)
    quarter_pat = re.compile(rf"\b{re.escape(quarter)}\b")

    # Scan rows for quarter and grab /13f/ link
    for row in soup.find_all(["tr", "div", "li"]):
        txt = row.get_text(" ", strip=True)
        if txt and quarter_pat.search(txt):
            a = row.find("a", href=re.compile(r"^/13f/"))
            if a and a.get("href"):
                return urljoin(BASE, a["href"])

    # fallback: /13f/ links, check parent text
    for a in soup.select('a[href^="/13f/"]'):
        parent_txt = a.parent.get_text(" ", strip=True) if a.parent else ""
        if quarter_pat.search(parent_txt):
            return urljoin(BASE, a["href"])

    return None


# -----------------------------
# Fast holdings table parsing (no pd.read_html)
# -----------------------------
def _largest_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    if not tables:
        return None
    return max(tables, key=lambda t: len(t.find_all("tr")))


def _table_to_df(table) -> pd.DataFrame:
    rows = table.find_all("tr")
    if not rows:
        return pd.DataFrame()

    header_cells = rows[0].find_all(["th", "td"])
    header = [get_text(c) for c in header_cells]
    header = [h if h else f"col_{i}" for i, h in enumerate(header)]

    data = []
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        data.append([get_text(c) for c in cells])

    df = pd.DataFrame(data, columns=header)

    # drop completely empty rows
    df = df.dropna(how="all")
    return df


def scrape_holdings_from_filing(filing_url: str) -> pd.DataFrame:
    r = SESSION.get(filing_url, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("No tables found on filing page")

    best = None
    best_rows = 0

    for t in tables:
        rows = t.find_all("tr")
        if not rows:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        header = [get_text(c).lower() for c in header_cells]
        # holdings table usually has cusip/value/shares headers
        if any("cusip" in h for h in header) and any("value" in h for h in header):
            if len(rows) > best_rows:
                best = t
                best_rows = len(rows)

    # fallback to largest if not found
    if best is None:
        best = max(tables, key=lambda t: len(t.find_all("tr")))

    df = _table_to_df(best)
    return df

def _pick_col(df: pd.DataFrame, patterns: list[str]) -> str | None:
    cols = [str(c) for c in df.columns]
    cols_l = [c.lower() for c in cols]
    for p in patterns:
        rp = re.compile(p, re.I)
        for c, cl in zip(cols, cols_l):
            if rp.search(cl):
                return c
    return None


def normalize_holdings_table(raw: pd.DataFrame, manager_url: str, cik: str | None, quarter: str, filing_url: str) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]

    col_sym = _pick_col(df, [r"^sym$", r"\bsym\b"])
    col_issuer = _pick_col(df, [r"issuer", r"name"])
    col_class = _pick_col(df, [r"^cl$", r"\bclass\b", r"title"])
    col_cusip = _pick_col(df, [r"cusip"])
    col_value = _pick_col(df, [r"value"])
    col_pct = _pick_col(df, [r"^%$", r"\b%\b", r"percent"])
    col_shares = _pick_col(df, [r"shares", r"shrs"])
    col_principal = _pick_col(df, [r"principal"])
    col_option = _pick_col(df, [r"option", r"put", r"call"])

    out = pd.DataFrame({
        "manager_url": manager_url,
        "cik": cik,
        "quarter": quarter,
        "filing_url": filing_url,
        "sym": df[col_sym] if col_sym else None,
        "issuer_name": df[col_issuer] if col_issuer else None,
        "class": df[col_class] if col_class else None,
        "cusip": df[col_cusip] if col_cusip else None,
        "value_000": df[col_value] if col_value else None,
        "pct": df[col_pct] if col_pct else None,
        "shares": df[col_shares] if col_shares else None,
        "principal": df[col_principal] if col_principal else None,
        "option_type": df[col_option] if col_option else None,
    })

    # clean numeric fields
    for c in ["value_000", "pct", "shares"]:
        out[c] = (
            out[c].astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # clean strings: turn 'nan' into None
    for c in ["sym", "issuer_name", "class", "cusip", "principal", "option_type", "filing_url", "manager_url", "cik", "quarter"]:
        out[c] = out[c].replace({"nan": None, "None": None})

    return out


# -----------------------------
# DB load from CSV in chunks (no full file in memory)
# -----------------------------
def insert_chunk(cur, df: pd.DataFrame):
    cols = [
        "manager_url", "cik", "quarter", "filing_url",
        "sym", "issuer_name", "class", "cusip",
        "value_000", "pct", "shares", "principal", "option_type"
    ]

    for c in cols:
        if c not in df.columns:
            df[c] = None

    df2 = df[cols].copy()

    # Ensure numeric types
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


# -----------------------------
# Main scrape loop
# -----------------------------
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
    if args.limit and args.limit > 0:
        manager_urls = manager_urls[: args.limit]
        print(f"⚠️ TEST MODE: limiting to first {args.limit} managers")

    remaining = [u for u in manager_urls if u not in done_set]
    print(f"▶️ Remaining managers: {len(remaining)} (of {len(manager_urls)} total)")

    # If nothing to scrape, just load DB from CSV (if CSV exists)
    if not remaining:
        if os.path.exists(out_csv):
            if os.path.exists(out_csv):
                print(f"📥 Loading DB from checkpoint CSV in chunks: {out_csv}")
                load_db_from_checkpoint_csv(out_csv, chunksize=args.db_chunksize)
            else:
                print(f"⚠️ No checkpoint CSV created (0 holdings). Skipping DB load.")
        else:
            print(f"✅ No remaining managers and no checkpoint CSV found: {out_csv}")
        return

    scraped_rows_buffer = []
    done_to_append = []
    fail_to_append = []

    def worker(manager_url: str):
        jitter()
        cik = parse_cik_from_manager_url(manager_url)

        filing_url = find_filing_link_for_quarter(manager_url, quarter)
        if not filing_url:
            return manager_url, "no_filing", None

        raw = scrape_holdings_from_filing(filing_url)
        normdf = normalize_holdings_table(raw, manager_url, cik, quarter, filing_url)
        return manager_url, "ok", filing_url, normdf

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
                manager_url, status, filing_url, normdf = fut.result()
            except Exception as e:
                fail_count += 1
                msg = f"{remaining[done-1]}\t{repr(e)}"
                print(f"[{done}/{total}] FAIL -> {e}")
                fail_to_append.append(msg)
                continue

            if status == "no_filing":
                skip_count += 1
                print(f"[{done}/{total}] SKIP {manager_url} -> no_filing")
                done_to_append.append(manager_url)  # ok to mark done
            else:
                if normdf is None or len(normdf) == 0:
                    print(f"[{done}/{total}] WARN {manager_url} -> holdings 0 rows (will retry later) | filing_url={filing_url}")
                    # DO NOT mark done
                    fail_to_append.append(f"{manager_url}\tzero_rows")
                else:
                    ok_count += 1
                    print(f"[{done}/{total}] OK {manager_url} -> holdings {len(normdf)} rows")
                    scraped_rows_buffer.append(normdf)
                    done_to_append.append(manager_url)  # mark done only if real rows

            # checkpoint every N managers
            if done % args.checkpoint_every == 0 or done == total:
                if scraped_rows_buffer:
                    batch_df = pd.concat(scraped_rows_buffer, ignore_index=True)
                    append_checkpoint_csv(out_csv, batch_df)
                    print(f"💾 Appended checkpoint: {out_csv} (+{len(batch_df)} rows)")
                    scraped_rows_buffer = []

                if done_to_append:
                    append_lines(done_file, done_to_append)
                    done_set.update(done_to_append)
                    done_to_append = []

                if fail_to_append:
                    append_lines(fail_file, fail_to_append)
                    fail_to_append = []

    print(f"✅ Scrape complete. ok={ok_count} skip={skip_count} fail={fail_count}")
    if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
        # also ensure it has at least 2 lines (header + 1 row)
        with open(out_csv, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f)
        if line_count >= 2:
            print(f"📥 Loading DB from checkpoint CSV in chunks: {out_csv}")
            load_db_from_checkpoint_csv(out_csv, chunksize=args.db_chunksize)
        else:
            print("⚠️ Checkpoint CSV has header only (0 rows). Skipping DB load.")
    else:
        print("⚠️ No checkpoint CSV created (0 holdings). Skipping DB load.")


if __name__ == "__main__":
    main()