"""
13f.info Holdings Scraper - OPTIMIZED VERSION

Optimizations:
1. Async HTTP with aiohttp (much faster than threading for I/O)
2. Fixed pagination to get ALL managers
3. Concurrent manager page + API fetching
4. Connection pooling with keep-alive
5. Batch processing with progress tracking

Usage:
  python download_13finfo_holdings_fast.py --quarter "Q3 2025" --max-workers 50

Test:
  python download_13finfo_holdings_fast.py --quarter "Q3 2025" --limit 100 --max-workers 20
"""
import argparse
import asyncio
import os
import re
import time
from urllib.parse import urljoin

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import execute_values


BASE = "https://13f.info"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) 13f-api-scraper/3.0"

TABLE = "public.expected_13finfo_holdings"

DEFAULT_OUT_CSV = "13finfo_holdings_checkpoint.csv"
DEFAULT_DONE_TXT = "13finfo_holdings_done_managers.txt"

# Rate limiting - be polite to the server
REQUESTS_PER_SECOND = 20  # Max requests per second
SEMAPHORE_LIMIT = 50      # Max concurrent requests


def norm_quarter(q: str) -> str:
    q = q.strip()
    if not re.match(r"^Q[1-4]\s+\d{4}$", q):
        raise ValueError('Quarter must look like "Q3 2025"')
    return q


def parse_cik_from_manager_url(manager_url: str) -> str | None:
    m = re.search(r"/manager/(\d+)-", manager_url)
    if m:
        # Always normalize to 10-digit padded format
        return m.group(1).lstrip('0').zfill(10)
    return None


def load_done_set(done_path: str) -> set[str]:
    if not os.path.exists(done_path):
        return set()
    with open(done_path, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def save_done_set(done_path: str, done_set: set[str]):
    with open(done_path, "w", encoding="utf-8") as f:
        for url in sorted(done_set):
            f.write(url + "\n")


async def fetch_page(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> str | None:
    """Fetch a page with rate limiting"""
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.text()
                return None
        except Exception:
            return None


async def fetch_json(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> dict | None:
    """Fetch JSON with rate limiting"""
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception:
            return None


async def collect_all_manager_urls(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> list[str]:
    """
    Collect ALL manager URLs by paginating through A-Z and 0-9 index pages.
    This is more reliable than following Next links.
    """
    urls = set()
    
    # Manager index pages: /managers, /managers/a, /managers/b, ..., /managers/z, /managers/0
    index_pages = [f"{BASE}/managers"] + [f"{BASE}/managers/{c}" for c in "abcdefghijklmnopqrstuvwxyz0"]
    
    print(f"📥 Fetching {len(index_pages)} manager index pages...")
    
    tasks = [fetch_page(session, url, semaphore) for url in index_pages]
    results = await asyncio.gather(*tasks)
    
    for html in results:
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select('a[href^="/manager/"]'):
            href = a.get("href", "")
            if "/manager/" in href:
                full_url = urljoin(BASE, href)
                urls.add(full_url)
    
    print(f"✅ Found {len(urls)} unique managers")
    return list(urls)


async def process_manager(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    manager_url: str,
    quarter: str
) -> tuple[str, str, list[dict] | None]:
    """
    Process a single manager:
    1. Fetch manager page to find filing ID
    2. Fetch holdings from JSON API
    
    Returns: (manager_url, status, holdings_list)
    """
    cik = parse_cik_from_manager_url(manager_url)
    
    # Step 1: Get manager page to find filing URL
    html = await fetch_page(session, manager_url, semaphore)
    if not html:
        return manager_url, "error", None
    
    # Find filing ID for this quarter
    soup = BeautifulSoup(html, "html.parser")
    quarter_pat = re.compile(rf"\b{re.escape(quarter)}\b", re.I)
    
    filing_id = None
    filing_url = None
    
    for row in soup.find_all(["tr", "div", "li"]):
        txt = row.get_text(" ", strip=True)
        if txt and quarter_pat.search(txt):
            a = row.find("a", href=re.compile(r"^/13f/"))
            if a and a.get("href"):
                href = a["href"]
                filing_url = urljoin(BASE, href)
                m = re.search(r"/13f/(\d+)-", href)
                if m:
                    filing_id = m.group(1)
                    break
    
    if not filing_id:
        return manager_url, "no_filing", None
    
    # Step 2: Fetch holdings from JSON API
    api_url = f"{BASE}/data/13f/{filing_id}"
    data = await fetch_json(session, api_url, semaphore)
    
    if not data or "data" not in data or not data["data"]:
        return manager_url, "no_data", None
    
    # Parse holdings
    holdings = []
    for row in data["data"]:
        if len(row) >= 9:
            holdings.append({
                "manager_url": manager_url,
                "cik": cik,
                "quarter": quarter,
                "filing_url": filing_url,
                "sym": row[0],
                "issuer_name": row[1],
                "class": row[2],
                "cusip": row[3],
                "value_000": row[4],
                "pct": row[5],
                "shares": row[6],
                "principal": row[7],
                "option_type": row[8],
            })
    
    return manager_url, "ok", holdings


async def process_batch(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    manager_urls: list[str],
    quarter: str,
    batch_num: int,
    total_batches: int
) -> tuple[list[dict], set[str], int, int, int]:
    """Process a batch of managers concurrently"""
    
    tasks = [process_manager(session, semaphore, url, quarter) for url in manager_urls]
    results = await asyncio.gather(*tasks)
    
    all_holdings = []
    done_urls = set()
    ok_count = 0
    skip_count = 0
    fail_count = 0
    
    for manager_url, status, holdings in results:
        if status == "ok" and holdings:
            all_holdings.extend(holdings)
            done_urls.add(manager_url)
            ok_count += 1
        elif status == "no_filing":
            done_urls.add(manager_url)  # Mark as done even if no filing
            skip_count += 1
        else:
            fail_count += 1
    
    print(f"  Batch {batch_num}/{total_batches}: ok={ok_count} skip={skip_count} fail={fail_count} holdings={len(all_holdings)}")
    
    return all_holdings, done_urls, ok_count, skip_count, fail_count


def save_checkpoint(all_holdings: list[dict], csv_path: str):
    """Save holdings to CSV"""
    if not all_holdings:
        return
    
    df = pd.DataFrame(all_holdings)
    
    # Ensure column order
    cols = ["manager_url", "cik", "quarter", "filing_url", "sym", "issuer_name", 
            "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    
    # Clean numeric fields
    for c in ["value_000", "pct", "shares"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    df.to_csv(csv_path, index=False)
    print(f"💾 Saved {len(df)} holdings to {csv_path}")


def load_db_from_csv(csv_path: str):
    """Load CSV into database"""
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("⚠️ DATABASE_URL not set, skipping DB load")
        return
    
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {TABLE};")
                
                df = pd.read_csv(csv_path)
                
                cols = ["manager_url", "cik", "quarter", "filing_url", "sym", "issuer_name",
                        "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]
                
                for c in cols:
                    if c not in df.columns:
                        df[c] = None
                
                for c in ["value_000", "pct", "shares"]:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                
                rows = list(df[cols].itertuples(index=False, name=None))
                
                if rows:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {TABLE}
                        (manager_url, cik, quarter, filing_url, sym, issuer_name,
                         class, cusip, value_000, pct, shares, principal, option_type)
                        VALUES %s
                        """,
                        rows,
                        page_size=5000,
                    )
                
                cur.execute(f"SELECT COUNT(*) FROM {TABLE};")
                count = cur.fetchone()[0]
                print(f"✅ Loaded {count} rows into {TABLE}")
    finally:
        conn.close()


async def main_async(args):
    quarter = norm_quarter(args.quarter)
    
    # Load previously done managers
    done_set = load_done_set(args.done_file)
    print(f"🔁 Resume: {len(done_set)} managers already done")
    
    # Setup connection pooling
    connector = aiohttp.TCPConnector(
        limit=SEMAPHORE_LIMIT,
        limit_per_host=SEMAPHORE_LIMIT,
        keepalive_timeout=30,
    )
    
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": UA}
    ) as session:
        
        # Collect all manager URLs
        all_manager_urls = await collect_all_manager_urls(session, semaphore)
        
        if args.limit and args.limit > 0:
            all_manager_urls = all_manager_urls[:args.limit]
            print(f"⚠️ TEST MODE: limiting to {args.limit} managers")
        
        # Filter out already done
        remaining = [u for u in all_manager_urls if u not in done_set]
        print(f"▶️ Remaining: {len(remaining)} managers")
        
        if not remaining:
            print("✅ All managers already processed!")
            if os.path.exists(args.out_csv):
                load_db_from_csv(args.out_csv)
            return
        
        # Process in batches
        batch_size = args.batch_size
        total_batches = (len(remaining) + batch_size - 1) // batch_size
        
        all_holdings = []
        total_ok = 0
        total_skip = 0
        total_fail = 0
        
        start_time = time.time()
        
        for i in range(0, len(remaining), batch_size):
            batch = remaining[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            holdings, done_urls, ok, skip, fail = await process_batch(
                session, semaphore, batch, quarter, batch_num, total_batches
            )
            
            all_holdings.extend(holdings)
            done_set.update(done_urls)
            total_ok += ok
            total_skip += skip
            total_fail += fail
            
            # Save progress periodically
            if batch_num % 5 == 0 or batch_num == total_batches:
                save_done_set(args.done_file, done_set)
                save_checkpoint(all_holdings, args.out_csv)
            
            # Progress
            elapsed = time.time() - start_time
            processed = i + len(batch)
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (len(remaining) - processed) / rate if rate > 0 else 0
            
            print(f"📊 Progress: {processed}/{len(remaining)} ({100*processed/len(remaining):.1f}%) | "
                  f"Rate: {rate:.1f}/s | ETA: {eta/60:.1f}min | Holdings: {len(all_holdings)}")
        
        # Final save
        save_done_set(args.done_file, done_set)
        save_checkpoint(all_holdings, args.out_csv)
        
        elapsed = time.time() - start_time
        print(f"\n✅ COMPLETE in {elapsed/60:.1f} minutes")
        print(f"   OK: {total_ok} | Skip: {total_skip} | Fail: {total_fail}")
        print(f"   Total holdings: {len(all_holdings)}")
        
        # Load to database
        if os.path.exists(args.out_csv):
            load_db_from_csv(args.out_csv)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarter", required=True, help='e.g. "Q3 2025"')
    ap.add_argument("--limit", type=int, default=0, help="Limit managers (0=all)")
    ap.add_argument("--batch-size", type=int, default=100, help="Managers per batch")
    ap.add_argument("--out-csv", default=DEFAULT_OUT_CSV)
    ap.add_argument("--done-file", default=DEFAULT_DONE_TXT)
    args = ap.parse_args()
    
    print("=" * 70)
    print("13f.info Holdings Scraper - OPTIMIZED")
    print("=" * 70)
    
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()