"""
13f.info Holdings Backfill - Scrape MISSING quarters only

This script:
1. Checks what (cik, quarter) combinations already exist in database
2. For each manager, finds ALL available quarters on 13f.info
3. Only scrapes quarters that are missing

Usage:
  python backfill_13finfo_holdings.py --max-workers 50

Test (limit managers):
  python backfill_13finfo_holdings.py --limit 100 --max-workers 20
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
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) 13f-backfill/1.0"

TABLE = "public.expected_13finfo_holdings"

DEFAULT_OUT_CSV = "13finfo_holdings_backfill.csv"

SEMAPHORE_LIMIT = 50


def parse_cik_from_manager_url(manager_url: str) -> str | None:
    m = re.search(r"/manager/(\d{10})-", manager_url)
    return m.group(1) if m else None


def get_existing_cik_quarters(db_url: str) -> set[tuple[str, str]]:
    """Get all (cik, quarter) combinations already in database"""
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT DISTINCT cik, quarter FROM {TABLE} WHERE cik IS NOT NULL")
            rows = cur.fetchall()
            return set((row[0], row[1]) for row in rows)
    finally:
        conn.close()


async def fetch_page(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> str | None:
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.text()
                return None
        except Exception:
            return None


async def fetch_json(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> dict | None:
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.json()
                return None
        except Exception:
            return None


async def collect_all_manager_urls(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> list[str]:
    """Collect ALL manager URLs from A-Z index pages"""
    urls = set()
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
                urls.add(urljoin(BASE, href))
    
    print(f"✅ Found {len(urls)} unique managers")
    return list(urls)


async def get_manager_filings(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    manager_url: str
) -> list[tuple[str, str, str]]:
    """
    Get all available filings for a manager.
    Returns list of (quarter, filing_id, filing_url)
    """
    html = await fetch_page(session, manager_url, semaphore)
    if not html:
        return []
    
    soup = BeautifulSoup(html, "html.parser")
    filings = []
    
    # Find all /13f/ links with quarter info
    for a in soup.select('a[href^="/13f/"]'):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        
        # Extract quarter from link text (e.g., "Q3 2025")
        quarter_match = re.match(r"^(Q[1-4]\s+\d{4})$", text)
        if quarter_match:
            quarter = quarter_match.group(1)
            filing_url = urljoin(BASE, href)
            
            # Extract filing ID
            id_match = re.search(r"/13f/(\d+)-", href)
            if id_match:
                filing_id = id_match.group(1)
                filings.append((quarter, filing_id, filing_url))
    
    return filings


async def fetch_holdings(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    filing_id: str
) -> list[list] | None:
    """Fetch holdings from JSON API"""
    api_url = f"{BASE}/data/13f/{filing_id}"
    data = await fetch_json(session, api_url, semaphore)
    
    if data and "data" in data:
        return data["data"]
    return None


async def process_manager(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    manager_url: str,
    existing_cik_quarters: set[tuple[str, str]]
) -> tuple[list[dict], int, int]:
    """
    Process a manager - only fetch missing quarters.
    Returns (holdings_list, num_scraped, num_skipped)
    """
    cik = parse_cik_from_manager_url(manager_url)
    if not cik:
        return [], 0, 0
    
    # Get all available filings for this manager
    filings = await get_manager_filings(session, semaphore, manager_url)
    if not filings:
        return [], 0, 0
    
    all_holdings = []
    num_scraped = 0
    num_skipped = 0
    
    for quarter, filing_id, filing_url in filings:
        # Check if we already have this (cik, quarter)
        if (cik, quarter) in existing_cik_quarters:
            num_skipped += 1
            continue
        
        # Fetch holdings
        rows = await fetch_holdings(session, semaphore, filing_id)
        if not rows:
            continue
        
        # Parse holdings
        for row in rows:
            if len(row) >= 9:
                all_holdings.append({
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
        
        num_scraped += 1
    
    return all_holdings, num_scraped, num_skipped


async def process_batch(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    manager_urls: list[str],
    existing_cik_quarters: set[tuple[str, str]],
    batch_num: int,
    total_batches: int
) -> tuple[list[dict], int, int]:
    """Process a batch of managers"""
    
    tasks = [
        process_manager(session, semaphore, url, existing_cik_quarters)
        for url in manager_urls
    ]
    results = await asyncio.gather(*tasks)
    
    all_holdings = []
    total_scraped = 0
    total_skipped = 0
    
    for holdings, scraped, skipped in results:
        all_holdings.extend(holdings)
        total_scraped += scraped
        total_skipped += skipped
    
    print(f"  Batch {batch_num}/{total_batches}: scraped={total_scraped} quarters, skipped={total_skipped} (already have), holdings={len(all_holdings)}")
    
    return all_holdings, total_scraped, total_skipped


def append_to_db(db_url: str, holdings: list[dict]):
    """Append holdings to database (no truncate)"""
    if not holdings:
        return 0
    
    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                cols = ["manager_url", "cik", "quarter", "filing_url", "sym", "issuer_name",
                        "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]
                
                rows = [tuple(h.get(c) for c in cols) for h in holdings]
                
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
                
                return len(rows)
    finally:
        conn.close()


def save_checkpoint(holdings: list[dict], csv_path: str, mode: str = "a"):
    """Save/append holdings to CSV"""
    if not holdings:
        return
    
    df = pd.DataFrame(holdings)
    
    cols = ["manager_url", "cik", "quarter", "filing_url", "sym", "issuer_name",
            "class", "cusip", "value_000", "pct", "shares", "principal", "option_type"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    
    for c in ["value_000", "pct", "shares"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    
    write_header = mode == "w" or not os.path.exists(csv_path)
    df.to_csv(csv_path, mode=mode, header=write_header, index=False)


async def main_async(args):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set")
        return
    
    # Get existing (cik, quarter) combinations
    print("📊 Checking existing data in database...")
    existing_cik_quarters = get_existing_cik_quarters(db_url)
    print(f"✅ Found {len(existing_cik_quarters)} existing (cik, quarter) combinations")
    
    # Setup
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
        
        print(f"▶️ Processing {len(all_manager_urls)} managers for missing quarters...")
        
        # Process in batches
        batch_size = args.batch_size
        total_batches = (len(all_manager_urls) + batch_size - 1) // batch_size
        
        total_holdings = 0
        total_quarters_scraped = 0
        total_quarters_skipped = 0
        
        start_time = time.time()
        
        # Clear/create CSV
        if os.path.exists(args.out_csv):
            os.remove(args.out_csv)
        
        for i in range(0, len(all_manager_urls), batch_size):
            batch = all_manager_urls[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            holdings, scraped, skipped = await process_batch(
                session, semaphore, batch, existing_cik_quarters, batch_num, total_batches
            )
            
            if holdings:
                # Append to database immediately
                inserted = append_to_db(db_url, holdings)
                total_holdings += inserted
                
                # Update existing set to avoid re-scraping in case of resume
                for h in holdings:
                    existing_cik_quarters.add((h["cik"], h["quarter"]))
                
                # Save to CSV as backup
                save_checkpoint(holdings, args.out_csv, mode="a")
            
            total_quarters_scraped += scraped
            total_quarters_skipped += skipped
            
            # Progress
            elapsed = time.time() - start_time
            processed = i + len(batch)
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (len(all_manager_urls) - processed) / rate if rate > 0 else 0
            
            print(f"📊 Progress: {processed}/{len(all_manager_urls)} ({100*processed/len(all_manager_urls):.1f}%) | "
                  f"Rate: {rate:.1f}/s | ETA: {eta/60:.1f}min | "
                  f"New quarters: {total_quarters_scraped} | Holdings: {total_holdings}")
        
        elapsed = time.time() - start_time
        print(f"\n✅ COMPLETE in {elapsed/60:.1f} minutes")
        print(f"   New quarters scraped: {total_quarters_scraped}")
        print(f"   Quarters skipped (already had): {total_quarters_skipped}")
        print(f"   New holdings added: {total_holdings}")
        
        # Final count
        conn = psycopg2.connect(db_url)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
                total = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(DISTINCT quarter) FROM {TABLE}")
                quarters = cur.fetchone()[0]
                cur.execute(f"SELECT COUNT(DISTINCT cik) FROM {TABLE}")
                managers = cur.fetchone()[0]
                print(f"\n📊 Database totals:")
                print(f"   Total holdings: {total:,}")
                print(f"   Unique quarters: {quarters}")
                print(f"   Unique managers: {managers:,}")
        finally:
            conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Limit managers (0=all)")
    ap.add_argument("--batch-size", type=int, default=100, help="Managers per batch")
    ap.add_argument("--out-csv", default=DEFAULT_OUT_CSV)
    args = ap.parse_args()
    
    print("=" * 70)
    print("13f.info Holdings Backfill - Missing Quarters Only")
    print("=" * 70)
    
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()