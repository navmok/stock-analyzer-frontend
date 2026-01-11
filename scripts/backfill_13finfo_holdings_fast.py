"""
13f.info Holdings Backfill - FULLY OPTIMIZED

Two-phase approach for maximum speed:
  Phase 1: Fetch ALL manager pages in parallel → collect missing (cik, quarter, filing_id)
  Phase 2: Fetch ALL holdings APIs in parallel (maximum concurrency)

This is much faster than processing managers sequentially.

Usage:
  python backfill_13finfo_holdings_fast.py

Test:
  python backfill_13finfo_holdings_fast.py --limit 200
"""
import argparse
import asyncio
import os
import re
import time
from urllib.parse import urljoin
from dataclasses import dataclass

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from dotenv import load_dotenv
load_dotenv()

import psycopg2
from psycopg2.extras import execute_values


BASE = "https://13f.info"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) 13f-backfill/2.0"

TABLE = "public.expected_13finfo_holdings"

# Concurrency settings - tune based on server tolerance
SEMAPHORE_LIMIT = 100       # Max concurrent requests
BATCH_SIZE_PHASE1 = 500     # Manager pages per batch
BATCH_SIZE_PHASE2 = 200     # Holdings API calls per batch
DB_INSERT_BATCH = 50000     # Holdings per DB insert


@dataclass
class FilingInfo:
    manager_url: str
    cik: str
    quarter: str
    filing_id: str
    filing_url: str


def parse_cik_from_manager_url(manager_url: str) -> str | None:
    m = re.search(r"/manager/(\d+)-", manager_url)
    if m:
        # Always normalize to 10-digit padded format
        return m.group(1).lstrip('0').zfill(10)
    return None


def get_existing_cik_quarters(db_url: str) -> set[tuple[str, str]]:
    """Get all (cik, quarter) combinations already in database"""
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            # Normalize CIK to 10-digit padded format for consistent matching
            cur.execute(f"""
                SELECT DISTINCT LPAD(LTRIM(cik, '0'), 10, '0'), quarter 
                FROM {TABLE} 
                WHERE cik IS NOT NULL
            """)
            return set(cur.fetchall())
    finally:
        conn.close()


async def fetch_text(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> str | None:
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.text()
        except:
            pass
        return None


async def fetch_json(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> dict | None:
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.json()
        except:
            pass
        return None


async def collect_manager_urls(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> list[str]:
    """Collect ALL manager URLs from A-Z index pages"""
    urls = set()
    index_pages = [f"{BASE}/managers"] + [f"{BASE}/managers/{c}" for c in "abcdefghijklmnopqrstuvwxyz0"]
    
    tasks = [fetch_text(session, url, semaphore) for url in index_pages]
    results = await asyncio.gather(*tasks)
    
    for html in results:
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.select('a[href^="/manager/"]'):
            href = a.get("href", "")
            if "/manager/" in href:
                urls.add(urljoin(BASE, href))
    
    return list(urls)


def parse_manager_page(html: str, manager_url: str) -> list[FilingInfo]:
    """Parse manager page to extract all filing info"""
    cik = parse_cik_from_manager_url(manager_url)
    if not cik:
        # Try extracting from URL with looser pattern and normalize
        m = re.search(r"/manager/(\d+)-", manager_url)
        if m:
            cik = m.group(1).lstrip('0').zfill(10)
        else:
            return []
    
    soup = BeautifulSoup(html, "html.parser")
    filings = []
    seen = set()
    
    for a in soup.select('a[href^="/13f/"]'):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        
        # Extract quarter from link text
        quarter_match = re.match(r"^(Q[1-4]\s+\d{4})$", text)
        if not quarter_match:
            continue
        
        quarter = quarter_match.group(1)
        
        # Extract filing ID
        id_match = re.search(r"/13f/(\d+)-", href)
        if not id_match:
            continue
        
        filing_id = id_match.group(1)
        
        # Dedupe
        key = (cik, quarter)
        if key in seen:
            continue
        seen.add(key)
        
        filings.append(FilingInfo(
            manager_url=manager_url,
            cik=cik,
            quarter=quarter,
            filing_id=filing_id,
            filing_url=urljoin(BASE, href)
        ))
    
    return filings


async def phase1_collect_filings(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    manager_urls: list[str],
    existing: set[tuple[str, str]]
) -> list[FilingInfo]:
    """
    Phase 1: Fetch all manager pages and collect missing filings
    """
    print(f"\n📥 Phase 1: Fetching {len(manager_urls)} manager pages...")
    
    all_filings = []
    total_available = 0
    
    for i in range(0, len(manager_urls), BATCH_SIZE_PHASE1):
        batch = manager_urls[i:i + BATCH_SIZE_PHASE1]
        batch_num = (i // BATCH_SIZE_PHASE1) + 1
        total_batches = (len(manager_urls) + BATCH_SIZE_PHASE1 - 1) // BATCH_SIZE_PHASE1
        
        # Fetch all pages in parallel
        tasks = [fetch_text(session, url, semaphore) for url in batch]
        results = await asyncio.gather(*tasks)
        
        # Parse results
        batch_filings = []
        batch_available = 0
        for html, manager_url in zip(results, batch):
            if not html:
                continue
            filings = parse_manager_page(html, manager_url)
            batch_available += len(filings)
            
            # Filter to only missing
            for f in filings:
                if (f.cik, f.quarter) not in existing:
                    batch_filings.append(f)
        
        all_filings.extend(batch_filings)
        total_available += batch_available
        
        print(f"  Batch {batch_num}/{total_batches}: "
              f"found {batch_available} quarters, {len(batch_filings)} missing | "
              f"Total missing: {len(all_filings)}")
    
    print(f"✅ Phase 1 complete: {total_available} total quarters, {len(all_filings)} missing")
    return all_filings


async def fetch_single_holding(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    filing: FilingInfo
) -> list[dict]:
    """Fetch holdings for a single filing"""
    api_url = f"{BASE}/data/13f/{filing.filing_id}"
    data = await fetch_json(session, api_url, semaphore)
    
    if not data or "data" not in data:
        return []
    
    # Ensure CIK is normalized (10-digit padded)
    normalized_cik = filing.cik.lstrip('0').zfill(10)
    
    holdings = []
    for row in data["data"]:
        if len(row) >= 9:
            holdings.append({
                "manager_url": filing.manager_url,
                "cik": normalized_cik,
                "quarter": filing.quarter,
                "filing_url": filing.filing_url,
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
    
    return holdings


async def phase2_fetch_holdings(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    filings: list[FilingInfo],
    db_url: str
) -> int:
    """
    Phase 2: Fetch all holdings in parallel and insert to DB
    """
    print(f"\n📥 Phase 2: Fetching holdings for {len(filings)} quarters...")
    
    total_holdings = 0
    total_batches = (len(filings) + BATCH_SIZE_PHASE2 - 1) // BATCH_SIZE_PHASE2
    
    holdings_buffer = []
    start_time = time.time()
    
    for i in range(0, len(filings), BATCH_SIZE_PHASE2):
        batch = filings[i:i + BATCH_SIZE_PHASE2]
        batch_num = (i // BATCH_SIZE_PHASE2) + 1
        
        # Fetch all holdings in parallel
        tasks = [fetch_single_holding(session, semaphore, f) for f in batch]
        results = await asyncio.gather(*tasks)
        
        # Collect holdings
        batch_holdings = []
        for holdings in results:
            batch_holdings.extend(holdings)
        
        holdings_buffer.extend(batch_holdings)
        
        # Insert to DB periodically
        if len(holdings_buffer) >= DB_INSERT_BATCH or batch_num == total_batches:
            if holdings_buffer:
                inserted = insert_holdings(db_url, holdings_buffer)
                total_holdings += inserted
                holdings_buffer = []
        
        # Progress
        elapsed = time.time() - start_time
        processed = i + len(batch)
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (len(filings) - processed) / rate if rate > 0 else 0
        
        print(f"  Batch {batch_num}/{total_batches}: +{len(batch_holdings)} holdings | "
              f"Total: {total_holdings:,} | Rate: {rate:.0f} quarters/s | ETA: {eta/60:.1f}min")
    
    # Insert remaining
    if holdings_buffer:
        inserted = insert_holdings(db_url, holdings_buffer)
        total_holdings += inserted
    
    print(f"✅ Phase 2 complete: {total_holdings:,} holdings inserted")
    return total_holdings


def insert_holdings(db_url: str, holdings: list[dict]) -> int:
    """Insert holdings to database"""
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
                    page_size=10000,
                )
                return len(rows)
    finally:
        conn.close()


async def main_async(args):
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set")
        return
    
    # Get existing data
    print("📊 Checking existing data in database...")
    existing = get_existing_cik_quarters(db_url)
    print(f"✅ Found {len(existing):,} existing (cik, quarter) combinations")
    
    # Setup connection pool
    connector = aiohttp.TCPConnector(
        limit=SEMAPHORE_LIMIT,
        limit_per_host=SEMAPHORE_LIMIT,
        keepalive_timeout=60,
        enable_cleanup_closed=True,
    )
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": UA}
    ) as session:
        
        # Collect manager URLs
        print("\n📥 Collecting manager URLs...")
        manager_urls = await collect_manager_urls(session, semaphore)
        print(f"✅ Found {len(manager_urls):,} managers")
        
        if args.limit and args.limit > 0:
            manager_urls = manager_urls[:args.limit]
            print(f"⚠️ TEST MODE: limiting to {args.limit} managers")
        
        start_time = time.time()
        
        # Phase 1: Collect all missing filings
        missing_filings = await phase1_collect_filings(
            session, semaphore, manager_urls, existing
        )
        
        if not missing_filings:
            print("\n✅ No missing quarters to backfill!")
            return
        
        # Phase 2: Fetch all holdings
        total_holdings = await phase2_fetch_holdings(
            session, semaphore, missing_filings, db_url
        )
        
        # Summary
        elapsed = time.time() - start_time
        print(f"\n{'='*70}")
        print(f"✅ BACKFILL COMPLETE in {elapsed/60:.1f} minutes")
        print(f"   Quarters backfilled: {len(missing_filings):,}")
        print(f"   Holdings added: {total_holdings:,}")
        
        # Final DB stats
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
    args = ap.parse_args()
    
    print("=" * 70)
    print("13f.info Holdings Backfill - FULLY OPTIMIZED")
    print("=" * 70)
    
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()