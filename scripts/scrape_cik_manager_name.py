#!/usr/bin/env python3
"""
Scrape CIK and manager name mappings from 13f.info
Creates/updates the 'cik_manager_name' table in PostgreSQL.

Optimized for speed with:
- Parallel HTTP requests via ThreadPoolExecutor
- Batch database inserts with execute_values
- Minimal parsing (only extract what's needed)
- Optional manager classification

Usage:
    python scrape_cik_manager_name.py
    python scrape_cik_manager_name.py --with-classification
"""
import argparse
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Configuration
BASE_URL = "https://13f.info"
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0"]
MAX_WORKERS = 12  # Parallel threads for fetching letter pages
REQUEST_TIMEOUT = 30

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CIK-Scraper/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Classification keywords
BANK_KEYWORDS = [
    "BANK", "BANC", "BANCORP", "TRUST", "CREDIT UNION",
    "JPMORGAN", "CHASE", "WELLS FARGO", "CITIGROUP", "CITI ",
    "MORGAN STANLEY", "GOLDMAN SACHS", "BANK OF AMERICA",
    "BARCLAYS", "UBS", "HSBC", "STATE STREET", "NORTHERN TRUST",
    "PNC ", "US BANK", "CITIZENS", "FIFTH THIRD", "TRUIST",
]

ASSET_MANAGER_KEYWORDS = [
    "VANGUARD", "BLACKROCK", "FIDELITY", "SCHWAB", "INVESCO",
    "T ROWE", "T. ROWE", "FRANKLIN TEMPLETON", "DIMENSIONAL",
    "PIMCO", "CAPITAL GROUP", "WELLINGTON", "AMUNDI",
    "GEODE", "SSGA", "NORTHERN TRUST INVEST", "PRUDENTIAL",
]

HEDGE_FUND_KEYWORDS = [
    "HEDGE", "MASTER FUND", "OPPORTUNITY FUND",
]


def classify_manager(name):
    """Classify manager into: bank, asset_manager, hedge_fund, or other."""
    upper_name = name.upper()
    
    # Check banks first (most specific)
    if any(kw in upper_name for kw in BANK_KEYWORDS):
        return "bank"
    
    # Check large asset managers
    if any(kw in upper_name for kw in ASSET_MANAGER_KEYWORDS):
        return "asset_manager"
    
    # Check hedge fund indicators
    if any(kw in upper_name for kw in HEDGE_FUND_KEYWORDS):
        return "hedge_fund"
    
    # Generic investment manager keywords -> hedge_fund (most 13F filers)
    generic_kw = ["CAPITAL", "MANAGEMENT", "PARTNERS", "ADVISOR", "ADVISER", "FUND", "INVEST", "LP", "LLC"]
    if any(kw in upper_name for kw in generic_kw):
        return "hedge_fund"
    
    return "other"


def create_session():
    """Create session with connection pooling and retry logic."""
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = create_session()

# Pre-compiled regex for CIK extraction from URL
RE_CIK = re.compile(r'/manager/(\d{10})-')


def fetch_managers_for_letter(letter):
    """
    Fetch all manager links from a single letter page.
    Returns list of (cik, manager_name) tuples.
    """
    url = f"{BASE_URL}/managers/{letter.lower()}"
    
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        
        soup = BeautifulSoup(r.content, "lxml")
        managers = []
        
        # Manager links look like: /manager/0001540358-a16z-capital-management-l-l-c
        for a in soup.select('a[href^="/manager/"]'):
            href = a.get("href", "")
            name = a.get_text(strip=True)
            
            if not href or not name:
                continue
            
            # Extract CIK from URL
            match = RE_CIK.search(href)
            if match:
                cik = match.group(1)
                managers.append((cik, name))
        
        return letter, managers, None
    
    except Exception as e:
        return letter, [], str(e)


def main():
    parser = argparse.ArgumentParser(description="Scrape CIK-manager name mappings from 13f.info")
    parser.add_argument("--with-classification", action="store_true", 
                        help="Also classify managers (bank, asset_manager, hedge_fund, other)")
    args = parser.parse_args()
    
    load_dotenv()
    
    if "DATABASE_URL" not in os.environ:
        print("❌ DATABASE_URL not set")
        return 1
    
    print("=" * 60)
    print("13f.info CIK-Manager Name Scraper")
    if args.with_classification:
        print("(with classification)")
    print("=" * 60)
    
    start_time = datetime.now()
    all_managers = []
    errors = []
    
    print(f"\n📥 Fetching {len(LETTERS)} letter pages with {MAX_WORKERS} workers...\n")
    
    # Parallel fetch all letter pages
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_managers_for_letter, letter): letter for letter in LETTERS}
        
        for future in as_completed(futures):
            letter, managers, error = future.result()
            
            if error:
                print(f"  ❌ /managers/{letter}: {error}")
                errors.append((letter, error))
            else:
                all_managers.extend(managers)
                print(f"  ✓ /managers/{letter}: {len(managers)} managers")
    
    # Deduplicate by CIK (keep first occurrence)
    seen_ciks = set()
    unique_managers = []
    for cik, name in all_managers:
        if cik not in seen_ciks:
            seen_ciks.add(cik)
            unique_managers.append((cik, name))
    
    print(f"\n📊 Total unique managers: {len(unique_managers)}")
    
    if not unique_managers:
        print("❌ No managers found")
        return 1
    
    # Classify if requested
    if args.with_classification:
        print("\n🏷️  Classifying managers...")
        classified = [(cik, name, classify_manager(name)) for cik, name in unique_managers]
        
        # Count by category
        counts = {}
        for _, _, cat in classified:
            counts[cat] = counts.get(cat, 0) + 1
        for cat, cnt in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"   {cat}: {cnt}")
    
    # Connect to database
    print("\n💾 Saving to database...")
    
    conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=30)
    conn.autocommit = False
    cur = conn.cursor()
    
    if args.with_classification:
        # Create table with classification
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cik_manager_name (
                cik TEXT PRIMARY KEY,
                manager_name TEXT NOT NULL,
                classification TEXT,
                source TEXT DEFAULT '13f.info',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cik_manager_name_name 
            ON cik_manager_name (manager_name)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cik_manager_name_classification 
            ON cik_manager_name (classification)
        """)
        
        # Upsert with classification
        execute_values(
            cur,
            """
            INSERT INTO cik_manager_name (cik, manager_name, classification, updated_at)
            VALUES %s
            ON CONFLICT (cik) DO UPDATE SET
                manager_name = EXCLUDED.manager_name,
                classification = EXCLUDED.classification,
                updated_at = NOW()
            """,
            [(cik, name, cat, datetime.now()) for cik, name, cat in classified],
            page_size=1000
        )
    else:
        # Create table without classification
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cik_manager_name (
                cik TEXT PRIMARY KEY,
                manager_name TEXT NOT NULL,
                classification TEXT,
                source TEXT DEFAULT '13f.info',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create index on manager_name for fast lookups
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_cik_manager_name_name 
            ON cik_manager_name (manager_name)
        """)
        
        # Upsert all managers (update name if CIK exists)
        execute_values(
            cur,
            """
            INSERT INTO cik_manager_name (cik, manager_name, updated_at)
            VALUES %s
            ON CONFLICT (cik) DO UPDATE SET
                manager_name = EXCLUDED.manager_name,
                updated_at = NOW()
            """,
            [(cik, name, datetime.now()) for cik, name in unique_managers],
            page_size=1000
        )
    
    conn.commit()
    
    # Get final count
    cur.execute("SELECT COUNT(*) FROM cik_manager_name")
    total_in_db = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    
    elapsed = datetime.now() - start_time
    
    print(f"\n" + "=" * 60)
    print("✅ COMPLETED")
    print(f"   Time: {elapsed.total_seconds():.1f}s")
    print(f"   Scraped: {len(unique_managers)} managers")
    print(f"   Total in DB: {total_in_db}")
    if errors:
        print(f"   Errors: {len(errors)} pages failed")
    print("=" * 60)
    
    # Show sample
    print("\n📋 Sample data (first 10):")
    if args.with_classification:
        for cik, name, cat in sorted(classified, key=lambda x: x[1])[:10]:
            print(f"   {cik} | {cat:15} | {name}")
    else:
        for cik, name in sorted(unique_managers, key=lambda x: x[1])[:10]:
            print(f"   {cik} | {name}")
    
    return 0


if __name__ == "__main__":
    exit(main())