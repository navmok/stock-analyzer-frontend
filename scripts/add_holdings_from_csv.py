import argparse
import asyncio
import csv
import os
import sys

import aiohttp
from dotenv import load_dotenv

# Add current directory to path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backfill_13finfo_holdings import (
    collect_all_manager_urls,
    get_manager_filings,
    collect_all_manager_urls,
    get_manager_filings,
    parse_cik_from_manager_url,
    UA,
    BASE
)

async def fetch_json(session, url, semaphore, retries=3):
    async with semaphore:
        for i in range(retries):
            try:
                # Add delay to avoid aggressive rate limiting
                if i > 0:
                    await asyncio.sleep(1 * i)
                    
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=45)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        print(f"⚠️ 429 Too Many Requests for {url}, retrying...")
                        await asyncio.sleep(2 + i)
                        continue
                    else:
                        if i == retries - 1:
                            print(f"❌ HTTP {resp.status} for {url}")
                        return None
            except Exception as e:
                if i == retries - 1:
                    print(f"❌ Exception for {url}: {e}")
                pass
        return None
from add_holdings_from_13finfo_filing import (
    extract_holdings_from_json,
    normalize_holding_row,
    save_holdings_to_db
)

from psycopg2.extras import execute_values
import psycopg2

TABLE_13FINFO = "public.expected_13finfo_holdings"

async def fetch_direct_task_data(session, semaphore, task):
    """
    Fetches and parses data for a single task but DOES NOT save to DB.
    Returns a tuple: (success_bool, result_data_or_error_msg)
    """
    cik = task.get("cik", "").strip().lstrip("0")
    quarter = task.get("quarter", "").strip()
    direct_url = task.get("filing_id", "").strip()
    
    if not direct_url:
        return False, f"⚠️ CIK={cik} Q={quarter}: No filing_id URL provided"

    # Construct manager_url for DB (best effort)
    manager_url = f"{BASE}/manager/{cik.zfill(10)}"
    api_url = direct_url

    payload = await fetch_json(session, api_url, semaphore)
    if not payload:
        return False, f"❌ CIK={cik} Q={quarter}: Failed to fetch JSON {api_url}"

    raw_holdings = extract_holdings_from_json(payload)
    
    holdings = []
    for d in raw_holdings:
        row = normalize_holding_row(d)
        if row:
            # Enriched row with metadata needed for DB
            row['manager_url'] = manager_url
            row['cik'] = cik
            row['quarter'] = quarter.upper()
            row['filing_url'] = direct_url
            holdings.append(row)
    
    if not holdings:
        return False, f"⚠️ CIK={cik} Q={quarter}: No valid holdings found in {api_url}"

    return True, holdings

def bulk_save_holdings_direct(all_holdings_lists, db_url, mode):
    """
    Saves a batch of holdings lists to the DB in a SINGLE connection/transaction.
    all_holdings_lists: list of lists of dicts
    """
    if not all_holdings_lists:
        return

    # Flatten the list of lists
    flat_rows = []
    filing_urls_to_clean = set()
    
    for h_list in all_holdings_lists:
        for h in h_list:
            flat_rows.append(h)
            if mode == "replace":
                filing_urls_to_clean.add(h['filing_url'])

    if not flat_rows:
        return

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. Bulk Delete (if replace mode)
                if mode == "replace" and filing_urls_to_clean:
                    cur.execute(
                        f"DELETE FROM {TABLE_13FINFO} WHERE filing_url = ANY(%s)",
                        (list(filing_urls_to_clean),)
                    )

                # 2. Bulk Insert
                # Prepare tuples
                # Columns matching add_holdings_from_13finfo_filing.py schema
                values = [
                    (
                        r["manager_url"],
                        r["cik"],
                        r["quarter"],
                        r["filing_url"],
                        r["sym"],
                        r["issuer_name"],
                        r["class"],
                        r["cusip"],
                        r["value_000"],
                        r["pct"],
                        r["shares"],
                        r["principal"],
                        r["option_type"]
                    )
                    for r in flat_rows
                ]
                
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {TABLE_13FINFO}
                    (
                      manager_url, cik, quarter, filing_url,
                      sym, issuer_name, class, cusip, value_000,
                      pct, shares, principal, option_type
                    )
                    VALUES %s
                    """,
                    values,
                    page_size=5000
                )
        print(f"✅ DB BATCH: Saved {len(flat_rows)} rows across {len(filing_urls_to_clean) if mode=='replace' else 'multiple'} filings.")
            
    except Exception as e:
        print(f"❌ DB BATCH ERROR: {e}")
    finally:
        conn.close()


async def process_manager_tasks(session, semaphore, manager_url, tasks, db_url, mode):
    """
    Process all tasks (quarters) for a single manager.
    Fetch manager page ONCE, then fetch all needed quarters.
    """
    cik = tasks[0]["cik"]
    quarters_needed = {t["quarter"].lower() for t in tasks}
    
    # Get filings for this manager (1 request)
    filings = await get_manager_filings(session, semaphore, manager_url)
    
    # Map quarter -> (filing_id, filing_url)
    q_map = {}
    for q, fid, furl in filings:
        q_map[q.lower()] = (fid, furl)
        
    results = []
    
    # Create lookup for form_type by quarter
    form_type_map = {t["quarter"].lower(): t.get("form_type", "UNKNOWN") for t in tasks}

    # Process each quarter for this manager
    for quarter in quarters_needed:
        form_type = form_type_map.get(quarter, "UNKNOWN")
        
        if quarter not in q_map:
            print(f"❌ CIK={cik} Q={quarter} Type={form_type}: Quarter not found")
            continue
            
        target_filing_id, target_filing_url = q_map[quarter]
        
        # Fetch holdings (1 request per quarter)
        api_url = f"{BASE}/data/13f/{target_filing_id}"
        payload = await fetch_json(session, api_url, semaphore)
        
        if not payload:
            print(f"❌ CIK={cik} Q={quarter} Type={form_type}: Failed to fetch JSON {api_url}")
            continue
            
        raw_holdings = extract_holdings_from_json(payload)
        
        holdings = []
        for d in raw_holdings:
            row = normalize_holding_row(d)
            if row:
                holdings.append(row)
        
        if not holdings:
            print(f"⚠️ CIK={cik} Q={quarter} Type={form_type}: No valid holdings found")
            continue
            
        # Insert (sync operation, but fast enough)
        try:
            save_holdings_to_db(holdings, db_url, mode, target_filing_url, manager_url, cik, quarter.upper())
            print(f"✅ CIK={cik} Q={quarter} Type={form_type}: Saved {len(holdings)} rows")
        except Exception as e:
            print(f"❌ CIK={cik} Q={quarter} Type={form_type}: DB Error: {e}")


async def main_async(csv_file, mode):
    load_dotenv()
    load_dotenv(".env.local")
    
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set")
        return

    # 1. Read CSV
    tasks = []
    has_filing_id = False
    
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        # Check if 'filing_id' or 'filing_add' is in the headers
        fieldnames = [n.lower() for n in (reader.fieldnames or [])]
        if "filing_id" in fieldnames or "filing_add" in fieldnames:
            has_filing_id = True
            
        for row in reader:
            # Flexible reading of cik/quarter
            cik = row.get("cik", "").strip().lstrip("0")
            quarter = row.get("quarter", "").strip()
            form_type = row.get("form_type", "UNKNOWN")
            
            # Look for the URL column
            fid = row.get("filing_id", "") or row.get("filing_add", "")
            
            if cik and quarter:
                task = {
                    "cik": cik, 
                    "quarter": quarter, 
                    "form_type": form_type,
                    "filing_id": fid
                }
                tasks.append(task)
    
    print(f"📋 Loaded {len(tasks)} tasks from {csv_file}")
    
    if not tasks:
        print("No tasks found.")
        return

    unique_ciks = set(t["cik"] for t in tasks)
    print(f"🔍 Found {len(unique_ciks)} unique CIKs")

    # Shared connector/session
    connector = aiohttp.TCPConnector(limit=100) # Increased limit for optimization
    semaphore = asyncio.Semaphore(100) # Increased concurrency
    
    async with aiohttp.ClientSession(connector=connector, headers={"User-Agent": UA}) as session:
        
        if has_filing_id:
            print("🚀 DIRECT MODE DETECTED: Optimized Batch Processing.")
            print(f"Processing {len(tasks)} requests...")
            
            # Batch size for Fetching AND Saving
            chunk_size = 50
            
            for i in range(0, len(tasks), chunk_size):
                chunk = tasks[i:i+chunk_size]
                batch_num = i//chunk_size + 1
                total_batches = (len(tasks)+chunk_size-1)//chunk_size
                print(f"⚡ Batch {batch_num}/{total_batches}: Fetching {len(chunk)}...")
                
                # 1. Concurrent Fetch
                coroutines = [
                    fetch_direct_task_data(session, semaphore, t) 
                    for t in chunk
                ]
                results = await asyncio.gather(*coroutines)
                
                # 2. Process Results
                batch_holdings = []
                for success, data in results:
                    if success:
                        batch_holdings.append(data)
                    else:
                        print(data) # Print error message
                
                # 3. Bulk Save (Sync but fast due to single transaction)
                if batch_holdings:
                    bulk_save_holdings_direct(batch_holdings, db_url, mode)
                
            print("✅ All direct tasks completed.")
            return

        # --- OLD LOGIC (Fallback) ---
        print("ℹ️ No 'filing_id' column found. Falling back to Manager Index lookup.")
        
        # Group tasks by CIK for the old logic
        tasks_by_cik = {}
        for t in tasks:
            tasks_by_cik.setdefault(t["cik"], []).append(t)

        print("📥 Fetching manager index to map CIKs to URLs...")
        all_manager_urls = await collect_all_manager_urls(session, semaphore)
        
        cik_to_url = {}
        for url in all_manager_urls:
            c = parse_cik_from_manager_url(url)
            if c:
                cik_to_url[c.lstrip("0")] = url
        
        print(f"✅ Mapped {len(cik_to_url)} managers")

        manager_coroutines = []
        for cik, manager_tasks in tasks_by_cik.items():
            manager_url = cik_to_url.get(cik)
            if not manager_url:
                print(f"❌ CIK={cik}: URL not found in index, skipping {len(manager_tasks)} tasks")
                continue
            
            manager_coroutines.append(
                process_manager_tasks(session, semaphore, manager_url, manager_tasks, db_url, mode)
            )
            
        print(f"🚀 Starting parallel processing for {len(manager_coroutines)} managers...")
        
        chunk_size = 50 
        for i in range(0, len(manager_coroutines), chunk_size):
            chunk = manager_coroutines[i:i+chunk_size]
            print(f"Processing manager batch {i//chunk_size + 1}/{(len(manager_coroutines)+chunk_size-1)//chunk_size}...")
            await asyncio.gather(*chunk)
            
        print("✅ All tasks completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", help="Path to CSV file with cik, quarter columns")
    parser.add_argument("--mode", choices=["replace", "append"], default="replace")
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: File {args.csv_file} does not exist")
        sys.exit(1)
        
    asyncio.run(main_async(args.csv_file, args.mode))
