"""
Fast SEC 13F Downloader - Uses Ruby's efficient strategy
Downloads 10-20x faster than original approach

Features:
- Processes ALL managers in manager_quarter table
- Skips already-processed periods (checks for existing holdings)
- Retry logic for network timeouts
- Bulk inserts with execute_values
- Ruby-style HTML directory parsing for XML URLs
"""
import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import io
import re
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from lxml import etree
import time

SEC_HEADERS = {
    "User-Agent": "Naveen Mokkapati navmok@gmail.com",
    "Accept": "*/*",
}

# ============================================================================
# CONFIGURATION
# ============================================================================
# Set to None or empty set to process ALL managers
TARGET_CIKS = None  # Process ALL managers
# TARGET_CIKS = {"0001595888"}  # Jane Street only
# TARGET_CIKS = {"0001067983", "0001649339"}  # Berkshire & ARK

# Skip periods that already have holdings in the database
SKIP_ALREADY_PROCESSED = True

REQUEST_DELAY_SEC = 0.1  # Be respectful to SEC
BATCH_SIZE = 50
MAX_RETRIES = 3
RETRY_DELAY_SEC = 5
# ============================================================================

SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)

# In-memory caches
QUARTERLY_INDEXES = {}  # {(year, quarter): {cik: [filings]}}


def download_quarterly_index(year, quarter):
    """Download and parse master.idx for a quarter - with retry logic"""
    key = (year, quarter)
    if key in QUARTERLY_INDEXES:
        return QUARTERLY_INDEXES[key]
    
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    
    for attempt in range(MAX_RETRIES):
        try:
            attempt_msg = f" (attempt {attempt + 1}/{MAX_RETRIES})" if attempt > 0 else ""
            print(f"📥 Downloading index {year} Q{quarter}{attempt_msg}...", flush=True)
            
            r = SESSION.get(url, timeout=90)
            r.raise_for_status()
            
            # Parse master.idx
            by_cik = {}
            lines = r.text.split('\n')
            header_found = False
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                    
                if not header_found:
                    if line.startswith("CIK|Company Name|Form Type|Date Filed|Filename"):
                        header_found = True
                    continue
                
                parts = line.split("|")
                if len(parts) < 5:
                    continue
                    
                cik, company_name, form_type, date_filed, filename = parts[:5]
                
                # Only 13F forms
                if not form_type.startswith("13F"):
                    continue
                
                # Build accession number from filename
                acc_no = filename.split("/")[-1].replace(".txt", "").replace("-", "")
                
                try:
                    filing_date = datetime.strptime(date_filed, "%Y-%m-%d").date()
                except ValueError:
                    continue
                
                cik_padded = cik.zfill(10)
                by_cik.setdefault(cik_padded, []).append({
                    "form_type": form_type,
                    "filing_date": filing_date,
                    "accession_no": acc_no,
                    "company_name": company_name,
                })
            
            print(f"✓ Indexed {len(by_cik)} managers for {year} Q{quarter}", flush=True)
            QUARTERLY_INDEXES[key] = by_cik
            return by_cik
            
        except requests.exceptions.Timeout:
            if attempt < MAX_RETRIES - 1:
                print(f"    ⚠️  Timeout, retrying in {RETRY_DELAY_SEC}s...", flush=True)
                time.sleep(RETRY_DELAY_SEC)
            else:
                print(f"❌ Failed to download index {year} Q{quarter} after {MAX_RETRIES} attempts", flush=True)
                QUARTERLY_INDEXES[key] = {}
                return {}
        except Exception as e:
            print(f"❌ Failed to download index {year} Q{quarter}: {e}", flush=True)
            QUARTERLY_INDEXES[key] = {}
            return {}
    
    return {}


def period_to_quarter(period_end):
    """Convert period_end date to (year, quarter)"""
    if isinstance(period_end, str):
        dt = datetime.strptime(period_end[:10], "%Y-%m-%d").date()
    elif isinstance(period_end, datetime):
        dt = period_end.date()
    else:
        dt = period_end
    
    quarter = (dt.month - 1) // 3 + 1
    return dt.year, quarter, dt


def find_13f_filing(cik, period_end, debug=False):
    """Find 13F filing for a CIK and period - fast local lookup"""
    cik_padded = str(cik).zfill(10)
    
    year, quarter, target_date = period_to_quarter(period_end)
    
    # Search in the next quarter's index (where the filing actually appears)
    filing_year = year
    filing_quarter = quarter + 1
    if filing_quarter > 4:
        filing_quarter = 1
        filing_year += 1
    
    index = download_quarterly_index(filing_year, filing_quarter)
    filings = index.get(cik_padded, [])
    
    if debug:
        print(f"    DEBUG: Looking for period ending {target_date}", flush=True)
        print(f"    DEBUG: Searching in {filing_year} Q{filing_quarter} index", flush=True)
        print(f"    DEBUG: Found {len(filings)} total filings for CIK {cik_padded}", flush=True)
    
    if not filings:
        return None
    
    start_date = target_date + timedelta(days=1)
    end_date = target_date + timedelta(days=90)
    
    candidates = []
    for f in filings:
        if start_date <= f["filing_date"] <= end_date:
            candidates.append(f)
    
    if not candidates:
        return None
    
    candidates.sort(key=lambda x: x["filing_date"])
    return candidates[0]


def get_xml_urls_from_directory(cik, accession_no):
    """Ruby's approach: Fetch HTML directory listing and extract all XML URLs."""
    cik_int = str(int(cik))
    directory_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_no}"
    
    try:
        r = SESSION.get(directory_url, timeout=30)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        
        xml_urls = []
        pattern = r'href="([^"]+\.xml)"'
        matches = re.findall(pattern, r.text, re.IGNORECASE)
        
        for href in matches:
            if href.startswith('/'):
                xml_urls.append(f"https://www.sec.gov{href}")
            elif href.startswith('http'):
                xml_urls.append(href)
            else:
                xml_urls.append(f"{directory_url}/{href}")
        
        return xml_urls
        
    except Exception as e:
        return []


def find_primary_doc_url(xml_urls):
    """Find primary_doc.xml by name pattern or content check."""
    for url in xml_urls:
        if re.search(r'primary.*doc', url, re.IGNORECASE):
            return url
    
    for url in xml_urls:
        try:
            r = SESSION.get(url, timeout=30)
            if r.status_code != 200:
                continue
            if b'edgarSubmission' in r.content or b'coverPage' in r.content:
                return url
        except:
            continue
    
    return None


def find_info_table_url(xml_urls):
    """Find info_table.xml by name pattern or content check."""
    for url in xml_urls:
        if re.search(r'info.*table', url, re.IGNORECASE):
            return url
    
    for url in xml_urls:
        try:
            r = SESSION.get(url, timeout=30)
            if r.status_code != 200:
                continue
            if b'informationTable' in r.content or b'infoTable' in r.content:
                return url
        except:
            continue
    
    return None


def fetch_value_total(xml_url):
    """Fetch total value from primary_doc.xml"""
    try:
        r = SESSION.get(xml_url, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        
        root = etree.fromstring(r.content)
        
        for elem in root.iter():
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        
        for elem in root.iter():
            if elem.tag in ['tableValueTotal', 'valueTotal', 'securitiesOwnedAggregateValue']:
                if elem.text and elem.text.strip():
                    try:
                        return int(elem.text.strip())
                    except ValueError:
                        pass
        
        return None
        
    except:
        return None


def fetch_holdings(xml_url, max_size_mb=25):
    """Fetch holdings from info_table.xml"""
    try:
        r = SESSION.get(xml_url, timeout=45, stream=True)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        
        content_length = r.headers.get('content-length')
        if content_length and int(content_length) > max_size_mb * 1_000_000:
            print(f"  ⚠️  Skipping large file: {int(content_length) / 1_000_000:.1f}MB", flush=True)
            return []
        
        xml_content = r.content
        holdings = []
        idx = 0
        
        for event, elem in etree.iterparse(io.BytesIO(xml_content), events=("end",), recover=True):
            if not isinstance(elem.tag, str):
                continue
                
            tag = elem.tag.split('}')[-1].lower() if '}' in elem.tag else elem.tag.lower()
            
            if tag != "infotable":
                continue
            
            idx += 1
            
            def get_child_text(parent, tag_name):
                want = tag_name.lower()
                for child in parent.iter():
                    if not isinstance(child.tag, str):
                        continue
                    child_tag = child.tag.split('}')[-1].lower() if '}' in child.tag else child.tag.lower()
                    if child_tag == want and child.text:
                        return child.text.strip()
                return None
            
            def get_child_int(parent, tag_name):
                val = get_child_text(parent, tag_name)
                if val:
                    try:
                        return int(val.replace(',', '').replace(' ', ''))
                    except (ValueError, AttributeError):
                        pass
                return None
            
            def get_child_float(parent, tag_name):
                val = get_child_text(parent, tag_name)
                if val:
                    try:
                        return float(val.replace(',', '').replace(' ', ''))
                    except (ValueError, AttributeError):
                        pass
                return None
            
            voting_elem = None
            for child in elem:
                child_tag = child.tag.split('}')[-1].lower() if '}' in child.tag else child.tag.lower()
                if child_tag == 'votingauthority':
                    voting_elem = child
                    break
            
            voting_sole = voting_shared = voting_none = None
            if voting_elem is not None:
                voting_sole = get_child_int(voting_elem, 'sole')
                voting_shared = get_child_int(voting_elem, 'shared')
                voting_none = get_child_int(voting_elem, 'none')
            
            holding = {
                "line_no": idx,
                "issuer": get_child_text(elem, "nameOfIssuer"),
                "title_of_class": get_child_text(elem, "titleOfClass"),
                "cusip": get_child_text(elem, "cusip"),
                "value_usd": (get_child_int(elem, "value") or 0) * 1000 if get_child_int(elem, "value") else None,
                "shares": get_child_float(elem, "sshPrnamt"),
                "share_type": get_child_text(elem, "sshPrnamtType"),
                "put_call": get_child_text(elem, "putCall"),
                "investment_discretion": get_child_text(elem, "investmentDiscretion"),
                "other_manager": get_child_text(elem, "otherManager"),
                "voting_sole": voting_sole,
                "voting_shared": voting_shared,
                "voting_none": voting_none,
            }
            
            holdings.append(holding)
            elem.clear()
            
            if idx % 2000 == 0:
                print(f"    ... {idx} holdings parsed", flush=True)
        
        return holdings
        
    except:
        return []


def get_processed_periods(cur, ciks=None):
    """Get set of (cik, period_end) that already have COMPLETE holdings in DB.
    
    Only skips periods where holdings have non-null shares data,
    ensuring incomplete/failed downloads get reprocessed.
    """
    print("📊 Checking for already-processed periods...", flush=True)
    
    if ciks:
        cur.execute("""
            SELECT DISTINCT cik, period_end 
            FROM manager_quarter_holding 
            WHERE cik = ANY(%s)
              AND shares IS NOT NULL
        """, (list(ciks),))
    else:
        cur.execute("""
            SELECT DISTINCT cik, period_end 
            FROM manager_quarter_holding
            WHERE shares IS NOT NULL
        """)
    
    processed = set()
    for row in cur.fetchall():
        processed.add((row[0], row[1]))
    
    print(f"✓ Found {len(processed)} already-processed periods", flush=True)
    return processed


def process_manager_period(cik, period_end, cur, conn, debug_first=False):
    """Process a single manager-period combination"""
    try:
        filing = find_13f_filing(cik, period_end, debug=debug_first)
        if not filing:
            return {"cik": cik, "period": period_end, "status": "no_filing"}
        
        acc_no = filing["accession_no"]
        
        xml_urls = get_xml_urls_from_directory(cik, acc_no)
        if not xml_urls:
            return {"cik": cik, "period": period_end, "status": "no_xml"}
        
        primary_doc_url = find_primary_doc_url(xml_urls)
        info_table_url = find_info_table_url(xml_urls)
        
        total_value = None
        if primary_doc_url:
            total_value = fetch_value_total(primary_doc_url)
        
        holdings = []
        if total_value and info_table_url:
            holdings = fetch_holdings(info_table_url)
        
        if total_value:
            total_value_m = total_value / 1_000_000.0
            cur.execute("""
                UPDATE manager_quarter
                SET total_value_m = %s
                WHERE cik = %s AND period_end = %s
            """, (total_value_m, cik, period_end))
        
        if holdings:
            cur.execute("""
                DELETE FROM manager_quarter_holding
                WHERE cik = %s AND period_end = %s AND accession_no = %s
            """, (cik, period_end, acc_no))
            
            values = []
            for h in holdings:
                values.append((
                    cik, period_end, acc_no, h["line_no"],
                    h["issuer"], h["title_of_class"], h["cusip"],
                    h["value_usd"], h["shares"], h["share_type"],
                    h["put_call"], h["investment_discretion"], h["other_manager"],
                    h["voting_sole"], h["voting_shared"], h["voting_none"]
                ))
            
            execute_values(cur, """
                INSERT INTO manager_quarter_holding (
                    cik, period_end, accession_no, line_no, issuer, title_of_class,
                    cusip, value_usd, shares, share_type, put_call, investment_discretion,
                    other_manager, voting_sole, voting_shared, voting_none
                ) VALUES %s
                ON CONFLICT (cik, period_end, accession_no, line_no) DO NOTHING
            """, values, page_size=1000)
            
            cur.execute("""
                UPDATE manager_quarter
                SET num_holdings = %s
                WHERE cik = %s AND period_end = %s
            """, (len(holdings), cik, period_end))
        
        time.sleep(REQUEST_DELAY_SEC)
        return {"cik": cik, "period": period_end, "status": "success", "holdings": len(holdings)}
        
    except Exception as e:
        # Rollback to clear the failed transaction state
        try:
            conn.rollback()
        except:
            pass
        # Just log the error briefly (full traceback is too verbose)
        print(f"\n    Error: {type(e).__name__}: {str(e)[:100]}", flush=True)
        return {"cik": cik, "period": period_end, "status": "error", "error": str(e)}


def main():
    print("=" * 70)
    print("Fast SEC 13F Downloader (Ruby Strategy)")
    print("=" * 70)
    
    load_dotenv()
    
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    conn.autocommit = False
    cur = conn.cursor()
    
    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS manager_quarter_holding (
            cik TEXT NOT NULL,
            period_end DATE NOT NULL,
            accession_no TEXT NOT NULL,
            line_no INTEGER NOT NULL,
            issuer TEXT,
            title_of_class TEXT,
            cusip TEXT,
            value_usd BIGINT,
            shares NUMERIC,
            share_type TEXT,
            put_call TEXT,
            investment_discretion TEXT,
            other_manager TEXT,
            voting_sole BIGINT,
            voting_shared BIGINT,
            voting_none BIGINT,
            created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (cik, period_end, accession_no, line_no)
        )
    """)
    conn.commit()
    
    # Get all tasks (manager-period combinations)
    if TARGET_CIKS:
        cur.execute("""
            SELECT cik, period_end
            FROM manager_quarter
            WHERE cik = ANY(%s)
            ORDER BY cik, period_end DESC
        """, (list(TARGET_CIKS),))
    else:
        cur.execute("""
            SELECT cik, period_end
            FROM manager_quarter
            ORDER BY cik, period_end DESC
        """)
    
    all_tasks = cur.fetchall()
    total_tasks = len(all_tasks)
    
    # Get already processed periods (to skip)
    processed_periods = set()
    if SKIP_ALREADY_PROCESSED:
        processed_periods = get_processed_periods(cur, TARGET_CIKS)
    
    # Filter out already processed
    tasks_to_process = [
        (cik, period_end) for cik, period_end in all_tasks
        if (cik, period_end) not in processed_periods
    ]
    
    skipped = total_tasks - len(tasks_to_process)
    
    # Get unique managers count
    unique_managers = len(set(cik for cik, _ in tasks_to_process))
    
    print(f"\n📋 Total manager-periods in database: {total_tasks}")
    print(f"⏭️  Already processed (skipping): {skipped}")
    print(f"📥 To process: {len(tasks_to_process)} periods from {unique_managers} managers")
    print(f"⚙️  Settings: delay={REQUEST_DELAY_SEC}s, batch={BATCH_SIZE}, retries={MAX_RETRIES}")
    
    # Time estimate (roughly 2-3 seconds per period based on Jane Street test)
    est_seconds = len(tasks_to_process) * 2.5
    est_time = timedelta(seconds=int(est_seconds))
    print(f"⏱️  Estimated time: {est_time} (at ~2.5s per period)")
    print("=" * 70)
    
    if len(tasks_to_process) == 0:
        print("\n✅ Nothing to process - all periods already have holdings!")
        cur.close()
        conn.close()
        return
    
    # Pre-download quarterly indexes
    quarters_needed = set()
    for cik, period_end in tasks_to_process:
        year, quarter, _ = period_to_quarter(period_end)
        # Need next quarter's index
        filing_quarter = quarter + 1
        filing_year = year
        if filing_quarter > 4:
            filing_quarter = 1
            filing_year += 1
        quarters_needed.add((filing_year, filing_quarter))
    
    print(f"\n📥 Pre-downloading {len(quarters_needed)} quarterly indexes...")
    for year, quarter in sorted(quarters_needed):
        download_quarterly_index(year, quarter)
    print("✓ All indexes cached\n")
    
    # Process in batches
    start_time = datetime.now()
    processed = 0
    failed = 0
    no_filing = 0
    
    for batch_start in range(0, len(tasks_to_process), BATCH_SIZE):
        batch = tasks_to_process[batch_start:batch_start + BATCH_SIZE]
        
        print(f"\n{'='*70}")
        print(f"Batch {batch_start // BATCH_SIZE + 1}/{(len(tasks_to_process) + BATCH_SIZE - 1) // BATCH_SIZE}: {len(batch)} periods")
        print(f"{'='*70}")
        
        for idx, (cik, period_end) in enumerate(batch):
            # Compact progress line
            progress_num = batch_start + idx + 1
            print(f"  [{progress_num}/{len(tasks_to_process)}] {cik} {period_end}...", end=" ", flush=True)
            
            result = process_manager_period(cik, period_end, cur, conn, debug_first=False)
            
            if result["status"] == "success":
                processed += 1
                holdings_count = result.get("holdings", 0)
                print(f"✓ {holdings_count} holdings", flush=True)
            elif result["status"] == "error":
                failed += 1
                print(f"❌ Error", flush=True)
            elif result["status"] == "no_filing":
                no_filing += 1
                print(f"⚠️ No filing", flush=True)
            elif result["status"] == "no_xml":
                no_filing += 1
                print(f"⚠️ No XML", flush=True)
        
        # Commit after each batch
        conn.commit()
        
        # Progress update
        done = batch_start + len(batch)
        elapsed = datetime.now() - start_time
        rate = done / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        remaining = len(tasks_to_process) - done
        eta_seconds = remaining / rate if rate > 0 else 0
        
        print(f"\n📊 Progress: {done}/{len(tasks_to_process)} ({100*done/len(tasks_to_process):.1f}%)")
        print(f"⏱️  Elapsed: {str(elapsed).split('.')[0]} | ETA: {str(timedelta(seconds=int(eta_seconds)))}")
        print(f"✓ {processed} success | ⚠️ {no_filing} no filing | ❌ {failed} failed")
    
    # Final summary
    total_time = datetime.now() - start_time
    print("\n" + "=" * 70)
    print("✅ COMPLETED")
    print(f"   Total time: {str(total_time).split('.')[0]}")
    print(f"   Processed: {processed} | No filing: {no_filing} | Failed: {failed} | Skipped: {skipped}")
    if len(tasks_to_process) > 0:
        print(f"   Avg per period: {total_time.total_seconds() / len(tasks_to_process):.2f}s")
    print("=" * 70)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()