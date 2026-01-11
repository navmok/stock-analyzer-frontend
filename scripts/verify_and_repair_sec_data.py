"""
SEC 13F Data Verification and Repair Script

This script:
1. Identifies missing periods (in manager_quarter but not manager_quarter_holding)
2. Identifies periods with potentially bad data (value_usd seems wrong)
3. Re-downloads only what's needed
4. Fixes the value multiplication bug (some filings are in dollars, not thousands)
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
# Set to True to only show what needs to be done (no actual downloads)
DRY_RUN = False

# Modes: "missing_only", "missing_and_bad", "partial", "bad_values", "all"
# - missing_only: Only download periods with no holdings at all
# - missing_and_bad: Missing + periods with 1000x value bug (RECOMMENDED)
# - partial: Download missing + periods with incomplete holdings (value mismatch)
# - bad_values: All of the above + suspicious values
# - all: Re-download everything (full refresh)
REPAIR_MODE = "missing_and_bad"  # RECOMMENDED: fixes 4311 missing + 84 bad values

# Value threshold - if sum(value_usd) > this, consider it potentially bad
# $1 quadrillion in holdings is clearly wrong data
BAD_VALUE_THRESHOLD = 1_000_000_000_000_000  # $1 quadrillion (clearly wrong)

# AGGRESSIVE RETRY SETTINGS for stubborn failures
REQUEST_DELAY_SEC = 0.3       # Longer delay between requests
MAX_RETRIES = 5               # More retries (was 3)
RETRY_DELAY_SEC = 10          # Longer wait between retries (was 5)
REQUEST_TIMEOUT = 120         # Timeout for requests in seconds
BATCH_SIZE = 50
# ============================================================================

SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)
QUARTERLY_INDEXES = {}


def request_with_retry(url, timeout=None, stream=False):
    """Make HTTP request with retry logic for stubborn failures"""
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            r = SESSION.get(url, timeout=timeout, stream=stream)
            if r.status_code == 429:  # Rate limited
                wait_time = RETRY_DELAY_SEC * (attempt + 1)
                print(f"    ⚠️ Rate limited, waiting {wait_time}s...", flush=True)
                time.sleep(wait_time)
                continue
            return r
        except requests.exceptions.Timeout:
            last_error = "timeout"
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY_SEC * (attempt + 1)
                time.sleep(wait_time)
        except requests.exceptions.ConnectionError:
            last_error = "connection error"
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY_SEC * (attempt + 1)
                time.sleep(wait_time)
        except Exception as e:
            last_error = str(e)
            break
    
    # Return a fake failed response
    class FailedResponse:
        status_code = 0
        content = b''
        text = ''
        def raise_for_status(self):
            raise requests.exceptions.RequestException(f"Failed after {MAX_RETRIES} retries: {last_error}")
    
    return FailedResponse()


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
            
            r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            
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
                
                if not form_type.startswith("13F"):
                    continue
                
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
    if isinstance(period_end, str):
        dt = datetime.strptime(period_end[:10], "%Y-%m-%d").date()
    elif isinstance(period_end, datetime):
        dt = period_end.date()
    else:
        dt = period_end
    
    quarter = (dt.month - 1) // 3 + 1
    return dt.year, quarter, dt


def find_13f_filing(cik, period_end, debug=False):
    cik_padded = str(cik).zfill(10)
    year, quarter, target_date = period_to_quarter(period_end)
    
    filing_year = year
    filing_quarter = quarter + 1
    if filing_quarter > 4:
        filing_quarter = 1
        filing_year += 1
    
    index = download_quarterly_index(filing_year, filing_quarter)
    filings = index.get(cik_padded, [])

    start_date = target_date + timedelta(days=1)
    end_date = target_date + timedelta(days=150)

    candidates = [f for f in filings if start_date <= f["filing_date"] <= end_date]

    # If not found, also check one more quarter (late / moved filings)
    if not candidates:
        next_q = filing_quarter + 1
        next_y = filing_year
        if next_q > 4:
            next_q = 1
            next_y += 1

        index2 = download_quarterly_index(next_y, next_q)
        filings2 = index2.get(cik_padded, [])
        candidates = [f for f in filings2 if start_date <= f["filing_date"] <= end_date]

    if not candidates:
        return None

    candidates.sort(key=lambda x: x["filing_date"])
    return candidates[0]


def get_xml_urls_from_directory(cik, accession_no):
    cik_int = str(int(cik))
    directory_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_no}"
    
    try:
        r = request_with_retry(directory_url)
        if r.status_code == 404 or r.status_code == 0:
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
    except:
        return []


def find_primary_doc_url(xml_urls):
    for url in xml_urls:
        if re.search(r'primary.*doc', url, re.IGNORECASE):
            return url
    
    for url in xml_urls:
        try:
            r = request_with_retry(url)
            if r.status_code != 200:
                continue
            if b'edgarSubmission' in r.content or b'coverPage' in r.content:
                return url
        except:
            continue
    return None


def find_info_table_url(xml_urls):
    for url in xml_urls:
        if re.search(r'info.*table', url, re.IGNORECASE):
            return url
    
    for url in xml_urls:
        try:
            r = request_with_retry(url)
            if r.status_code != 200:
                continue
            if b'informationTable' in r.content or b'infoTable' in r.content:
                return url
        except:
            continue
    return None


def fetch_value_total(xml_url):
    """Fetch total value from primary_doc.xml - returns value in THOUSANDS (as reported)"""
    try:
        r = request_with_retry(xml_url)
        if r.status_code == 404 or r.status_code == 0:
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
                        # This is in THOUSANDS as per SEC spec
                        return int(elem.text.strip())
                    except ValueError:
                        pass
        return None
    except:
        return None


def fetch_holdings(xml_url, max_size_mb=25):
    """
    Fetch holdings from info_table.xml
    
    IMPORTANT: SEC 13F values are reported in THOUSANDS of dollars.
    So we multiply by 1000 to get actual USD.
    """
    try:
        r = request_with_retry(xml_url, timeout=REQUEST_TIMEOUT * 2, stream=True)
        if r.status_code == 404 or r.status_code == 0:
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
                    except:
                        pass
                return None
            
            def get_child_float(parent, tag_name):
                val = get_child_text(parent, tag_name)
                if val:
                    try:
                        return float(val.replace(',', '').replace(' ', ''))
                    except:
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
            
            # Get value in thousands (as reported in SEC filing)
            value_thousands = get_child_int(elem, "value")
            
            # Convert to actual USD (multiply by 1000)
            # SEC 13F always reports values in thousands
            value_usd = value_thousands * 1000 if value_thousands else None
            
            holding = {
                "line_no": idx,
                "issuer": get_child_text(elem, "nameOfIssuer"),
                "title_of_class": get_child_text(elem, "titleOfClass"),
                "cusip": get_child_text(elem, "cusip"),
                "value_usd": value_usd,
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
    except Exception as e:
        print(f"  ⚠️  Error: {e}", flush=True)
        return []


def process_manager_period(cik, period_end, cur, conn):
    """Process a single manager-period combination"""
    try:
        filing = find_13f_filing(cik, period_end)
        if not filing:
            return {"status": "no_filing"}
        
        acc_no = filing["accession_no"]
        
        xml_urls = get_xml_urls_from_directory(cik, acc_no)
        if not xml_urls:
            return {"status": "no_xml"}
        
        primary_doc_url = find_primary_doc_url(xml_urls)
        info_table_url = find_info_table_url(xml_urls)
        
        total_value = None
        if primary_doc_url:
            total_value = fetch_value_total(primary_doc_url)
        
        holdings = []
        if info_table_url:
            holdings = fetch_holdings(info_table_url)
        
        if total_value:
            # total_value is in thousands, convert to millions for total_value_m
            total_value_m = total_value / 1_000.0
            cur.execute("""
                UPDATE manager_quarter
                SET total_value_m = %s
                WHERE cik = %s AND period_end = %s
            """, (total_value_m, cik, period_end))
        
        if holdings:
            # Delete old holdings first
            cur.execute("""
                DELETE FROM manager_quarter_holding
                WHERE cik = %s AND period_end = %s
            """, (cik, period_end))
            
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
        return {"status": "success", "holdings": len(holdings)}
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        return {"status": "error", "error": str(e)}


def main():
    print("=" * 70)
    print("SEC 13F Data Verification and Repair")
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
    
    # Ensure table exists
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
    
    # =========================================================================
    # STEP 1: Analyze current state
    # =========================================================================
    print("\n📊 Analyzing current data state...\n")
    
    # Get all expected periods from manager_quarter
    cur.execute("""
        SELECT cik, period_end FROM manager_quarter ORDER BY cik, period_end
    """)
    all_expected = set(cur.fetchall())
    print(f"   Total expected (cik, period_end) pairs: {len(all_expected)}")
    
    # Get all periods that have holdings data
    cur.execute("""
        SELECT DISTINCT cik, period_end FROM manager_quarter_holding
    """)
    has_holdings = set(cur.fetchall())
    print(f"   Periods with holdings data: {len(has_holdings)}")
    
    # Find missing periods
    missing_periods = all_expected - has_holdings
    print(f"   Missing periods (no holdings): {len(missing_periods)}")
    
    # Find periods with potentially bad values (sum > threshold)
    cur.execute("""
        SELECT cik, period_end, SUM(value_usd) as total
        FROM manager_quarter_holding
        GROUP BY cik, period_end
        HAVING SUM(value_usd) > %s
    """, (BAD_VALUE_THRESHOLD,))
    bad_value_periods = set((row[0], row[1]) for row in cur.fetchall())
    print(f"   Periods with suspicious values (>$1Q): {len(bad_value_periods)}")
    
    # Find periods with PARTIAL holdings (value mismatch indicates incomplete data)
    # Note: num_holdings was set from what we downloaded, so count check won't work
    # Instead, check if SUM(value_usd) differs significantly from total_value_m
    # Also check if MAX(line_no) >> COUNT(*) which indicates gaps
    cur.execute("""
        SELECT 
            mq.cik, 
            mq.period_end,
            mq.total_value_m,
            COUNT(mqh.line_no) as holdings_count,
            MAX(mqh.line_no) as max_line_no,
            SUM(mqh.value_usd) / 1000000.0 as sum_value_m
        FROM manager_quarter mq
        INNER JOIN manager_quarter_holding mqh 
            ON mq.cik = mqh.cik AND mq.period_end = mqh.period_end
        WHERE mq.total_value_m IS NOT NULL AND mq.total_value_m > 0
        GROUP BY mq.cik, mq.period_end, mq.total_value_m
        HAVING 
            -- Value mismatch: our sum differs from expected by more than 20%
            ABS(SUM(mqh.value_usd) / 1000000.0 - mq.total_value_m) > mq.total_value_m * 0.20
            OR
            -- Line number gaps: max line_no is much higher than count (indicates skipped rows)
            MAX(mqh.line_no) > COUNT(mqh.line_no) * 1.1
    """)
    partial_holdings_rows = cur.fetchall()
    partial_periods = set((row[0], row[1]) for row in partial_holdings_rows)
    print(f"   Periods with PARTIAL holdings (incomplete download): {len(partial_periods)}")
    
    # Show some examples of partial downloads
    if partial_holdings_rows and len(partial_holdings_rows) > 0:
        print("\n   Examples of partial downloads:")
        for row in partial_holdings_rows[:10]:
            cik, period, exp_val_m, count, max_line, sum_val_m = row
            exp_val_f = float(exp_val_m) if exp_val_m else 0
            sum_val_f = float(sum_val_m) if sum_val_m else 0
            pct_diff = abs(sum_val_f - exp_val_f) / exp_val_f * 100 if exp_val_f > 0 else 0
            print(f"      {cik} {period}: {count} holdings (max line {max_line}), ${sum_val_f/1e3:.1f}B vs ${exp_val_f/1e3:.1f}B expected ({pct_diff:.0f}% diff)")
    
    # =========================================================================
    # STEP 2: Determine what to repair
    # =========================================================================
    if REPAIR_MODE == "missing_only":
        tasks = list(missing_periods)
        print(f"\n🔧 Mode: missing_only - will download {len(tasks)} missing periods")
    elif REPAIR_MODE == "missing_and_bad":
        tasks = list(missing_periods | bad_value_periods)
        print(f"\n🔧 Mode: missing_and_bad - will repair {len(missing_periods)} missing + {len(bad_value_periods)} bad values = {len(tasks)} periods")
    elif REPAIR_MODE == "partial":
        tasks = list(missing_periods | partial_periods)
        print(f"\n🔧 Mode: partial - will repair {len(missing_periods)} missing + {len(partial_periods)} partial = {len(tasks)} periods")
    elif REPAIR_MODE == "bad_values":
        tasks = list(missing_periods | bad_value_periods | partial_periods)
        print(f"\n🔧 Mode: bad_values - will repair {len(tasks)} periods (missing + bad values + partial)")
    else:  # all
        tasks = list(all_expected)
        print(f"\n🔧 Mode: all - will re-download all {len(tasks)} periods")
    
    if not tasks:
        print("\n✅ Nothing to repair! All data is complete.")
        cur.close()
        conn.close()
        return
    
    # Sort for consistent processing
    tasks.sort()
    
    # Time estimate
    est_seconds = len(tasks) * 2.5
    est_time = timedelta(seconds=int(est_seconds))
    print(f"⏱️  Estimated time: {est_time}")
    
    if DRY_RUN:
        print("\n🔍 DRY RUN - showing first 20 tasks:")
        for cik, period in tasks[:20]:
            cur.execute("SELECT manager_name FROM manager_quarter WHERE cik = %s LIMIT 1", (cik,))
            row = cur.fetchone()
            name = row[0] if row else "Unknown"
            print(f"   {cik} {period} - {name}")
        if len(tasks) > 20:
            print(f"   ... and {len(tasks) - 20} more")
        cur.close()
        conn.close()
        return
    
    # =========================================================================
    # STEP 3: Pre-download quarterly indexes
    # =========================================================================
    quarters_needed = set()
    for cik, period_end in tasks:
        year, quarter, _ = period_to_quarter(period_end)
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
    
    # =========================================================================
    # STEP 4: Process repairs
    # =========================================================================
    start_time = datetime.now()
    processed = 0
    failed = 0
    no_filing = 0
    
    for batch_start in range(0, len(tasks), BATCH_SIZE):
        batch = tasks[batch_start:batch_start + BATCH_SIZE]
        
        print(f"\n{'='*70}")
        print(f"Batch {batch_start // BATCH_SIZE + 1}/{(len(tasks) + BATCH_SIZE - 1) // BATCH_SIZE}")
        print(f"{'='*70}")
        
        for idx, (cik, period_end) in enumerate(batch):
            progress_num = batch_start + idx + 1
            print(f"  [{progress_num}/{len(tasks)}] {cik} {period_end}...", end=" ", flush=True)
            
            result = process_manager_period(cik, period_end, cur, conn)
            
            if result["status"] == "success":
                processed += 1
                print(f"✓ {result.get('holdings', 0)} holdings", flush=True)
            elif result["status"] == "no_filing":
                no_filing += 1
                print(f"⚠️ No filing", flush=True)
            else:
                failed += 1
                print(f"❌ Error", flush=True)
        
        conn.commit()
        
        # Progress
        done = batch_start + len(batch)
        elapsed = datetime.now() - start_time
        rate = done / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        remaining = len(tasks) - done
        eta = timedelta(seconds=int(remaining / rate)) if rate > 0 else timedelta(0)
        
        print(f"\n📊 Progress: {done}/{len(tasks)} ({100*done/len(tasks):.1f}%)")
        print(f"⏱️  Elapsed: {str(elapsed).split('.')[0]} | ETA: {eta}")
        print(f"✓ {processed} success | ⚠️ {no_filing} no filing | ❌ {failed} failed")
    
    # Final summary
    total_time = datetime.now() - start_time
    print("\n" + "=" * 70)
    print("✅ REPAIR COMPLETED")
    print(f"   Total time: {str(total_time).split('.')[0]}")
    print(f"   Processed: {processed} | No filing: {no_filing} | Failed: {failed}")
    print("=" * 70)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()