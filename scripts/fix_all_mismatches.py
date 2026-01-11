"""
Fix ALL mismatched holdings from CSV file

This script reads the mismatch CSV and re-downloads all periods
with correct value parsing.
"""
import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import io
import re
import csv
from datetime import datetime, timedelta
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
# Path to the CSV file with mismatched holdings
CSV_FILE = "value_holdings_chk_mismatch_filtered.csv"

# Set to True to only show what would be done
DRY_RUN = False

REQUEST_TIMEOUT = 120
REQUEST_DELAY = 0.3
MAX_RETRIES = 5
RETRY_DELAY = 10
BATCH_SIZE = 50
# ============================================================================

SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)

QUARTERLY_INDEXES = {}


def request_with_retry(url, timeout=None, stream=False):
    """Make HTTP request with retry logic"""
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            r = SESSION.get(url, timeout=timeout, stream=stream)
            if r.status_code == 429:  # Rate limited
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"    ⚠️ Rate limited, waiting {wait_time}s...", flush=True)
                time.sleep(wait_time)
                continue
            return r
        except requests.exceptions.Timeout:
            last_error = "timeout"
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except requests.exceptions.ConnectionError:
            last_error = "connection error"
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            last_error = str(e)
            break
    
    class FailedResponse:
        status_code = 0
        content = b''
        text = ''
        def raise_for_status(self):
            raise requests.exceptions.RequestException(f"Failed: {last_error}")
    
    return FailedResponse()


def download_quarterly_index(year, quarter):
    """Download and parse master.idx for a quarter"""
    key = (year, quarter)
    if key in QUARTERLY_INDEXES:
        return QUARTERLY_INDEXES[key]
    
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    print(f"📥 Downloading index {year} Q{quarter}...", flush=True)
    
    try:
        r = request_with_retry(url)
        if r.status_code != 200:
            QUARTERLY_INDEXES[key] = {}
            return {}
        
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
        
    except Exception as e:
        print(f"❌ Failed: {e}", flush=True)
        QUARTERLY_INDEXES[key] = {}
        return {}


def period_to_quarter(period_end):
    if isinstance(period_end, str):
        # Handle both formats: "6/30/2019" and "2019-06-30"
        if '/' in period_end:
            dt = datetime.strptime(period_end, "%m/%d/%Y").date()
        else:
            dt = datetime.strptime(period_end[:10], "%Y-%m-%d").date()
    else:
        dt = period_end
    quarter = (dt.month - 1) // 3 + 1
    return dt.year, quarter, dt


def find_13f_filing(cik, period_end):
    """Find 13F filing for a CIK and period"""
    cik_padded = str(cik).zfill(10)
    year, quarter, target_date = period_to_quarter(period_end)
    
    # Filing appears in next quarter's index
    filing_year = year
    filing_quarter = quarter + 1
    if filing_quarter > 4:
        filing_quarter = 1
        filing_year += 1
    
    index = download_quarterly_index(filing_year, filing_quarter)
    filings = index.get(cik_padded, [])
    
    if not filings:
        return None
    
    start_date = target_date + timedelta(days=1)
    end_date = target_date + timedelta(days=90)
    
    candidates = [f for f in filings if start_date <= f["filing_date"] <= end_date]
    
    if not candidates:
        return None
    
    candidates.sort(key=lambda x: x["filing_date"])
    return candidates[0]


def get_xml_urls_from_directory(cik, accession_no):
    """Get all XML URLs from filing directory"""
    cik_int = str(int(cik))
    directory_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_no}"
    
    try:
        r = request_with_retry(directory_url)
        if r.status_code != 200:
            return []
        
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


def find_info_table_url(xml_urls):
    """Find the info table XML URL - prefer raw XML over XSLT-transformed"""
    # Filter out XSLT-transformed versions
    raw_urls = [u for u in xml_urls if 'xsl' not in u.lower()]
    
    # First try raw URLs with infotable pattern
    for url in raw_urls:
        if re.search(r'info.*table', url, re.IGNORECASE):
            return url
    
    # Try all URLs with infotable pattern (fallback)
    for url in xml_urls:
        if re.search(r'info.*table', url, re.IGNORECASE) and 'xsl' not in url.lower():
            return url
    
    # Try each raw XML to find one with infoTable content
    for url in raw_urls:
        try:
            r = request_with_retry(url, timeout=60)
            if r.status_code == 200 and (b'informationTable' in r.content or b'infoTable' in r.content):
                return url
        except:
            continue
    return None


def fetch_holdings_fixed(xml_url):
    """
    Fetch holdings with CORRECT value parsing.
    SEC 13F values are in THOUSANDS of dollars.
    """
    try:
        r = request_with_retry(xml_url, timeout=REQUEST_TIMEOUT * 2)
        if r.status_code != 200:
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
            
            def get_text(parent, tag_name):
                want = tag_name.lower()
                for child in parent.iter():
                    if not isinstance(child.tag, str):
                        continue
                    child_tag = child.tag.split('}')[-1].lower() if '}' in child.tag else child.tag.lower()
                    if child_tag == want and child.text:
                        return child.text.strip()
                return None
            
            def get_int(parent, tag_name):
                val = get_text(parent, tag_name)
                if val:
                    try:
                        return int(val.replace(',', '').replace(' ', ''))
                    except:
                        pass
                return None
            
            def get_float(parent, tag_name):
                val = get_text(parent, tag_name)
                if val:
                    try:
                        return float(val.replace(',', '').replace(' ', ''))
                    except:
                        pass
                return None
            
            # Get voting authority
            voting_elem = None
            for child in elem:
                child_tag = child.tag.split('}')[-1].lower() if '}' in child.tag else child.tag.lower()
                if child_tag == 'votingauthority':
                    voting_elem = child
                    break
            
            voting_sole = voting_shared = voting_none = None
            if voting_elem is not None:
                voting_sole = get_int(voting_elem, 'sole')
                voting_shared = get_int(voting_elem, 'shared')
                voting_none = get_int(voting_elem, 'none')
            
            # Get value in THOUSANDS and convert to actual USD
            value_thousands = get_int(elem, "value")
            value_usd = value_thousands * 1000 if value_thousands else None
            
            holding = {
                "line_no": idx,
                "issuer": get_text(elem, "nameOfIssuer"),
                "title_of_class": get_text(elem, "titleOfClass"),
                "cusip": get_text(elem, "cusip"),
                "value_usd": value_usd,
                "shares": get_float(elem, "sshPrnamt"),
                "share_type": get_text(elem, "sshPrnamtType"),
                "put_call": get_text(elem, "putCall"),
                "investment_discretion": get_text(elem, "investmentDiscretion"),
                "other_manager": get_text(elem, "otherManager"),
                "voting_sole": voting_sole,
                "voting_shared": voting_shared,
                "voting_none": voting_none,
            }
            
            holdings.append(holding)
            elem.clear()
        
        return holdings
        
    except Exception as e:
        return []


def fix_manager_period(cik, period_end, cur, conn):
    """Fix a single manager-period"""
    cik_padded = str(cik).zfill(10)
    
    # Convert period_end to proper format
    if '/' in str(period_end):
        period_date = datetime.strptime(str(period_end), "%m/%d/%Y").date()
    else:
        period_date = datetime.strptime(str(period_end)[:10], "%Y-%m-%d").date()
    
    # Find the filing
    filing = find_13f_filing(cik_padded, period_end)
    if not filing:
        return {"status": "no_filing"}
    
    acc_no = filing["accession_no"]
    
    xml_urls = get_xml_urls_from_directory(cik_padded, acc_no)
    if not xml_urls:
        return {"status": "no_xml"}
    
    info_table_url = find_info_table_url(xml_urls)
    if not info_table_url:
        return {"status": "no_infotable"}
    
    # Fetch holdings
    holdings = fetch_holdings_fixed(info_table_url)
    
    if not holdings:
        return {"status": "no_holdings"}
    
    # Delete old holdings
    cur.execute("""
        DELETE FROM manager_quarter_holding
        WHERE cik = %s AND period_end = %s
    """, (cik_padded, period_date))
    
    # Insert new holdings
    values = []
    for h in holdings:
        values.append((
            cik_padded, period_date, acc_no, h["line_no"],
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
        ON CONFLICT (cik, period_end, accession_no, line_no) DO UPDATE SET
            value_usd = EXCLUDED.value_usd,
            shares = EXCLUDED.shares
    """, values, page_size=500)
    
    # Update manager_quarter
    total_value_m = sum(h["value_usd"] or 0 for h in holdings) / 1_000_000.0
    cur.execute("""
        UPDATE manager_quarter
        SET total_value_m = %s, num_holdings = %s
        WHERE cik = %s AND period_end = %s
    """, (total_value_m, len(holdings), cik_padded, period_date))
    
    time.sleep(REQUEST_DELAY)
    return {"status": "success", "holdings": len(holdings), "value_m": total_value_m}


def load_fixes_from_csv(csv_file):
    """Load unique (cik, period_end) pairs from CSV"""
    fixes = []
    seen = set()
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cik = row['cik'].strip()
            period_end = row['period_end'].strip()
            key = (cik, period_end)
            
            if key not in seen:
                seen.add(key)
                fixes.append({
                    'cik': cik,
                    'period_end': period_end,
                    'manager_name': row.get('manager_name', 'Unknown'),
                })
    
    return fixes


def main():
    print("=" * 70)
    print("SEC 13F Holdings Mismatch Fixer")
    print("=" * 70)
    
    load_dotenv()
    
    # Load fixes from CSV
    if not os.path.exists(CSV_FILE):
        print(f"❌ CSV file not found: {CSV_FILE}")
        return
    
    fixes = load_fixes_from_csv(CSV_FILE)
    print(f"\n📋 Loaded {len(fixes)} unique (cik, period_end) pairs to fix")
    
    # Time estimate
    est_seconds = len(fixes) * 2.5
    est_time = timedelta(seconds=int(est_seconds))
    print(f"⏱️  Estimated time: {est_time}")
    
    if DRY_RUN:
        print("\n🔍 DRY RUN - showing first 20:")
        for fix in fixes[:20]:
            print(f"   {fix['cik']} {fix['period_end']} - {fix['manager_name'][:40]}")
        if len(fixes) > 20:
            print(f"   ... and {len(fixes) - 20} more")
        return
    
    # Connect to database
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()
    
    # Pre-download quarterly indexes
    print("\n📥 Pre-downloading quarterly indexes...")
    quarters_needed = set()
    for fix in fixes:
        year, quarter, _ = period_to_quarter(fix['period_end'])
        filing_quarter = quarter + 1
        filing_year = year
        if filing_quarter > 4:
            filing_quarter = 1
            filing_year += 1
        quarters_needed.add((filing_year, filing_quarter))
    
    for year, quarter in sorted(quarters_needed):
        download_quarterly_index(year, quarter)
    print("✓ All indexes cached\n")
    
    # Process fixes
    start_time = datetime.now()
    success = 0
    failed = 0
    no_filing = 0
    
    for batch_start in range(0, len(fixes), BATCH_SIZE):
        batch = fixes[batch_start:batch_start + BATCH_SIZE]
        
        print(f"\n{'='*60}")
        print(f"Batch {batch_start // BATCH_SIZE + 1}/{(len(fixes) + BATCH_SIZE - 1) // BATCH_SIZE}")
        print(f"{'='*60}")
        
        for idx, fix in enumerate(batch):
            progress = batch_start + idx + 1
            print(f"  [{progress}/{len(fixes)}] {fix['cik']} {fix['period_end']}...", end=" ", flush=True)
            
            try:
                result = fix_manager_period(fix['cik'], fix['period_end'], cur, conn)
                
                if result["status"] == "success":
                    success += 1
                    print(f"✓ {result['holdings']} holdings, ${result['value_m']:.1f}M", flush=True)
                elif result["status"] == "no_filing":
                    no_filing += 1
                    print(f"⚠️ No filing", flush=True)
                else:
                    failed += 1
                    print(f"❌ {result['status']}", flush=True)
                    
            except Exception as e:
                failed += 1
                print(f"❌ Error: {str(e)[:50]}", flush=True)
                try:
                    conn.rollback()
                except:
                    pass
        
        # Commit after each batch
        try:
            conn.commit()
        except Exception as e:
            print(f"  ⚠️ Commit error: {e}", flush=True)
            conn.rollback()
        
        # Progress update
        done = batch_start + len(batch)
        elapsed = datetime.now() - start_time
        rate = done / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        remaining = len(fixes) - done
        eta = timedelta(seconds=int(remaining / rate)) if rate > 0 else timedelta(0)
        
        print(f"\n📊 Progress: {done}/{len(fixes)} ({100*done/len(fixes):.1f}%)")
        print(f"⏱️  Elapsed: {str(elapsed).split('.')[0]} | ETA: {eta}")
        print(f"✓ {success} success | ⚠️ {no_filing} no filing | ❌ {failed} failed")
    
    # Final summary
    total_time = datetime.now() - start_time
    print("\n" + "=" * 70)
    print("✅ COMPLETED")
    print(f"   Total time: {str(total_time).split('.')[0]}")
    print(f"   Success: {success} | No filing: {no_filing} | Failed: {failed}")
    print("=" * 70)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()