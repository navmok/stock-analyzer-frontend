"""
Fix specific manager's holdings data

This script re-downloads and correctly parses data for managers with
known issues (value mismatch, missing holdings).

The root cause: SEC 13F values are in THOUSANDS, but some XML files
may have different formatting or our parser had issues.
"""
import requests
import psycopg2
from psycopg2.extras import execute_values
import os
import io
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from lxml import etree
import time

SEC_HEADERS = {
    "User-Agent": "Naveen Mokkapati navmok@gmail.com",
    "Accept": "*/*",
}

# ============================================================================
# CONFIGURATION - Add CIKs and periods to fix
# ============================================================================
# Format: (cik, period_end, info_table_url)
# You can find the correct URL from SEC EDGAR

FIXES = [
    # Only the one that failed - let auto-find get the correct URL
    ("0001632097", "2025-09-30", None),
]

REQUEST_TIMEOUT = 120
REQUEST_DELAY = 0.5
# ============================================================================

SESSION = requests.Session()
SESSION.headers.update(SEC_HEADERS)

QUARTERLY_INDEXES = {}


def download_quarterly_index(year, quarter):
    """Download and parse master.idx for a quarter"""
    key = (year, quarter)
    if key in QUARTERLY_INDEXES:
        return QUARTERLY_INDEXES[key]
    
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    print(f"📥 Downloading index {year} Q{quarter}...", flush=True)
    
    try:
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
        
    except Exception as e:
        print(f"❌ Failed: {e}", flush=True)
        return {}


def period_to_quarter(period_end):
    if isinstance(period_end, str):
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
        r = SESSION.get(directory_url, timeout=REQUEST_TIMEOUT)
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
    # Filter out XSLT-transformed versions (they have different structure)
    raw_urls = [u for u in xml_urls if 'xsl' not in u.lower()]
    
    # First try raw URLs with infotable pattern
    for url in raw_urls:
        if re.search(r'info.*table', url, re.IGNORECASE):
            return url
    
    # Then try all URLs with infotable pattern (fallback)
    for url in xml_urls:
        if re.search(r'info.*table', url, re.IGNORECASE) and 'xsl' not in url.lower():
            return url
    
    # Try each raw XML to find one with infoTable content
    for url in raw_urls:
        try:
            r = SESSION.get(url, timeout=60)
            if r.status_code == 200 and (b'informationTable' in r.content or b'infoTable' in r.content):
                return url
        except:
            continue
    return None


def fetch_holdings_fixed(xml_url):
    """
    Fetch holdings with CORRECT value parsing.
    
    SEC 13F values are in THOUSANDS of dollars.
    value="1234" means $1,234,000
    """
    print(f"    Fetching: {xml_url}", flush=True)
    
    try:
        r = SESSION.get(xml_url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            print(f"    ❌ HTTP {r.status_code}", flush=True)
            return []
        
        xml_content = r.content
        print(f"    Downloaded {len(xml_content):,} bytes", flush=True)
        
        holdings = []
        idx = 0
        
        # Parse with iterparse for memory efficiency
        for event, elem in etree.iterparse(io.BytesIO(xml_content), events=("end",), recover=True):
            if not isinstance(elem.tag, str):
                continue
            
            # Get local tag name (remove namespace)
            tag = elem.tag.split('}')[-1].lower() if '}' in elem.tag else elem.tag.lower()
            
            if tag != "infotable":
                continue
            
            idx += 1
            
            def get_text(parent, tag_name):
                """Get text from descendant element"""
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
            
            # Get value in THOUSANDS (as reported by SEC)
            value_thousands = get_int(elem, "value")
            
            # Convert to actual USD: multiply by 1000
            value_usd = value_thousands * 1000 if value_thousands else None
            
            # Debug first few holdings
            if idx <= 3:
                print(f"    DEBUG holding {idx}: value_thousands={value_thousands}, value_usd={value_usd}", flush=True)
            
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
            
            if idx % 500 == 0:
                print(f"    ... {idx} holdings parsed", flush=True)
        
        print(f"    ✓ Parsed {len(holdings)} total holdings", flush=True)
        
        # Calculate total value
        total_value = sum(h["value_usd"] or 0 for h in holdings)
        print(f"    ✓ Total value: ${total_value:,.0f} (${total_value/1e9:.2f}B)", flush=True)
        
        return holdings
        
    except Exception as e:
        print(f"    ❌ Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return []


def fix_manager_period(cik, period_end, info_table_url, cur, conn):
    """Fix a single manager-period"""
    cik_padded = str(cik).zfill(10)
    
    print(f"\n{'='*60}", flush=True)
    print(f"Fixing {cik_padded} {period_end}", flush=True)
    print(f"{'='*60}", flush=True)
    
    # Find the filing if URL not provided
    if not info_table_url:
        filing = find_13f_filing(cik_padded, period_end)
        if not filing:
            print(f"  ❌ No filing found", flush=True)
            return False
        
        acc_no = filing["accession_no"]
        print(f"  Found filing: {acc_no}", flush=True)
        
        xml_urls = get_xml_urls_from_directory(cik_padded, acc_no)
        if not xml_urls:
            print(f"  ❌ No XML files found", flush=True)
            return False
        
        info_table_url = find_info_table_url(xml_urls)
        if not info_table_url:
            print(f"  ❌ No info table found", flush=True)
            return False
    
    # Get accession number from URL
    # URL format: .../edgar/data/CIK/ACCESSION/filename.xml
    url_parts = info_table_url.split('/')
    acc_no = None
    for i, part in enumerate(url_parts):
        if part == 'data' and i + 2 < len(url_parts):
            acc_no = url_parts[i + 2]
            break
    
    if not acc_no:
        acc_no = "unknown"
    
    print(f"  Accession: {acc_no}", flush=True)
    
    # Fetch holdings with correct parsing
    holdings = fetch_holdings_fixed(info_table_url)
    
    if not holdings:
        print(f"  ❌ No holdings parsed", flush=True)
        return False
    
    # Delete old holdings
    cur.execute("""
        DELETE FROM manager_quarter_holding
        WHERE cik = %s AND period_end = %s
    """, (cik_padded, period_end))
    deleted = cur.rowcount
    print(f"  Deleted {deleted} old holdings", flush=True)
    
    # Insert new holdings
    values = []
    for h in holdings:
        values.append((
            cik_padded, period_end, acc_no, h["line_no"],
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
    """, (total_value_m, len(holdings), cik_padded, period_end))
    
    conn.commit()
    
    print(f"  ✓ Inserted {len(holdings)} holdings", flush=True)
    print(f"  ✓ Total value: ${total_value_m:,.1f}M", flush=True)
    
    time.sleep(REQUEST_DELAY)
    return True


def main():
    print("=" * 70)
    print("SEC 13F Data Fixer - Specific Manager Periods")
    print("=" * 70)
    
    load_dotenv()
    
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        connect_timeout=30,
    )
    conn.autocommit = False
    cur = conn.cursor()
    
    success = 0
    failed = 0
    
    for cik, period_end, url in FIXES:
        try:
            if fix_manager_period(cik, period_end, url, cur, conn):
                success += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ❌ Error: {e}", flush=True)
            conn.rollback()
            failed += 1
    
    print("\n" + "=" * 70)
    print(f"✅ COMPLETED: {success} fixed, {failed} failed")
    print("=" * 70)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()