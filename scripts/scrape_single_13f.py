#!/usr/bin/env python3
"""
Scrape SEC 13F holdings for CIK/period_end pairs from a CSV file.
Updates public.manager_quarter_holding table.

Optimized for speed with:
- Parallel HTTP requests via ThreadPoolExecutor
- Pre-compiled regex patterns
- Batched database commits
- Connection pooling
- Efficient XML streaming

Usage:
    python scrape_single_13f.py --csv input.csv
    python scrape_single_13f.py --csv input.csv --workers 12
"""
import argparse
import csv
import os
import io
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Lock
from lxml import etree
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Configuration
SEC_HEADERS = {
    "User-Agent": "Naveen Mokkapati navmok@gmail.com", 
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov"
}
REQUEST_TIMEOUT = 60
MAX_WORKERS = 10  # Can run higher worker count, rate limiter controls throughput
BATCH_COMMIT_SIZE = 50
SEC_RATE_LIMIT_INTERVAL = 0.11  # ~9 requests/sec (safety buffer for 10/sec limit)

# Pre-compiled regex patterns for speed
RE_XML_HREF = re.compile(r'href="([^"]+\.xml)"', re.IGNORECASE)
RE_PRIMARY_DOC = re.compile(r'primary.*doc', re.IGNORECASE)
RE_INFO_TABLE = re.compile(r'info.*table', re.IGNORECASE)

# Thread-safe index cache
QUARTERLY_INDEXES = {}
INDEX_LOCK = Lock()

# Global Rate Limiter Lock & State
RATE_LIMIT_LOCK = Lock()
LAST_REQUEST_TIME = 0.0

class RateLimitedSession(requests.Session):
    """Session that strictly adheres to SEC rate limits (10 req/sec)."""
    def request(self, method, url, *args, **kwargs):
        global LAST_REQUEST_TIME
        with RATE_LIMIT_LOCK:
            now = time.time()
            elapsed = now - LAST_REQUEST_TIME
            if elapsed < SEC_RATE_LIMIT_INTERVAL:
                time.sleep(SEC_RATE_LIMIT_INTERVAL - elapsed)
            LAST_REQUEST_TIME = time.time()
        return super().request(method, url, *args, **kwargs)

def create_session():
    """Create session with connection pooling, retry logic, and rate limiting."""
    session = RateLimitedSession()
    session.headers.update(SEC_HEADERS)
    # Increased retries and backoff to survive rate limit hiccups
    retry = Retry(
        total=8, 
        backoff_factor=1.0, # 1s, 2s, 4s, 8s...
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = create_session()


def download_quarterly_index(year, quarter):
    """Download and parse master.idx for a quarter (thread-safe with caching)."""
    key = (year, quarter)
    
    with INDEX_LOCK:
        if key in QUARTERLY_INDEXES:
            return QUARTERLY_INDEXES[key]
    
    url = f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
    
    try:
        r = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        
        by_cik = {}
        lines = r.text.split('\n')
        header_idx = next((i for i, l in enumerate(lines) if l.startswith("CIK|")), -1)
        
        if header_idx >= 0:
            for line in lines[header_idx + 1:]:
                if '|' not in line:
                    continue
                parts = line.split("|")
                if len(parts) < 5 or not parts[2].startswith("13F"):
                    continue
                
                cik, _, form_type, date_filed, filename = parts[:5]
                acc_no = filename.split("/")[-1].replace(".txt", "").replace("-", "")
                
                try:
                    filing_date = datetime.strptime(date_filed, "%Y-%m-%d").date()
                except ValueError:
                    continue
                
                cik_padded = cik.zfill(10)
                if cik_padded not in by_cik:
                    by_cik[cik_padded] = []
                by_cik[cik_padded].append({
                    "form_type": form_type,
                    "filing_date": filing_date,
                    "accession_no": acc_no,
                })
        
        with INDEX_LOCK:
            QUARTERLY_INDEXES[key] = by_cik
        print(f"✓ Indexed {year} Q{quarter}: {len(by_cik)} managers")
        return by_cik
    except Exception as e:
        print(f"❌ Failed index {year} Q{quarter}: {e}")
        with INDEX_LOCK:
            QUARTERLY_INDEXES[key] = {}
        return {}


def period_to_quarter(period_end):
    """Convert period_end to (year, quarter, date)."""
    if isinstance(period_end, str):
        dt = datetime.strptime(period_end[:10], "%Y-%m-%d").date()
    elif isinstance(period_end, datetime):
        dt = period_end.date()
    else:
        dt = period_end
    return dt.year, (dt.month - 1) // 3 + 1, dt


def find_13f_filing(cik, period_end):
    """Find the 13F filing. Prefers original over amendments."""
    cik_padded = str(cik).zfill(10)
    year, quarter, target_date = period_to_quarter(period_end)
    
    filing_year, filing_quarter = (year, quarter + 1) if quarter < 4 else (year + 1, 1)
    
    start_date = target_date + timedelta(days=1)
    end_date = target_date + timedelta(days=150)
    
    # Check primary quarter
    index = download_quarterly_index(filing_year, filing_quarter)
    candidates = [f for f in index.get(cik_padded, []) if start_date <= f["filing_date"] <= end_date]
    
    # Check next quarter if needed
    if not candidates:
        next_y, next_q = (filing_year, filing_quarter + 1) if filing_quarter < 4 else (filing_year + 1, 1)
        index2 = download_quarterly_index(next_y, next_q)
        candidates = [f for f in index2.get(cik_padded, []) if start_date <= f["filing_date"] <= end_date]
    
    if not candidates:
        return None
    
    # Prefer original (no /A) over amendments, then earliest date
    candidates.sort(key=lambda x: (x["form_type"].endswith("/A"), x["filing_date"]))
    return candidates[0]


def get_filing_xml_urls(cik, accession_no):
    """Get XML URLs and identify primary_doc and info_table in one request."""
    cik_int = str(int(cik))
    directory_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_no}"
    
    try:
        r = SESSION.get(directory_url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None, None
        
        xml_urls = []
        for href in RE_XML_HREF.findall(r.text):
            if href.startswith('/'):
                xml_urls.append(f"https://www.sec.gov{href}")
            elif href.startswith('http'):
                xml_urls.append(href)
            else:
                xml_urls.append(f"{directory_url}/{href}")
        
        # Quick pattern matching first
        primary_url = next((u for u in xml_urls if RE_PRIMARY_DOC.search(u)), None)
        info_url = next((u for u in xml_urls if RE_INFO_TABLE.search(u)), None)
        
        # Fallback: check content if patterns didn't match
        if not primary_url or not info_url:
            for url in xml_urls:
                if primary_url and info_url:
                    break
                try:
                    r = SESSION.get(url, timeout=30)
                    if r.status_code != 200:
                        continue
                    content = r.content[:8192]  # Check first 8KB to handle large headers
                    if not primary_url and (b'edgarSubmission' in content or b'coverPage' in content):
                        primary_url = url
                    elif not info_url and (b'informationTable' in content or b'infoTable' in content):
                        info_url = url
                except:
                    continue
        
        return primary_url, info_url
    except:
        return None, None


def fetch_value_total(xml_url):
    """Fetch total value from primary_doc.xml (in thousands). Fast string search."""
    try:
        r = SESSION.get(xml_url, timeout=30)
        if r.status_code != 200:
            return None
        
        content = r.content
        for tag in (b'tableValueTotal', b'valueTotal', b'securitiesOwnedAggregateValue'):
            idx = content.find(tag)
            if idx != -1:
                start = content.find(b'>', idx) + 1
                end = content.find(b'<', start)
                if start > 0 and end > start:
                    try:
                        return int(content[start:end].strip())
                    except:
                        pass
        return None
    except:
        return None


def fetch_holdings_raw(xml_url):
    """Fetch holdings from info_table.xml. Returns RAW values (not multiplied)."""
    try:
        r = SESSION.get(xml_url, timeout=120)
        if r.status_code != 200:
            return []
        
        content_length = r.headers.get('content-length')
        if content_length and int(content_length) > 25_000_000:
            return []  # Skip files > 25MB
        
        holdings = []
        idx = 0
        
        # Use simple byte strings for tag matching to ignore namespaces 
        # (e.g. {http://www.sec.gov/edgar/document/thirteenf/informationtable}infoTable)
        
        try:
            # Parse XML and handle namespaces by stripping them via localname()
            # However, etree.iterparse with events='end' and localname checks is fast.
            for _, elem in etree.iterparse(io.BytesIO(r.content), events=("end",), recover=True):
                if not isinstance(elem.tag, str):
                    continue
                
                # Check tag name ignoring namespace
                tag = elem.tag.rsplit('}', 1)[-1].lower()
                
                if tag != "infotable":
                    continue
                
                idx += 1
                
                def get_val(element, target_tag):
                    # Search specifically among direct children
                    target_tag = target_tag.lower()
                    for child in element:
                        if not isinstance(child.tag, str): 
                            continue
                        ctag = child.tag.rsplit('}', 1)[-1].lower()
                        if ctag == target_tag:
                            if child.text: 
                                return child.text.strip()
                            # Sometimes values are in nested tags (e.g. sshPrnamt -> sshPrnamt)
                            # But usually text is direct.
                    # Recurse if not found (for deep structures like votingAuthority)
                    for child in element.iter():
                         if not isinstance(child.tag, str): continue
                         ctag = child.tag.rsplit('}', 1)[-1].lower()
                         if ctag == target_tag and child.text:
                             return child.text.strip()
                    return None
                 
                def get_int(element, name):
                    v = get_val(element, name)
                    if v:
                        try: return int(v.replace(',', '').replace(' ', ''))
                        except: pass
                    return None
                
                def get_float(element, name):
                    v = get_val(element, name)
                    if v:
                        try: return float(v.replace(',', '').replace(' ', ''))
                        except: pass
                    return None

                # Capture Voting Authority (nested)
                voting_sole = voting_shared = voting_none = None
                
                # Find votingAuthority specifically
                voting_node = None
                for child in elem:
                     if child.tag.rsplit('}', 1)[-1].lower() == 'votingauthority':
                         voting_node = child
                         break
                
                if voting_node is not None:
                    # Look inside votingAuthority
                    for vchild in voting_node:
                        if not isinstance(vchild.tag, str):
                            continue
                        vtag = vchild.tag.rsplit('}', 1)[-1].lower()
                        if vtag == 'sole': 
                             try: voting_sole = int(vchild.text.replace(',',''))
                             except: pass
                        elif vtag == 'shared':
                             try: voting_shared = int(vchild.text.replace(',',''))
                             except: pass
                        elif vtag == 'none':
                             try: voting_none = int(vchild.text.replace(',',''))
                             except: pass

                holdings.append((
                    idx,
                    get_val(elem, "nameofissuer"),
                    get_val(elem, "titleofclass"),
                    get_val(elem, "cusip"),
                    get_int(elem, "value"),
                    get_float(elem, "sshprnamt"),
                    get_val(elem, "sshprnamttype"),
                    get_val(elem, "putcall"),
                    get_val(elem, "investmentdiscretion"),
                    get_val(elem, "othermanager"),
                    voting_sole,
                    voting_shared,
                    voting_none,
                ))
                
                elem.clear() # Free memory
                
        except Exception as e:
            # Fallback for malformed XML or other parsing errors
            if not holdings:
                print(f"XML Parse Error on {xml_url}: {e}")
            pass

        return holdings
    except:
        return []


def determine_value_multiplier(holdings_raw, total_value_thousands):
    """
    Determine if holdings values are in thousands or actual dollars.
    
    Strategies:
    1. AUM Cap Heuristic:
       - If 'total_value_thousands' > 25,000,000 (Implies $25 Trillion AUM), 
         the header is definitely in Dollars, not Thousands. 
         (Largest asset managers ~10T).
       - In this case, if Raw Sum matches Header, Raw is also Dollars -> Multiplier 1.
       
    2. Share Ratio Heuristic (Avg Price):
       - If RawValue/Shares > 1.5, likely Dollars ($1.50/share is very low for Thousands representation).
       - Normal stock ~ $50. In Thousands that's 0.05.
       
    3. Consistency check.
    """
    if not holdings_raw:
        return 1000
    
    # Sum raw values
    total_raw_val = sum(h[4] for h in holdings_raw if h[4] is not None)
    
    # 1. AUM Cap Heuristic (Catch filers putting Dollars in Header)
    # 25,000,000 in 'Thousands' = 25 Trillion. safely above BlackRock.
    if total_value_thousands and total_value_thousands > 25_000_000:
        # Header is definitely Dollars.
        # Check consistency with Raw
        if total_raw_val > 0:
            ratio = total_raw_val / total_value_thousands
            if 0.8 < ratio < 1.2:
                print(f"   ℹ️  Detected DOLLAR format via AUM Cap ({total_value_thousands})")
                return 1

    # 2. Share Ratio Heuristic
    total_shares = sum(h[5] for h in holdings_raw if h[5] is not None and h[5] > 0)
    
    if total_shares > 0:
        avg_raw_ratio = total_raw_val / total_shares
        
        # Lowered threshold to 2.0 (Implies avg share price $2000 if in thousands)
        # Most portfolios average $50-$200.
        if avg_raw_ratio > 2.0:
            print(f"   ℹ️  Detected DOLLAR format via Implied Price ({avg_raw_ratio:.2f})")
            return 1

    # 3. Consistency Check (Fallback)
    if not total_value_thousands or total_raw_val == 0:
        return 1000
    
    total_value_dollars = total_value_thousands * 1000
    
    ratio_if_thousands = total_raw_val / total_value_thousands
    ratio_if_dollars = total_raw_val / total_value_dollars
    
    diff_thousands = abs(ratio_if_thousands - 1.0)
    diff_dollars = abs(ratio_if_dollars - 1.0)
    
    if diff_dollars < diff_thousands and diff_dollars < 0.2:
        return 1
    else:
        return 1000


def apply_value_multiplier(holdings_raw, multiplier):
    """Apply the determined multiplier to convert raw values to USD."""
    return [
        (h[0], h[1], h[2], h[3], 
         h[4] * multiplier if h[4] is not None else None,  # value_usd
         h[5], h[6], h[7], h[8], h[9], h[10], h[11], h[12])
        for h in holdings_raw
    ]


def process_filing(task):
    """Process a single filing. Returns (cik, period_end, status, holdings_data, total_value, acc_no, multiplier)."""
    cik, period_end = task
    
    try:
        filing = find_13f_filing(cik, period_end)
        if not filing:
            return (cik, period_end, "no_filing", None, None, None, None)
        
        acc_no = filing["accession_no"]
        # time.sleep(REQUEST_DELAY_SEC)  <-- Handled by RateLimitSession
        
        primary_url, info_url = get_filing_xml_urls(cik, acc_no)
        
        # Fetch total value first (needed to determine multiplier)
        total_value = None
        if primary_url:
            total_value = fetch_value_total(primary_url)
        
        # Fetch raw holdings
        holdings_raw = []
        if info_url:
            # time.sleep(REQUEST_DELAY_SEC) <-- Handled by RateLimitSession
            holdings_raw = fetch_holdings_raw(info_url)
        
        if not holdings_raw:
            return (cik, period_end, "no_holdings", None, total_value, acc_no, None)
        
        # Determine if values are in thousands or dollars
        multiplier = determine_value_multiplier(holdings_raw, total_value)
        
        # Apply multiplier to get final USD values
        holdings = apply_value_multiplier(holdings_raw, multiplier)
        
        return (cik, period_end, "success", holdings, total_value, acc_no, multiplier)
    
    except Exception as e:
        return (cik, period_end, "error", None, None, None, None)


def main():
    parser = argparse.ArgumentParser(description="Scrape SEC 13F data from CSV (optimized)")
    parser.add_argument("--csv", required=True, help="CSV file with cik,period_end columns")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Parallel workers (default: 8)")
    args = parser.parse_args()
    
    load_dotenv()
    
    if "DATABASE_URL" not in os.environ:
        print("❌ DATABASE_URL not set")
        return 1
    
    # Read CSV
    tasks = []
    with open(args.csv, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cik = row.get('cik', '').strip().zfill(10)
            period_end = row.get('period_end', '').strip()
            if cik and period_end:
                tasks.append((cik, period_end))
    
    if not tasks:
        print("❌ No valid rows in CSV")
        return 1
    
    print("=" * 60)
    print("SEC 13F Optimized Batch Scraper")
    print(f"Tasks: {len(tasks)} | Workers: {args.workers}")
    print("=" * 60)
    
    # Pre-download indexes in parallel
    quarters_needed = set()
    for cik, period_end in tasks:
        year, quarter, _ = period_to_quarter(period_end)
        fy, fq = (year, quarter + 1) if quarter < 4 else (year + 1, 1)
        quarters_needed.add((fy, fq))
        ny, nq = (fy, fq + 1) if fq < 4 else (fy + 1, 1)
        quarters_needed.add((ny, nq))
    
    print(f"\n📥 Pre-downloading {len(quarters_needed)} indexes...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda q: download_quarterly_index(q[0], q[1]), sorted(quarters_needed)))
    
    # Database connection
    conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=30)
    conn.autocommit = False
    cur = conn.cursor()
    
    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS manager_quarter_holding (
            cik TEXT NOT NULL, period_end DATE NOT NULL, accession_no TEXT NOT NULL,
            line_no INTEGER NOT NULL, issuer TEXT, title_of_class TEXT, cusip TEXT,
            value_usd BIGINT, shares NUMERIC, share_type TEXT, put_call TEXT,
            investment_discretion TEXT, other_manager TEXT, voting_sole BIGINT,
            voting_shared BIGINT, voting_none BIGINT, created_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (cik, period_end, accession_no, line_no)
        )
    """)
    conn.commit()
    
    # Process in parallel
    start_time = datetime.now()
    success = no_filing = errors = 0
    pending_inserts = []
    
    print(f"\n🚀 Processing {len(tasks)} filings with {args.workers} workers...\n")
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_filing, t): t for t in tasks}
        
        for i, future in enumerate(as_completed(futures), 1):
            cik, period_end, status, holdings, total_value, acc_no, multiplier = future.result()
            
            if status == "success":
                success += 1
                pending_inserts.append((cik, period_end, holdings, total_value, acc_no))
                mult_info = " [x1 dollars]" if multiplier == 1 else ""
                print(f"[{i}/{len(tasks)}] ✓ {cik} {period_end}: {len(holdings)} holdings{mult_info}")
            elif status == "no_filing":
                no_filing += 1
                print(f"[{i}/{len(tasks)}] ⚠️ {cik} {period_end}: no filing")
            else:
                errors += 1
                print(f"[{i}/{len(tasks)}] ❌ {cik} {period_end}: {status}")
            
            # Batch commit to database
            if len(pending_inserts) >= BATCH_COMMIT_SIZE:
                for p_cik, p_period, p_holdings, p_total, p_acc in pending_inserts:
                    if p_total:
                        cur.execute("UPDATE manager_quarter SET total_value_m = %s WHERE cik = %s AND period_end = %s",
                                    (p_total / 1000.0, p_cik, p_period))
                    cur.execute("DELETE FROM manager_quarter_holding WHERE cik = %s AND period_end = %s", (p_cik, p_period))
                    values = [(p_cik, p_period, p_acc, *h) for h in p_holdings]
                    execute_values(cur, """
                        INSERT INTO manager_quarter_holding (cik, period_end, accession_no, line_no, issuer,
                        title_of_class, cusip, value_usd, shares, share_type, put_call, investment_discretion,
                        other_manager, voting_sole, voting_shared, voting_none) VALUES %s
                        ON CONFLICT (cik, period_end, accession_no, line_no) DO NOTHING
                    """, values, page_size=1000)
                    cur.execute("UPDATE manager_quarter SET num_holdings = %s WHERE cik = %s AND period_end = %s",
                                (len(p_holdings), p_cik, p_period))
                conn.commit()
                pending_inserts.clear()
            
            # Progress every 25 items
            if i % 25 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = i / elapsed if elapsed > 0 else 0
                eta = timedelta(seconds=int((len(tasks) - i) / rate)) if rate > 0 else timedelta(0)
                print(f"📊 Progress: {i}/{len(tasks)} | {rate:.1f}/s | ETA: {eta}")
    
    # Final commit for remaining inserts
    if pending_inserts:
        for p_cik, p_period, p_holdings, p_total, p_acc in pending_inserts:
            if p_total:
                cur.execute("UPDATE manager_quarter SET total_value_m = %s WHERE cik = %s AND period_end = %s",
                            (p_total / 1000.0, p_cik, p_period))
            cur.execute("DELETE FROM manager_quarter_holding WHERE cik = %s AND period_end = %s", (p_cik, p_period))
            values = [(p_cik, p_period, p_acc, *h) for h in p_holdings]
            execute_values(cur, """
                INSERT INTO manager_quarter_holding (cik, period_end, accession_no, line_no, issuer,
                title_of_class, cusip, value_usd, shares, share_type, put_call, investment_discretion,
                other_manager, voting_sole, voting_shared, voting_none) VALUES %s
                ON CONFLICT (cik, period_end, accession_no, line_no) DO NOTHING
            """, values, page_size=1000)
            cur.execute("UPDATE manager_quarter SET num_holdings = %s WHERE cik = %s AND period_end = %s",
                        (len(p_holdings), p_cik, p_period))
        conn.commit()
    
    total_time = datetime.now() - start_time
    rate = len(tasks) / total_time.total_seconds() if total_time.total_seconds() > 0 else 0
    
    print("\n" + "=" * 60)
    print("✅ COMPLETED")
    print(f"   Time: {str(total_time).split('.')[0]} ({rate:.2f} filings/sec)")
    print(f"   Success: {success} | No filing: {no_filing} | Errors: {errors}")
    print("=" * 60)
    
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    exit(main())