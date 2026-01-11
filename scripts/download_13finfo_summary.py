import time
import re
import random
import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
load_dotenv()

BASE = "https://13f.info"
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0"]

# Be polite to the site (avoid hammering)
MIN_SLEEP = 0.0
MAX_SLEEP = 0.1
MAX_WORKERS = 8   # start with 8; if stable, try 12

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) 13f-scraper/1.0",
    "Accept-Language": "en-US,en;q=0.9",
}

session = requests.Session()
session.headers.update(HEADERS)

DB = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "stock_analyzer"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
}

OUT_CSV = "13finfo_summary_checkpoint.csv"

def sleep_a_bit():
    time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))

def get_soup(url: str) -> BeautifulSoup:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def parse_int(s: str):
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return int(s)
    except:
        return None

def collect_manager_urls():
    manager_urls = set()
    for ch in LETTERS:
        url = f"{BASE}/managers/{ch}"
        print(f"Listing: {url}")
        soup = get_soup(url)

        # Manager links look like: /manager/0001540358-a16z-capital-management-l-l-c
        for a in soup.select('a[href^="/manager/"]'):
            href = a.get("href")
            if not href:
                continue
            full = urljoin(BASE, href)
            manager_urls.add(full)

        sleep_a_bit()

    return sorted(manager_urls)

def scrape_manager_page(url: str):
    soup = get_soup(url)

    # CIK is printed on the page, but easiest is from the URL:
    # https://13f.info/manager/0001540358-a16z-capital...
    m = re.search(r"/manager/(\d{10})-", url)
    cik = m.group(1) if m else None

    rows = []
    table = soup.find("table")
    if not table:
        return rows

    # The first table rows are the summary rows we need:
    # Quarter | Holdings | Value ($000) | ...
    for tr in table.select("tr"):
        tds = tr.find_all(["td", "th"])
        if len(tds) < 3:
            continue

        quarter = tds[0].get_text(strip=True)
        holdings = parse_int(tds[1].get_text(strip=True))
        value_thousands = parse_int(tds[2].get_text(strip=True))

        top_holdings = tds[3].get_text(strip=True)
        form_type = tds[4].get_text(strip=True)
        date_filed = tds[5].get_text(strip=True)
        filing_id = tds[6].get_text(strip=True)

        # Skip header row and junk
        if quarter.lower() == "quarter":
            continue
        if not quarter or holdings is None or value_thousands is None:
            continue

        value_usd = value_thousands * 1000  # 13f.info shows Value ($000)

        rows.append({
            "manager_url": url,   # ← REQUIRED for resume
            "cik": cik,
            "quarter": quarter,
            "holdings": holdings,
            "value_usd": value_usd,
            "top_holdings": top_holdings,
            "form_type": form_type,
            "date_filed": date_filed,
            "filing_id": filing_id,
        })

    return rows

def overwrite_expected_table(df: pd.DataFrame):
    # Ensure correct dtypes for DB
    df = df.copy()
    df["cik"] = df["cik"].astype(str).str.strip()
    df["quarter"] = df["quarter"].astype(str).str.strip()
    df["holdings"] = pd.to_numeric(df["holdings"], errors="coerce").astype("Int64")
    df["value_usd"] = pd.to_numeric(df["value_usd"], errors="coerce")
    df["top_holdings"] = df["top_holdings"].astype(str).fillna("")
    df["form_type"] = df["form_type"].astype(str).fillna("")
    df["filing_id"] = df["filing_id"].astype(str).fillna("")

    # date_filed -> python date (not timestamp)
    df["date_filed"] = pd.to_datetime(df["date_filed"], errors="coerce").dt.date

    rows = [
        (
            r.cik,
            r.quarter,
            None if pd.isna(r.holdings) else int(r.holdings),
            None if pd.isna(r.value_usd) else float(r.value_usd),
            None if r.top_holdings == "nan" else r.top_holdings,
            None if r.form_type == "nan" else r.form_type,
            r.date_filed,  # already date or None
            None if r.filing_id == "nan" else r.filing_id,
        )
        for r in df.itertuples(index=False)
    ]

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Overwrite (fast + simple)
            cur.execute("TRUNCATE TABLE public.expected_13finfo_summary;")

            insert_sql = """
                INSERT INTO public.expected_13finfo_summary
                (cik, quarter, holdings, value_usd, top_holdings, form_type, date_filed, filing_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            # Insert in chunks to avoid huge single execute
            chunk_size = 5000
            for i in range(0, len(rows), chunk_size):
                cur.executemany(insert_sql, rows[i:i+chunk_size])

        conn.commit()
        print(f"DB updated: public.expected_13finfo_summary ({len(rows)} rows)")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def main():
    # If checkpoint exists, load it and skip scraping
    existing_df = None
    completed_urls = set()

    manager_urls = collect_manager_urls()

    existing_df = None
    completed_urls = set()

    if os.path.exists(OUT_CSV):
        existing_df = pd.read_csv(OUT_CSV)
        if "manager_url" in existing_df.columns:
            completed_urls = set(existing_df["manager_url"].dropna().unique())
        print(f"🔁 Resume mode: {len(completed_urls)} managers already scraped")

    # Always filter remaining managers (whether CSV exists or not)
    if completed_urls:
        manager_urls = [u for u in manager_urls if u not in completed_urls]

    print(f"▶️ Remaining managers to scrape: {len(manager_urls)}")

    out = []
    total = len(manager_urls)

    def worker(url):
        time.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))
        return url, scrape_manager_page(url)

    done = 0
    if total > 0:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(worker, url) for url in manager_urls]

            for fut in as_completed(futures):
                done += 1
                try:
                    url, rows = fut.result()
                    out.extend(rows)
                    print(f"[{done}/{total}] OK {url} -> {len(rows)} rows")
                except Exception as e:
                    print(f"[{done}/{total}] FAIL -> {e}")

                # checkpoint every 500 managers
                if done % 500 == 0 or done == total:
                    new_df = pd.DataFrame(out)
                    if existing_df is not None and not existing_df.empty:
                        combined = pd.concat([existing_df, new_df], ignore_index=True)
                    else:
                        combined = new_df

                    combined.to_csv(OUT_CSV, index=False)
                    print(f"💾 Checkpoint saved: {OUT_CSV} ({len(combined)} rows)")
    else:
        print("✅ No remaining managers to scrape.")

    # Set df for the rest of the pipeline
    if os.path.exists(OUT_CSV):
        df = pd.read_csv(OUT_CSV)
    else:
        df = pd.DataFrame(out)
    
    df["date_filed"] = pd.to_datetime(df["date_filed"], errors="coerce")

    # Optional: sort for convenience
    # cik small->large, quarter newer->older (approx sort by year then quarter number)
    def quarter_sort_key(q):
        # q like "Q3 2025"
        m = re.match(r"Q([1-4])\s+(\d{4})", str(q))
        if not m:
            return (0, 0)
        qn = int(m.group(1))
        yr = int(m.group(2))
        return (yr, qn)

    if not df.empty:
        df["_qkey"] = df["quarter"].apply(quarter_sort_key)
        df = df.sort_values(["cik", "_qkey"], ascending=[True, False]).drop(columns=["_qkey"])

    # Write directly to Postgres (overwrite table)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    # FINAL STEP: always load full checkpoint CSV into DB
    df = pd.read_csv(OUT_CSV)
    print(f"📥 Loading {len(df)} rows from checkpoint into DB")

    overwrite_expected_table(df)

if __name__ == "__main__":
    main()