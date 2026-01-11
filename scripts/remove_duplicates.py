"""
Remove duplicates from public.expected_13finfo_holdings

The issue: CIK format is inconsistent ('0000102909' vs '102909')
This causes duplicate rows for the same holding.

This script:
1. Identifies duplicates
2. Keeps one row per unique (cik_normalized, quarter, cusip, sym, class)
3. Normalizes CIK to 10-digit padded format
"""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

TABLE = "public.expected_13finfo_holdings"


def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set")
        return
    
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    
    try:
        with conn.cursor() as cur:
            # Step 1: Count before
            print("📊 Checking current state...")
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            total_before = cur.fetchone()[0]
            print(f"   Total rows: {total_before:,}")
            
            # Check for duplicates
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT LTRIM(cik, '0') as cik_norm, quarter, cusip, 
                           COALESCE(sym, '') as sym, COALESCE(class, '') as class
                    FROM {TABLE}
                    GROUP BY 1, 2, 3, 4, 5
                    HAVING COUNT(*) > 1
                ) dupes
            """)
            dupe_groups = cur.fetchone()[0]
            print(f"   Duplicate groups: {dupe_groups:,}")
            
            if dupe_groups == 0:
                print("✅ No duplicates found!")
                return
            
            # Step 2: Create temp table with deduplicated data
            print("\n🔄 Deduplicating...")
            cur.execute(f"""
                CREATE TEMP TABLE holdings_deduped AS
                SELECT DISTINCT ON (LTRIM(cik, '0'), quarter, cusip, COALESCE(sym, ''), COALESCE(class, ''))
                    manager_url,
                    LPAD(LTRIM(cik, '0'), 10, '0') as cik,
                    quarter,
                    filing_url,
                    sym,
                    issuer_name,
                    class,
                    cusip,
                    value_000,
                    pct,
                    shares,
                    principal,
                    option_type,
                    scraped_at
                FROM {TABLE}
                ORDER BY LTRIM(cik, '0'), quarter, cusip, COALESCE(sym, ''), COALESCE(class, ''), 
                         scraped_at DESC NULLS LAST
            """)
            
            cur.execute("SELECT COUNT(*) FROM holdings_deduped")
            deduped_count = cur.fetchone()[0]
            print(f"   Unique rows: {deduped_count:,}")
            print(f"   Duplicates to remove: {total_before - deduped_count:,}")
            
            # Step 3: Replace table contents
            print("\n🗑️  Replacing table contents...")
            cur.execute(f"TRUNCATE TABLE {TABLE}")
            
            cur.execute(f"""
                INSERT INTO {TABLE} 
                    (manager_url, cik, quarter, filing_url, sym, issuer_name, class, cusip, 
                     value_000, pct, shares, principal, option_type, scraped_at)
                SELECT * FROM holdings_deduped
            """)
            
            # Step 4: Verify
            cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
            total_after = cur.fetchone()[0]
            
            # Check no remaining duplicates
            cur.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT cik, quarter, cusip, sym, class
                    FROM {TABLE}
                    GROUP BY 1, 2, 3, 4, 5
                    HAVING COUNT(*) > 1
                ) dupes
            """)
            remaining_dupes = cur.fetchone()[0]
            
            # Cleanup temp table
            cur.execute("DROP TABLE holdings_deduped")
            
            # Commit
            conn.commit()
            
            print(f"\n✅ DONE!")
            print(f"   Before: {total_before:,} rows")
            print(f"   After:  {total_after:,} rows")
            print(f"   Removed: {total_before - total_after:,} duplicates")
            print(f"   Remaining duplicates: {remaining_dupes}")
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()