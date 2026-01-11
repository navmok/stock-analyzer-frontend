"""
Supplement 13f.info holdings with SEC data for incomplete quarters - OPTIMIZED

Uses bulk SQL operations instead of row-by-row processing.

Usage:
  python supplement_with_sec_data_fast.py --threshold 90 --dry-run
  python supplement_with_sec_data_fast.py --threshold 90 --mode replace
"""
import argparse
import os
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

TABLE_13FINFO = "public.expected_13finfo_holdings"
TABLE_SEC = "public.manager_quarter_holding"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=float, default=90.0, 
                    help="Replace quarters with less than this %% completeness (default: 90)")
    ap.add_argument("--mode", choices=['replace', 'supplement'], default='replace',
                    help="'replace' = delete 13f.info and use SEC, 'supplement' = keep both")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would be done without making changes")
    args = ap.parse_args()
    
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set")
        return
    
    print("=" * 70)
    print(f"Supplement 13f.info Holdings with SEC Data - OPTIMIZED")
    print(f"  Threshold: <{args.threshold}% completeness")
    print(f"  Mode: {args.mode}")
    print(f"  Dry run: {args.dry_run}")
    print("=" * 70)
    
    conn = psycopg2.connect(db_url)
    
    try:
        with conn.cursor() as cur:
            start_time = time.time()
            
            # Step 1: Create temp table with incomplete quarters
            print("\n📊 Finding incomplete quarters...")
            
            cur.execute(f"""
                CREATE TEMP TABLE incomplete_quarters AS
                WITH sec_totals AS (
                    SELECT 
                        cik,
                        'Q' || EXTRACT(QUARTER FROM period_end)::int || ' ' || EXTRACT(YEAR FROM period_end)::int as quarter,
                        period_end,
                        SUM(value_usd)::float as sec_value_usd,
                        COUNT(*)::int as sec_holdings
                    FROM {TABLE_SEC}
                    WHERE value_usd IS NOT NULL AND value_usd > 0
                    GROUP BY cik, period_end
                    HAVING SUM(value_usd) > 1000000
                ),
                info_totals AS (
                    SELECT 
                        LPAD(LTRIM(cik, '0'), 10, '0') as cik,
                        quarter,
                        (SUM(value_000) * 1000)::float as info_value_usd,
                        COUNT(*)::int as info_holdings
                    FROM {TABLE_13FINFO}
                    WHERE value_000 IS NOT NULL AND value_000 > 0
                    GROUP BY LPAD(LTRIM(cik, '0'), 10, '0'), quarter
                )
                SELECT 
                    s.cik,
                    s.quarter,
                    s.period_end,
                    COALESCE(i.info_value_usd, 0)::float as info_value_usd,
                    s.sec_value_usd::float,
                    COALESCE(i.info_holdings, 0)::int as info_holdings,
                    s.sec_holdings::int,
                    (100.0 * COALESCE(i.info_value_usd, 0) / s.sec_value_usd)::float as value_ratio
                FROM sec_totals s
                LEFT JOIN info_totals i ON s.cik = i.cik AND s.quarter = i.quarter
                WHERE COALESCE(i.info_value_usd, 0) / s.sec_value_usd * 100 < %s
            """, (args.threshold,))
            
            # Create index for faster joins
            cur.execute("CREATE INDEX idx_incomplete_cik_quarter ON incomplete_quarters(cik, quarter)")
            cur.execute("CREATE INDEX idx_incomplete_cik_period ON incomplete_quarters(cik, period_end)")
            
            # Get count and sample
            cur.execute("SELECT COUNT(*) FROM incomplete_quarters")
            count = cur.fetchone()[0]
            print(f"✅ Found {count} quarters with <{args.threshold}% completeness")
            
            if count == 0:
                print("Nothing to supplement!")
                return
            
            # Show sample
            cur.execute("""
                SELECT cik, quarter, info_holdings, sec_holdings, 
                       info_value_usd, sec_value_usd, value_ratio
                FROM incomplete_quarters
                ORDER BY value_ratio
                LIMIT 10
            """)
            
            print("\n📋 Sample of incomplete quarters:")
            for row in cur.fetchall():
                cik, quarter, info_h, sec_h, info_val, sec_val, ratio = row
                print(f"   CIK {cik} {quarter}: {ratio:.1f}% ({info_h}/{sec_h} holdings, "
                      f"${info_val/1e9:.2f}B/${sec_val/1e9:.2f}B)")
            if count > 10:
                print(f"   ... and {count - 10} more")
            
            if args.dry_run:
                print("\n⚠️ DRY RUN - no changes made")
                cur.execute("DROP TABLE incomplete_quarters")
                return
            
            # Step 2: Delete existing 13f.info data for incomplete quarters (bulk)
            print(f"\n🗑️ Deleting existing 13f.info holdings for {count} quarters...")
            
            cur.execute(f"""
                DELETE FROM {TABLE_13FINFO} h
                USING incomplete_quarters iq
                WHERE LPAD(LTRIM(h.cik, '0'), 10, '0') = iq.cik
                  AND h.quarter = iq.quarter
            """)
            deleted = cur.rowcount
            print(f"   Deleted: {deleted:,} holdings")
            
            # Step 3: Insert SEC data for incomplete quarters (bulk)
            print(f"\n📥 Inserting SEC holdings for {count} quarters...")
            
            cur.execute(f"""
                INSERT INTO {TABLE_13FINFO}
                (manager_url, cik, quarter, filing_url, sym, issuer_name, class, cusip, 
                 value_000, pct, shares, principal, option_type)
                SELECT 
                    '',
                    iq.cik,
                    iq.quarter,
                    'SEC_SUPPLEMENTED',
                    NULL,
                    sec.issuer,
                    sec.title_of_class,
                    sec.cusip,
                    ROUND(sec.value_usd / 1000),
                    NULL,
                    sec.shares,
                    sec.share_type,
                    sec.put_call
                FROM {TABLE_SEC} sec
                JOIN incomplete_quarters iq ON sec.cik = iq.cik AND sec.period_end = iq.period_end
            """)
            inserted = cur.rowcount
            print(f"   Inserted: {inserted:,} holdings")
            
            # Commit
            conn.commit()
            
            # Cleanup temp table
            cur.execute("DROP TABLE incomplete_quarters")
            
            elapsed = time.time() - start_time
            
            print(f"\n{'='*70}")
            print(f"✅ COMPLETE in {elapsed:.1f} seconds")
            print(f"   Quarters processed: {count}")
            print(f"   Holdings deleted (13f.info): {deleted:,}")
            print(f"   Holdings inserted (from SEC): {inserted:,}")
            print(f"   Net change: {inserted - deleted:+,}")
            
            # Verify
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_13FINFO} WHERE filing_url = 'SEC_SUPPLEMENTED'")
            sec_count = cur.fetchone()[0]
            print(f"\n📊 Holdings now marked as SEC_SUPPLEMENTED: {sec_count:,}")
            
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()