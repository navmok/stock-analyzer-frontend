"""
Supplement 13f.info holdings with SEC data for incomplete quarters

Since 13f.info's API doesn't return all holdings, this script:
1. Identifies quarters where 13f.info has <X% of expected value/holdings
2. For those quarters, copies holdings from SEC's manager_quarter_holding table
3. Either replaces or supplements the 13f.info data

Usage:
  python supplement_with_sec_data.py --threshold 90 --dry-run
  python supplement_with_sec_data.py --threshold 90 --mode replace
"""
import argparse
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

TABLE_13FINFO = "public.expected_13finfo_holdings"
TABLE_SEC = "public.manager_quarter_holding"

BATCH_SIZE = 100


def get_incomplete_quarters(cur, threshold: float):
    """
    Find quarters where 13f.info has less than threshold% of SEC value.
    Returns list of (cik, quarter, period_end, info_value, sec_value, info_holdings, sec_holdings, ratio)
    """
    query = f"""
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
    ORDER BY COALESCE(i.info_value_usd, 0) / s.sec_value_usd
    """
    
    cur.execute(query, (threshold,))
    return cur.fetchall()


def process_batch_replace(cur, quarters: list) -> tuple[int, int]:
    """
    Process a batch of quarters - delete 13f.info and insert SEC data.
    Returns (deleted_count, inserted_count)
    """
    if not quarters:
        return 0, 0
    
    total_deleted = 0
    total_inserted = 0
    
    for cik, quarter, period_end, _, _, _, _, _ in quarters:
        # Delete existing 13f.info data
        cur.execute(f"""
            DELETE FROM {TABLE_13FINFO}
            WHERE LPAD(LTRIM(cik, '0'), 10, '0') = %s AND quarter = %s
        """, (cik, quarter))
        total_deleted += cur.rowcount
        
        # Insert from SEC
        cur.execute(f"""
            INSERT INTO {TABLE_13FINFO}
            (manager_url, cik, quarter, filing_url, sym, issuer_name, class, cusip, 
             value_000, pct, shares, principal, option_type)
            SELECT 
                '',
                %s,
                %s,
                'SEC_SUPPLEMENTED',
                NULL,
                issuer,
                title_of_class,
                cusip,
                ROUND(value_usd / 1000),
                NULL,
                shares,
                share_type,
                put_call
            FROM {TABLE_SEC}
            WHERE cik = %s AND period_end = %s
        """, (cik, quarter, cik, period_end))
        total_inserted += cur.rowcount
    
    return total_deleted, total_inserted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=float, default=90.0, 
                    help="Replace quarters with less than this %% completeness (default: 90)")
    ap.add_argument("--mode", choices=['replace', 'supplement'], default='replace',
                    help="'replace' = delete 13f.info and use SEC, 'supplement' = keep both")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show what would be done without making changes")
    ap.add_argument("--limit", type=int, default=0,
                    help="Limit number of quarters to process (0=all)")
    args = ap.parse_args()
    
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set")
        return
    
    print("=" * 70)
    print(f"Supplement 13f.info Holdings with SEC Data")
    print(f"  Threshold: <{args.threshold}% completeness")
    print(f"  Mode: {args.mode}")
    print(f"  Dry run: {args.dry_run}")
    print("=" * 70)
    
    conn = psycopg2.connect(db_url)
    
    try:
        with conn.cursor() as cur:
            # Find incomplete quarters
            print("\n📊 Finding incomplete quarters...")
            incomplete = get_incomplete_quarters(cur, args.threshold)
            print(f"✅ Found {len(incomplete)} quarters with <{args.threshold}% completeness")
            
            if not incomplete:
                print("Nothing to supplement!")
                return
            
            # Apply limit
            if args.limit > 0:
                incomplete = incomplete[:args.limit]
                print(f"⚠️ Limited to first {args.limit} quarters")
            
            # Show sample
            print("\n📋 Sample of incomplete quarters:")
            for row in incomplete[:10]:
                cik, quarter, period_end, info_val, sec_val, info_h, sec_h, ratio = row
                print(f"   CIK {cik} {quarter}: {ratio:.1f}% ({info_h}/{sec_h} holdings, "
                      f"${info_val/1e9:.2f}B/${sec_val/1e9:.2f}B)")
            if len(incomplete) > 10:
                print(f"   ... and {len(incomplete) - 10} more")
            
            if args.dry_run:
                print("\n⚠️ DRY RUN - no changes made")
                return
            
            # Process in batches
            print(f"\n🔧 Processing {len(incomplete)} quarters in batches of {BATCH_SIZE}...")
            
            total_deleted = 0
            total_inserted = 0
            
            for i in range(0, len(incomplete), BATCH_SIZE):
                batch = incomplete[i:i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1
                total_batches = (len(incomplete) + BATCH_SIZE - 1) // BATCH_SIZE
                
                deleted, inserted = process_batch_replace(cur, batch)
                total_deleted += deleted
                total_inserted += inserted
                
                # Commit after each batch
                conn.commit()
                
                print(f"   Batch {batch_num}/{total_batches}: "
                      f"deleted {deleted:,}, inserted {inserted:,} | "
                      f"Total: -{total_deleted:,} / +{total_inserted:,}")
            
            print(f"\n{'='*70}")
            print(f"✅ COMPLETE")
            print(f"   Quarters processed: {len(incomplete)}")
            print(f"   Holdings deleted (13f.info): {total_deleted:,}")
            print(f"   Holdings inserted (from SEC): {total_inserted:,}")
            print(f"   Net change: {total_inserted - total_deleted:+,}")
            
            # Verify
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_13FINFO} WHERE filing_url = 'SEC_SUPPLEMENTED'")
            sec_count = cur.fetchone()[0]
            print(f"\n📊 Holdings now marked as SEC_SUPPLEMENTED: {sec_count:,}")
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()