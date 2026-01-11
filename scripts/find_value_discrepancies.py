"""
Compare 13f.info holdings vs SEC holdings to find value discrepancies

This identifies managers where 13f.info values are ~1000x different from SEC data.
SEC data is the source of truth.
"""
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("❌ DATABASE_URL not set")
        return
    
    conn = psycopg2.connect(db_url)
    
    print("=" * 70)
    print("Comparing 13f.info vs SEC holdings data")
    print("=" * 70)
    
    # Query to find value discrepancies at the quarter level
    # Compare total portfolio value per (cik, quarter) between both sources
    query = """
    WITH sec_totals AS (
        -- SEC data: value_usd is actual USD
        SELECT 
            cik,
            'Q' || EXTRACT(QUARTER FROM period_end)::text || ' ' || EXTRACT(YEAR FROM period_end)::text as quarter,
            SUM(value_usd) / 1000 as sec_value_000,  -- Convert to thousands for comparison
            COUNT(*) as sec_holdings
        FROM manager_quarter_holding
        WHERE value_usd IS NOT NULL
        GROUP BY cik, period_end
    ),
    info_totals AS (
        -- 13f.info data: value_000 is already in thousands
        SELECT 
            LPAD(LTRIM(cik, '0'), 10, '0') as cik,
            quarter,
            SUM(value_000) as info_value_000,
            COUNT(*) as info_holdings
        FROM public.expected_13finfo_holdings
        WHERE value_000 IS NOT NULL
        GROUP BY LPAD(LTRIM(cik, '0'), 10, '0'), quarter
    ),
    manager_names AS (
        -- Get manager names from manager_quarter
        SELECT DISTINCT cik, manager_name
        FROM manager_quarter
        WHERE manager_name IS NOT NULL
    ),
    comparison AS (
        SELECT 
            COALESCE(s.cik, i.cik) as cik,
            COALESCE(s.quarter, i.quarter) as quarter,
            s.sec_value_000,
            i.info_value_000,
            s.sec_holdings,
            i.info_holdings,
            CASE 
                WHEN s.sec_value_000 > 0 THEN i.info_value_000 / s.sec_value_000
                ELSE NULL 
            END as value_ratio
        FROM sec_totals s
        FULL OUTER JOIN info_totals i ON s.cik = i.cik AND s.quarter = i.quarter
        WHERE s.sec_value_000 > 1000  -- At least $1M to avoid noise
    )
    SELECT 
        c.cik,
        m.manager_name,
        c.quarter,
        c.sec_value_000,
        c.info_value_000,
        c.sec_holdings,
        c.info_holdings,
        c.value_ratio,
        CASE
            WHEN c.value_ratio BETWEEN 900 AND 1100 THEN '1000x_HIGH'
            WHEN c.value_ratio BETWEEN 0.0009 AND 0.0011 THEN '1000x_LOW'
            WHEN c.value_ratio BETWEEN 0.9 AND 1.1 THEN 'OK'
            WHEN c.value_ratio > 100 THEN 'WAY_HIGH'
            WHEN c.value_ratio < 0.01 THEN 'WAY_LOW'
            ELSE 'MISMATCH'
        END as status
    FROM comparison c
    LEFT JOIN manager_names m ON c.cik = m.cik
    WHERE c.value_ratio IS NOT NULL
      AND (c.value_ratio < 0.5 OR c.value_ratio > 2)  -- More than 2x difference
    ORDER BY c.value_ratio DESC
    """
    
    print("\n📊 Finding value discrepancies...")
    df = pd.read_sql(query, conn)
    
    print(f"\n✅ Found {len(df)} quarters with significant value differences")
    
    # Summary by status
    print("\n📈 Summary by status:")
    status_counts = df['status'].value_counts()
    for status, count in status_counts.items():
        print(f"   {status}: {count}")
    
    # Show worst offenders (1000x issues)
    print("\n" + "=" * 70)
    print("🚨 1000x VALUE ISSUES (13f.info is ~1000x too high)")
    print("=" * 70)
    
    high_1000x = df[df['status'] == '1000x_HIGH'].head(30)
    if len(high_1000x) > 0:
        for _, row in high_1000x.iterrows():
            sec_val = row['sec_value_000'] / 1000  # Convert to millions
            info_val = row['info_value_000'] / 1000
            print(f"\n  {row['manager_name'][:50]}")
            print(f"    CIK: {row['cik']} | Quarter: {row['quarter']}")
            print(f"    SEC: ${sec_val:,.1f}M | 13f.info: ${info_val:,.1f}M | Ratio: {row['value_ratio']:.1f}x")
    else:
        print("   None found")
    
    # Save full report to CSV
    csv_file = "value_discrepancy_report.csv"
    df.to_csv(csv_file, index=False)
    print(f"\n💾 Full report saved to: {csv_file}")
    
    # Count affected quarters
    issues_1000x = df[df['status'].isin(['1000x_HIGH', '1000x_LOW'])]
    print(f"\n📊 Total quarters with 1000x issue: {len(issues_1000x)}")
    
    conn.close()
    
    return df


if __name__ == "__main__":
    main()