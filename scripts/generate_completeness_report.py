"""
SEC 13F Data Completeness Report Generator

Generates CSV reports comparing expected vs downloaded holdings/values
for each manager and period.
"""
import psycopg2
import csv
import os
from datetime import datetime
from dotenv import load_dotenv

def main():
    print("=" * 70)
    print("SEC 13F Data Completeness Report")
    print("=" * 70)
    
    load_dotenv()
    
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        connect_timeout=30,
    )
    cur = conn.cursor()
    
    # =========================================================================
    # SUMMARY STATS
    # =========================================================================
    print("\n📊 SUMMARY STATISTICS\n")
    
    cur.execute("SELECT COUNT(*) FROM manager_quarter")
    total_periods = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT (cik, period_end)) FROM manager_quarter_holding")
    periods_with_holdings = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT cik) FROM manager_quarter")
    total_managers = cur.fetchone()[0]
    
    print(f"   Total manager-periods expected: {total_periods:,}")
    print(f"   Periods with holdings data:     {periods_with_holdings:,}")
    print(f"   Missing periods:                {total_periods - periods_with_holdings:,}")
    print(f"   Total unique managers:          {total_managers:,}")
    
    # =========================================================================
    # STATUS BREAKDOWN
    # =========================================================================
    print("\n📊 STATUS BREAKDOWN\n")
    
    cur.execute("""
        SELECT 
            status,
            COUNT(*) as period_count,
            COUNT(DISTINCT cik) as manager_count
        FROM (
            SELECT 
                mq.cik,
                mq.period_end,
                CASE
                    WHEN h.actual_holdings IS NULL THEN 'MISSING'
                    WHEN mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000 THEN 'VALUE_1000X_HIGH'
                    WHEN mq.total_value_m > 0 AND ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) > mq.total_value_m * 0.2 THEN 'VALUE_MISMATCH'
                    WHEN mq.num_holdings > 0 AND COALESCE(h.actual_holdings, 0) < mq.num_holdings * 0.95 THEN 'PARTIAL'
                    ELSE 'OK'
                END as status
            FROM manager_quarter mq
            LEFT JOIN (
                SELECT 
                    cik,
                    period_end,
                    COUNT(*) as actual_holdings,
                    SUM(value_usd) / 1000000.0 as sum_value_m
                FROM manager_quarter_holding
                GROUP BY cik, period_end
            ) h ON mq.cik = h.cik AND mq.period_end = h.period_end
        ) sub
        GROUP BY status
        ORDER BY 
            CASE status 
                WHEN 'OK' THEN 1
                WHEN 'PARTIAL' THEN 2
                WHEN 'VALUE_MISMATCH' THEN 3
                WHEN 'VALUE_1000X_HIGH' THEN 4
                WHEN 'MISSING' THEN 5
            END
    """)
    
    print(f"   {'Status':<20} {'Periods':>10} {'Managers':>10}")
    print(f"   {'-'*20} {'-'*10} {'-'*10}")
    for row in cur.fetchall():
        status, period_count, manager_count = row
        print(f"   {status:<20} {period_count:>10,} {manager_count:>10,}")
    
    # =========================================================================
    # EXPORT FULL REPORT TO CSV
    # =========================================================================
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Full comparison report
    print(f"\n📄 Generating full comparison report...")
    
    cur.execute("""
        SELECT 
            mq.cik,
            mq.period_end,
            mq.manager_name,
            mq.num_holdings as expected_holdings,
            COALESCE(h.actual_holdings, 0) as downloaded_holdings,
            mq.total_value_m as expected_value_millions,
            ROUND(COALESCE(h.sum_value_m, 0)::numeric, 2) as downloaded_value_millions,
            CASE 
                WHEN mq.num_holdings > 0 THEN 
                    ROUND(100.0 * COALESCE(h.actual_holdings, 0) / mq.num_holdings, 1)
                ELSE NULL 
            END as holdings_pct_complete,
            CASE 
                WHEN mq.total_value_m > 0 THEN 
                    ROUND(100.0 * COALESCE(h.sum_value_m, 0) / mq.total_value_m, 1)
                ELSE NULL 
            END as value_pct_complete,
            CASE
                WHEN h.actual_holdings IS NULL THEN 'MISSING'
                WHEN mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000 THEN 'VALUE_1000X_HIGH'
                WHEN mq.total_value_m > 0 AND ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) > mq.total_value_m * 0.2 THEN 'VALUE_MISMATCH'
                WHEN mq.num_holdings > 0 AND COALESCE(h.actual_holdings, 0) < mq.num_holdings * 0.95 THEN 'PARTIAL'
                ELSE 'OK'
            END as status
        FROM manager_quarter mq
        LEFT JOIN (
            SELECT 
                cik,
                period_end,
                COUNT(*) as actual_holdings,
                SUM(value_usd) / 1000000.0 as sum_value_m
            FROM manager_quarter_holding
            GROUP BY cik, period_end
        ) h ON mq.cik = h.cik AND mq.period_end = h.period_end
        ORDER BY mq.cik, mq.period_end
    """)
    
    rows = cur.fetchall()
    columns = [
        'cik', 'period_end', 'manager_name', 
        'expected_holdings', 'downloaded_holdings',
        'expected_value_millions', 'downloaded_value_millions',
        'holdings_pct_complete', 'value_pct_complete', 'status'
    ]
    
    filename = f"data_completeness_full_{timestamp}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    
    print(f"   ✓ Saved: {filename} ({len(rows):,} rows)")
    
    # =========================================================================
    # EXPORT PROBLEMS ONLY TO CSV
    # =========================================================================
    print(f"\n📄 Generating problems-only report...")
    
    cur.execute("""
        SELECT 
            mq.cik,
            mq.period_end,
            mq.manager_name,
            mq.num_holdings as expected_holdings,
            COALESCE(h.actual_holdings, 0) as downloaded_holdings,
            mq.total_value_m as expected_value_millions,
            ROUND(COALESCE(h.sum_value_m, 0)::numeric, 2) as downloaded_value_millions,
            CASE 
                WHEN mq.total_value_m > 0 THEN 
                    ROUND(100.0 * COALESCE(h.sum_value_m, 0) / mq.total_value_m, 1)
                ELSE NULL 
            END as value_pct,
            CASE
                WHEN h.actual_holdings IS NULL THEN 'MISSING'
                WHEN mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000 THEN 'VALUE_1000X_HIGH'
                WHEN mq.total_value_m > 0 AND ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) > mq.total_value_m * 0.2 THEN 'VALUE_MISMATCH'
                WHEN mq.num_holdings > 0 AND COALESCE(h.actual_holdings, 0) < mq.num_holdings * 0.95 THEN 'PARTIAL'
                ELSE 'OK'
            END as status
        FROM manager_quarter mq
        LEFT JOIN (
            SELECT 
                cik,
                period_end,
                COUNT(*) as actual_holdings,
                SUM(value_usd) / 1000000.0 as sum_value_m
            FROM manager_quarter_holding
            GROUP BY cik, period_end
        ) h ON mq.cik = h.cik AND mq.period_end = h.period_end
        WHERE 
            h.actual_holdings IS NULL
            OR (mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000)
            OR (mq.total_value_m > 0 AND ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) > mq.total_value_m * 0.2)
            OR (mq.num_holdings > 0 AND COALESCE(h.actual_holdings, 0) < mq.num_holdings * 0.95)
        ORDER BY status, mq.manager_name, mq.period_end
    """)
    
    rows = cur.fetchall()
    columns = [
        'cik', 'period_end', 'manager_name', 
        'expected_holdings', 'downloaded_holdings',
        'expected_value_millions', 'downloaded_value_millions',
        'value_pct', 'status'
    ]
    
    filename = f"data_completeness_problems_{timestamp}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    
    print(f"   ✓ Saved: {filename} ({len(rows):,} rows)")
    
    # =========================================================================
    # EXPORT MANAGER SUMMARY TO CSV
    # =========================================================================
    print(f"\n📄 Generating manager summary report...")
    
    cur.execute("""
        SELECT 
            mq.cik,
            mq.manager_name,
            COUNT(*) as total_periods,
            SUM(CASE WHEN h.actual_holdings IS NOT NULL 
                     AND (mq.total_value_m IS NULL OR mq.total_value_m = 0 OR ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) <= mq.total_value_m * 0.2)
                     AND (mq.num_holdings IS NULL OR mq.num_holdings = 0 OR COALESCE(h.actual_holdings, 0) >= mq.num_holdings * 0.95)
                     THEN 1 ELSE 0 END) as ok_periods,
            SUM(CASE WHEN h.actual_holdings IS NULL THEN 1 ELSE 0 END) as missing_periods,
            SUM(CASE WHEN mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000 THEN 1 ELSE 0 END) as value_1000x_periods,
            SUM(CASE WHEN h.actual_holdings IS NOT NULL 
                     AND mq.total_value_m > 0 
                     AND COALESCE(h.sum_value_m, 0) <= mq.total_value_m * 1000
                     AND ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) > mq.total_value_m * 0.2 
                     THEN 1 ELSE 0 END) as value_mismatch_periods,
            SUM(CASE WHEN h.actual_holdings IS NOT NULL 
                     AND mq.num_holdings > 0 
                     AND COALESCE(h.actual_holdings, 0) < mq.num_holdings * 0.95 
                     THEN 1 ELSE 0 END) as partial_periods
        FROM manager_quarter mq
        LEFT JOIN (
            SELECT 
                cik,
                period_end,
                COUNT(*) as actual_holdings,
                SUM(value_usd) / 1000000.0 as sum_value_m
            FROM manager_quarter_holding
            GROUP BY cik, period_end
        ) h ON mq.cik = h.cik AND mq.period_end = h.period_end
        GROUP BY mq.cik, mq.manager_name
        ORDER BY mq.manager_name
    """)
    
    rows = cur.fetchall()
    columns = [
        'cik', 'manager_name', 'total_periods', 'ok_periods',
        'missing_periods', 'value_1000x_periods', 'value_mismatch_periods', 'partial_periods'
    ]
    
    filename = f"data_completeness_by_manager_{timestamp}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    
    print(f"   ✓ Saved: {filename} ({len(rows):,} rows)")
    
    # =========================================================================
    # SHOW TOP PROBLEM MANAGERS
    # =========================================================================
    print(f"\n🔍 TOP 20 MANAGERS WITH ISSUES:\n")
    
    cur.execute("""
        SELECT 
            mq.cik,
            mq.manager_name,
            COUNT(*) as total_periods,
            SUM(CASE WHEN h.actual_holdings IS NULL THEN 1 ELSE 0 END) as missing,
            SUM(CASE WHEN mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000 THEN 1 ELSE 0 END) as val_1000x,
            SUM(CASE WHEN h.actual_holdings IS NOT NULL 
                     AND mq.total_value_m > 0 
                     AND COALESCE(h.sum_value_m, 0) <= mq.total_value_m * 1000
                     AND ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) > mq.total_value_m * 0.2 
                     THEN 1 ELSE 0 END) as val_mismatch
        FROM manager_quarter mq
        LEFT JOIN (
            SELECT cik, period_end, COUNT(*) as actual_holdings, SUM(value_usd) / 1000000.0 as sum_value_m
            FROM manager_quarter_holding GROUP BY cik, period_end
        ) h ON mq.cik = h.cik AND mq.period_end = h.period_end
        GROUP BY mq.cik, mq.manager_name
        HAVING 
            SUM(CASE WHEN h.actual_holdings IS NULL THEN 1 ELSE 0 END) > 0
            OR SUM(CASE WHEN mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000 THEN 1 ELSE 0 END) > 0
            OR SUM(CASE WHEN h.actual_holdings IS NOT NULL AND mq.total_value_m > 0 
                        AND COALESCE(h.sum_value_m, 0) <= mq.total_value_m * 1000
                        AND ABS(COALESCE(h.sum_value_m, 0) - mq.total_value_m) > mq.total_value_m * 0.2 
                        THEN 1 ELSE 0 END) > 0
        ORDER BY 
            SUM(CASE WHEN h.actual_holdings IS NULL THEN 1 ELSE 0 END) DESC,
            SUM(CASE WHEN mq.total_value_m > 0 AND COALESCE(h.sum_value_m, 0) > mq.total_value_m * 1000 THEN 1 ELSE 0 END) DESC
        LIMIT 20
    """)
    
    print(f"   {'Manager':<40} {'Total':>6} {'Miss':>6} {'1000x':>6} {'Mismatch':>8}")
    print(f"   {'-'*40} {'-'*6} {'-'*6} {'-'*6} {'-'*8}")
    for row in cur.fetchall():
        cik, name, total, missing, val_1000x, val_mismatch = row
        name_short = (name[:37] + '...') if len(name) > 40 else name
        print(f"   {name_short:<40} {total:>6} {missing:>6} {val_1000x:>6} {val_mismatch:>8}")
    
    print("\n" + "=" * 70)
    print("✅ Reports generated successfully!")
    print("=" * 70)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()