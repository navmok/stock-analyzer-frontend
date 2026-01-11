#!/usr/bin/env python3
"""
Monitor the backfill script progress by tracking database updates
"""
import os
import psycopg2
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        connect_timeout=10,
    )
    cur = conn.cursor()
    
    print("📊 Backfill Script Progress Monitor")
    print("=" * 60)
    
    # Get stats on what's been updated
    cur.execute("""
        SELECT 
            COUNT(DISTINCT cik) as unique_managers,
            COUNT(*) as total_records,
            MIN(period_end) as earliest_date,
            MAX(period_end) as latest_date
        FROM manager_quarter
    """)
    
    total_unique, total_records, earliest, latest = cur.fetchone()
    
    print(f"Total Managers in DB: {total_unique}")
    print(f"Total Quarterly Records: {total_records}")
    print(f"Data Range: {earliest} to {latest}")
    
    # Check for recent updates
    cur.execute("""
        SELECT COUNT(*) as updated_today
        FROM manager_quarter
        WHERE total_value_m IS NOT NULL
        AND total_value_m > 0
        AND (total_value_m < 1 OR total_value_m > 100000)  -- Reasonable range in millions
        LIMIT 1
    """)
    
    reasonable = cur.fetchone()[0]
    print(f"\nRecords with reasonable values: ~{reasonable}")
    
    # Sample of recent managers
    cur.execute("""
        SELECT cik, manager_name, period_end, total_value_m
        FROM manager_quarter
        WHERE cik IN (
            SELECT cik
            FROM (
                SELECT DISTINCT cik
                FROM manager_quarter
            ) AS distinct_ciks
            ORDER BY random()
            LIMIT 5
        )
        ORDER BY cik, period_end DESC
    """)
    
    print("\nSample of recent data:")
    print("-" * 60)
    current_cik = None
    for cik, name, period_end, aum in cur.fetchall():
        if cik != current_cik:
            print(f"\n{name} ({cik})")
            current_cik = cik
        print(f"  {period_end}: {aum:.2f}M")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    print("Make sure the backfill script is running and DATABASE_URL is set")
