#!/usr/bin/env python3
"""
Monitor the backfill script progress without interrupting it.
Queries the database to see what's been updated.
"""
import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        os.environ["DATABASE_URL"],
        connect_timeout=10,
    )
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("BACKFILL PROGRESS STATUS")
    print("=" * 70)
    print(f"Checked at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Get total managers
    cur.execute("SELECT COUNT(DISTINCT cik) FROM manager_quarter")
    total_managers = cur.fetchone()[0]

    # Get managers that have been processed (have reasonable values)
    # Reasonable values are between 0.1M and 100B (100,000,000M)
    cur.execute("""
        SELECT COUNT(DISTINCT cik)
        FROM manager_quarter
        WHERE total_value_m IS NOT NULL
        AND total_value_m > 0.01
        AND total_value_m < 100000000
    """)

    processed_managers = cur.fetchone()[0]

    # Get total quarterly records
    cur.execute("SELECT COUNT(*) FROM manager_quarter")
    total_records = cur.fetchone()[0]

    # Get processed quarterly records
    cur.execute("""
        SELECT COUNT(*)
        FROM manager_quarter
        WHERE total_value_m IS NOT NULL
        AND total_value_m > 0.01
        AND total_value_m < 100000000
    """)

    processed_records = cur.fetchone()[0]

    # Calculate percentages
    manager_pct = (processed_managers / total_managers * 100) if total_managers > 0 else 0
    record_pct = (processed_records / total_records * 100) if total_records > 0 else 0

    print("MANAGERS:")
    print(f"   Processed: {processed_managers:,} / {total_managers:,} ({manager_pct:.1f}%)")
    print(f"   Remaining: {total_managers - processed_managers:,}")

    print("\nQUARTERLY RECORDS:")
    print(f"   Processed: {processed_records:,} / {total_records:,} ({record_pct:.1f}%)")
    print(f"   Remaining: {total_records - processed_records:,}")

    # Get some sample data to show it's working
    cur.execute("""
        SELECT cik, manager_name, COUNT(*) as records,
               MIN(total_value_m) as min_aum, MAX(total_value_m) as max_aum
        FROM manager_quarter
        WHERE total_value_m IS NOT NULL
        AND total_value_m > 0.01
        AND total_value_m < 100000000
        GROUP BY cik, manager_name
        ORDER BY cik DESC
        LIMIT 10
    """)

    samples = cur.fetchall()
    if samples:
        print("\nRECENTLY PROCESSED MANAGERS (latest 10):")
        print("-" * 70)
        for cik, name, count, min_aum, max_aum in samples:
            print(f"   {cik} | {name[:40]:<40} | {count} records")
            if min_aum and max_aum:
                print(f"      AUM Range: ${min_aum:,.0f}M - ${max_aum:,.0f}M")

    # Get data age
    cur.execute("""
        SELECT MAX(period_end), MIN(period_end)
        FROM manager_quarter
        WHERE total_value_m IS NOT NULL
        AND total_value_m > 0.01
    """)

    latest, earliest = cur.fetchone()
    if latest:
        print("\nDATA COVERAGE:")
        print(f"   Latest: {latest}")
        print(f"   Earliest: {earliest}")

    print("\n" + "=" * 70)
    print("TIP: Run this script again in a few minutes to see updated progress")
    print("=" * 70 + "\n")

    cur.close()
    conn.close()

except Exception as e:
    print(f"\nError: {e}")
    print("Make sure DATABASE_URL is set correctly in .env file")
