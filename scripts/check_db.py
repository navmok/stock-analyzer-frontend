import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(
    os.environ["DATABASE_URL"],
    connect_timeout=10,
)
cur = conn.cursor()

cur.execute("""
    SELECT cik, manager_name, period_end, total_value_m
    FROM manager_quarter
    WHERE cik = '0001453885'
    ORDER BY period_end DESC
    LIMIT 10
""")

rows = cur.fetchall()
print("Beach Point Capital Management LP data:")
print("Period End\t\tAUM (M)")
print("-" * 40)
for cik, name, period_end, aum in rows:
    print(f"{period_end}\t{aum:.2f}")

cur.close()
conn.close()
