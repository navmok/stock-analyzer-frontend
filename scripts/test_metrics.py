import os
import psycopg2
from dotenv import load_dotenv

def pct(curr, prev):
    if prev is None or prev == 0:
        return None
    return (curr - prev) / prev

load_dotenv()
conn = psycopg2.connect(
    os.environ["DATABASE_URL"],
    connect_timeout=10,
)
cur = conn.cursor()

# Simulate the hedgefunds.js query for Q3 2025
qEnd = "2025-09-30"
sql = f"""
  with base as (
    select cik, manager_name, period_end, total_value_m, num_holdings
    from manager_quarter
    where period_end = '{qEnd}'::date
  ),
  prev as (
    select cik, total_value_m as prev_qtr
    from manager_quarter
    where period_end = ('{qEnd}'::date - interval '3 months')::date
  ),
  yoy as (
    select cik, total_value_m as prev_yoy
    from manager_quarter
    where period_end = ('{qEnd}'::date - interval '12 months')::date
  )
  select
    b.cik,
    b.manager_name,
    b.period_end,
    b.total_value_m,
    p.prev_qtr,
    y.prev_yoy
  from base b
  left join prev p using (cik)
  left join yoy y using (cik)
  WHERE b.cik = '0001453885'
"""

cur.execute(sql)
row = cur.fetchone()

if row:
    cik, manager, period_end, curr, prevQ, prevY = row
    qoq = pct(curr, prevQ)
    yoy = pct(curr, prevY)
    
    print(f"Manager: {manager}")
    print(f"Period: {period_end}")
    print(f"Current AUM: {curr:.2f}M")
    print(f"Prev Quarter AUM: {prevQ}")
    print(f"Prev Year AUM: {prevY}")
    print(f"QoQ: {qoq}")
    print(f"YoY: {yoy}")
    
    if qoq is not None:
        print(f"QoQ %: {qoq*100:.2f}%")
    if yoy is not None:
        print(f"YoY %: {yoy*100:.2f}%")
else:
    print("No data found")

cur.close()
conn.close()
