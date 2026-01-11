import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    os.environ["DATABASE_URL"],
    connect_timeout=10,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5,
)

cur = conn.cursor()
cur.execute("select 1;")
print(cur.fetchone())

cur.close()
conn.close()
