import os
import psycopg2
from dotenv import load_dotenv

def fix():
    load_dotenv('.env.local')
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not found in .env.local")
        return
        
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    print("Checking/Creating indices for speed...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mq_period_end ON manager_quarter (period_end);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mq_cik ON manager_quarter (cik);")
    conn.commit()
    print("✓ Indices created.")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    fix()
