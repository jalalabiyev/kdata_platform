import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "*****"),
        
        user=os.getenv("PGUSER", "app"),
        password=os.getenv("PGPASSWORD", "*****"),
        host=os.getenv("PGHOST", "*********"),
        port=os.getenv("PGPORT", "5432"),
        sslmode=os.getenv("PGSSLMODE", "prefer"),
        connect_timeout=10
    )

def execute(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            if params is None:
                cur.execute(sql)          
            else:
                cur.execute(sql, params) 
        conn.commit()

def fetchall(sql, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

def executemany(sql, seq_params):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, seq_params)
        conn.commit()
