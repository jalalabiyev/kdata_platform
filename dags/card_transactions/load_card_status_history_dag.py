# file: load_card_status_history_dag.py
from datetime import datetime, timedelta
import os, random
import oracledb
from airflow import DAG
from airflow.operators.python import PythonOperator

# Same connection parameters as previous files
def _connect_via_airflow():
    c = BaseHook.get_connection("oracle_corebank")
    ex = c.extra_dejson or {}
    return oracledb.connect(
        user=c.login,
        password=c.password,
        dsn=ex.get("dsn"),
        # either parameter works; give both for clarity
        config_dir=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_location=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_password=ex.get("wallet_password"),
    )
#  Status sets (aligned with CHECK constraint)
STATUSES = ["ACTIVE", "BLOCKED", "EXPIRED", "REPLACED"]

#  Allowed transitions (old -> new)
TRANSITIONS = {
    "ACTIVE":   ["BLOCKED", "EXPIRED", "REPLACED"],
    "BLOCKED":  ["ACTIVE", "REPLACED"],
    "EXPIRED":  ["REPLACED"],
    "REPLACED": ["ACTIVE"]  
}

REASONS = [
    "Customer request", "Fraud suspicion", "Card expired",
    "Limit adjustment", "Card replacement", "Compliance review"
]

OPERATORS = ["SYSTEM"] + [f"OPERATOR_{i:03d}" for i in range(1, 50)]

def load_card_status_history(**_):
    # ⬇️ only the connection line changed to use Airflow Connection
    conn = _connect_via_airflow()
    cur = conn.cursor()

    #    Only a random sample to keep each run realistic
    cur.execute("""
        SELECT card_id, status, created_at, max_limit
        FROM cardapp.card_information
        ORDER BY dbms_random.value
        FETCH FIRST NVL(:n_rows, 500) ROWS ONLY
    """, dict(n_rows=int(os.getenv("CARD_STATUS_BATCH", "500"))))
    cards = cur.fetchall()
    if not cards:
        print("No cards found to generate history.")
        cur.close(); conn.close(); return

    cur.execute("SELECT NVL(MAX(history_id), 0) FROM cardapp.card_status_history")
    next_id = cur.fetchone()[0] + 1

    out = []
    now = datetime.now()

    for i, (card_id, old_status, created_at, max_limit) in enumerate(cards):
        old_status = old_status if old_status in STATUSES else "ACTIVE"
        candidates = TRANSITIONS.get(old_status, ["ACTIVE"])
        new_status = random.choice(candidates)

        min_dt = created_at if isinstance(created_at, datetime) else now - timedelta(days=1800)
        change_dt = min_dt + timedelta(days=random.randint(0, max(1, (now - min_dt).days)),
                                       seconds=random.randint(0, 86399))

        reason = random.choice(REASONS)
        changed_by = random.choice(OPERATORS)

        if reason == "Limit adjustment":
            base = float(max_limit) if max_limit is not None else random.uniform(2000, 20000)
            old_limit = round(base, 2)
            delta = round(random.uniform(-0.25, 0.35) * base, 2)  # -25% … +35%
            new_limit = max(0.0, round(base + delta, 2))
        else:
            old_limit = None
            new_limit = None

        note = None

        out.append((
            next_id + i,      # 1  HISTORY_ID (PK)
            card_id,          # 2  CARD_ID (FK -> CARD_INFORMATION)
            old_status,       # 3  OLD_STATUS  (CHECK set)
            new_status,       # 4  NEW_STATUS  (CHECK set)
            reason,           # 5  CHANGE_REASON
            changed_by,       # 6  CHANGED_BY
            change_dt,        # 7  CHANGE_DATE (NOT NULL)
            old_limit,        # 8  OLD_LIMIT (nullable)
            new_limit,        # 9  NEW_LIMIT (nullable)
            note              # 10 NOTE (nullable)
        ))

    cur.executemany("""
        INSERT INTO cardapp.card_status_history (
            history_id, card_id, old_status, new_status,
            change_reason, changed_by, change_date,
            old_limit, new_limit, note
        ) VALUES (
            :1,:2,:3,:4,:5,:6,:7,:8,:9,:10
        )
    """, out, batcherrors=True)

    for e in cur.getbatcherrors():
        print(f"[BATCH ERROR] row={e.offset} msg={e.message}")

    conn.commit()
    print(f"✅ Inserted {len(out)} rows into CARD_STATUS_HISTORY")
    cur.close(); conn.close()

with DAG(
    dag_id="generate_card_status_history_daily",
    start_date=datetime(2025, 10, 27),
    schedule="0 21 * * *",     #  daily 21:00
    catchup=False,
    tags=["cardapp","oracle","history","cards"]
) as dag:
    PythonOperator(
        task_id="load_card_status_history",
        python_callable=load_card_status_history
    )
