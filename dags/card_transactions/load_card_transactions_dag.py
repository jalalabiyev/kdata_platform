# dags/card_transaction/load_card_transactions_dag.py
from datetime import datetime, timedelta
import os, random, string
import oracledb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook   


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

# --- helpers ---
def gen_txn_id():
    # EN: 20-char (A-Z,0-9) unique ID
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choices(alphabet, k=20))

OFFICES = [
    "ATM", "POS_MARKET", "ONLINE_SHOP", "WIRE_TRANSFER",
    "WESTERN_UNION", "MOBILE_APP", "INTERNET_BANKING"
]

def load_card_transactions(**_):
    conn = _connect_via_airflow()
    cur = None
    try:                                      
        cur = conn.cursor()

        cur.execute("""
            SELECT cb.account_no,
                   cb.card_currency_id,
                   cb.currency,
                   cb.balance,
                   c.created_at
            FROM cardapp.card_balances cb
            JOIN cardapp.customers c
              ON c.account_no = cb.account_no
        """)
        base_rows = cur.fetchall()
        if not base_rows:
            print("No base rows in CARD_BALANCES/CUSTOMERS.")
            return

        # Map/list for sampling
        n_rows = int(os.getenv("CARD_TXN_BATCH", "500"))
        out = []
        seen_ids = set()

        for _ in range(n_rows):
            acc_no, cur_id, curr, bal, created_at = random.choice(base_rows)

            # TXN date between created_at and now
            start = created_at
            end = datetime.now()
            if start > end:  # guard if created_at > now
                start, end = end - timedelta(days=1), end
            span_days = max(0, (end - start).days)
            txn_dt = start + timedelta(
                days=random.randint(0, max(0, span_days)),
                seconds=random.randint(0, 86399)
            )

            # amounts
            max_out = max(0.0, float(bal))                 # AMOUNT_OUT ≤ BALANCE
            amount_out = round(random.uniform(0, max_out), 2)

            amount_in = 0.0 if random.random() < 0.5 else round(random.uniform(1, 10000), 2)

            # transaction id (unique)
            txid = gen_txn_id()
            while txid in seen_ids:
                txid = gen_txn_id()
            seen_ids.add(txid)

            office = random.choice(OFFICES)
            desc = f"{office} {curr} txn"

            out.append((
                txid,           # TRANSACTION_ID (PK/UNIQUE, VARCHAR2(20))
                acc_no,         # ACCOUNT_NO (FK -> CUSTOMERS)
                cur_id,         # CARD_CURRENCY_ID (FK -> CARD_BALANCES)
                txn_dt,         # TRANSACTION_DATE (<= now, >= created_at)
                amount_out,     # AMOUNT_OUT
                amount_in,      # AMOUNT_IN
                curr,           # CURRENCY
                office,         # TRANSACTION_OFFICE
                desc            # DESCRIPTION
            ))

        cur.executemany(
            """
            INSERT INTO cardapp.card_transactions
              (transaction_id, account_no, card_currency_id, transaction_date,
               amount_out, amount_in, currency, transaction_office, description)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
            """,
            out,
            batcherrors=True
        )

        # log batch errors (e.g., unique/PK collisions)
        for e in cur.getbatcherrors():
            print(f"BATCH ERROR at row {e.offset}: {e.message}")

        conn.commit()
        print(f"✅ Inserted {len(out)} rows into CARD_TRANSACTIONS.")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

with DAG(
    dag_id="load_card_transactions_daily",
    start_date=datetime(2025, 10, 27),
    schedule="0 21 * * *",   #  every day 21:00
    catchup=False,
    tags=["cardapp", "transactions", "oracle"]
) as dag:
    PythonOperator(
        task_id="load_card_transactions",
        python_callable=load_card_transactions
    )
