# file: load_card_information_dag.py
# This DAG loads synthetic card data into cardapp.CARD_INFORMATION.
# - IBAN_MASKED is a virtual column in DB → do NOT insert it.
# - OCI Autonomous DB wallet (Thin) is used via Airflow Connection.
#   with:
#     Login: <db user>        Password: <db password>
#     Extra (JSON): {
#       "dsn": "ss6qxowv****_high",
#       "config_dir": "/opt/airflow/oci_wallet",
#       "wallet_password": "******"
#     }

from datetime import datetime, timedelta
import os, random, string
import oracledb
from faker import Faker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook  

# --- Connection helper: read from Airflow Connection ------------------------
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

fake = Faker()

BANKS = [
  ("Deutsche Bank","DEUTDEFF"), ("Commerzbank","COBADEFF"),
  ("ING","INGDDEFF"), ("UniCredit","HYVEDEMM"),
  ("BNP Paribas","BNPAFRPP"), ("Barclays","BARCGB22"),
  ("Santander","BSCHESMM"), ("Raiffeisen","RZBAATWW"),
]

CARD_TYPES  = ["CREDIT","DEBIT","SALARY","VISA","MASTERCARD","AMEX","VIRTUAL"]
STATUSES    = ["ACTIVE","BLOCKED","EXPIRED"]

def _mask(iban: str) -> str:
    if len(iban) <= 8: return iban
    return iban[:6] + "*"*(len(iban)-10) + iban[-4:]

def _make_iban(country="DE", length=22):
    n = max(22, min(length, 34))
    return country + "".join(random.choices(string.digits, k=n-2))

def _pick_weighted(items, weights):
    return random.choices(items, weights=weights, k=1)[0]

# --- Core loader
def load_card_information(**_):
    # ⬇️ only the connection line changed to use Airflow Connection
    conn = _connect_via_airflow()
    cur = conn.cursor()

    # pick account numbers from CUSTOMERS without a card yet
    cur.execute("""
        SELECT c.account_no, c.first_name||' '||c.last_name AS full_name, c.created_at
        FROM cardapp.customers c
        WHERE NOT EXISTS (
          SELECT 1 FROM cardapp.card_information i
          WHERE i.account_no = c.account_no
        )
        FETCH FIRST 1000 ROWS ONLY
    """)
    rows_src = cur.fetchall()
    if not rows_src:
        print("No new accounts to create cards for.")
        cur.close(); conn.close(); return

    # next CARD_ID
    cur.execute("SELECT NVL(MAX(card_id),0) FROM cardapp.card_information")
    start_id = cur.fetchone()[0] + 1

    out = []
    now = datetime.now()

    for i, (acc_no, holder, cust_created_at) in enumerate(rows_src, start=0):
        bank_name, bank_code = random.choice(BANKS)
        card_type = random.choice(CARD_TYPES)
        status = _pick_weighted(STATUSES, weights=[0.9, 0.07, 0.03])

        min_dt = cust_created_at if isinstance(cust_created_at, datetime) else now - timedelta(days=1800)
        created_at = min_dt + timedelta(days=random.randint(0, max(1,(now-min_dt).days)),
                                        seconds=random.randint(0, 86399))

        country = random.choice(["DE","FR","NL","ES","IT","AT","BE","GB"])
        iban_full = _make_iban(country=country, length=random.randint(22, 30))
        iban_masked = _mask(iban_full)   # virtual column in DB calculates this; kept for symmetry

        max_limit        = round(random.uniform(2_000, 20_000), 2)
        daily_limit      = round(random.uniform(300, 3_000), 2)
        transfer_limit   = round(random.uniform(1_000, 10_000), 2)
        foreign_transfer = round(random.uniform(500, 5_000), 2)
        monthly_fee      = round(random.uniform(0, 9.99), 2)
        cashback_percent = round(random.uniform(0, 5), 2)

        # do NOT include iban_masked (virtual column)
        out.append((
            start_id + i,           # 1  CARD_ID
            acc_no,                 # 2  ACCOUNT_NO
            iban_full,              # 3  IBAN_FULL
            bank_name,              # 4  BANK_NAME
            bank_code,              # 5  BANK_CODE
            created_at,             # 6  CREATED_AT
            card_type,              # 7  CARD_TYPE
            holder[:100],           # 8  CARD_HOLDER_NAME
            max_limit,              # 9  MAX_LIMIT
            daily_limit,            # 10 DAILY_LIMIT
            transfer_limit,         # 11 TRANSFER_LIMIT
            foreign_transfer,       # 12 FOREIGN_TRANSFER_MAX
            monthly_fee,            # 13 MONTHLY_FEE
            cashback_percent,       # 14 CASHBACK_PERCENT
            _pick_weighted(STATUSES, [0.9, 0.07, 0.03])  # 15 STATUS
        ))

    # INSERT (no IBAN_MASKED)
    cur.executemany("""
        INSERT INTO cardapp.card_information (
            card_id, account_no, iban_full, bank_name, bank_code,
            created_at, card_type, card_holder_name,
            max_limit, daily_limit, transfer_limit,
            foreign_transfer_max, monthly_fee, cashback_percent, status
        ) VALUES (
            :1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15
        )
    """, out, batcherrors=True)

    for e in cur.getbatcherrors():
        print(f"[BATCH ERROR] row={e.offset} msg={e.message}")

    conn.commit()
    print(f"✅ Inserted {len(out)} rows into CARD_INFORMATION")
    cur.close(); conn.close()

with DAG(
    dag_id="generate_card_information_daily",
    start_date=datetime(2025, 10, 27),
    schedule="0 21 * * *",
    catchup=False,
    tags=["cardapp","oracle","faker","cards"]
) as dag:
    PythonOperator(
        task_id="load_card_information",
        python_callable=load_card_information
    )
