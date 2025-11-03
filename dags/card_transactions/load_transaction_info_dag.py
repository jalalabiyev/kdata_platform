# -*- coding: utf-8 -*-

from datetime import datetime
import os, random, string
from faker import Faker
import oracledb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook  


fake = Faker()
Faker.seed(42)

# ---- EU banks & channel lists
EU_BANKS = [
    ("DE", "Deutsche Bank",      "DEUTDEFF"),
    ("DE", "Commerzbank",        "COBADEFF"),
    ("FR", "BNP Paribas",        "BNPAFRPP"),
    ("FR", "Société Générale",   "SOGEFRPP"),
    ("NL", "ING Bank",           "INGBNL2A"),
    ("NL", "ABN AMRO",           "ABNANL2A"),
    ("IT", "UniCredit",          "UNCRITMM"),
    ("ES", "Banco Santander",    "BSCHESMM"),
    ("GB", "Barclays",           "BARCGB22"),
    ("GB", "HSBC UK",            "HBUKGB4B"),
    ("CH", "UBS",                "UBSWCHZH"),
    ("AT", "Erste Group",        "GIBAATWW"),
    ("SE", "SEB",                "ESSESESS"),
    ("DK", "Danske Bank",        "DABADKKK"),
]

# Channel values (keep in sync with CHECK constraint: ONLINE/ATM/POS/BRANCH)
CHANNEL_TYPES = ["ONLINE", "ATM", "POS", "BRANCH"]

def _connect_via_airflow():
    c = BaseHook.get_connection("oracle_corebank")
    ex = c.extra_dejson or {}
    return oracledb.connect(
        user=c.login,
        password=c.password,
        dsn=ex.get("dsn"),  # e.g. ss6qxowv0803m1wg_high
        # either is enough; we pass both (matches working pattern)
        config_dir=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_location=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_password=ex.get("wallet_password"),
    )

def _bic_like(bic_base: str) -> str:
    """Lightly randomized BIC/SWIFT-like code."""
    tail = "".join(random.choices(string.ascii_uppercase + string.digits, k=3))
    return (bic_base + tail)[:11]

def _branch_code(country: str) -> str:
    # Country code + 6–10 alphanumeric chars
    core = "".join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(6, 10)))
    return f"{country}-{core}"

def _terminal_id() -> str:
    # 16–24 alphanumeric terminal id
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(16, 24)))

def _merchant_name() -> str:
    # Realistic merchant names
    return random.choice([
        "LIDL Supermarkt", "ALDI Markt", "REWE Store", "Tesco Online",
        "Carrefour Express", "Zalando GmbH", "IKEA Möbelhaus",
        "Shell Fuel", "Amazon Marketplace", "MediaMarkt"
    ])

def _notes() -> str:
    return random.choice([
        "Customer purchase", "Online order", "ATM operation",
        "Branch service", "Card-present POS", "Recurring payment",
        "Fee adjustment"
    ])

def load_transaction_info(**_):
    # only the connection line changed to use Airflow Connection
    conn = _connect_via_airflow()
    cur = None
    try:
        cur = conn.cursor()

        # 1) Fetch TRANSACTION_IDs not yet in TRANSACTION_INFO (batch size via env)
        batch = int(os.getenv("TXN_INFO_BATCH", "500"))
        cur.execute("""
            SELECT t.transaction_id
            FROM cardapp.card_transactions t
            LEFT JOIN cardapp.transaction_info i
              ON i.transaction_id = t.transaction_id
            WHERE i.transaction_id IS NULL
            FETCH FIRST :n ROWS ONLY
        """, n=batch)

        txn_ids = [r[0] for r in cur.fetchall()]
        if not txn_ids:
            print("No new transaction_ids found.")
            return

        # 2) Build rows
        rows = []
        for txid in txn_ids:
            country, bank, bic = random.choice(EU_BANKS)
            channel = random.choice(CHANNEL_TYPES)

            rows.append((
                txid,                           # TRANSACTION_ID (FK)
                f"{bank} {fake.city()}",        # BRANCH_NAME
                _branch_code(country),          # BRANCH_CODE
                country,                        # COUNTRY_CODE (CHAR2)
                bank,                           # BANK_NAME
                _bic_like(bic),                 # BANK_BIK (BIC/SWIFT-like)
                channel,                        # CHANNEL_TYPE (ONLINE/ATM/POS/BRANCH)
                _terminal_id(),                 # TERMINAL_ID
                _merchant_name(),               # MERCHANT_NAME
                _notes(),                       # NOTES
            ))

        # 3) Insert
        cur.executemany("""
            INSERT /*+ APPEND */ INTO cardapp.transaction_info
            (transaction_id, branch_name, branch_code, country_code,
             bank_name, bank_bik, channel_type, terminal_id,
             merchant_name, notes)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)
        """, rows, batcherrors=True)

        # show batch errors (e.g., FK/CHK)
        for e in cur.getbatcherrors():
            print(f"BATCH ERROR at row {e.offset}: {e.message}")

        conn.commit()
        print(f"✅ Inserted {len(rows)} rows into TRANSACTION_INFO.")

    except Exception as e:
        print("❌ Error:", e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# --- DAG (daily 21:00) ------------------------------------------------------
with DAG(
    dag_id="generate_transaction_info_daily",
    start_date=datetime(2025, 10, 27),
    schedule="0 21 * * *",       # daily 21:00
    catchup=False,
    tags=["cardapp", "txn_info", "oracle"]
) as dag:
    PythonOperator(
        task_id="load_transaction_info",
        python_callable=load_transaction_info
    )
