from datetime import datetime, timedelta
import os, random, string
from faker import Faker
import oracledb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook   # CHANGED

# --- helper: Airflow Connection read ---
def _connect_via_airflow():               # CHANGED
    c = BaseHook.get_connection("oracle_corebank")
    ex = c.extra_dejson or {}
    return oracledb.connect(
        user=c.login,
        password=c.password,
        dsn=ex.get("dsn"),
        # 2 parameter
        config_dir=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_location=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_password=ex.get("wallet_password"),
    )

def test_oracle_connection():
    conn = _connect_via_airflow()         # CHANGED
    # (istəsən buraya sadə test query əlavə edərsən)
    conn.close()

fake_en = Faker("en_US")
fake_de = Faker("de_DE")

def gen_passport():
    #  DE1234567
    prefix = random.choice(["DE", "EN", "GB", "FR", "NL"])
    return prefix + "".join(random.choices(string.digits, k=7))

def gen_account_no(length=26):
    L = random.randint(24, 34)
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choices(alphabet, k=L))

def random_created_at():
    # 2015-01-01 … today
    start = datetime(2015, 1, 1)
    end = datetime.now()
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days),
                             seconds=random.randint(0, 86400))

def random_dob():
    # 18–65 
    end = datetime.now() - timedelta(days=18*365)
    start = datetime.now() - timedelta(days=65*365)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).date()

def make_rows(n_rows=500):
    rows = []
    seen_accounts = set()
    for _ in range(n_rows):
        # EN and DE
        if random.random() < 0.5:
            first, last = fake_en.first_name(), fake_en.last_name()
            phone = fake_en.phone_number()
        else:
            first, last = fake_de.first_name(), fake_de.last_name()
            phone = fake_de.phone_number()

        # unikal ACCOUNT_NO
        acc = gen_account_no()
        while acc in seen_accounts:
            acc = gen_account_no()
        seen_accounts.add(acc)

        rows.append((
            first,                 # FIRST_NAME
            last,                  # LAST_NAME
            gen_passport(),        # PASSPORT_ID
            random_dob(),          # DOB (DATE)
            phone[:20],            # PHONE
            acc,                   # ACCOUNT_NO (UNIQUE)
            random_created_at(),   # CREATED_AT (TIMESTAMP)
        ))
    return rows

def load_customers(**context):
    # Oracle bağlantısı
    conn = _connect_via_airflow()         # CHANGED
    try:
        cur = conn.cursor()

        # batch insert  data
        rows = make_rows(n_rows=int(os.getenv("CUSTOMERS_BATCH", "500")))

        cur.executemany(
            """
            INSERT INTO cardapp.customers
              (first_name, last_name, passport_id, dob, phone, account_no, created_at)
            VALUES (:1, :2, :3, :4, :5, :6, :7)
            """,
            rows,
            batcherrors=True,
        )

        for e in cur.getbatcherrors():
            print(f"BATCH ERROR at row {e.offset}: {e.message}")

        conn.commit()
        print("✅ Data inserted successfully!")

    except Exception as e:
        print("❌ Error:", e)
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

with DAG(
    dag_id="generate_customers_daily",
    start_date=datetime(2025, 10, 27),
    schedule="0 21 * * *",     # hər gün 21:00
    catchup=False,
    tags=["cardapp", "faker", "oracle"]
) as dag:
    PythonOperator(
        task_id="load_customers",
        python_callable=load_customers
    )
