# -*- coding: utf-8 -*-
# load_card_balances_dag.py
# This DAG loads synthetic data into cardapp.CARD_BALANCES.


from datetime import datetime, timedelta
import os
import random
import oracledb

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook  # <-- added

# --- helper: read Oracle creds from Airflow Connection ---
def _connect_via_airflow():
    c = BaseHook.get_connection("oracle_corebank")
    ex = c.extra_dejson or {}
    return oracledb.connect(
        user=c.login,
        password=c.password,
        dsn=ex.get("dsn"),
        # either of these is enough; we pass both (matches your working code)
        config_dir=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_location=ex.get("config_dir", os.getenv("TNS_ADMIN")),
        wallet_password=ex.get("wallet_password"),
    )


def load_card_balances(**context):
    """
    Connect to Oracle, read (account_no, created_at) mapping,
    ensure (ACCOUNT_NO, CURRENCY) uniqueness, generate PK via SEQ,
    and perform batch insert.
    """
    # --- Oracle Connection (from Airflow Connection) ---
    conn = _connect_via_airflow()

    cur = None
    try:
        cur = conn.cursor()

        # 1) Read existing (ACCOUNT_NO, CURRENCY) pairs to avoid duplicates
        cur.execute("SELECT account_no, currency FROM cardapp.card_balances")
        existing_pairs = set(cur.fetchall())

        # 2) Map ACCOUNT_NO → CREATED_AT from customers table
        cur.execute("SELECT account_no, created_at FROM cardapp.customers")
        created_map = dict(cur.fetchall())

        # 3) Currency and amount ranges
        currencies = ["EUR", "USD", "AZN", "GBP", "CHF", "SEK", "CZK"]
        amount_ranges = {
            "EUR": (100, 100000),
            "USD": (100, 100000),
            "AZN": (50, 50000),
            "GBP": (100, 80000),
            "CHF": (100, 90000),
            "SEK": (1000, 900000),
            "CZK": (1000, 3000000),
        }

        # 4) Target batch size
        target_n = int(os.getenv("CARD_BALANCES_BATCH", "500"))

        # 5) List of existing accounts
        accounts = list(created_map.keys())
        random.shuffle(accounts)

        # 6) Rows to insert (account_no, currency, balance, updated_at)
        rows = []
        for acc in accounts:
            if len(rows) >= target_n:
                break

            cur_code = random.choice(currencies)

            # Ensure (ACCOUNT_NO, CURRENCY) uniqueness
            tries = 0
            while (acc, cur_code) in existing_pairs and tries < 10:
                cur_code = random.choice(currencies)
                tries += 1
            if (acc, cur_code) in existing_pairs:
                continue
            existing_pairs.add((acc, cur_code))

            # UPDATED_AT ≥ customers.CREATED_AT
            base_dt = created_map.get(acc, datetime(2015, 1, 1))
            upd = base_dt + timedelta(days=random.randint(0, 3650),
                                      seconds=random.randint(0, 86400))

            lo, hi = amount_ranges[cur_code]
            balance = round(random.uniform(lo, hi), 2)

            rows.append((acc, cur_code, balance, upd))

        if not rows:
            print("ℹ️ No new rows to insert (all (ACCOUNT_NO,CURRENCY) pairs exist).")
            return

        # 7) Batch insert — PK via SEQ_CARD_BALANCES.NEXTVAL
        cur.executemany(
            """
            INSERT INTO cardapp.card_balances
              (card_currency_id, account_no, currency, balance, updated_at)
            VALUES (cardapp.SEQ_CARD_BALANCES.NEXTVAL, :1, :2, :3, :4)
            """,
            rows,
            batcherrors=True,
        )

        # 8) Show batch errors (if any)
        for e in cur.getbatcherrors():
            print(f"BATCH ERROR at row {e.offset}: {e.message}")

        conn.commit()
        print(f"✅ Inserted {len(rows)} rows into CARD_BALANCES.")

    except Exception as e:
        print("❌ Error:", e)
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
        try:
            conn.close()
        except Exception:
            pass


with DAG(
    dag_id="generate_card_balances_daily",
    start_date=datetime(2025, 10, 27),
    schedule="0 21 * * *",   # daily at 21:00
    catchup=False,
    tags=["cardapp", "balances", "oracle"],
) as dag:
    PythonOperator(
        task_id="load_card_balance",
        python_callable=load_card_balances,
    )
