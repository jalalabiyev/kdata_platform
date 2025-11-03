# kdata_platform/scripts/job_payments.py
from datetime import date, timedelta
from kdata_platform.db import execute, fetchall  # SELECT-lər üçün fetchall

JOB = "JOB_B_PAYMENTS"


def upsert_job_marker(status, win_start, win_end, ins=0, upd=0):
    sql = """
    INSERT INTO job_runs(job_name,window_start,window_end,status,inserted_rows,updated_rows)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (job_name,window_start,window_end)
    DO UPDATE SET status=EXCLUDED.status,
                  inserted_rows=EXCLUDED.inserted_rows,
                  updated_rows=EXCLUDED.updated_rows;
    """
    execute(sql, (JOB, win_start, win_end, status, ins, upd))


def fetch_scalar(sql: str) -> float:
    """COUNT(*) kimi 1 sütunlu SELECT üçün təhlükəsiz oxuyucu."""
    rows = fetchall(sql)
    if not rows:
        return 0
    row = rows[0]
    # tuple/list
    try:
        return row[0]
    except Exception:
        pass
    # dict tipe
    if isinstance(row, dict):
        # ən çox rast gəlinən açarlar
        for k in ("count", "cnt", "coalesce", "c"):
            if k in row:
                return row[k]
        # 1-te value
        return list(row.values())[0]
    # null
    return 0


def main():
    today = date.today()
    # pay_date = today - timedelta(days=1)  
    pay_date = today
    pay_date_sql = pay_date.strftime("%Y-%m-%d")  # 'YYYY-MM-DD'

    upsert_job_marker("STARTED", pay_date, pay_date)

    ins_sql = f"""
    WITH stats AS (
        SELECT CEIL(COUNT(*) * 0.10)::int AS n
          FROM kredit_stammdaten
         WHERE vertragsstatus = 'AKTIV'
    ),
    pick AS (
        SELECT
            ks.kredit_id,
            ks.kunden_konto_nummer,
            ks.konto_sub,
            ks.filiale_code,
            ks.zinssatz_p_a,
            ks.waehrung                           AS waehrung_code,
            GREATEST(ks.hauptbetrag,0)::numeric  AS principal_cap
          FROM kredit_stammdaten ks
          LEFT JOIN zahlungen z
            ON  z.kunden_konto_nummer   = ks.kunden_konto_nummer
            AND z.konto_sub             = ks.konto_sub
            AND z.zahlung_zeitpunkt::date = DATE '{pay_date_sql}'
         WHERE ks.vertragsstatus = 'AKTIV'
           AND z.kredit_id IS NULL
         ORDER BY random()
         LIMIT (SELECT n FROM stats)
    )
    INSERT INTO zahlungen
      (kredit_id, kunden_konto_nummer, konto_sub, filiale_code,
       betrag_principal, betrag_zinsen, betrag_strafe, zahlung_zeitpunkt,
       erstellt_am, waehrung_code, waehrung_name)
    SELECT
        p.kredit_id,
        p.kunden_konto_nummer,
        p.konto_sub,
        p.filiale_code,
        amt.betrag_principal,
        ROUND((amt.betrag_principal * p.zinssatz_p_a / 12.0)::numeric, 2),
        CASE WHEN random() < 0.10 THEN ROUND((random()*10)::numeric, 2) ELSE 0 END,
        (DATE '{pay_date_sql}' + (random() * interval '1 day'))::timestamp,
        now(),
        p.waehrung_code,
        CASE p.waehrung_code
          WHEN 'EUR' THEN 'Euro'
          WHEN 'USD' THEN 'US-Dollar'
          WHEN 'GBP' THEN 'Britisches Pfund'
          WHEN 'AZN' THEN 'Aserbaidschan-Manat'
          ELSE 'Unbekannt'
        END
      FROM pick p
      CROSS JOIN LATERAL (
          SELECT ROUND(LEAST(random()*0.10*p.principal_cap, p.principal_cap)::numeric, 2)
      ) amt(betrag_principal)
    ON CONFLICT DO NOTHING
    RETURNING 1;
    """

    # 2) insert  (fetch_scalar )
    cnt_before = fetch_scalar(
        f"SELECT COUNT(*) FROM zahlungen WHERE zahlung_zeitpunkt::date = DATE '{pay_date_sql}'"
    )

    execute(ins_sql)

    cnt_after = fetch_scalar(
        f"SELECT COUNT(*) FROM zahlungen WHERE zahlung_zeitpunkt::date = DATE '{pay_date_sql}'"
    )
    ins_count = int(cnt_after - cnt_before)

    # 3) Master update (kunden_konto_nummer + konto_sub )
    upd_sql = f"""
    WITH t AS (
        SELECT
            kunden_konto_nummer,
            konto_sub,
            SUM(betrag_principal) AS sum_p,
            MAX(zahlung_zeitpunkt) AS max_dt
          FROM zahlungen
         WHERE zahlung_zeitpunkt::date = DATE '{pay_date_sql}'
         GROUP BY 1,2
    )
    UPDATE kredit_stammdaten ks
       SET letzte_zahlung_betrag = t.sum_p,
           letzte_zahlung_datum  = t.max_dt,
           aktualisiert_am       = now()
      FROM t
     WHERE ks.kunden_konto_nummer = t.kunden_konto_nummer
       AND ks.konto_sub           = t.konto_sub;
    """
    execute(upd_sql)

    upsert_job_marker("DONE", pay_date, pay_date, ins=ins_count, upd=0)
    print(f"[{JOB}] DONE: inserted≈{ins_count}")


if __name__ == "__main__":
    main()
