# kdata_platform/scripts/job_creditflow_snapshot.py
from datetime import date
from kdata_platform.db import fetchall, execute, get_conn

JOB = "JOB_C_CREDITFLOW"

def upsert_job_marker(status, d, ins=0, upd=0):
    sql = """
    INSERT INTO job_runs(job_name,window_start,window_end,status,inserted_rows,updated_rows)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (job_name,window_start,window_end)
    DO UPDATE SET status=EXCLUDED.status,
                  inserted_rows=EXCLUDED.inserted_rows,
                  updated_rows=EXCLUDED.updated_rows;
    """
    execute(sql, (JOB, d, d, status, ins, upd))

def main():
    snap = date.today()
    upsert_job_marker("STARTED", snap)

    # Kreditlərin snapshot üçün qaynağı:
    # - dpd: today - due_date (due_date: next_due_date -> enddatum -> auszahlungsdatum)
    rows = fetchall("""
        WITH last_pay AS (
          SELECT
            z.kredit_id,
            z.kunden_konto_nummer,
            z.konto_sub,
            -- ən son ödəniş
            MAX(z.zahlung_zeitpunkt) AS last_dt
          FROM zahlungen z
          GROUP BY 1,2,3
        ),
        pay_det AS (
          SELECT
            z.kredit_id, z.kunden_konto_nummer, z.konto_sub, z.zahlung_zeitpunkt,
            z.betrag_principal, z.betrag_zinsen, z.betrag_strafe
          FROM zahlungen z
          JOIN last_pay lp
            ON lp.kredit_id = z.kredit_id
           AND lp.kunden_konto_nummer = z.kunden_konto_nummer
           AND lp.konto_sub = z.konto_sub
           AND lp.last_dt = z.zahlung_zeitpunkt
        )
        SELECT
          k.kredit_id,
          k.kunden_konto_nummer,
          k.konto_sub,
          k.kredit_art,
          k.filiale_code::text AS filiale_code,
          -- due_date seçimi (sıra dəyişməyib)
          COALESCE(k.next_due_date, k.enddatum, k.auszahlungsdatum) AS due_date,
          pd.zahlung_zeitpunkt AS last_pay_dt,
          pd.betrag_principal  AS last_p,
          pd.betrag_zinsen     AS last_i,
          pd.betrag_strafe     AS last_s
        FROM kredit_stammdaten k
        LEFT JOIN pay_det pd
          ON pd.kredit_id = k.kredit_id
         AND pd.kunden_konto_nummer = k.kunden_konto_nummer
         AND pd.konto_sub = k.konto_sub
    """)  # dict cursor 

    if not rows:
        upsert_job_marker("DONE", snap, ins=0, upd=0)
        print(f"[{JOB}] DONE snapshot={snap} upserted=0")
        return

    # dpd -> bucket/stufe 
    def map_bucket_stufe(dpd: int):
        if dpd <= 0:
            return "BUCKET_0", "INIT"
        if dpd <= 30:
            return "BUCKET_1", "CHECK"
        if dpd <= 60:
            return "BUCKET_2", "REVIEW"
        if dpd <= 90:
            return "BUCKET_3", "LEGAL_PREP"
        if dpd <= 180:
            return "BUCKET_4", "LEGAL"
        return "BUCKET_5", "ENFORCEMENT"

    payload = []
    for r in rows:
        due = r["due_date"]
        # due_date  dpd=0 
        dpd = 0
        if due is not None:
            dpd = max((snap - due).days, 0)

        bucket, stufe = map_bucket_stufe(dpd)

        payload.append((
            r["kredit_id"],
            r["kunden_konto_nummer"],
            r["konto_sub"],
            snap,
            dpd,
            bucket,
            stufe,
            r["kredit_art"],                
            r["last_pay_dt"],
            r["last_p"] or 0,
            r["last_i"] or 0,
            r["last_s"] or 0,
            r["filiale_code"],
        ))

    sql = """
    INSERT INTO creditflow(
      kredit_id, kunden_konto_nummer, konto_sub, snapshot_datum,
      dpd, bucket, stufe, kredit_art,
      letzte_zahlung_datum, letzte_betrag_principal, letzte_betrag_zinsen,
      letzte_betrag_strafe, filiale_code
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (kredit_id, kunden_konto_nummer, konto_sub, snapshot_datum)
    DO UPDATE SET
      dpd = EXCLUDED.dpd,
      bucket = EXCLUDED.bucket,
      stufe = EXCLUDED.stufe,
      kredit_art = EXCLUDED.kredit_art,
      letzte_zahlung_datum = EXCLUDED.letzte_zahlung_datum,
      letzte_betrag_principal = EXCLUDED.letzte_betrag_principal,
      letzte_betrag_zinsen = EXCLUDED.letzte_betrag_zinsen,
      letzte_betrag_strafe = EXCLUDED.letzte_betrag_strafe,
      filiale_code = EXCLUDED.filiale_code;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, payload)
        conn.commit()

    upsert_job_marker("DONE", snap, ins=len(payload), upd=0)
    print(f"[{JOB}] DONE snapshot={snap} upserted={len(payload)}")


if __name__ == "__main__":
    main()
