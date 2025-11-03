# -*- coding: utf-8 -*-
# kdata_platform/scripts/job_backfill_overdue_credits.py
from datetime import date
from kdata_platform.db import execute

JOB = "JOB_Z_BACKFILL_OVERDUE_CREDITS"

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

def main(total=5000, start_iso="2015-01-01"):
    today = date.today()
    upsert_job_marker("STARTED", today, today)

    ins_sql = """
    WITH loan_map(art, min_r, max_r, lo_amt, hi_amt) AS (
      VALUES
        ('Verbraucherkredit',  0.12, 0.19,   3000::numeric,   25000::numeric),
        ('Autokredit',         0.07, 0.14,   4000::numeric,   60000::numeric),
        ('Baufinanzierung',    0.02, 0.06,  50000::numeric,  500000::numeric),
        ('Unternehmenskredit', 0.09, 0.18,  10000::numeric,  600000::numeric),
        ('Investitionskredit', 0.06, 0.12,  10000::numeric,  300000::numeric),
        ('Kontokorrentkredit', 0.10, 0.20,   2000::numeric,   50000::numeric),
        ('Kreditkarte-Linie',  0.12, 0.25,   1000::numeric,    20000::numeric),
        ('Avalkredit',         0.02, 0.07,   5000::numeric,   500000::numeric)
    ),
    g AS (
      SELECT
        gs AS seq,
        LPAD(((random()*9000)::int + 1000)::text, 4, '0')                        AS kredit_id,
        LPAD(((random()*900000)::int + 100000)::text, 6, '0')                    AS kunden_konto_nummer,
        LPAD(((random()*1000)::int)::text, 3, '0')                               AS konto_sub,
        COALESCE((ARRAY['FB013','RB010','AB001','MB002'])[floor(random()*4)::int + 1], 'FB000') AS filiale_code,
        COALESCE((ARRAY['Berlin Stadt Bank','München Stadt Bank','Köln Stadt Bank','Bonn Stadt Bank'])
                 [floor(random()*4)::int + 1], 'Unbekannte Filiale')             AS filiale_name,
        (DATE %s + ((random() * (CURRENT_DATE - DATE %s))::int) )                AS auszahlungsdatum,
        (SELECT art  FROM loan_map ORDER BY random() LIMIT 1)                     AS kredit_art,
        (SELECT round((min_r + random()*(max_r-min_r))::numeric, 4)
           FROM loan_map ORDER BY random() LIMIT 1)                               AS zinssatz_p_a,
        (SELECT round((lo_amt + random()*(hi_amt-lo_amt))::numeric, 2)
           FROM loan_map ORDER BY random() LIMIT 1)                               AS hauptbetrag,
        CASE WHEN random() < 0.7 THEN 'EUR' ELSE 'USD' END                        AS waehrung,
        ((random()*72)::int + 12)                                                 AS term_months,
        ('Kunde ' || ((random()*900000)::int + 100000)::text)                     AS muster_name,
        ('kunde' || ((random()*9000000)::int + 1000000)::text || '@example.net')  AS email,
        COALESCE((ARRAY['Berlin','München','Köln','Bonn','Hamburg','Leipzig'])
                 [floor(random()*6)::int + 1], 'Berlin')                          AS adresse_stadt,
        COALESCE(('Strasse ' || ((random()*500)::int + 1)::text), 'Strasse 1')    AS adresse_strasse,
        LPAD((floor(random()*1e10))::bigint::text, 10, '0')                       AS telefon,
        CASE
          WHEN random() < 0.45
            THEN (CURRENT_DATE - (1 + floor(random()*180))::int)
            ELSE (CURRENT_DATE + (1 + floor(random()*60))::int)
        END AS next_due_date,
        CASE
          WHEN random() < 0.45
            THEN ((CURRENT_DATE - (1 + floor(random()*180))::int) - (15 + floor(random()*46))::int)
            ELSE  (CURRENT_DATE - (5 + floor(random()*56))::int)
        END AS letzte_zahlung_datum
      FROM generate_series(1, %s) AS gs
    ),
    enriched AS (
      SELECT
        g.*,
        CASE
          WHEN g.zinssatz_p_a > 0 THEN round(
            (g.hauptbetrag * (g.zinssatz_p_a/12.0)) /
            NULLIF(1 - POWER(1 + (g.zinssatz_p_a/12.0), -g.term_months), 0), 2)
          ELSE round(g.hauptbetrag / NULLIF(g.term_months,0), 2)
        END AS monthly_payment_calc,
        g.hauptbetrag AS rest_principal_calc,
        GREATEST((CURRENT_DATE - g.next_due_date)::int, 0) AS dpd_calc,
        CASE
          WHEN (CURRENT_DATE - g.next_due_date) <= 0 THEN 0
          WHEN (CURRENT_DATE - g.next_due_date) <= 30 THEN round(g.hauptbetrag * (g.zinssatz_p_a / 12.0), 2)
          WHEN (CURRENT_DATE - g.next_due_date) <= 90 THEN round(g.hauptbetrag * (g.zinssatz_p_a / 6.0), 2)
          ELSE round(g.hauptbetrag * (g.zinssatz_p_a / 2.0), 2)
        END AS interest_debt_calc,
        CASE
          WHEN (CURRENT_DATE - g.next_due_date) <= 0 THEN 0
          WHEN (CURRENT_DATE - g.next_due_date) <= 30 THEN 0
          WHEN (CURRENT_DATE - g.next_due_date) <= 90 THEN round(g.hauptbetrag * 0.01, 2)
          ELSE round(g.hauptbetrag * 0.03, 2)
        END AS penalty_debt_calc
      FROM g
    )
    INSERT INTO kredit_stammdaten(
      kredit_id, kunden_konto_nummer, konto_sub,
      filiale_code, filiale_name,
      muster_id, muster_name, muster_vatername, telefon, email,
      adresse_stadt, adresse_strasse,
      kredit_art, hauptbetrag, waehrung,
      zinssatz_p_a, auszahlungsdatum, enddatum,
      letzte_zahlung_betrag, letzte_zahlung_datum,
      strafgebuehr_betrag,
      buerge_vorname, buerge_nachname, buerge_telefon, buerge_adresse_stadt, buerge_adresse_strasse,
      sachbearbeiter_name, ausweis_id, vertragsnummer,
      vertragsstatus, erstellt_am, aktualisiert_am,
      term_months, next_due_date,
      rest_principal, interest_debt, penalty_debt, monthly_payment
    )
    SELECT
      e.kredit_id, e.kunden_konto_nummer, e.konto_sub,
      e.filiale_code, e.filiale_name,
      ((random()*90000)::int + 10000)                                            AS muster_id,
      e.muster_name, 'Mustermann'                                                AS muster_vatername,
      e.telefon, e.email,
      e.adresse_stadt, e.adresse_strasse,
      e.kredit_art, e.hauptbetrag, e.waehrung,
      e.zinssatz_p_a, e.auszahlungsdatum,
      (e.auszahlungsdatum + ((e.term_months*30)::int))::date                     AS enddatum,
      0.00::numeric, e.letzte_zahlung_datum,
      0.00::numeric,
      'Vorname' || ((random()*999)::int)::text, 'Nachname' || ((random()*999)::int)::text,
      LPAD((floor(random()*1e10))::bigint::text, 10, '0'),
      COALESCE((ARRAY['Berlin','Hamburg','Köln','Bonn'])[floor(random()*4)::int + 1], 'Berlin'),
      COALESCE(('Strasse ' || ((random()*300)::int + 1)::text), 'Strasse 1'),
      'Sachbearbeiter ' || ((random()*9000)::int + 1000)::text,
      'ID' || ((random()*90000000)::int + 10000000)::text,
      (e.filiale_code || 'X' || TO_CHAR(e.auszahlungsdatum,'YYYYMMDD') || RIGHT(e.kunden_konto_nummer,6)),
      'AKTIV',
      now(), now(),
      e.term_months, e.next_due_date,
      e.rest_principal_calc, e.interest_debt_calc, e.penalty_debt_calc, e.monthly_payment_calc
    FROM enriched e
    ON CONFLICT (kredit_id, kunden_konto_nummer, konto_sub) DO NOTHING;
    """

    # (start_iso, start_iso, total)
    execute(ins_sql, (start_iso, start_iso, total))

    upsert_job_marker("DONE", today, today, ins=total, upd=0)
    print(f"[{JOB}] DONE: inserted≈{total}, window={start_iso}..{today}")

if __name__ == "__main__":
    main()
