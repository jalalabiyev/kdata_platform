# kdata_platform/scripts/job_portfolio_daily.py
import random
import string
from datetime import date
from faker import Faker

from kdata_platform.db import execute

JOB = "JOB_A_PORTFOLIO_DAILY"
fake = Faker("de_DE")


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


def _digits(n: int) -> str:
    return "".join(random.choices(string.digits, k=n))


def main():
    today = date.today()
    win_start = today
    win_end = today
    upsert_job_marker("STARTED", win_start, win_end)

    # 0) Schema —  waehrung_code)
    execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='rest_principal'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN rest_principal numeric(14,2);
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='interest_debt'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN interest_debt numeric(14,2) DEFAULT 0;
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='penalty_debt'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN penalty_debt numeric(14,2) DEFAULT 0;
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='monthly_payment'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN monthly_payment numeric(14,2);
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='next_due_date'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN next_due_date date;
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='term_months'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN term_months int;
      END IF;

      -- ADDED: kredit_art (kredit növü) sütunu təminatı
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='kredit_art'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN kredit_art text;
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='zinssatz_p_a'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN zinssatz_p_a numeric(6,4);
      END IF;

      -- YENİ: zahlungen-lə eyni addə bir sütun
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='kredit_stammdaten' AND column_name='waehrung_code'
      ) THEN
        ALTER TABLE kredit_stammdaten ADD COLUMN waehrung_code text;
      END IF;
    END
    $$;
    """)

    # 1)  (random)
    execute("""
        UPDATE kredit_stammdaten
           SET vertragsstatus = 'GESCHLOSSEN',
               aktualisiert_am = now()
         WHERE vertragsstatus <> 'GESCHLOSSEN'
           AND random() < 0.01;
    """)

    # 2) 300  kredit – generator (70% EUR / 30% USD)
    ins_count = 300
    ins_sql = """
    WITH loan_map(art, min_r, max_r) AS (
      VALUES
        ('Verbraucherkredit', 0.12, 0.19),
        ('Autokredit',        0.07, 0.14),
        ('Baufinanzierung',   0.02, 0.06),
        ('Unternehmenskredit',0.09, 0.18),
        ('Investitionskredit',0.06, 0.12),
        ('Kontokorrentkredit',0.10, 0.20),
        ('Kreditkarte-Linie', 0.12, 0.25),
        ('Avalkredit',        0.02, 0.07)
    ),
    gen AS (
      SELECT
        LPAD((floor(random()*9000)+1000)::text, 4, '0')                      AS kredit_id,
        LPAD((floor(random()*900000)+100000)::text, 6, '0')                  AS kunden_konto_nummer,
        LPAD((floor(random()*1000))::text, 3, '0')                           AS konto_sub,
        (ARRAY['FB013','RB010','AB001','MB002'])[floor(random()*4)+1]        AS filiale_code,
        (ARRAY['Berlin Stadt Bank','München Stadt Bank','Köln Stadt Bank','Bonn Stadt Bank'])
          [floor(random()*4)+1]                                              AS filiale_name,
        (floor(random()*90000)+10000)                                        AS muster_id,
        %s                                                                    AS muster_name,
        'Mustermann'                                                          AS muster_vatername,
        LPAD((floor(random()*10^10))::text, 10, '0')                         AS telefon,
        %s                                                                    AS email,
        %s                                                                    AS adresse_stadt,
        %s                                                                    AS adresse_strasse,
        (ARRAY['Verbraucherkredit','Autokredit','Baufinanzierung','Unternehmenskredit',
                'Investitionskredit','Kontokorrentkredit','Kreditkarte-Linie','Avalkredit'])
          [floor(random()*8)+1]                                              AS kredit_art,
        round((random()*90000+1000)::numeric, 2)                              AS hauptbetrag,
        (CASE WHEN random() < 0.7 THEN 'EUR' ELSE 'USD' END)                 AS waehrung,
        (floor(random()*48)+12)                                              AS term_months_gen,
        CURRENT_DATE                                                          AS auszahlungsdatum,
        CURRENT_DATE + (floor(random()*720)+90)::int                         AS enddatum,
        %s                                                                    AS sachbearbeiter_name,
        %s                                                                    AS ausweis_id
      FROM generate_series(1,%s)
    ),
    enriched AS (
      SELECT
        g.*,
        -- növə uyğun faiz: min + u*(max-min)
        round((lm.min_r + (random() * (lm.max_r - lm.min_r)))::numeric, 4) AS zinssatz_p_a,
        ( (lm.min_r + (random() * (lm.max_r - lm.min_r)))::numeric / 12 )  AS r_month,
        g.term_months_gen                                                  AS term_months_calc,
        CASE
          WHEN (lm.min_r + (random() * (lm.max_r - lm.min_r))) > 0 THEN
            round(
              (
                g.hauptbetrag::numeric
                * ((lm.min_r + (random() * (lm.max_r - lm.min_r)))::numeric / 12)
              )
              / NULLIF(
                  1 - power(1 + ((lm.min_r + (random() * (lm.max_r - lm.min_r)))::numeric / 12),
                            -g.term_months_gen::numeric),
                  0
                )
            , 2)
          ELSE
            round((g.hauptbetrag / NULLIF(g.term_months_gen,0))::numeric, 2)
        END AS monthly_payment_calc,
        g.hauptbetrag                         AS rest_principal_calc,
        0.00::numeric                         AS interest_debt_calc,
        0.00::numeric                         AS penalty_debt_calc,
        CURRENT_DATE + INTERVAL '30 day'      AS next_due_date_calc
      FROM gen g
      JOIN loan_map lm ON lm.art = g.kredit_art
    )
    INSERT INTO kredit_stammdaten
    (kredit_id, kunden_konto_nummer, konto_sub, filiale_code, filiale_name,
     muster_id, muster_name, muster_vatername, telefon, email,
     adresse_stadt, adresse_strasse, kredit_art, hauptbetrag, waehrung,
     waehrung_code,  -- YENİ: zahlungen-lə eyni adlı sütun
     zinssatz_p_a, auszahlungsdatum, enddatum, letzte_zahlung_betrag,
     letzte_zahlung_datum, strafgebuehr_betrag, buerge_vorname, buerge_nachname,
     buerge_telefon, buerge_adresse_stadt, buerge_adresse_strasse,
     sachbearbeiter_name, ausweis_id, vertragsnummer,
     vertragsstatus, erstellt_am, aktualisiert_am,
     rest_principal, interest_debt, penalty_debt, monthly_payment, next_due_date, term_months)
    SELECT
      e.kredit_id, e.kunden_konto_nummer, e.konto_sub, e.filiale_code, e.filiale_name,
      e.muster_id, e.muster_name, e.muster_vatername, e.telefon, e.email,
      e.adresse_stadt, e.adresse_strasse, e.kredit_art, e.hauptbetrag, e.waehrung,
      e.waehrung,     -- waehrung_code = eyni dəyər (EUR/USD)
      e.zinssatz_p_a, e.auszahlungsdatum, e.enddatum,
      NULL::numeric, NULL::date, NULL::numeric,
      NULL::text, NULL::text, NULL::text, NULL::text, NULL::text,
      e.sachbearbeiter_name, e.ausweis_id,
      (e.filiale_code || UPPER(SUBSTR(e.filiale_name,1,2))
         || TO_CHAR(CURRENT_DATE,'YYYYMMDD')
         || RIGHT(e.kunden_konto_nummer,6))            AS vertragsnummer,
      'AKTIV' AS vertragsstatus,
      now()   AS erstellt_am,
      now()   AS aktualisiert_am,
      e.rest_principal_calc,
      e.interest_debt_calc,
      e.penalty_debt_calc,
      e.monthly_payment_calc,
      e.next_due_date_calc::date,
      e.term_months_calc
    FROM enriched e
    ON CONFLICT (kredit_id, kunden_konto_nummer, konto_sub) DO NOTHING;
    """

    # SQL-də 7 ədəd %s  → 7 parametr 
    execute(
        ins_sql,
        (
            fake.name(),            # muster_name
            fake.email(),           # email
            fake.city(),            # adresse_stadt
            fake.street_address(),  # adresse_strasse
            fake.name(),            # sachbearbeiter_name
            f"ID{_digits(8)}",      # ausweis_id
            ins_count,              # generate_series üçün say
        ),
    )

    upsert_job_marker("DONE", win_start, win_end, ins=ins_count, upd=0)
    print(f"[{JOB}] DONE: inserted={ins_count}")


if __name__ == "__main__":
    main()
