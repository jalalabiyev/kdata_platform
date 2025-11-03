# -*- coding: utf-8 -*-
from datetime import date
import random
from kdata_platform.db import fetchall, get_conn

JOB = "JOB_D_ROUTING"

DEPTS = ["EARLY", "MID", "LEGAL_PREP", "LEGAL", "ENFORCEMENT"]
AGENTS_PER_DEPT = 50

AGENT_NAMES = [
    "Fabian Klement", "Heinz-Willi Mentzel", "Julia Hartmann", "Nina Schubert",
    "Thomas Reiter", "Sabine Adler", "Lukas Neumann", "Klara Voigt",
    "Paul Schneider", "Mia Krüger"
]

COMMENTS_DE = [
    "Zahlung ist überfällig",
    "Teilzahlung eingegangen",
    "An das Gericht weitergeleitet",
    "Kunde wurde kontaktiert",
    "Vom Gehalt abgezogen",
    "Unter Beobachtung",
    "Zahlung erwartet",
    "Konto geschlossen",
]

def dept_for_stufe(stufe: str) -> str:
    return {
        "INIT": "EARLY",
        "CHECK": "EARLY",
        "REVIEW": "MID",
        "LEGAL_PREP": "LEGAL_PREP",
        "LEGAL": "LEGAL",
        "ENFORCEMENT": "ENFORCEMENT",
    }.get(stufe or "", "EARLY")

def main():
    snap = date.today()

    #  snapshot
    rows = fetchall("""
      SELECT
        kredit_id, kunden_konto_nummer, konto_sub,
        dpd, bucket, stufe
      FROM creditflow
      WHERE snapshot_datum = %s AND dpd > 0;
    """, (snap,))

    if not rows:
        print(f"[{JOB}] no records found to route for {snap}")
        return

    # 1..50 agentlərə round-robin 
    buckets = {d: [] for d in DEPTS}
    for r in rows:
        d = dept_for_stufe(r["stufe"])
        buckets[d].append(r)

    assigns = []
    for dept, items in buckets.items():
        for i, r in enumerate(items):
            agent_id = (i % AGENTS_PER_DEPT) + 1
            mit_id = f"A{20000 + agent_id:05d}"
            mit_name = AGENT_NAMES[i % len(AGENT_NAMES)]
            kommentar = random.choice(COMMENTS_DE)

            assigns.append((
                r["kredit_id"], r["kunden_konto_nummer"], r["konto_sub"],
                dept, agent_id, snap,
                mit_id, mit_name,
                r["dpd"], r["bucket"], r["stufe"],
                "ASSIGNED", kommentar
            ))

    # zuweisung 
    sql = """
    INSERT INTO zuweisung(
      kredit_id, kunden_konto_nummer, konto_sub,
      dept, agent_id, snapshot_datum,
      mit_id, mit_name,
      dpd, bucket, stufe,
      status, kommentar
    ) VALUES (
      %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    ON CONFLICT (kredit_id, kunden_konto_nummer, konto_sub, snapshot_datum)
    DO UPDATE SET
      dept = EXCLUDED.dept,
      agent_id = EXCLUDED.agent_id,
      mit_id = EXCLUDED.mit_id,
      mit_name = EXCLUDED.mit_name,
      dpd = EXCLUDED.dpd,
      bucket = EXCLUDED.bucket,
      stufe = EXCLUDED.stufe,
      status = EXCLUDED.status,
      kommentar = EXCLUDED.kommentar;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.executemany(sql, assigns)
        conn.commit()

    print(f"[{JOB}] assigned {len(assigns)} records for {snap}")

if __name__ == "__main__":
    main()
