import os

import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL not set"

UNIQUES = [
    ("document", "uq_document_sha256"),
    ("rule", "uq_rule_code"),
    ("event", "uq_event_key"),
]

GEOM_CHECKS = [
    ("asset", "centroid"),
    ("asset", "footprint"),
    ("permit", "geom"),
    ("satellite_tile", "footprint"),
    ("iot_stream", "location"),
]


def main():
    ok = True
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Check unique constraints exist
            for tbl, con in UNIQUES:
                cur.execute(
                    """
                    SELECT 1
                    FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    JOIN pg_namespace n ON t.relnamespace = n.oid
                    WHERE c.conname = %s AND t.relname = %s AND n.nspname='public';
                    """,
                    (con, tbl),
                )
                present = cur.fetchone() is not None
                print(f"[unique] {tbl}.{con}: {'OK' if present else 'MISSING'}")
                ok &= present

            # Check invalid geometries count
            for tbl, col in GEOM_CHECKS:
                cur.execute(
                    psycopg.sql.SQL(
                        "SELECT COUNT(*) FROM {} WHERE {} IS NOT NULL AND NOT ST_IsValid({});"
                    ).format(
                        psycopg.sql.Identifier(tbl),
                        psycopg.sql.Identifier(col),
                        psycopg.sql.Identifier(col),
                    )
                )
                bad = cur.fetchone()[0]
                status = "OK" if bad == 0 else f"INVALID={bad}"
                print(f"[geom] {tbl}.{col}: {status}")
                ok &= bad == 0

            # Quick duplicates probe (should be 0 with constraints)
            cur.execute("SELECT COUNT(*) FROM document d GROUP BY d.doc_sha256 HAVING COUNT(*)>1;")
            dup_docs = len(cur.fetchall())
            cur.execute("SELECT COUNT(*) FROM rule r GROUP BY r.rule_code HAVING COUNT(*)>1;")
            dup_rules = len(cur.fetchall())
            cur.execute("SELECT COUNT(*) FROM event e GROUP BY e.event_key HAVING COUNT(*)>1;")
            dup_events = len(cur.fetchall())
            print(f"[dupes] document={dup_docs}, rule={dup_rules}, event={dup_events}")
            ok &= dup_docs == dup_rules == dup_events == 0

    if not ok:
        raise SystemExit("M1.2 verification failed (see logs).")
    print("M1.2 verification passed.")


if __name__ == "__main__":
    main()
