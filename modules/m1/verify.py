from __future__ import annotations

import psycopg

from .common import CSV_OUT_DIR, DATABASE_URL, GRAPH_PATH, META_PATH


def verify() -> None:
    """Aggregate verifier for Module 1."""
    # constraints present?
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        for tbl, con in [
            ("document", "uq_document_sha256"),
            ("rule", "uq_rule_code"),
            ("event", "uq_event_key"),
        ]:
            cur.execute(
                """
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE c.conname=%s AND t.relname=%s AND n.nspname='public';
                """,
                (con, tbl),
            )
            ok = cur.fetchone() is not None
            print(f"[m1.2] unique {tbl}.{con}: {'OK' if ok else 'MISSING'}")
            if not ok:
                raise SystemExit("[m1] FAILED: missing uniqueness")

        # counts & geom validity
        cur.execute("SELECT COUNT(*) FROM asset;")
        assets = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM permit;")
        permits = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM organization;")
        orgs = cur.fetchone()[0]
        print(f"[m1.3] counts asset={assets}, permit={permits}, org={orgs}")
        if not (assets >= 3 and permits >= 5 and orgs >= 2):
            raise SystemExit("[m1] FAILED: counts outside expected")

        for tbl, col in [
            ("asset", "footprint"),
            ("asset", "centroid"),
            ("permit", "geom"),
        ]:
            cur.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL AND NOT ST_IsValid({col});"
            )
            bad = cur.fetchone()[0]
            print(f"[m1.3] geom {tbl}.{col}: invalid={bad}")
            if bad != 0:
                raise SystemExit("[m1] FAILED: invalid geometries")

    # artifacts exist?
    csv_ok = all(
        (CSV_OUT_DIR / f"{t}.csv").exists()
        for t in [
            "organization",
            "asset",
            "permit",
            "document",
            "policy_clause",
            "satellite_tile",
            "iot_stream",
            "event",
            "rule",
            "proof_bundle",
        ]
    )
    graph_ok = GRAPH_PATH.exists() and META_PATH.exists()
    print(f"[m1.4] csv_all={'OK' if csv_ok else 'MISSING'}")
    print(f"[m1.4] graphml={'OK' if graph_ok else 'MISSING'}")
    if not (csv_ok and graph_ok):
        raise SystemExit("[m1] FAILED: export artifacts missing")

    print("[m1] PASSED")
