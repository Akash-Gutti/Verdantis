import os

import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL not set"


def q(cur, sql_txt, params=None):
    cur.execute(sql_txt, params or ())
    return cur.fetchall()


def main():
    ok = True
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Counts
            assets = q(cur, "SELECT COUNT(*) FROM asset;")[0][0]
            permits = q(cur, "SELECT COUNT(*) FROM permit;")[0][0]
            orgs = q(cur, "SELECT COUNT(*) FROM organization;")[0][0]
            print(f"[counts] asset={assets}, permit={permits}, org={orgs}")
            ok &= assets >= 3 and assets <= 20  # slightly lenient
            ok &= permits >= 5 and permits <= 50
            ok &= orgs >= 2

            # Geometry validity
            bad = q(
                cur,
                (
                    "SELECT COUNT(*) FROM asset "
                    "WHERE footprint IS NOT NULL "
                    "AND NOT ST_IsValid(footprint);"
                ),
            )[0][0]
            print(f"[geom] asset.footprint invalid={bad}")
            ok &= bad == 0
            bad = q(
                cur,
                (
                    "SELECT COUNT(*) FROM asset "
                    "WHERE centroid IS NOT NULL "
                    "AND NOT ST_IsValid(centroid);"
                ),
            )[0][0]
            print(f"[geom] asset.centroid invalid={bad}")
            ok &= bad == 0
            bad = q(
                cur,
                (
                    "SELECT COUNT(*) FROM permit "
                    "WHERE geom IS NOT NULL "
                    "AND NOT ST_IsValid(geom);"
                ),
            )[0][0]
            print(f"[geom] permit.geom invalid={bad}")
            ok &= bad == 0

            # Link integrity
            orphans = q(cur, "SELECT COUNT(*) FROM permit WHERE asset_id IS NULL;")[0][0]
            print(f"[links] permits without asset_id={orphans}")
            ok &= orphans == 0

            # Sample preview
            cur.execute(
                """
                SELECT a.name, a.kind, a.country_code,
                       ST_AsGeoJSON(a.centroid) AS centroid,
                       o.name AS owner
                FROM asset a
                LEFT JOIN organization o ON a.owner_org_id = o.id
                ORDER BY a.created_at DESC
                LIMIT 3;
            """
            )
            print("[sample assets]")
            for r in cur.fetchall():
                print("  -", r)

    if not ok:
        raise SystemExit("M1.3 verification failed (see logs).")
    print("M1.3 verification passed.")


if __name__ == "__main__":
    main()
