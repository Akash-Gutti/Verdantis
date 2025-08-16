import json
import os
from datetime import date, timedelta
from glob import glob
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv
from psycopg import sql

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL not set in .env"

# ---------- PATH LOCKS (your structure) ----------
ASSETS_PATH = Path("data/raw/assets/assets.geojson")
PERMITS_PDF_GLOBS = [
    "data/raw/permits/Permit_synth_*.pdf",
    "data/raw/permits/Permit_real_*.pdf",
]
# -------------------------------------------------


def _normalize_org_key(name: str | None, country_code: str | None) -> tuple[str, str]:
    name = (name or "").strip().lower()
    country_code = (country_code or "").strip().upper()
    return name, country_code


def _load_assets_geojson() -> list[dict]:
    if not ASSETS_PATH.exists():
        raise FileNotFoundError(
            f"Assets GeoJSON not found at {ASSETS_PATH}. "
            "Place your file exactly at data/raw/assets/assets.geojson."
        )
    gj = json.loads(ASSETS_PATH.read_text(encoding="utf-8"))
    feats = gj.get("features", [])
    if not feats:
        raise ValueError(f"No features found in {ASSETS_PATH}")
    print(f"✓ Using assets from {ASSETS_PATH} (features={len(feats)})")
    return feats


def _build_permit_manifest(asset_names: list[str]) -> pd.DataFrame:
    pdfs = sorted({p for pat in PERMITS_PDF_GLOBS for p in glob(pat)})
    if not pdfs:
        raise FileNotFoundError(
            "No permit PDFs found. Expected files matching:\n  " + "\n  ".join(PERMITS_PDF_GLOBS)
        )
    rows = []
    names = sorted(asset_names)
    today = date.today()
    for i, pdf in enumerate(pdfs):
        stem = Path(pdf).stem  # e.g., Permit_synth_001
        rows.append(
            {
                "asset_name": names[i % len(names)],
                "org_name": "Regulatory Authority",
                "permit_type": "Environmental Permit",
                "status": "active",
                "issue_date": (today - timedelta(days=120 + i * 2)).isoformat(),
                "expiry_date": (today + timedelta(days=365)).isoformat(),
                "reference_id": stem,
                "country_code": "AE",
                "geom_geojson": "",
            }
        )
    print(f"✓ Built permit manifest from PDFs → rows={len(rows)}")
    return pd.DataFrame(rows)


def upsert_org(cur, name: str | None, org_type: str | None, country_code: str | None) -> str | None:
    if not name:
        return None
    key_name, key_cc = _normalize_org_key(name, country_code)
    cur.execute(
        """
        SELECT id FROM organization
        WHERE lower(name)=%s AND COALESCE(country_code,'')=COALESCE(%s,'');
        """,
        (key_name, key_cc or None),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        """
        INSERT INTO organization (name, org_type, country_code)
        VALUES (%s, %s, %s)
        RETURNING id;
        """,
        (name.strip(), org_type, (country_code or None)),
    )
    return cur.fetchone()[0]


def insert_asset(cur, feat: dict) -> str:
    props = feat.get("properties", {})
    geom = feat.get("geometry")
    gjson = json.dumps(geom) if geom else None

    owner_org_name = props.get("owner_org") or props.get("owner") or props.get("operator") or None
    owner_org_id = upsert_org(cur, owner_org_name, "owner", props.get("country_code"))

    # Cast to text; ST_GeomFromGeoJSON(NULL::text) -> NULL
    cur.execute(
        sql.SQL(
            """
            WITH g AS (
                SELECT
                    ST_SetSRID(
                        ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))),
                        4326
                    ) AS fp
            )
            INSERT INTO asset
                (name, kind, status, owner_org_id, centroid, footprint,
                 address, city, region, country_code)
            SELECT
                %s, %s, %s, %s,
                ST_Centroid(g.fp), g.fp,
                %s, %s, %s, %s
            FROM g
            RETURNING id;
            """
        ),
        (
            gjson,
            props.get("name"),
            props.get("kind"),
            props.get("status") or "active",
            owner_org_id,
            props.get("address"),
            props.get("city"),
            props.get("region"),
            props.get("country_code"),
        ),
    )
    return cur.fetchone()[0]


def insert_permit(cur, row: pd.Series, asset_name_to_id: dict) -> str:
    asset_id = asset_name_to_id.get(str(row.get("asset_name")))
    org_id = upsert_org(cur, row.get("org_name"), "regulator/issuer", row.get("country_code"))
    gjson = row.get("geom_geojson")
    gjson = None if (pd.isna(gjson) or str(gjson).strip() == "") else str(gjson)
    cur.execute(
        sql.SQL(
            """
            WITH g AS (
                SELECT
                    ST_SetSRID(
                        ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))),
                        4326
                    ) AS gm
            )
            INSERT INTO permit
                (asset_id, org_id, permit_type, status, issue_date, expiry_date,
                 document_id, reference_id, geom)
            SELECT
                %s, %s, %s, %s,
                %s::date, %s::date, NULL,
                %s, g.gm
            FROM g
            RETURNING id;
            """
        ),
        (
            gjson,
            asset_id,
            org_id,
            row.get("permit_type"),
            row.get("status"),
            row.get("issue_date"),
            row.get("expiry_date"),
            row.get("reference_id"),
        ),
    )
    return cur.fetchone()[0]


def main():
    features = _load_assets_geojson()
    asset_names = [
        f.get("properties", {}).get("name", f"asset_{i}") for i, f in enumerate(features)
    ]
    permits_df = _build_permit_manifest(asset_names)

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Hard reset so we overwrite prior demo rows (no dupes)
            print("! RESET: truncating permit and asset (CASCADE)")
            cur.execute("TRUNCATE TABLE permit RESTART IDENTITY CASCADE;")
            cur.execute("TRUNCATE TABLE asset  RESTART IDENTITY CASCADE;")

            # Seed assets
            asset_name_to_id = {}
            for feat in features:
                name = feat.get("properties", {}).get("name")
                aid = insert_asset(cur, feat)
                asset_name_to_id[name] = aid

            # Seed permits (round-robin map PDFs -> assets)
            for _, row in permits_df.iterrows():
                aname = str(row.get("asset_name"))
                if aname not in asset_name_to_id:
                    raise RuntimeError(f"Unknown asset in permit manifest: {aname}")
                insert_permit(cur, row, asset_name_to_id)

        conn.commit()

    print(f"✓ Seed complete → assets={len(asset_name_to_id)} permits={len(permits_df)}")


if __name__ == "__main__":
    main()
