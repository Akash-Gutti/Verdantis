# scripts/1.3_seed.py
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

# ---------- PATH LOCKS ----------
ASSETS_PATHS = [
    Path("data/raw/assets/assets.geo.json"),  # your repo path
    Path("data/raw/geo/assets.geojson"),  # fallback
    Path("data/raw/assets.geojson"),  # fallback
]
PERMITS_CSV_PATHS = [
    Path("data/raw/permits/permits.csv"),  # optional if you add metadata later
    Path("data/raw/seed/permits.csv"),  # optional fallback
]
PERMITS_PDF_GLOBS = [
    "data/raw/permits/permit_synth.0*.pdf",  # your current naming
    "data/raw/permits/permit_synth.*.pdf",  # broader catch
]
# ----------------------------------


def _normalize_org_key(name: str | None, country_code: str | None) -> tuple[str, str]:
    name = (name or "").strip().lower()
    country_code = (country_code or "").strip().upper()
    return name, country_code


def _load_assets_geojson() -> list[dict]:
    print("→ Checking asset GeoJSON paths (in order):")
    for p in ASSETS_PATHS:
        print(f"   - {p}")
        if p.exists():
            gj = json.loads(p.read_text(encoding="utf-8"))
            feats = gj.get("features", [])
            if feats:
                print(f"✓ Using assets from {p} (features={len(feats)})")
                return feats

    # Fallback: generate 3 demo assets
    print("! assets file not found; generating minimal demo assets")
    features = []
    demo = [
        ("Shams Solar Park A", "Dubai", "Dubai", "AE", 55.2708, 25.2048, 0.02),
        ("Sir Bani Yas Wind B", "Abu Dhabi", "Abu Dhabi", "AE", 54.3773, 24.4539, 0.025),
        ("Riyadh Desal C", "Riyadh", "Riyadh", "SA", 46.6753, 24.7136, 0.02),
    ]
    for name, city, region, cc, lon, lat, size in demo:
        poly = {
            "type": "Polygon",
            "coordinates": [
                [
                    [lon - size, lat - size],
                    [lon + size, lat - size],
                    [lon + size, lat + size],
                    [lon - size, lat + size],
                    [lon - size, lat - size],
                ]
            ],
        }
        features.append(
            {
                "type": "Feature",
                "geometry": poly,
                "properties": {
                    "name": name,
                    "kind": (
                        "solar_farm"
                        if "Solar" in name
                        else ("wind_farm" if "Wind" in name else "plant")
                    ),
                    "status": "active",
                    "owner_org": "Verdantis Demo Energy",
                    "address": f"{name}, {city}",
                    "city": city,
                    "region": region,
                    "country_code": cc,
                },
            }
        )
    return features


def _generate_demo_permits(asset_names: list[str]) -> pd.DataFrame:
    rows = []
    today = date.today()
    names = asset_names or ["Unknown Asset"]
    for i, aname in enumerate(names):
        rows.append(
            {
                "asset_name": aname,
                "org_name": "Verdantis Demo Energy",
                "permit_type": "Air Emissions Permit",
                "status": "active",
                "issue_date": (today - timedelta(days=365 + i * 10)).isoformat(),
                "expiry_date": (today + timedelta(days=365 * 2)).isoformat(),
                "reference_id": f"AIR-{1000+i}",
                "country_code": "AE",
                "geom_geojson": "",
            }
        )
    for j in range(2):
        rows.append(
            {
                "asset_name": names[0],
                "org_name": "Dubai Environmental Dept.",
                "permit_type": "Water Discharge Permit",
                "status": "requested" if j == 0 else "active",
                "issue_date": (today - timedelta(days=200 - j * 5)).isoformat(),
                "expiry_date": (today + timedelta(days=365)).isoformat(),
                "reference_id": f"WAT-{2000+j}",
                "country_code": "AE",
                "geom_geojson": "",
            }
        )
    return pd.DataFrame(rows)


def _load_permits(asset_names: list[str]) -> pd.DataFrame:
    # CSV takes precedence
    for p in PERMITS_CSV_PATHS:
        if p.exists():
            print(f"✓ Using permits CSV from {p}")
            return pd.read_csv(p)

    # Otherwise manifest from PDFs
    pdfs: list[str] = []
    for pat in PERMITS_PDF_GLOBS:
        pdfs.extend(glob(pat))
    pdfs = sorted(set(pdfs))
    print(f"→ PDF scan: matched {len(pdfs)} file(s)")

    if not pdfs:
        print("! no permits CSV or PDFs found; generating minimal demo permits")
        return _generate_demo_permits(asset_names)

    rows = []
    names = sorted(asset_names) if asset_names else ["Unknown Asset"]
    today = date.today()
    for i, pdf_path in enumerate(pdfs):
        asset_name = names[i % len(names)]
        reference_id = Path(pdf_path).stem
        rows.append(
            {
                "asset_name": asset_name,
                "org_name": "Verdantis Demo Energy",
                "permit_type": "Generic Permit",
                "status": "active",
                "issue_date": (today - timedelta(days=120 + i * 3)).isoformat(),
                "expiry_date": (today + timedelta(days=365)).isoformat(),
                "reference_id": reference_id,
                "country_code": "AE",
                "geom_geojson": "",
            }
        )
    print(f"✓ Built permits manifest from PDFs → rows={len(rows)}")
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

    owner_org_name = props.get("owner_org")
    owner_org_id = upsert_org(cur, owner_org_name, "owner", props.get("country_code"))

    # NOTE: cast parameter to text; ST_GeomFromGeoJSON(NULL::text) -> NULL (no CASE needed)
    cur.execute(
        sql.SQL(
            """
            WITH g AS (
                SELECT ST_SetSRID(ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))), 4326) AS fp
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
            gjson,  # %s::text inside ST_GeomFromGeoJSON
            props.get("name"),
            props.get("kind"),
            props.get("status"),
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
                SELECT ST_SetSRID(ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))), 4326) AS gm
            )
            INSERT INTO permit
                (asset_id, org_id, permit_type, status, issue_date, expiry_date, document_id,
                 reference_id, geom)
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
    permits_df = _load_permits(asset_names)

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            asset_name_to_id = {}
            for feat in features:
                name = feat.get("properties", {}).get("name")
                aid = insert_asset(cur, feat)
                asset_name_to_id[name] = aid

            for _, row in permits_df.iterrows():
                aname = str(row.get("asset_name"))
                if aname not in asset_name_to_id:
                    print(f"! Skipping permit with unknown asset: {aname}")
                    continue
                insert_permit(cur, row, asset_name_to_id)

        conn.commit()

    print(f"✓ Seed complete → assets={len(asset_name_to_id)} permits={len(permits_df)}")


if __name__ == "__main__":
    main()
