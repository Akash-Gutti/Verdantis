from __future__ import annotations

import json
from datetime import date, timedelta
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import psycopg

from .common import ASSETS_PATH, DATABASE_URL, PERMITS_PDF_GLOBS


def _normalize_org_key(name: Optional[str], cc: Optional[str]) -> tuple[str, str]:
    return (name or "").strip().lower(), (cc or "").strip().upper()


def _upsert_org(cur, name: Optional[str], org_type: Optional[str], cc: Optional[str]):
    if not name:
        return None
    key_name, key_cc = _normalize_org_key(name, cc)
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
        (name.strip(), org_type, (cc or None)),
    )
    return cur.fetchone()[0]


def _load_assets() -> List[dict]:
    if not ASSETS_PATH.exists():
        raise FileNotFoundError("Assets GeoJSON not found at data/raw/assets/assets.geojson")
    gj = json.loads(ASSETS_PATH.read_text(encoding="utf-8"))
    feats = gj.get("features", [])
    if not feats:
        raise ValueError("No features in assets.geojson")
    print(f"[m1.3] assets from {ASSETS_PATH} (features={len(feats)})")
    return feats


def _build_permit_manifest(asset_names: List[str]) -> pd.DataFrame:
    pdfs = sorted({p for pat in PERMITS_PDF_GLOBS for p in glob(pat)})
    if not pdfs:
        raise FileNotFoundError(
            "No permit PDFs found. Expected:\n  " + "\n  ".join(PERMITS_PDF_GLOBS)
        )
    rows = []
    names = sorted(asset_names)
    today = date.today()
    for i, pdf in enumerate(pdfs):
        stem = Path(pdf).stem
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
    print(f"[m1.3] permit manifest from PDFs → rows={len(rows)}")
    return pd.DataFrame(rows)


def seed() -> None:
    """M1.3: load assets+permits (overwrite previous demo rows)."""
    features = _load_assets()
    asset_names = [
        f.get("properties", {}).get("name", f"asset_{i}") for i, f in enumerate(features)
    ]
    permits_df = _build_permit_manifest(asset_names)

    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        print("[m1.3] RESET: TRUNCATE permit, asset (CASCADE)")
        cur.execute("TRUNCATE TABLE permit RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE asset  RESTART IDENTITY CASCADE;")

        name_to_id: Dict[str, str] = {}

        # assets
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry")
            gjson = json.dumps(geom) if geom else None

            owner_name = (
                props.get("owner_org") or props.get("owner") or props.get("operator") or None
            )
            owner_id = _upsert_org(cur, owner_name, "owner", props.get("country_code"))

            cur.execute(
                """
                WITH g AS (
                  SELECT ST_SetSRID(
                           ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))),
                           4326
                         ) AS fp
                )
                INSERT INTO asset
                  (name, kind, status, owner_org_id,
                   centroid, footprint, address, city, region, country_code)
                SELECT
                  %s, %s, %s, %s,
                  ST_Centroid(g.fp), g.fp,
                  %s, %s, %s, %s
                FROM g
                RETURNING id;
                """,
                (
                    gjson,
                    props.get("name"),
                    props.get("kind"),
                    props.get("status") or "active",
                    owner_id,
                    props.get("address"),
                    props.get("city"),
                    props.get("region"),
                    props.get("country_code"),
                ),
            )
            aid = cur.fetchone()[0]
            name_to_id[props.get("name")] = aid

        # permits
        for _, row in permits_df.iterrows():
            aname = str(row.get("asset_name"))
            if aname not in name_to_id:
                raise RuntimeError(f"Unknown asset in manifest: {aname}")

            asset_id = name_to_id[aname]
            org_id = _upsert_org(
                cur, row.get("org_name"), "regulator/issuer", row.get("country_code")
            )
            gjson = row.get("geom_geojson")
            gjson = None if (pd.isna(gjson) or str(gjson).strip() == "") else str(gjson)

            cur.execute(
                """
                WITH g AS (
                  SELECT ST_SetSRID(
                           ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))),
                           4326
                         ) AS gm
                )
                INSERT INTO permit
                  (asset_id, org_id, permit_type, status,
                   issue_date, expiry_date, document_id, reference_id, geom)
                SELECT
                  %s, %s, %s, %s,
                  %s::date, %s::date, NULL, %s, g.gm
                FROM g;
                """,
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

        conn.commit()

    print("[m1.3] seeding complete")
