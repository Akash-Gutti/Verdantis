from __future__ import annotations

import json
import os
from hashlib import md5
from typing import Dict, Iterable, Optional, Tuple

import psycopg


def _dsn() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "verdantis")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def _pick(props: Dict[str, object], keys: Iterable[str]) -> Optional[str]:
    for k in keys:
        v = props.get(k)
        if v is not None:
            s = str(v).strip()
            if s:
                return s
    return None


def _derive_id(name: Optional[str], city: Optional[str], country: Optional[str]) -> str:
    base = "|".join([name or "", city or "", country or ""]).encode("utf-8")
    return md5(base).hexdigest()  # stable uid


def seed_from_geojson(
    path: str = "data/raw/assets/assets.geojson", table: str = "assets"
) -> Tuple[int, int]:
    """
    Seed or upsert assets from a GeoJSON FeatureCollection.
    Returns (inserted, updated).
    """
    if not os.path.exists(path):
        raise SystemExit(f"GeoJSON not found at: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    if not features:
        raise SystemExit("No features found in GeoJSON.")

    inserted = 0
    updated = 0

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id TEXT PRIMARY KEY,
        name TEXT,
        asset_type TEXT,
        city TEXT,
        country TEXT,
        geom geometry(Geometry, 4326)
    );
    """
    upsert_sql = f"""
    INSERT INTO {table} (id, name, asset_type, city, country, geom)
    VALUES (
        %(id)s, %(name)s, %(asset_type)s, %(city)s, %(country)s,
        ST_SetSRID(ST_GeomFromGeoJSON(%(geom)s), 4326)
    )
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        asset_type = EXCLUDED.asset_type,
        city = EXCLUDED.city,
        country = EXCLUDED.country,
        geom = EXCLUDED.geom;
    """

    id_keys = ("id", "asset_id", "code", "slug")
    name_keys = ("name", "asset_name", "project", "project_name", "site_name", "title")
    type_keys = ("asset_type", "type", "category", "sector")
    city_keys = ("city", "town", "municipality")
    country_keys = ("country", "nation", "state", "emirate")

    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute(create_sql)

        for feat in features:
            props = feat.get("properties", {}) or {}
            geom = feat.get("geometry")
            if not geom:
                continue

            name = _pick(props, name_keys)
            city = _pick(props, city_keys)
            country = _pick(props, country_keys)
            asset_id = _pick(props, id_keys) or _derive_id(name, city, country)

            payload = {
                "id": asset_id,
                "name": name or "Unknown",
                "asset_type": _pick(props, type_keys),
                "city": city,
                "country": country,
                "geom": json.dumps(geom),
            }

            cur.execute(upsert_sql, payload)
            # psycopg3 can't directly tell insert/update per row here;
            # treat as inserted if row didn't exist before using a simple check:
            # this is kept simple for portability
            inserted += 1  # count fed rows

        conn.commit()

    # We cannot precisely split inserted vs updated without extra queries.
    # Return fed rows as inserted, 0 updated for simplicity.
    return inserted, updated
