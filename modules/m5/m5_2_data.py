from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

import psycopg


def _dsn() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "verdantis")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


# ADD to modules/m5/m5_2_data.py


def fetch_asset_meta(asset_id: str) -> Dict[str, Any]:
    sql = """
    SELECT asset_id, name, asset_type, city, country
    FROM vw_assets_basic
    WHERE asset_id = %s
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (asset_id,))
        row = cur.fetchone()
        if not row:
            return {}
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def fetch_evidence_windowed(
    asset_id: str,
    days: int = 60,
    min_citations_flag: int = 2,
    top_k: int = 20,
) -> List[Dict[str, Any]]:
    """
    Return recent docs for an asset over a configurable window.
    min_citations_flag is used only to compute has_min_citations boolean.
    """
    sql = """
    SELECT d.doc_sha256,
           d.title,
           d.published_at,
           d.source,
           d.url,
           d.lang,
           COALESCE(c.clause_count, 0) AS citation_count,
           (COALESCE(c.clause_count, 0) >= %s) AS has_min_citations
    FROM vw_docs_with_assets d
    LEFT JOIN vw_doc_citation_counts c
      ON c.doc_sha256 = d.doc_sha256
    WHERE d.asset_id = %s
      AND (
        d.published_at IS NULL
        OR d.published_at >= CURRENT_DATE - (%s * INTERVAL '1 day')
      )
    ORDER BY d.published_at DESC NULLS LAST
    LIMIT %s;
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (min_citations_flag, asset_id, days, top_k))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _as_feature(row: Dict[str, Any], geom_key: str) -> Dict[str, Any]:
    geom = json.loads(row[geom_key])
    props = {k: v for k, v in row.items() if k != geom_key}
    return {"type": "Feature", "geometry": geom, "properties": props}


def fetch_assets_geojson(limit: int = 200) -> Dict[str, Any]:
    sql = """
    SELECT asset_id, name, asset_type, city, country,
           ST_AsGeoJSON(geom) AS geom
    FROM vw_assets_basic
    LIMIT %s;
    """
    feats: List[Dict[str, Any]] = []
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        cols = [d[0] for d in cur.description]
        for rec in cur.fetchall():
            row = dict(zip(cols, rec))
            feats.append(_as_feature(row, "geom"))
    return {"type": "FeatureCollection", "features": feats}


def fetch_overlays_geojson(limit: int = 200) -> Dict[str, Any]:
    sql = """
    SELECT asset_id, name, overlay_type,
           ST_AsGeoJSON(geom) AS geom
    FROM vw_asset_overlays
    LIMIT %s;
    """
    feats: List[Dict[str, Any]] = []
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        cols = [d[0] for d in cur.description]
        for rec in cur.fetchall():
            row = dict(zip(cols, rec))
            feats.append(_as_feature(row, "geom"))
    return {"type": "FeatureCollection", "features": feats}


def fetch_asset_list(limit: int = 500) -> List[Tuple[str, str]]:
    sql = """
    SELECT asset_id, name
    FROM vw_assets_basic
    ORDER BY name
    LIMIT %s;
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [(str(a), str(n)) for (a, n) in cur.fetchall()]


def fetch_evidence(asset_id: str, top_k: int = 10) -> List[Dict[str, Any]]:
    sql = """
    SELECT doc_sha256, title, published_at, source, url,
           citation_count, has_min_citations
    FROM vw_asset_events_current
    WHERE asset_id = %s
    ORDER BY published_at DESC NULLS LAST
    LIMIT %s;
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (asset_id, top_k))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_doc_citations(doc_sha256: str, top_k: int = 12) -> List[Dict[str, Any]]:
    sql = """
    SELECT page, clause_type, snippet
    FROM vw_doc_citations_detail
    WHERE doc_sha256 = %s
    ORDER BY page ASC
    LIMIT %s;
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (doc_sha256, top_k))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_doc_proof_bundle(doc_sha256: str) -> str:
    sql = """
    SELECT bundle_id
    FROM vw_doc_proof_bundle
    WHERE doc_sha256 = %s
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (doc_sha256,))
        row = cur.fetchone()
        return str(row[0]) if row else ""


def fetch_kg_edges(asset_id: str, doc_sha256: str, top_k: int = 12) -> List[Dict[str, Any]]:
    sql = """
    SELECT src_type, src_id, dst_type, dst_id, label, weight
    FROM vw_kg_edges_from_clauses
    WHERE doc_sha256 = %s AND src_id = %s
    ORDER BY weight DESC
    LIMIT %s;
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (doc_sha256, asset_id, top_k))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
