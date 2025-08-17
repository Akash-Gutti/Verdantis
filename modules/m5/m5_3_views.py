from __future__ import annotations

import os
from typing import Tuple

import psycopg


def _dsn() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "verdantis")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def _exec(cur: psycopg.Cursor, sql: str) -> None:
    cur.execute(sql)


def create_m53_views() -> None:
    """
    Create M5.3 views:
      - vw_doc_citations_detail
      - vw_kg_edges_from_clauses
      - vw_doc_proof_bundle
    Uses simple, reversible overrides where needed.
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        # Ensure override table for proof bundles exists
        _exec(
            cur,
            """
            CREATE TABLE IF NOT EXISTS public.proof_bundle_override(
              doc_sha256 TEXT PRIMARY KEY,
              bundle_id  TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
        )

        # Citations detail (from doc_clauses)
        _exec(
            cur,
            """
            DROP VIEW IF EXISTS vw_doc_citations_detail CASCADE;
            CREATE VIEW vw_doc_citations_detail AS
            SELECT
              dc.doc_sha256,
              COALESCE(dc.page, 1)        AS page,
              COALESCE(dc.clause_type,'') AS clause_type,
              LEFT(COALESCE(dc.text, ''), 300) AS snippet
            FROM public.doc_clauses dc;
            """,
        )

        # KG edges synthesized from clause types (upgradeable later)
        _exec(
            cur,
            """
            DROP VIEW IF EXISTS vw_kg_edges_from_clauses CASCADE;
            CREATE VIEW vw_kg_edges_from_clauses AS
            SELECT
              md5(dc.doc_sha256 || ':' || COALESCE(dc.clause_type,'') ||
                  ':' || COALESCE(dc.page::text,'')) AS edge_id,
              'asset'::text   AS src_type,
              d.asset_id::text AS src_id,
              CASE
                WHEN lower(COALESCE(dc.clause_type,'')) = 'metric'
                  THEN 'metric'
                WHEN lower(COALESCE(dc.clause_type,'')) = 'permit'
                  THEN 'permit'
                WHEN lower(COALESCE(dc.clause_type,'')) = 'operational'
                  THEN 'operation'
                ELSE 'policy'
              END AS dst_type,
              -- Simple deterministic dst_id from clause text/type
              md5(COALESCE(dc.clause_type,'') || ':' ||
                  COALESCE(dc.text,'')) AS dst_id,
              COALESCE(dc.clause_type,'') AS label,
              0.7::float AS weight,
              dc.doc_sha256
            FROM public.doc_clauses dc
            JOIN vw_doc_index_norm d
              ON d.doc_sha256 = dc.doc_sha256;
            """,
        )

        # Proof bundle id per doc: override → fallback hash
        _exec(
            cur,
            """
            DROP VIEW IF EXISTS vw_doc_proof_bundle CASCADE;
            CREATE VIEW vw_doc_proof_bundle AS
            SELECT
              d.doc_sha256,
              COALESCE(pbo.bundle_id,
                       'pb-' || substr(md5(d.doc_sha256), 1, 12)) AS bundle_id
            FROM vw_doc_index_norm d
            LEFT JOIN proof_bundle_override pbo
              ON pbo.doc_sha256 = d.doc_sha256;
            """,
        )

        conn.commit()


def verify_m53() -> Tuple[int, int, int]:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vw_doc_citations_detail;")
        citations = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM vw_kg_edges_from_clauses;")
        edges = int(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM vw_doc_proof_bundle;")
        bundles = int(cur.fetchone()[0])
    return citations, edges, bundles
