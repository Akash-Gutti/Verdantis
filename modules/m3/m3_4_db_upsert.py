from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd

try:
    import psycopg
except ModuleNotFoundError:  # pragma: no cover
    print("❌ psycopg not installed. Run: pip install psycopg[binary]", file=sys.stderr)
    raise

# Inputs
ENTITIES_FP = Path("data/interim/doc_entities.parquet")
CLAUSES_FP = Path("data/interim/doc_clauses.parquet")

# -------------------- Connection helpers --------------------


def _conninfo_from_env() -> str:
    """
    Build a psycopg3 conninfo from env:
      - DATABASE_URL = postgresql://user:pass@host:5432/dbname   (preferred)
      or PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
    """
    url = os.getenv("DATABASE_URL")
    if url:
        return url  # must start with postgresql://
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    db = os.getenv("PGDATABASE", "verdantis")
    # psycopg3 accepts a DSN string:
    return f"host={host} port={port} user={user} password={password} dbname={db}"


# -------------------- DDL --------------------

DDL_ENTITIES = """
CREATE TABLE IF NOT EXISTS doc_entities (
    doc_sha256   TEXT NOT NULL,
    doc_id       TEXT NOT NULL,
    doc_path     TEXT NOT NULL,
    source       TEXT NOT NULL,
    page         INTEGER NOT NULL,
    span_start   INTEGER NOT NULL,
    span_end     INTEGER NOT NULL,
    text         TEXT NOT NULL,
    label        TEXT NOT NULL,
    lang         TEXT NOT NULL,
    extractor    TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (doc_sha256, page, span_start, label)
);
"""

DDL_CLAUSES = """
CREATE TABLE IF NOT EXISTS doc_clauses (
    doc_sha256   TEXT NOT NULL,
    doc_id       TEXT NOT NULL,
    doc_path     TEXT NOT NULL,
    source       TEXT NOT NULL,
    page         INTEGER NOT NULL,
    clause_type  TEXT NOT NULL,
    span_start   INTEGER NOT NULL,
    span_end     INTEGER NOT NULL,
    text         TEXT NOT NULL,
    lang         TEXT NOT NULL,
    pattern      TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (doc_sha256, page, span_start, clause_type)
);
"""

UPSERT_ENTITIES = """
INSERT INTO doc_entities (
  doc_sha256, doc_id, doc_path, source, page,
  span_start, span_end, text, label, lang, extractor
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (doc_sha256, page, span_start, label) DO UPDATE SET
  span_end   = EXCLUDED.span_end,
  text       = EXCLUDED.text,
  lang       = EXCLUDED.lang,
  extractor  = EXCLUDED.extractor,
  doc_id     = EXCLUDED.doc_id,
  doc_path   = EXCLUDED.doc_path,
  source     = EXCLUDED.source;
"""

UPSERT_CLAUSES = """
INSERT INTO doc_clauses (
  doc_sha256, doc_id, doc_path, source, page,
  clause_type, span_start, span_end, text, lang, pattern
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (doc_sha256, page, span_start, clause_type) DO UPDATE SET
  span_end   = EXCLUDED.span_end,
  text       = EXCLUDED.text,
  lang       = EXCLUDED.lang,
  pattern    = EXCLUDED.pattern,
  doc_id     = EXCLUDED.doc_id,
  doc_path   = EXCLUDED.doc_path,
  source     = EXCLUDED.source;
"""

# -------------------- Upsert runners --------------------


def _iter_entity_rows(df: pd.DataFrame) -> Iterable[Tuple]:
    cols = [
        "doc_sha256",
        "doc_id",
        "doc_path",
        "source",
        "page",
        "span_start",
        "span_end",
        "text",
        "label",
        "lang",
        "extractor",
    ]
    for row in df.itertuples(index=False):
        yield tuple(getattr(row, c) for c in cols)


def _iter_clause_rows(df: pd.DataFrame) -> Iterable[Tuple]:
    cols = [
        "doc_sha256",
        "doc_id",
        "doc_path",
        "source",
        "page",
        "clause_type",
        "span_start",
        "span_end",
        "text",
        "lang",
        "pattern",
    ]
    for row in df.itertuples(index=False):
        yield tuple(getattr(row, c) for c in cols)


def run_upsert(batch_size: int = 1000) -> int:
    # Load inputs
    if not ENTITIES_FP.exists() or not CLAUSES_FP.exists():
        print("❌ M3.4 Upsert: parquet outputs missing. Run m3.ner first.", file=sys.stderr)
        return 1

    ents = pd.read_parquet(ENTITIES_FP)
    cls = pd.read_parquet(CLAUSES_FP)

    # Connect
    conninfo = _conninfo_from_env()
    try:
        with psycopg.connect(conninfo) as conn:
            conn.execute(DDL_ENTITIES)
            conn.execute(DDL_CLAUSES)

            inserted_ents = _upsert_table(
                conn, UPSERT_ENTITIES, _iter_entity_rows(ents), batch_size
            )
            inserted_cls = _upsert_table(conn, UPSERT_CLAUSES, _iter_clause_rows(cls), batch_size)

            print(f"✅ M3.4 Upsert complete: entities={inserted_ents:,}, clauses={inserted_cls:,}")
            print("Tables: doc_entities, doc_clauses")
            return 0
    except Exception as e:
        print(f"❌ M3.4 Upsert error: {e}", file=sys.stderr)
        return 1


def _upsert_table(
    conn: "psycopg.Connection", sql: str, rows: Iterable[Tuple], batch_size: int
) -> int:
    total = 0
    buf: List[Tuple] = []
    with conn.cursor() as cur:
        for r in rows:
            buf.append(r)
            if len(buf) >= batch_size:
                cur.executemany(sql, buf)
                total += len(buf)
                buf.clear()
        if buf:
            cur.executemany(sql, buf)
            total += len(buf)
    conn.commit()
    return total
