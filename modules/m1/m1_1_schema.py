from __future__ import annotations

from pathlib import Path

import psycopg

from .common import DATABASE_URL, run_sql_file


def schema() -> None:
    """M1.1: core schema migration."""
    path = Path("db/migrations/0001_create_core_schema.sql")
    if not path.exists():
        raise SystemExit(f"[m1.1] missing {path}")
    with psycopg.connect(DATABASE_URL) as conn:
        run_sql_file(conn, path)
    print("[m1.1] schema migration applied")
