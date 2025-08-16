from __future__ import annotations

from pathlib import Path

import psycopg

from .common import DATABASE_URL, run_sql_file


def constraints() -> None:
    """M1.2: constraints + indexes migration."""
    path = Path("db/migrations/0002_constraints_indexes.sql")
    if not path.exists():
        raise SystemExit(f"[m1.2] missing {path}")
    with psycopg.connect(DATABASE_URL) as conn:
        run_sql_file(conn, path)
    print("[m1.2] constraints migration applied")
