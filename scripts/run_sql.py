import os
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import psycopg
from dotenv import load_dotenv
from psycopg import sql

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL not set in .env"


def _admin_url_and_db(url: str):
    """Return (admin_url, target_dbname). Admin URL points to 'postgres' DB."""
    p = urlparse(url)
    dbname = p.path.lstrip("/") or "postgres"
    admin_path = "/postgres"
    admin_url = urlunparse((p.scheme, p.netloc, admin_path, "", "", ""))
    return admin_url, dbname


def ensure_database(url: str):
    """Create the target database if it doesn't exist."""
    admin_url, target_db = _admin_url_and_db(url)
    # autocommit needed for CREATE DATABASE
    with psycopg.connect(admin_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
                print(f"Created database: {target_db}")


def run_sql_file(url: str, path: str):
    sql_text = Path(path).read_text(encoding="utf-8")
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python scripts/run_sql.py db/migrations/0001_create_core_schema.sql")
        raise SystemExit(1)
    ensure_database(DATABASE_URL)
    run_sql_file(DATABASE_URL, sys.argv[1])
    print(f"Applied migration: {sys.argv[1]}")
