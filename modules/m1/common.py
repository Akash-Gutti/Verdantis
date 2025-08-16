from __future__ import annotations

import hashlib
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL not set in .env")

# -------- PATH LOCKS (match your repo) --------
ASSETS_PATH = Path("data/raw/assets/assets.geojson")
PERMITS_PDF_GLOBS = [
    "data/raw/permits/Permit_synth_*.pdf",
    "data/raw/permits/Permit_real_*.pdf",
]
CSV_OUT_DIR = Path("data/processed/kg/csv")
GRAPH_DIR = Path("data/processed/kg/graph")
GRAPH_PATH = GRAPH_DIR / "verdantis_kg.graphml"
META_PATH = GRAPH_DIR / "metadata.json"
# ----------------------------------------------


def run_sql_file(conn: psycopg.Connection, path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()
