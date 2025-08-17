# tools/m3_make_stub_docs.py
from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import psycopg
from dotenv import load_dotenv

# Load project .env automatically (tools/.. → repo root)
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=ROOT / ".env", override=False)


def _dsn() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "verdantis")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def _fetch_docs(limit: int) -> List[Tuple[str, str, str, str]]:
    sql = """
    SELECT doc_sha256, COALESCE(title, '') AS title,
           COALESCE(asset_id, '') AS asset_id,
           COALESCE(TO_CHAR(published_at, 'YYYY-MM-DD'), '') AS pub
    FROM vw_doc_index_norm
    ORDER BY published_at DESC NULLS LAST, doc_sha256
    LIMIT %s;
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [(str(a), str(b), str(c), str(d)) for (a, b, c, d) in cur.fetchall()]


def _write_file(path: Path, title: str, asset_id: str, pub: str) -> None:
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    text = (
        f"{title or 'Untitled'}\n\n"
        f"Asset: {asset_id or 'unknown'}\n"
        f"Published: {pub or 'unknown'}\n"
        f"Generated: {now}\n\n"
        "Summary:\n"
        "- Environmental compliance notice.\n"
        "- Emissions reporting excerpt.\n"
        "- Operational change affecting ESG indicators.\n\n"
        "Details:\n"
        "This document contains policy statements and quantitative metrics that "
        "may be interpreted as clauses for verification. Example clauses:\n"
        "1) NOx limit: 50 mg/Nm3 during Q3.\n"
        "2) Water withdrawal reduced by 12% YoY.\n"
        "3) Permit ref: PERM-2025-07-AX9 valid through 2026-06-30.\n"
    )
    path.write_text(text, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser("Generate stub docs for M3 from DB")
    ap.add_argument("--out", default="data/docs", help="Output folder")
    ap.add_argument("--limit", type=int, default=1000, help="Max docs")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    rows = _fetch_docs(limit=args.limit)
    if not rows:
        print("No rows in vw_doc_index_norm. Bind docs first.")
        return 1

    written = 0
    for doc_sha, title, asset_id, pub in rows:
        fp = out / f"{doc_sha}.txt"
        if not fp.exists():
            _write_file(fp, title=title, asset_id=asset_id, pub=pub)
            written += 1

    print(f"Wrote {written} file(s) to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
