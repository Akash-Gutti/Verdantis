# tools/m3_seed_clauses_dyn.py
from __future__ import annotations

import argparse
import hashlib
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import psycopg

try:
    from dotenv import load_dotenv

    ROOT = Path(__file__).resolve().parents[1]
    load_dotenv(ROOT / ".env", override=False)
except Exception:
    pass


def _dsn() -> str:
    h = os.getenv("PGHOST", "localhost")
    p = os.getenv("PGPORT", "5432")
    d = os.getenv("PGDATABASE", "verdantis")
    u = os.getenv("PGUSER", "postgres")
    w = os.getenv("PGPASSWORD", "")
    return f"host={h} port={p} dbname={d} user={u} password={w}"


def _fetch_docs(limit: int) -> List[Tuple[str, str, str]]:
    sql = """
    SELECT doc_sha256, COALESCE(title, '') AS title,
           COALESCE(asset_id, '') AS asset_id
    FROM vw_doc_index_norm
    ORDER BY published_at DESC NULLS LAST, doc_sha256
    LIMIT %s;
    """
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql, (limit,))
        return [(str(a), str(b), str(c)) for (a, b, c) in cur.fetchall()]


def _ensure_table(cur: psycopg.Cursor) -> None:
    cur.execute(
        """
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name='doc_clauses'
          ) THEN
            CREATE TABLE public.doc_clauses(
              doc_sha256 TEXT NOT NULL,
              page INT NULL,
              clause_type TEXT NULL,
              text TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_doc_clauses_doc
              ON public.doc_clauses(doc_sha256);
          END IF;
        END $$;
        """
    )


def _schema(cur: psycopg.Cursor) -> List[Tuple[str, str, str, str]]:
    cur.execute(
        """
        SELECT column_name, data_type, is_nullable, COALESCE(column_default,'')
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='doc_clauses'
        ORDER BY ordinal_position;
        """
    )
    return [(str(a), str(b), str(c), str(d)) for (a, b, c, d) in cur.fetchall()]


def _hash_int(s: str, bits: int = 63) -> int:
    h = int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16)
    return h & ((1 << bits) - 1)


def _read_body(dir_path: Optional[Path], sha: str) -> str:
    if dir_path:
        fp = dir_path / f"{sha}.txt"
        if fp.exists():
            try:
                return fp.read_text(encoding="utf-8")[:2000]
            except Exception:
                return ""
    return ""


def _default_for(
    name: str,
    data_type: str,
    sha: str,
    title: str,
    asset_id: str,
    body: str,
    dir_path: Optional[Path],
) -> object:
    now = datetime.utcnow()
    lname = name.lower()

    # Specifics first
    if lname in {"doc_path", "path"}:
        if dir_path:
            return str((dir_path / f"{sha}.txt").as_posix())
        return f"/docs/{sha}.txt"
    if lname in {"url", "uri"}:
        return f"https://example.local/docs/{sha}"
    if lname in {"asset_id"}:
        return asset_id or "unknown"
    if lname in {"doc_id", "document_id"}:
        if data_type in {"integer"}:
            return _hash_int(f"{sha}|{title}", bits=31)
        if data_type in {"bigint", "numeric"}:
            return _hash_int(f"{sha}|{title}", bits=63)
        return sha  # text/uuid handled as text here
    if lname in {"clause_hash"}:
        return hashlib.md5(f"{sha}|{title}".encode("utf-8")).hexdigest()
    if lname in {"created_at", "updated_at", "extracted_at"}:
        return now
    if lname in {"source", "extractor", "model", "language"}:
        return "seed"
    if lname in {"confidence"}:
        return 0.9
    if lname in {"severity"}:
        return "low"

    # Heuristics by type
    if data_type in {"timestamp without time zone", "timestamp with time zone"}:
        return now
    if data_type in {"integer"}:
        return _hash_int(sha, bits=31)
    if data_type in {"bigint", "numeric"}:
        return _hash_int(sha, bits=63)
    if data_type in {"boolean"}:
        return False
    # text / varchar fallback
    if "path" in lname and dir_path:
        return str((dir_path / f"{sha}.txt").as_posix())
    return "seed"


def main() -> int:
    ap = argparse.ArgumentParser("Schema-adaptive clause seeder")
    ap.add_argument("--limit", type=int, default=2000)
    ap.add_argument("--per-doc", type=int, default=3)
    ap.add_argument("--dir", help="Folder with data/docs/<sha>.txt (optional)")
    args = ap.parse_args()

    dir_path = Path(args.dir) if args.dir else None
    docs = _fetch_docs(args.limit)
    if not docs:
        print("No documents in vw_doc_index_norm. Bind docs first.")
        return 1

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    inserted = 0

    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        _ensure_table(cur)
        cols = _schema(cur)
        colnames = [c for (c, _, _, _) in cols]
        types: Dict[str, str] = {c: t for (c, t, _, _) in cols}
        nullable: Dict[str, bool] = {c: (n == "YES") for (c, _, n, _) in cols}
        defaults: Dict[str, str] = {c: d for (c, _, _, d) in cols}

        # delete existing for these docs to avoid dupes
        cur.execute(
            "DELETE FROM public.doc_clauses WHERE doc_sha256 = ANY(%s);",
            ([d[0] for d in docs],),
        )

        # Essentials we will try to include if present
        essentials = [c for c in ("doc_sha256", "page", "clause_type", "text") if c in colnames]

        # Build dynamic column list:
        dyn_cols: List[str] = list(essentials)
        for c in colnames:
            if c in dyn_cols:
                continue
            need_val = (not nullable[c]) and (defaults[c] == "")
            if need_val:
                dyn_cols.append(c)
            # Also include common useful optional columns
            elif c in {
                "asset_id",
                "doc_id",
                "clause_hash",
                "created_at",
                "updated_at",
                "extracted_at",
                "source",
                "extractor",
                "model",
                "language",
                "confidence",
                "severity",
                "doc_path",
                "url",
                "uri",
            }:
                dyn_cols.append(c)

        cols_sql = ", ".join(f'"{c}"' for c in dyn_cols)
        ph = ", ".join(["%s"] * len(dyn_cols))
        sql_ins = f"INSERT INTO public.doc_clauses ({cols_sql}) VALUES ({ph});"

        templates = [
            "Compliance clause: NOx limit 50 mg/Nm3 (demo).",
            "Metric clause: Water withdrawal reduced 12% YoY (demo).",
            "Permit clause: PERM-AX9 valid through 2026-06-30 (demo).",
            "Operational clause: Flaring below threshold in Q3 (demo).",
        ]

        for sha, title, asset_id in docs:
            body = _read_body(dir_path, sha)
            for i in range(args.per_doc):
                row: Dict[str, object] = {}
                if "doc_sha256" in dyn_cols:
                    row["doc_sha256"] = sha
                if "page" in dyn_cols:
                    row["page"] = i + 1
                if "clause_type" in dyn_cols:
                    row["clause_type"] = ["policy", "metric", "permit", "operational"][i % 4]
                if "text" in dyn_cols:
                    snip = f" | {body[:120]}" if body else ""
                    row["text"] = (
                        f"{templates[i % len(templates)]} "
                        f"(doc='{title[:64]}', seeded {now_str}){snip}"
                    )

                # Fill any other required/selected columns
                for c in dyn_cols:
                    if c in row:
                        continue
                    row[c] = _default_for(
                        c,
                        types.get(c, "text"),
                        sha,
                        title,
                        asset_id,
                        body,
                        dir_path,
                    )

                params = [row[c] for c in dyn_cols]
                cur.execute(sql_ins, params)
                inserted += 1

        conn.commit()

    print(f"Inserted {inserted} clause row(s) into public.doc_clauses")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
