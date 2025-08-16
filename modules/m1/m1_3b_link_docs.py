from __future__ import annotations

from datetime import datetime
from glob import glob
from pathlib import Path

import psycopg

from .common import DATABASE_URL, PERMITS_PDF_GLOBS, sha256_file


def link_docs() -> None:
    """M1.3b: ingest permit PDFs into document table and link permits."""
    pdfs = sorted({Path(p) for pat in PERMITS_PDF_GLOBS for p in glob(pat)})
    if not pdfs:
        raise SystemExit("No permit PDFs found to ingest.")
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        # clean prior local sources to avoid cruft
        cur.execute("DELETE FROM document WHERE source='permits_local';")

        docs = links = 0
        for p in pdfs:
            stem = p.stem
            digest = sha256_file(p)
            doc_date = datetime.fromtimestamp(p.stat().st_mtime).date()

            cur.execute(
                """
                INSERT INTO document
                    (title, source, lang, url, storage_path, doc_sha256, doc_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_sha256) DO UPDATE
                  SET storage_path = EXCLUDED.storage_path
                RETURNING id;
                """,
                (stem, "permits_local", "en", None, str(p), digest, doc_date),
            )
            doc_id = cur.fetchone()[0]
            docs += 1

            cur.execute(
                "UPDATE permit SET document_id=%s WHERE reference_id=%s;",
                (doc_id, stem),
            )
            links += cur.rowcount

        conn.commit()
    print(f"[m1.3b] documents upserted={docs}, permits linked={links}")
