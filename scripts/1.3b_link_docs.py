import hashlib
import os
from datetime import datetime
from glob import glob
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL not set"

PDF_GLOBS = [
    "data/raw/permits/Permit_synth_*.pdf",
    "data/raw/permits/Permit_real_*.pdf",
]


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main():
    pdfs = sorted({Path(p) for pat in PDF_GLOBS for p in glob(pat)})
    if not pdfs:
        raise FileNotFoundError(
            "No permit PDFs found to ingest. Expected:\n  " + "\n  ".join(PDF_GLOBS)
        )

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            # Clean previous local-permit docs so we don't accumulate cruft
            print("! RESET: removing prior 'permits_local' documents (will relink)")
            cur.execute("DELETE FROM document WHERE source='permits_local';")

            docs = 0
            links = 0
            for p in pdfs:
                stem = p.stem
                digest = sha256_file(p)
                doc_date = datetime.fromtimestamp(p.stat().st_mtime).date()

                # Insert (unique on doc_sha256)
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

                # Link by reference_id == PDF stem
                cur.execute(
                    "UPDATE permit SET document_id=%s WHERE reference_id=%s;",
                    (doc_id, stem),
                )
                links += cur.rowcount

        conn.commit()

    print(f"✓ 1.3b done → documents upserted={docs}, permits linked={links}")


if __name__ == "__main__":
    main()
