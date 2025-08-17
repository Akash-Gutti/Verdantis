# modules/m3/cli.py
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .m3_1_loader import run_loader
from .m3_2_chunker import run_chunker
from .m3_3_ner_clause import run_ner_clause
from .m3_4_db_upsert import run_upsert


def register(subparsers: argparse._SubParsersAction, verifiers: dict) -> None:
    # m3.load
    p_load = subparsers.add_parser(
        "m3.load",
        help="M3.1 — Load docs/news/policy, extract pages & language, write doc_pages.parquet",
    )
    p_load.set_defaults(func=_cmd_load)

    # m3.chunk
    p_chunk = subparsers.add_parser(
        "m3.chunk",
        help="M3.2 — Sentence-aware chunking (400–800 words), guarantee ≥1 chunk/doc ≥120 words",
    )
    p_chunk.add_argument("--target-min", type=int, default=400)
    p_chunk.add_argument("--target-max", type=int, default=800)
    p_chunk.add_argument("--guarantee-min-words", type=int, default=120)
    p_chunk.set_defaults(func=_cmd_chunk)

    # m3.ner
    p_ner = subparsers.add_parser(
        "m3.ner",
        help="M3.3 — NER (spaCy EN + Arabic regex fallback) "
        "and clause harvesting (emissions/water/waste/energy)",
    )
    p_ner.add_argument("--batch-size", type=int, default=32)
    p_ner.set_defaults(func=_cmd_ner)

    # m3.upsert
    p_up = subparsers.add_parser(
        "m3.upsert", help="M3.4 — Upsert doc_entities & doc_clauses into Postgres (idempotent)"
    )
    p_up.add_argument("--batch-size", type=int, default=1000)
    p_up.set_defaults(func=_cmd_upsert)

    # allow `verify -m m3`
    verifiers["m3"] = verify


def _cmd_load(_: argparse.Namespace) -> int:
    return run_loader()


def _cmd_chunk(args: argparse.Namespace) -> int:
    return run_chunker(
        target_min=args.target_min,
        target_max=args.target_max,
        guarantee_min_words=args.guarantee_min_words,
    )


def _cmd_ner(args: argparse.Namespace) -> int:
    return run_ner_clause(batch_size=args.batch_size)


def _cmd_upsert(args: argparse.Namespace) -> int:
    return run_upsert(batch_size=args.batch_size)


def verify() -> None:
    """Verify M3 gates: M3.1 pages>0, M3.2 chunks>0, M3.3 coverage≥0.6 for English docs."""
    # M3.1
    load_metrics = Path("data/interim/load_metrics.json")
    if not load_metrics.exists():
        raise SystemExit("M3 verify: load_metrics.json missing (run `m3.load`).")
    lm = json.loads(load_metrics.read_text(encoding="utf-8"))
    if int(lm.get("pages_total", 0)) <= 0:
        raise SystemExit("M3 verify: pages_total==0 (loader produced no pages).")

    # M3.2
    chunk_metrics = Path("data/interim/clean_chunk_metrics.json")
    if not chunk_metrics.exists():
        raise SystemExit("M3 verify: clean_chunk_metrics.json missing (run `m3.chunk`).")
    cm = json.loads(chunk_metrics.read_text(encoding="utf-8"))
    if int(cm.get("chunks_total", 0)) <= 0:
        raise SystemExit("M3 verify: chunks_total==0 (chunker produced no chunks).")

    # M3.3
    ner_metrics = Path("data/interim/ner_clause_metrics.json")
    if not ner_metrics.exists():
        raise SystemExit("M3 verify: ner_clause_metrics.json missing (run `m3.ner`).")
    nm = json.loads(ner_metrics.read_text(encoding="utf-8"))
    coverage = float(nm.get("english_ner_coverage", 0.0))
    # Gate: entities extracted for ≥60% English docs
    if coverage < 0.60:
        raise SystemExit(
            f"M3 verify: English NER coverage {coverage:.2f} < 0.60. "
            "Ensure spaCy model is installed (`python -m spacy download en_core_web_sm`) "
            "or adjust content/threshold."
        )

    # --- M3.4: DB upsert verification ---
    # Ensure psycopg is available
    try:
        import psycopg
    except Exception:
        raise SystemExit("M3 verify: psycopg not installed. Run: pip install psycopg[binary]")

    import pandas as pd

    from .m3_4_db_upsert import _conninfo_from_env

    ents_pq = Path("data/interim/doc_entities.parquet")
    cls_pq = Path("data/interim/doc_clauses.parquet")
    if not ents_pq.exists() or not cls_pq.exists():
        raise SystemExit("M3 verify: parquet outputs missing (run `m3.ner` and `m3.upsert`).")

    ents_df = pd.read_parquet(ents_pq)
    cls_df = pd.read_parquet(cls_pq)

    # If there are no rows at all, that's unexpected after your successful m3.ner
    if ents_df.empty and cls_df.empty:
        raise SystemExit("M3 verify: entity/clauses parquet are empty. Re-run `m3.ner`.")

    # Build current run's doc_sha set (keeps verification focused & idempotent)
    doc_shas = sorted(set(ents_df["doc_sha256"].tolist()) | set(cls_df["doc_sha256"].tolist()))

    conninfo = _conninfo_from_env()
    try:
        with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
            # Tables exist?
            cur.execute(
                "SELECT to_regclass('public.doc_entities'), to_regclass('public.doc_clauses')"
            )
            ent_tbl, cls_tbl = cur.fetchone()
            if ent_tbl is None or cls_tbl is None:
                raise SystemExit("M3 verify: doc_entities/doc_clauses not found. Run `m3.upsert`.")

            # Compare counts for this run's doc_sha set
            if doc_shas:
                cur.execute(
                    "SELECT COUNT(*) FROM doc_entities WHERE doc_sha256 = ANY(%s)", (doc_shas,)
                )
                db_ents = cur.fetchone()[0]
                cur.execute(
                    "SELECT COUNT(*) FROM doc_clauses  WHERE doc_sha256 = ANY(%s)", (doc_shas,)
                )
                db_cls = cur.fetchone()[0]

                # Expect DB to have at least as many rows as parquet (idempotent upsert)
                ents_needed = int(ents_df.shape[0])
                cls_needed = int(cls_df.shape[0])
                if db_ents < ents_needed or db_cls < cls_needed:
                    raise SystemExit(
                        f"M3 verify: DB rows (ents={db_ents}, clauses={db_cls}) "
                        f"< parquet (ents={ents_needed}, clauses={cls_needed}). Upsert incomplete."
                    )
    except Exception as e:
        raise SystemExit(f"M3 verify: DB check failed: {e}")

    print(
        f"M3 verify OK: pages={lm.get('pages_total')}, chunks={cm.get('chunks_total')}, "
        f"EN_coverage={coverage:.2f}"
    )
