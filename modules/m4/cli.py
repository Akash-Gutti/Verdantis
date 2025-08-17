from __future__ import annotations

import argparse
from pathlib import Path

from .m4_1_index import build_index
from .m4_2_draft_verify import ask as _ask
from .m4_3_coverage import ask_with_coverage as _ask_cov


def _cmd_m4_index(args: argparse.Namespace) -> None:
    build_index(
        chunks_path=Path(args.chunks),
        links_csv=Path(args.doc_links),
        out_dir=Path(args.out),
        model_name=args.model,
        batch_size=args.batch,
        save_vectors=args.save_vectors,
    )


def _cmd_m4_draftverify(args: argparse.Namespace) -> None:
    res = _ask(
        query=args.query,
        index_dir=Path(args.index_dir),
        topk=args.k,
        max_sentences=args.max_sentences,
    )
    # Print pretty JSON to stdout
    import json as _json  # local import to avoid top-level costs

    print(_json.dumps(res, ensure_ascii=False, indent=2))
    if args.out is not None:
        Path(args.out).write_text(_json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")


def _cmd_m4_coverage(args: argparse.Namespace) -> None:
    res = _ask_cov(
        query=args.query,
        index_dir=Path(args.index_dir),
        topk=args.k,
        max_sentences=args.max_sentences,
    )
    # Apply optional gate
    if args.gate and res.get("coverage", 0.0) < float(args.threshold):
        payload = {"needs_clarification": True, "coverage": res["coverage"]}
    else:
        payload = res

    import json as _json

    print(_json.dumps(payload, ensure_ascii=False, indent=2))
    if args.out is not None:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(_json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def register(subparsers: argparse._SubParsersAction, verifiers: dict) -> None:
    """
    Registrar for Module 4 commands. Call this from scripts/verdctl.py
    """
    p = subparsers.add_parser(
        "m4.index",
        help="M4.1 Build FAISS index for RAG retrieval",
    )
    p.add_argument(
        "--chunks",
        type=str,
        default="data/interim/doc_chunks.parquet",
        help="Path to doc_chunks parquet",
    )
    p.add_argument(
        "--doc-links",
        type=str,
        default="data/processed/kg/doc_links.csv",
        help="Optional doc→kg links CSV",
    )
    p.add_argument(
        "--out",
        type=str,
        default="data/index/m4_faiss",
        help="Output directory for index artifacts",
    )
    p.add_argument(
        "--model",
        type=str,
        default="intfloat/e5-base",
        help="SentenceTransformers model name or local path",
    )
    p.add_argument(
        "--batch",
        type=int,
        default=64,
        help="Embedding batch size",
    )
    p.add_argument(
        "--save-vectors",
        action="store_true",
        help="Also save vectors.npy (for audits)",
    )
    p.set_defaults(func=_cmd_m4_index)

    # m4.draftverify (NEW)
    q = subparsers.add_parser(
        "m4.draftverify",
        help="M4.2 Draft + Verify (CPU NLI, no LLM)",
    )
    q.add_argument("--q", "--query", dest="query", type=str, required=True)
    q.add_argument("--index-dir", type=str, default="data/index/m4_faiss")
    q.add_argument("--k", type=int, default=5)
    q.add_argument("--max-sentences", type=int, default=4)
    q.add_argument("--out", type=str, default=None)
    q.set_defaults(func=_cmd_m4_draftverify)

    # M4.3
    c = subparsers.add_parser(
        "m4.coverage",
        help="M4.3 Coverage over M4.2 result; optional gating",
    )
    c.add_argument("--q", "--query", dest="query", type=str, required=True)
    c.add_argument("--index-dir", type=str, default="data/index/m4_faiss")
    c.add_argument("--k", type=int, default=5)
    c.add_argument("--max-sentences", type=int, default=4)
    c.add_argument("--threshold", type=float, default=0.6)
    c.add_argument(
        "--gate",
        action="store_true",
        help="Emit {needs_clarification:true} if coverage<threshold",
    )
    c.add_argument("--out", type=str, default=None)
    c.set_defaults(func=_cmd_m4_coverage)
