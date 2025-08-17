from __future__ import annotations

import argparse
from pathlib import Path

from .m4_1_index import build_index


def _cmd_m4_index(args: argparse.Namespace) -> None:
    build_index(
        chunks_path=Path(args.chunks),
        links_csv=Path(args.doc_links),
        out_dir=Path(args.out),
        model_name=args.model,
        batch_size=args.batch,
        save_vectors=args.save_vectors,
    )


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
