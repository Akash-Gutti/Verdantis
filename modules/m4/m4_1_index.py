"""
M4.1 Index Build: Create a FAISS index over document chunks.

- Loads data/interim/doc_chunks.parquet
- Optional join to data/processed/kg/doc_links.csv to attach kg_nodes
- Embeds with intfloat/e5-base (SentenceTransformers)
- Saves FAISS index + metadata + manifest under data/index/m4_faiss
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import faiss  # type: ignore
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

LOGGER = logging.getLogger("m4.index")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _coalesce_column(
    df: pd.DataFrame,
    candidates: List[str],
    target_name: str,
    default: Optional[int | str] = None,
) -> Tuple[pd.DataFrame, bool]:
    """Rename the first existing candidate to target_name; optionally create default."""
    for col in candidates:
        if col in df.columns:
            if col != target_name:
                df = df.rename(columns={col: target_name})
            return df, True
    if default is not None:
        df[target_name] = default
        return df, True
    return df, False


def _load_chunks(chunks_path: Path) -> pd.DataFrame:
    if not chunks_path.exists():
        raise FileNotFoundError(f"Missing chunks parquet: {chunks_path}")

    df = pd.read_parquet(chunks_path)

    # Normalize expected columns:
    # page_num ← one of ["page_num", "page", "page_number", "page_idx"]
    df, _ = _coalesce_column(
        df, ["page_num", "page", "page_number", "page_idx"], "page_num", default=0
    )

    # chunk_id ← one of ["chunk_id", "chunk_idx", "chunk_uid", "chunk_no", "chunk_index"]
    df, has_chunk_id = _coalesce_column(
        df, ["chunk_id", "chunk_idx", "chunk_uid", "chunk_no", "chunk_index"], "chunk_id"
    )

    # Ensure other required fields exist + sane types
    required_base = {"doc_sha256", "text"}
    missing_base = required_base - set(df.columns)
    if missing_base:
        raise ValueError(f"doc_chunks missing columns: {sorted(missing_base)}")

    # lang optional → default "en"
    if "lang" not in df.columns:
        df["lang"] = "en"

    # Clean & types
    df["text"] = df["text"].astype(str)
    df["doc_sha256"] = df["doc_sha256"].astype(str)
    df["lang"] = df["lang"].astype(str)
    df["page_num"] = df["page_num"].astype("int64")

    # Fill chunk_id deterministically if absent
    if not has_chunk_id:
        # Stable order: by doc, then page, then original row order
        df = df.reset_index(drop=True)
        df = df.sort_values(["doc_sha256", "page_num"]).copy()
        df["_seq"] = df.groupby(["doc_sha256", "page_num"]).cumcount()
        df["chunk_id"] = (
            df["doc_sha256"].str[:12]
            + ":"
            + df["page_num"].astype(str)
            + ":"
            + df["_seq"].astype(str)
        )
        df = df.drop(columns=["_seq"])

    # Final required set
    required = {"doc_sha256", "page_num", "lang", "text", "chunk_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"doc_chunks still missing columns after normalization: {sorted(missing)}")

    # Drop empty/whitespace-only texts
    df = df[df["text"].str.strip().str.len() > 0].copy()
    return df


def _load_doc_links(links_csv: Path) -> Optional[pd.DataFrame]:
    if not links_csv.exists():
        LOGGER.info("No doc_links found at %s (optional). Proceeding.", links_csv)
        return None
    df = pd.read_csv(links_csv)
    required = {"doc_sha256", "kg_node_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"doc_links missing columns: {sorted(missing)}")
    return df


def _attach_kg_nodes(chunks: pd.DataFrame, links: Optional[pd.DataFrame]) -> pd.DataFrame:
    if links is None or links.empty:
        chunks["kg_nodes"] = [[] for _ in range(len(chunks))]
        return chunks

    groups = (
        links.groupby("doc_sha256")["kg_node_id"]
        .apply(lambda s: sorted(set(map(str, s.tolist()))))
        .reset_index()
        .rename(columns={"kg_node_id": "kg_nodes"})
    )
    out = chunks.merge(groups, on="doc_sha256", how="left")
    out["kg_nodes"] = out["kg_nodes"].apply(lambda x: x if isinstance(x, list) else [])
    return out


def _prefix_e5_passage(texts: Iterable[str]) -> List[str]:
    # E5 expects "passage: ..." for passage embeddings
    return [f"passage: {t}" for t in texts]


@dataclass
class IndexArtifacts:
    index_path: Path
    meta_path: Path
    manifest_path: Path
    vectors_path: Path


def _embed_batches(
    model: SentenceTransformer, texts: List[str], batch_size: int = 64
) -> np.ndarray:
    embs: List[np.ndarray] = []
    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
        batch = texts[i : i + batch_size]
        em = model.encode(
            batch,
            normalize_embeddings=True,
            show_progress_bar=False,
            convert_to_numpy=True,
        )
        embs.append(em.astype(np.float32))
    return np.vstack(embs)


def _build_faiss_index(vectors: np.ndarray) -> faiss.Index:
    # Cosine similarity via inner product; vectors are normalized
    dim = vectors.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(vectors)
    return index


def _save_artifacts(
    out_dir: Path,
    index: faiss.Index,
    meta: pd.DataFrame,
    model_name: str,
    vectors: Optional[np.ndarray] = None,
) -> IndexArtifacts:
    _ensure_dir(out_dir)
    index_path = out_dir / "m4.index"
    faiss.write_index(index, str(index_path))

    meta_path = out_dir / "m4_index_meta.parquet"
    # Keep text, ids, etc. Avoid super wide columns.
    meta.to_parquet(meta_path, index=False)

    vectors_path = out_dir / "vectors.npy"
    if vectors is not None:
        np.save(vectors_path, vectors)

    manifest = {
        "model": model_name,
        "dim": int(index.d),
        "count": int(index.ntotal),
        "created_at": int(time.time()),
        "files": {
            "index": str(index_path),
            "meta": str(meta_path),
            "vectors": str(vectors_path) if vectors is not None else None,
        },
    }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return IndexArtifacts(index_path, meta_path, manifest_path, vectors_path)


def build_index(
    chunks_path: Path,
    links_csv: Path,
    out_dir: Path,
    model_name: str = "intfloat/e5-base",
    batch_size: int = 64,
    save_vectors: bool = False,
) -> IndexArtifacts:
    LOGGER.info("Loading chunks from %s", chunks_path)
    chunks = _load_chunks(chunks_path)

    LOGGER.info("Attaching kg_nodes (if available) from %s", links_csv)
    links = _load_doc_links(links_csv)
    chunks = _attach_kg_nodes(chunks, links)

    # Prepare texts
    texts = chunks["text"].astype(str).tolist()
    prefixed = _prefix_e5_passage(texts)

    LOGGER.info("Loading embedding model: %s", model_name)
    model = SentenceTransformer(model_name)

    LOGGER.info("Embedding %d chunks", len(prefixed))
    vectors = _embed_batches(model, prefixed, batch_size=batch_size)

    LOGGER.info("Building FAISS index (dim=%d, n=%d)", vectors.shape[1], vectors.shape[0])
    index = _build_faiss_index(vectors)

    # Build metadata frame
    meta = pd.DataFrame(
        {
            "vector_id": np.arange(len(chunks), dtype=np.int32),
            "doc_id": chunks["doc_sha256"].astype(str),
            "page": chunks["page_num"].astype(int),
            "lang": chunks["lang"].astype(str),
            "kg_nodes": chunks["kg_nodes"],
            "chunk_id": chunks["chunk_id"].astype(str),
            "text": chunks["text"].astype(str),
        }
    )
    # Optional: include source_path if present in chunks
    if "source_path" in chunks.columns:
        meta["source_path"] = chunks["source_path"].astype(str)
    else:
        meta["source_path"] = ""

    LOGGER.info("Saving artifacts to %s", out_dir)
    artifacts = _save_artifacts(
        out_dir=out_dir,
        index=index,
        meta=meta,
        model_name=model_name,
        vectors=vectors if save_vectors else None,
    )
    LOGGER.info(
        "M4.1 Index Build complete: index=%s, meta=%s, manifest=%s",
        artifacts.index_path,
        artifacts.meta_path,
        artifacts.manifest_path,
    )
    return artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description="M4.1 Build FAISS index for RAG")
    parser.add_argument(
        "--chunks",
        type=Path,
        default=Path("data/interim/doc_chunks.parquet"),
        help="Path to doc_chunks parquet",
    )
    parser.add_argument(
        "--doc-links",
        type=Path,
        default=Path("data/processed/kg/doc_links.csv"),
        help="Optional doc→kg links CSV",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/index/m4_faiss"),
        help="Output directory for index artifacts",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="intfloat/e5-base",
        help="SentenceTransformers model name or local path",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=64,
        help="Embedding batch size",
    )
    parser.add_argument(
        "--save-vectors",
        action="store_true",
        help="Also save vectors.npy (for audits)",
    )
    args = parser.parse_args()

    try:
        build_index(
            chunks_path=args.chunks,
            links_csv=args.doc_links,
            out_dir=args.out,
            model_name=args.model,
            batch_size=args.batch,
            save_vectors=args.save_vectors,
        )
    except FileNotFoundError as exc:
        LOGGER.error("File not found: %s", exc)
        return 2
    except ValueError as exc:
        LOGGER.error("Validation error: %s", exc)
        return 3
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Unexpected error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
