"""
M4.2 Draft + Verify (CPU/GPU), no LLM

Pipeline:
- Load FAISS index + metadata from M4.1
- Encode query with E5 ("query: ...")
- Retrieve top-k chunks
- Build an extractive draft (sentence overlap)
- Verify sentences with XNLI (entailment/contradiction)
- Return structured JSON (answer, citations, pages, kg_nodes, nli_scores, elapsed_ms)

Device selection:
- FORCE_DEVICE=cpu      (local dev)
- FORCE_DEVICE=cuda:0   (GPU, e.g., Runpod/Colab) — uses fp16
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import faiss  # type: ignore
import numpy as np
import pandas as pd
import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

LOGGER = logging.getLogger("m4.draftverify")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


# ------------------------- device helpers -------------------------
def _device_from_env() -> str:
    dev = os.getenv("FORCE_DEVICE", "").strip().lower()
    if dev:
        return dev
    return "cuda:0" if torch.cuda.is_available() else "cpu"


def _pipeline_device_index(device: str) -> int:
    if device.startswith("cuda"):
        parts = device.split(":")
        return int(parts[1]) if len(parts) == 2 and parts[1].isdigit() else 0
    return -1


# ------------------------- load artifacts -------------------------
@dataclass
class Retriever:
    index: faiss.Index
    meta: pd.DataFrame
    embedder: SentenceTransformer
    dim: int


def _ensure_paths(index_dir: Path) -> Tuple[Path, Path]:
    manifest = index_dir / "manifest.json"
    if not manifest.exists():
        raise FileNotFoundError(f"Missing manifest: {manifest}")
    data = json.loads(manifest.read_text(encoding="utf-8"))
    index_path = Path(data["files"]["index"])
    meta_path = Path(data["files"]["meta"])
    if not index_path.exists():
        raise FileNotFoundError(f"Missing index: {index_path}")
    if not meta_path.exists():
        raise FileNotFoundError(f"Missing meta: {meta_path}")
    return index_path, meta_path


def load_retriever(index_dir: Path) -> Retriever:
    index_path, meta_path = _ensure_paths(index_dir)
    LOGGER.info("Loading FAISS index: %s", index_path)
    index = faiss.read_index(str(index_path))
    LOGGER.info("Loading metadata: %s", meta_path)
    meta = pd.read_parquet(meta_path)

    device = _device_from_env()
    LOGGER.info("Loading embedder: intfloat/e5-base on %s", device)
    emb = SentenceTransformer("intfloat/e5-base", device=device)
    return Retriever(index=index, meta=meta, embedder=emb, dim=int(index.d))


# ------------------------- retrieval + draft -------------------------
def _embed_query(embedder: SentenceTransformer, query: str) -> np.ndarray:
    q = f"query: {query}"
    vec = embedder.encode(
        [q],
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    ).astype(np.float32)
    return vec[0]


def _to_list_of_str(x: Any) -> List[str]:
    """Normalize kg_nodes to a Python list[str] safely."""
    if x is None:
        return []
    if isinstance(x, float) and math.isnan(x):
        return []
    if isinstance(x, (list, tuple)):
        return [str(v) for v in x if v is not None]
    if isinstance(x, np.ndarray):
        return [str(v) for v in x.tolist() if v is not None]
    if isinstance(x, pd.Series):
        return [str(v) for v in x.dropna().tolist()]
    s = str(x).strip()
    if s in ("", "nan", "None"):
        return []
    try:
        j = json.loads(s)
        if isinstance(j, list):
            return [str(v) for v in j]
    except Exception:
        pass
    return [s]


def retrieve(r: Retriever, query: str, topk: int = 5) -> List[Dict[str, Any]]:
    qv = _embed_query(r.embedder, query)
    qv = np.expand_dims(qv, axis=0)
    scores, idx = r.index.search(qv, topk)
    scores = scores[0].tolist()
    ids = idx[0].tolist()

    hits: List[Dict[str, Any]] = []
    for score, vid in zip(scores, ids):
        if vid < 0:
            continue
        row = r.meta.iloc[int(vid)]
        hits.append(
            {
                "vector_id": int(vid),
                "score": float(score),
                "doc_id": str(row.get("doc_id", "")),
                "page": int(row.get("page", 0)),
                "lang": str(row.get("lang", "en")),
                "kg_nodes": _to_list_of_str(row.get("kg_nodes", [])),
                "chunk_id": str(row.get("chunk_id", "")),
                "text": str(row.get("text", "")),
                "source_path": str(row.get("source_path", "")),
            }
        )
    return hits


_SENT_SPLIT = re.compile(r"(?<=[\.\!\?])\s+")


def _tokenize_simple(text: str) -> List[str]:
    return re.findall(r"[A-Za-z0-9]+", text.lower())


def _sentence_score(query: str, sentence: str) -> float:
    qt = set(_tokenize_simple(query))
    st = set(_tokenize_simple(sentence))
    if not qt or not st:
        return 0.0
    inter = len(qt & st)
    union = len(qt | st)
    return (inter + 0.5) / (union + 0.5)


def build_draft(query: str, hits: List[Dict[str, Any]], max_sentences: int = 4) -> List[str]:
    candidates: List[Tuple[float, str]] = []
    for h in hits:
        sentences = [s.strip() for s in _SENT_SPLIT.split(h["text"]) if s.strip()]
        for s in sentences:
            sc = _sentence_score(query, s)
            if sc > 0:
                candidates.append((sc, s))
    candidates.sort(reverse=True, key=lambda x: x[0])

    seen: set = set()
    draft: List[str] = []
    for _, s in candidates:
        k = " ".join(_tokenize_simple(s))[:80]
        if k in seen:
            continue
        seen.add(k)
        draft.append(s)
        if len(draft) >= max_sentences:
            break
    return draft


# ------------------------- verifier (XNLI) -------------------------
@dataclass
class Verifier:
    pipe: Any  # transformers pipeline


def load_verifier(model_name: str = "joeddav/xlm-roberta-large-xnli") -> Verifier:
    device = _device_from_env()
    dev_idx = _pipeline_device_index(device)
    use_fp16 = device.startswith("cuda")

    tok = AutoTokenizer.from_pretrained(model_name)
    mdl = AutoModelForSequenceClassification.from_pretrained(
        model_name,
        torch_dtype=torch.float16 if use_fp16 else torch.float32,
    )
    if use_fp16:
        mdl = mdl.to(device)

    nli = pipeline(
        "text-classification",
        model=mdl,
        tokenizer=tok,
        return_all_scores=True,
        truncation=True,
        padding=True,
        max_length=256,
        device=dev_idx,
    )
    return Verifier(pipe=nli)


def _nli_probs(scores: List[Dict[str, float]]) -> Dict[str, float]:
    out = {d["label"].lower(): float(d["score"]) for d in scores}
    return {
        "entailment": out.get("entailment", 0.0),
        "contradiction": out.get("contradiction", 0.0),
        "neutral": out.get("neutral", 0.0),
    }


def verify_draft(
    v: Verifier,
    draft_sents: List[str],
    hits: List[Dict[str, Any]],
    entail_thresh: float = 0.55,
    contra_max: float = 0.30,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    verified: List[str] = []
    scores_out: List[Dict[str, Any]] = []

    for s in draft_sents:
        best_ent = -1.0
        best_contra = 1.0
        best_ref: Optional[Dict[str, Any]] = None

        pairs = [{"text": h["text"][:1200], "text_pair": s} for h in hits]
        if not pairs:
            continue

        results = v.pipe(pairs)
        for h, res in zip(hits, results):
            probs = _nli_probs(res)
            ent = probs["entailment"]
            contra = probs["contradiction"]
            if ent > best_ent:
                best_ent = ent
                best_contra = contra
                best_ref = h

        if best_ref is None:
            continue

        scores_out.append(
            {
                "sentence": s,
                "entailment": float(best_ent),
                "contradiction": float(best_contra),
                "best_doc_id": best_ref["doc_id"],
                "best_page": int(best_ref["page"]),
            }
        )

        if best_ent >= entail_thresh and best_contra <= contra_max:
            verified.append(s)

    return verified, scores_out


def assemble_answer(verified_sents: List[str]) -> str:
    return "" if not verified_sents else " ".join(verified_sents)


# ------------------------- public API -------------------------
def ask(
    query: str,
    index_dir: Path = Path("data/index/m4_faiss"),
    topk: int = 5,
    max_sentences: int = 4,
) -> Dict[str, Any]:
    t0 = time.time()

    r = load_retriever(index_dir)
    hits = retrieve(r, query, topk=topk)
    draft_sents = build_draft(query, hits, max_sentences=max_sentences)

    v = load_verifier()
    verified_sents, nli_scores = verify_draft(v, draft_sents, hits)

    answer = assemble_answer(verified_sents)

    citations = []
    seen = set()
    for h in hits:
        key = (h["doc_id"], h["page"])
        if key in seen:
            continue
        seen.add(key)
        citations.append({"doc_id": h["doc_id"], "page": int(h["page"])})

    pages = [
        {
            "doc_id": h["doc_id"],
            "page": int(h["page"]),
            "text": h["text"],
            "score": float(h["score"]),
        }
        for h in hits
    ]

    kg_nodes: List[str] = []
    for h in hits:
        for n in h.get("kg_nodes", []) or []:
            if n not in kg_nodes:
                kg_nodes.append(str(n))

    elapsed_ms = int(math.floor((time.time() - t0) * 1000.0))
    return {
        "query": query,
        "answer": answer,
        "citations": citations,
        "pages": pages,
        "kg_nodes": kg_nodes,
        "nli_scores": nli_scores,
        "elapsed_ms": elapsed_ms,
    }


# ------------------------- CLI (standalone) -------------------------
def _main() -> int:
    parser = argparse.ArgumentParser(description="M4.2 Draft+Verify (CPU/GPU)")
    parser.add_argument("--q", "--query", dest="query", type=str, required=True)
    parser.add_argument(
        "--index-dir",
        type=Path,
        default=Path("data/index/m4_faiss"),
        help="Directory with manifest.json and index/meta files",
    )
    parser.add_argument("--k", type=int, default=5, help="Top-k retrieval")
    parser.add_argument(
        "--max-sentences",
        type=int,
        default=4,
        help="Max sentences before verification",
    )
    parser.add_argument("--out", type=Path, default=None, help="Write JSON result")
    args = parser.parse_args()

    try:
        result = ask(
            query=args.query,
            index_dir=args.index_dir,
            topk=args.k,
            max_sentences=args.max_sentences,
        )
    except FileNotFoundError as exc:
        LOGGER.error("Missing artifact: %s", exc)
        return 2
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Unexpected error: %s", exc)
        return 1

    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.out is not None:
        args.out.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
