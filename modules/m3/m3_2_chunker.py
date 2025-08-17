from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

DOC_PAGES_FP = Path("data/interim/doc_pages.parquet")
CHUNKS_FP = Path("data/interim/doc_chunks.parquet")
METRICS_FP = Path("data/interim/clean_chunk_metrics.json")

# Sentence split regex: ends with . ! ? or Arabic ؟ ۔ then whitespace
# next char is a plausible sentence starter (letter/number/Arabic)
SENT_SPLIT = re.compile(r"(?<=[\.\!\?؟۔])\s+(?=[A-Za-z0-9\u0600-\u06FF])")

# Very small abbreviation guard (kept minimal to avoid complexity/offline deps)
ABBR = {"e.g.", "i.e.", "mr.", "mrs.", "dr.", "vs.", "u.s.", "u.k.", "etc."}

# Arabic detection heuristic (same spirit as M3.1)
ARABIC_BLOCK = re.compile(r"[\u0600-\u06FF]")


@dataclass
class ChunkRec:
    doc_id: str
    doc_path: str
    source: str
    doc_sha256: str
    chunk_index: int
    text: str
    lang: str
    n_words: int
    n_sentences: int
    page_start: int
    page_end: int


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def is_ar(text: str) -> bool:
    if not text:
        return False
    ar_chars = len(ARABIC_BLOCK.findall(text))
    return ar_chars / max(1, len(text)) >= 0.10


def simple_sent_split(text: str, lang_hint: str) -> List[str]:
    """
    Lightweight, offline sentence splitter.
    1) Lowercase a copy to protect common abbreviations from splits.
    2) Split with SENT_SPLIT.
    3) Merge fragments if we accidentally split on abbreviations.
    """
    if not text:
        return []

    parts = SENT_SPLIT.split(text.strip())
    if len(parts) <= 1:
        return [text.strip()] if text.strip() else []

    # Merge if previous ends with abbreviation token
    merged: List[str] = []
    for part in parts:
        p = part.strip()
        if not p:
            continue
        if merged:
            prev = merged[-1]
            prev_tail = prev[-5:].lower()  # small window is enough for ABBR
            # If prev ends with "... e.g." or similar, append current to previous
            if any(prev_tail.endswith(a[-5:]) and prev.lower().endswith(a) for a in ABBR):
                merged[-1] = normalize_ws(prev + " " + p)
                continue
        merged.append(p)

    return merged


def word_count(text: str) -> int:
    # treat words as whitespace-delimited tokens
    return len([w for w in (text.split()) if w])


def chunk_sentences(
    sents: List[Tuple[str, int]],  # (sentence, page_no)
    target_min: int,
    target_max: int,
    guarantee_min_words: int,
) -> List[Tuple[List[Tuple[str, int]], int, int]]:
    """
    Greedy packer: accumulate sentences until >= target_min words.
    Allow overshoot up to 20% beyond target_max; otherwise roll to next chunk.
    Returns list of (sentences_with_pages, page_start, page_end).
    """
    chunks: List[Tuple[List[Tuple[str, int]], int, int]] = []
    cur: List[Tuple[str, int]] = []
    cur_words = 0
    page_start = None
    page_end = None

    def flush():
        nonlocal cur, cur_words, page_start, page_end
        if cur:
            chunks.append((cur, page_start or cur[0][1], page_end or cur[-1][1]))
        cur = []
        cur_words = 0
        page_start = None
        page_end = None

    for sent, pg in sents:
        sw = word_count(sent)
        if not sw:
            continue
        if not cur:
            page_start = pg
        page_end = pg

        # If we already exceed target_min, decide whether to flush or add this one
        if cur_words >= target_min:
            projected = cur_words + sw
            # allow up to 20% overshoot over target_max
            overshoot_ok = projected <= int(target_max * 1.2)
            if projected > target_max and not overshoot_ok:
                flush()
            # start fresh chunk for this sentence
        cur.append((sent, pg))
        cur_words += sw

        if cur_words >= target_min and cur_words >= target_max:
            flush()

    # flush tail
    if cur:
        flush()

    return chunks


def run_chunker(
    target_min: int = 400,
    target_max: int = 800,
    guarantee_min_words: int = 120,
) -> int:
    Path("data/interim").mkdir(parents=True, exist_ok=True)

    if not DOC_PAGES_FP.exists():
        print(
            "❌ M3.2 Chunker: data/interim/doc_pages.parquet not found. Run m3.load first.",
            file=sys.stderr,
        )
        return 1

    df = pd.read_parquet(DOC_PAGES_FP)
    if df.empty:
        print("❌ M3.2 Chunker: doc_pages.parquet is empty.", file=sys.stderr)
        return 1

    # Ensure required columns are present
    required_cols = {"doc_id", "doc_path", "source", "page", "text", "lang", "doc_sha256", "words"}
    missing = required_cols - set(df.columns)
    if missing:
        print(f"❌ M3.2 Chunker: missing columns in doc_pages.parquet: {missing}", file=sys.stderr)
        return 1

    # Build chunks per document
    records: List[ChunkRec] = []
    docs_failed: List[str] = []
    docs_guaranteed: List[str] = []
    per_source_chunks: Dict[str, int] = {}

    # Grouping key: (doc_sha256, doc_id, doc_path, source)
    group_cols = ["doc_sha256", "doc_id", "doc_path", "source"]
    for (doc_sha, doc_id, doc_path, source), g in df.sort_values(["doc_id", "page"]).groupby(
        group_cols
    ):
        # Concatenate sentences across pages in order
        total_words_doc = int(g["words"].sum())
        lang_majority = g["lang"].mode().iat[0] if not g["lang"].mode().empty else "en"

        sents_with_pages: List[Tuple[str, int]] = []
        for _, row in g.sort_values("page").iterrows():
            text = normalize_ws(row["text"])
            if not text:
                continue
            # choose splitter based on majority lang (heuristic); still works fine mixed
            sents = simple_sent_split(text, lang_majority)
            # fallback: if splitter failed, treat whole page as one sentence
            if not sents:
                sents = [text]
            sents_with_pages.extend([(s, int(row["page"])) for s in sents])

        # Build chunks
        raw_chunks = chunk_sentences(
            sents_with_pages,
            target_min=target_min,
            target_max=target_max,
            guarantee_min_words=guarantee_min_words,
        )

        # Guarantee: if doc has ≥ guarantee_min_words and no chunks were produced,
        # emit a single chunk with all text concatenated (lenient fallback).
        if not raw_chunks and total_words_doc >= guarantee_min_words:
            docs_guaranteed.append(doc_id)
            # Concatenate all text (ordered by page),
            # then cap to ~1.2 * target_max words to avoid huge chunk
            all_text = normalize_ws(
                " ".join([normalize_ws(t) for t in g.sort_values("page")["text"].tolist() if t])
            )
            words = all_text.split()
            cap = int(target_max * 1.2)
            capped_text = " ".join(words[:cap]) if len(words) > cap else all_text
            page_start = int(g["page"].min())
            page_end = int(g["page"].max())
            raw_chunks = [([(capped_text, page_start)], page_start, page_end)]

        # Emit records
        if raw_chunks:
            for idx, (sent_pack, p_start, p_end) in enumerate(raw_chunks, start=1):
                chunk_text = normalize_ws(" ".join([s for s, _ in sent_pack]))
                n_words = word_count(chunk_text)
                n_sents = len(sent_pack)
                rec = ChunkRec(
                    doc_id=doc_id,
                    doc_path=doc_path,
                    source=source,
                    doc_sha256=doc_sha,
                    chunk_index=idx,
                    text=chunk_text,
                    lang=("ar" if is_ar(chunk_text) else "en") if chunk_text else lang_majority,
                    n_words=n_words,
                    n_sentences=n_sents,
                    page_start=int(p_start),
                    page_end=int(p_end),
                )
                records.append(rec)
            per_source_chunks[source] = per_source_chunks.get(source, 0) + len(raw_chunks)
        else:
            # Doc had < guarantee_min_words and produced no chunks → acceptable; record for metrics
            docs_failed.append(doc_id)

    # Build DataFrame and write
    if records:
        cdf = pd.DataFrame([r.__dict__ for r in records])
        cdf.to_parquet(CHUNKS_FP, index=False)
    else:
        cdf = pd.DataFrame(columns=[f.name for f in ChunkRec.__dataclass_fields__.values()])
        cdf.to_parquet(CHUNKS_FP, index=False)

    chunks_total = int(cdf.shape[0])
    docs_total = int(df.groupby(["doc_sha256", "doc_id"]).ngroups)
    docs_with_chunks = (
        int(cdf[["doc_sha256", "doc_id"]].drop_duplicates().shape[0]) if chunks_total else 0
    )

    # Metrics
    words_per_chunk = cdf["n_words"].tolist() if chunks_total else []
    sentences_per_chunk = cdf["n_sentences"].tolist() if chunks_total else []

    metrics = {
        "params": {
            "target_min_words": target_min,
            "target_max_words": target_max,
            "guarantee_min_words": guarantee_min_words,
        },
        "docs_total": docs_total,
        "docs_with_chunks": docs_with_chunks,
        "chunks_total": chunks_total,
        "chunks_per_source": per_source_chunks,
        "docs_failed_zero_chunks": sorted(set(docs_failed)),
        "docs_guaranteed": sorted(set(docs_guaranteed)),
        "words_per_chunk": {
            "mean": float(np.mean(words_per_chunk)) if words_per_chunk else 0.0,
            "median": float(np.median(words_per_chunk)) if words_per_chunk else 0.0,
            "p95": float(np.percentile(words_per_chunk, 95)) if words_per_chunk else 0.0,
            "min": int(min(words_per_chunk)) if words_per_chunk else 0,
            "max": int(max(words_per_chunk)) if words_per_chunk else 0,
        },
        "sentences_per_chunk": {
            "mean": float(np.mean(sentences_per_chunk)) if sentences_per_chunk else 0.0,
            "median": float(np.median(sentences_per_chunk)) if sentences_per_chunk else 0.0,
        },
    }

    METRICS_FP.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    # Hard gate
    if chunks_total == 0:
        print("❌ M3.2 Chunker: produced 0 chunks — failing fast.", file=sys.stderr)
        return 1

    # Soft per-doc gate will be enforced in verify(): any doc with ≥120 words must appear in cdf
    print(
        f"✅ M3.2 Chunker: chunks={chunks_total}, docs_with_chunks={docs_with_chunks}/{docs_total}, "
        f"guaranteed={len(metrics['docs_guaranteed'])}"
    )
    print("→ data/interim/doc_chunks.parquet")
    print("→ data/interim/clean_chunk_metrics.json")
    return 0
