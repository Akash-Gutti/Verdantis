from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import pandas as pd

DOC_PAGES_FP = Path("data/interim/doc_pages.parquet")
ENTITIES_FP = Path("data/interim/doc_entities.parquet")
CLAUSES_FP = Path("data/interim/doc_clauses.parquet")
METRICS_FP = Path("data/interim/ner_clause_metrics.json")

# ----- Sentence splitting (same spirit as M3.2) -----
SENT_SPLIT = re.compile(r"(?<=[\.\!\?؟۔])\s+(?=[A-Za-z0-9\u0600-\u06FF])")


def sent_spans(text: str) -> List[Tuple[int, int, str]]:
    """Return [(start, end, sentence)] using regex splitter with offsets."""
    s = text.strip()
    if not s:
        return []
    spans: List[Tuple[int, int, str]] = []
    start = 0
    for part in SENT_SPLIT.split(s):
        p = part.strip()
        if not p:
            continue
        # find p within s starting at 'start' to get stable offsets
        idx = s.find(p, start)
        if idx == -1:
            # fallback: scan from 0 (rare)
            idx = s.find(p)
        end = idx + len(p)
        spans.append((idx, end, p))
        start = end
    if not spans:
        spans.append((0, len(s), s))
    return spans


# ----- Clause keyword regexes -----
EMISSIONS_RX = re.compile(
    (
        r"\b("
        r"emission(?:s)?|"
        r"ghg|"
        r"greenhouse\s+gas(?:es)?|"
        r"co2|co₂|"
        r"carbon|"
        r"scope\s*[123]|"
        r"tco2e|"
        r"ch4|"
        r"methane"
        r")\b"
    ),
    re.I,
)
WATER_RX = re.compile(
    r"\b(water|withdrawal|discharge|wastewater|effluent|consumption|irrigation)\b", re.I
)
WASTE_RX = re.compile(
    r"\b(waste|recycl(?:e|ing)|landfill|hazardous|incineration|solid\s+waste)\b", re.I
)
ENERGY_RX = re.compile(
    r"\b(energy|electricity|kwh|mwh|renewable|solar|wind|efficien(?:t|cy))\b", re.I
)

CLAUSE_SPECS = [
    ("emissions", EMISSIONS_RX),
    ("water", WATER_RX),
    ("waste", WASTE_RX),
    ("energy", ENERGY_RX),
]

ARABIC_BLOCK = re.compile(r"[\u0600-\u06FF]")


def is_ar(text: str) -> bool:
    if not text:
        return False
    ar_chars = len(ARABIC_BLOCK.findall(text))
    return ar_chars / max(1, len(text)) >= 0.10


# ----- spaCy loader with safe fallbacks -----
def load_spacy_en():
    try:
        import spacy

        try:
            return spacy.load("en_core_web_sm")
        except Exception:
            # Build minimal fallback: blank English + EntityRuler with a few patterns
            nlp = spacy.blank("en")
            ruler = nlp.add_pipe("entity_ruler")
            patterns = [
                {
                    "label": "ORG",
                    "pattern": [
                        {"IS_TITLE": True},
                        {"IS_TITLE": True, "OP": "?"},
                        {"LOWER": {"IN": ["llc", "plc", "pjsc", "inc.", "inc"]}, "OP": "?"},
                    ],
                },
                {
                    "label": "GPE",
                    "pattern": [
                        {
                            "TEXT": {
                                "REGEX": "UAE|Dubai|Abu Dhabi|KSA|Saudi|Qatar|Bahrain|Oman|Kuwait"
                            }
                        }
                    ],
                },
                {"label": "PERCENT", "pattern": [{"LIKE_NUM": True}, {"TEXT": "%"}]},
                {"label": "MONEY", "pattern": [{"IS_CURRENCY": True}, {"LIKE_NUM": True}]},
                {"label": "DATE", "pattern": [{"SHAPE": "dddd"}]},  # 2025, 2030
            ]
            ruler.add_patterns(patterns)
            nlp.add_pipe("sentencizer")
            return nlp
    except Exception:
        return None


# ----- Data classes -----
@dataclass
class EntRec:
    doc_sha256: str
    doc_id: str
    doc_path: str
    source: str
    page: int
    span_start: int
    span_end: int
    text: str
    label: str
    lang: str
    extractor: str  # spacy_en | ar_regex


@dataclass
class ClauseRec:
    doc_sha256: str
    doc_id: str
    doc_path: str
    source: str
    page: int
    clause_type: str  # emissions|water|waste|energy
    span_start: int
    span_end: int
    text: str
    lang: str
    pattern: str


# ----- Arabic lightweight entity fallback -----
AR_DATE_RX = re.compile(r"(20\d{2}|19\d{2})")
AR_PERCENT_RX = re.compile(r"\d+\s*%")
AR_MEASURE_RX = re.compile(
    r"\b(\d+(?:[\.,]\d+)?\s*(?:tco2e|co2|co₂|kwh|mwh|gwh|tonnes?|m3|m³))\b", re.I
)


def arabic_entities(text: str) -> List[Tuple[int, int, str, str]]:
    """Return list of (start, end, label, extractor='ar_regex')."""
    res: List[Tuple[int, int, str, str]] = []
    for m in AR_DATE_RX.finditer(text):
        res.append((m.start(), m.end(), "DATE", "ar_regex"))
    for m in AR_PERCENT_RX.finditer(text):
        res.append((m.start(), m.end(), "PERCENT", "ar_regex"))
    for m in AR_MEASURE_RX.finditer(text):
        res.append((m.start(), m.end(), "QUANTITY", "ar_regex"))
    return res


# ----- Clause harvesting -----
def find_clauses(text: str, lang_hint: str) -> List[Tuple[int, int, str, str]]:
    """
    Return list of (start, end, clause_type, pattern_name) at sentence granularity.
    If multiple types match the same sentence, we emit multiple rows (one per type).
    """
    out: List[Tuple[int, int, str, str]] = []
    spans = sent_spans(text)
    for s_start, s_end, s_txt in spans:
        for ctype, rx in CLAUSE_SPECS:
            if rx.search(s_txt):
                out.append((s_start, s_end, ctype, rx.pattern))
    return out


# ----- Main runner -----
def run_ner_clause(batch_size: int = 32) -> int:
    Path("data/interim").mkdir(parents=True, exist_ok=True)

    if not DOC_PAGES_FP.exists():
        print(
            "❌ M3.3 NER+Clause: data/interim/doc_pages.parquet not found. Run m3.load first.",
            file=sys.stderr,
        )
        return 1

    pages = pd.read_parquet(DOC_PAGES_FP)
    if pages.empty:
        print("❌ M3.3 NER+Clause: doc_pages.parquet is empty.", file=sys.stderr)
        return 1

    # Required cols
    need = {"doc_sha256", "doc_id", "doc_path", "source", "page", "text", "lang"}
    miss = need - set(pages.columns)
    if miss:
        print(f"❌ M3.3: missing columns in doc_pages.parquet: {miss}", file=sys.stderr)
        return 1

    # Load spaCy EN (with fallback)
    nlp = load_spacy_en()

    # Split rows by language hint
    pages["lang_hint"] = pages["lang"].fillna("en").astype(str)
    en_rows = pages[pages["lang_hint"] == "en"].copy()
    ar_rows = pages[pages["lang_hint"] == "ar"].copy()

    entities: List[EntRec] = []
    clauses: List[ClauseRec] = []

    # --- English: spaCy pipeline in batches ---
    if not en_rows.empty and nlp is not None:
        texts = en_rows["text"].fillna("").tolist()
        meta = en_rows[
            ["doc_sha256", "doc_id", "doc_path", "source", "page", "lang_hint"]
        ].values.tolist()
        # Pipe
        try:
            docs = nlp.pipe(texts, batch_size=batch_size)
        except Exception:
            # If pipe fails (rare), fallback to per-row
            docs = (nlp(t) for t in texts)
        for doc, m in zip(docs, meta):
            doc_sha, doc_id, doc_path, source, page, lang_hint = m
            raw_text = doc.text
            # Entities
            for ent in getattr(doc, "ents", []):
                if not ent.text.strip():
                    continue
                entities.append(
                    EntRec(
                        doc_sha256=doc_sha,
                        doc_id=doc_id,
                        doc_path=doc_path,
                        source=source,
                        page=int(page),
                        span_start=int(ent.start_char),
                        span_end=int(ent.end_char),
                        text=ent.text.strip(),
                        label=ent.label_,
                        lang="en",
                        extractor="spacy_en",
                    )
                )
            # Clauses (sentence-level) on page text
            for s_start, s_end, ctype, pat in find_clauses(raw_text, "en"):
                clauses.append(
                    ClauseRec(
                        doc_sha256=doc_sha,
                        doc_id=doc_id,
                        doc_path=doc_path,
                        source=source,
                        page=int(page),
                        clause_type=ctype,
                        span_start=int(s_start),
                        span_end=int(s_end),
                        text=raw_text[s_start:s_end].strip(),
                        lang="en",
                        pattern=pat,
                    )
                )

    # --- Arabic: regex-only lightweight fallback ---
    if not ar_rows.empty:
        for _, r in ar_rows.iterrows():
            raw_text = str(r["text"] or "")
            doc_sha = r["doc_sha256"]
            doc_id = r["doc_id"]
            doc_path = r["doc_path"]
            source = r["source"]
            page = int(r["page"])
            # Entities (regex)
            for s, e, label, extractor in arabic_entities(raw_text):
                entities.append(
                    EntRec(
                        doc_sha256=doc_sha,
                        doc_id=doc_id,
                        doc_path=doc_path,
                        source=source,
                        page=page,
                        span_start=int(s),
                        span_end=int(e),
                        text=raw_text[s:e],
                        label=label,
                        lang="ar",
                        extractor=extractor,
                    )
                )
            # Clauses (same keyword sets; many are English/Latin units present in Arabic docs)
            for s_start, s_end, ctype, pat in find_clauses(raw_text, "ar"):
                clauses.append(
                    ClauseRec(
                        doc_sha256=doc_sha,
                        doc_id=doc_id,
                        doc_path=doc_path,
                        source=source,
                        page=page,
                        clause_type=ctype,
                        span_start=int(s_start),
                        span_end=int(s_end),
                        text=raw_text[s_start:s_end].strip(),
                        lang="ar",
                        pattern=pat,
                    )
                )

    # Write outputs
    ent_df = (
        pd.DataFrame([e.__dict__ for e in entities])
        if entities
        else pd.DataFrame(columns=[f.name for f in EntRec.__dataclass_fields__.values()])
    )
    cls_df = (
        pd.DataFrame([c.__dict__ for c in clauses])
        if clauses
        else pd.DataFrame(columns=[f.name for f in ClauseRec.__dataclass_fields__.values()])
    )

    ent_df.to_parquet(ENTITIES_FP, index=False)
    cls_df.to_parquet(CLAUSES_FP, index=False)

    # Metrics & coverage gate prep
    # English doc coverage: percent of English docs that got ≥1 entity
    by_doc = (
        pages.groupby(["doc_sha256", "doc_id"])["lang_hint"]
        .agg(lambda s: s.mode().iat[0] if not s.mode().empty else "en")
        .reset_index(name="majority_lang")
    )
    en_docs = by_doc[by_doc["majority_lang"] == "en"][["doc_sha256", "doc_id"]].drop_duplicates()

    if not ent_df.empty:
        ent_docs = ent_df[ent_df["lang"] == "en"][["doc_sha256", "doc_id"]].drop_duplicates()
        covered = en_docs.merge(ent_docs, on=["doc_sha256", "doc_id"], how="inner")
        coverage = (covered.shape[0] / en_docs.shape[0]) if en_docs.shape[0] else 1.0
    else:
        coverage = 0.0 if en_docs.shape[0] else 1.0

    metrics = {
        "entities_rows": int(ent_df.shape[0]),
        "clauses_rows": int(cls_df.shape[0]),
        "english_docs_total": int(en_docs.shape[0]),
        "english_docs_with_entities": int(covered.shape[0]) if "covered" in locals() else 0,
        "english_ner_coverage": float(round(coverage, 4)),
    }
    METRICS_FP.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    # Print summary
    print(
        f"✅ M3.3 NER+Clause: entities={metrics['entities_rows']}, "
        f"clauses={metrics['clauses_rows']}, EN_coverage={metrics['english_ner_coverage']:.2f}"
    )
    print("→ data/interim/doc_entities.parquet")
    print("→ data/interim/doc_clauses.parquet")
    print("→ data/interim/ner_clause_metrics.json")
    return 0
