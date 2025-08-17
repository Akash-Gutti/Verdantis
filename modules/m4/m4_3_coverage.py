"""
M4.3 Coverage Metric (token-weighted evidence fraction)

- Consumes M4.2 output.
- Computes coverage in [0,1] using NLI scores per verified sentence.
- Optional gating: if --gate and coverage < --threshold, emit {needs_clarification:true}.

CLI (standalone):
  python -m modules.m4.m4_3_coverage --q "question" [--index-dir ...] [--k 5] \
    [--max-sentences 4] [--threshold 0.6] [--gate] [--out outputs/m4_cov.json]
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .m4_2_draft_verify import ask as ask_m4_2

LOGGER = logging.getLogger("m4.coverage")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

_SENT_SPLIT = re.compile(r"(?<=[\.\!\?])\s+")


def _tokenize_simple(text: str) -> List[str]:
    return re.findall(r"[A-Za-z0-9]+", text.lower())


def _sentences(text: str) -> List[str]:
    if not text:
        return []
    return [s.strip() for s in _SENT_SPLIT.split(text) if s.strip()]


def _nli_lookup(nli_scores: List[Dict[str, Any]]) -> Dict[str, Tuple[float, float]]:
    """
    Build a dictionary: sentence -> (entailment, contradiction)
    Exact string match is fine because M4.2 answer concatenates verified sentences verbatim.
    If duplicates exist, keep the best entailment.
    """
    best: Dict[str, Tuple[float, float]] = {}
    for it in nli_scores:
        s = str(it.get("sentence", "")).strip()
        ent = float(it.get("entailment", 0.0))
        con = float(it.get("contradiction", 0.0))
        if not s:
            continue
        cur = best.get(s)
        if cur is None or ent > cur[0]:
            best[s] = (ent, con)
    return best


def compute_coverage(answer: str, nli_scores: List[Dict[str, Any]]) -> float:
    """
    Token-weighted, NLI-weighted coverage over the verified answer.
    coverage = sum(tokens(sent) * ent * (1 - contra)) / sum(tokens(sent))
    """
    sents = _sentences(answer)
    if not sents:
        return 0.0

    lookup = _nli_lookup(nli_scores)
    num = 0.0
    den = 0.0
    for s in sents:
        toks = _tokenize_simple(s)
        tok_n = float(len(toks))
        if tok_n == 0.0:
            continue
        ent, con = lookup.get(s, (0.0, 1.0))
        weight = max(0.0, min(1.0, ent)) * max(0.0, min(1.0, 1.0 - con))
        num += tok_n * weight
        den += tok_n
    if den <= 0.0:
        return 0.0
    cov = num / (den + 1e-9)
    # Bound to [0,1] for safety
    return max(0.0, min(1.0, cov))


def ask_with_coverage(
    query: str,
    index_dir: Path,
    topk: int,
    max_sentences: int,
) -> Dict[str, Any]:
    """
    Runs M4.2 and appends 'coverage' to the result.
    """
    res = ask_m4_2(
        query=query,
        index_dir=index_dir,
        topk=topk,
        max_sentences=max_sentences,
    )
    cov = compute_coverage(res.get("answer", ""), res.get("nli_scores", []))
    res["coverage"] = round(float(cov), 6)
    return res


def _main() -> int:
    parser = argparse.ArgumentParser(description="M4.3 Coverage Metric")
    parser.add_argument("--q", "--query", dest="query", type=str, required=True)
    parser.add_argument("--index-dir", type=Path, default=Path("data/index/m4_faiss"))
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--max-sentences", type=int, default=4)
    parser.add_argument("--threshold", type=float, default=0.6)
    parser.add_argument(
        "--gate",
        action="store_true",
        help="If set, emit {needs_clarification:true} when coverage < threshold.",
    )
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    try:
        res = ask_with_coverage(
            query=args.query,
            index_dir=args.index_dir,
            topk=args.k,
            max_sentences=args.max_sentences,
        )
        if args.gate and res.get("coverage", 0.0) < float(args.threshold):
            gated = {
                "needs_clarification": True,
                "coverage": res.get("coverage", 0.0),
            }
            text = json.dumps(gated, ensure_ascii=False, indent=2)
        else:
            text = json.dumps(res, ensure_ascii=False, indent=2)
        print(text)
        if args.out is not None:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(text, encoding="utf-8")
    except FileNotFoundError as exc:
        LOGGER.error("Missing artifact: %s", exc)
        return 2
    except Exception as exc:  # pragma: no cover
        LOGGER.exception("Unexpected error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
