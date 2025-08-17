from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

# Use absolute import if the module is part of your project structure
from modules.m4.m4_3_coverage import ask_with_coverage


def compute_nli_pass_rate(
    nli_scores: List[Dict[str, Any]],
    ent_thresh: float = 0.55,
    contra_max: float = 0.30,
) -> float:
    if not nli_scores:
        return 0.0
    ok = 0
    for s in nli_scores:
        if (
            float(s.get("entailment", 0.0)) >= ent_thresh
            and float(s.get("contradiction", 1.0)) <= contra_max
        ):
            ok += 1
    return ok / max(1, len(nli_scores))


@dataclass
class RAGEngine:
    index_dir: str
    k: int
    max_sentences: int
    gate_enabled: bool
    coverage_threshold: float

    def ask(
        self, q: str, k: int | None = None, max_sentences: int | None = None
    ) -> Tuple[Dict[str, Any], float, float, bool]:
        """Run the pipeline and apply gating. Returns (payload, coverage, nli_pass, gated)."""
        k_eff = k if k is not None else self.k
        ms_eff = max_sentences if max_sentences is not None else self.max_sentences

        t0 = time.time()
        res = ask_with_coverage(q, self.index_dir, k_eff, ms_eff)
        dur_ms = (time.time() - t0) * 1000.0  # keep local, caller may use

        cov = float(res.get("coverage", 0.0))
        nli_pass = compute_nli_pass_rate(res.get("nli_scores", []))

        # prefer model-reported elapsed_ms, else use measured
        elapsed_ms = float(res.get("elapsed_ms", dur_ms))

        if self.gate_enabled and cov < float(self.coverage_threshold):
            payload: Dict[str, Any] = {
                "needs_clarification": True,
                "coverage": cov,
                "elapsed_ms": elapsed_ms,
            }
            return payload, cov, nli_pass, True

        # ensure elapsed_ms is present in the success payload
        if "elapsed_ms" not in res:
            res["elapsed_ms"] = elapsed_ms
        return res, cov, nli_pass, False
