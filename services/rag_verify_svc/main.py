from __future__ import annotations

import statistics
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from modules.m4.m4_3_coverage import ask_with_coverage  # M4.2+M4.3 combo
from services.common.config import settings

from .models import manager  # your existing manager (embedder/NLI warmup)

app = FastAPI(title="rag-verify-svc", version="0.2.0")


# --------- API models ---------
class AskIn(BaseModel):
    query: str = Field(..., description="User question")
    # Optional overrides per-request (fall back to settings)
    k: Optional[int] = Field(None, ge=1, le=32)
    max_sentences: Optional[int] = Field(None, ge=1, le=12)


# --------- simple in-memory metrics ---------
@dataclass
class Metrics:
    requests_total: int = 0
    needs_clarification_total: int = 0
    latencies_ms: Deque[float] = deque(maxlen=settings.rvs_metrics_window)
    coverage_vals: Deque[float] = deque(maxlen=settings.rvs_metrics_window)
    nli_pass_vals: Deque[float] = deque(maxlen=settings.rvs_metrics_window)

    def record(
        self, duration_ms: float, coverage: float, nli_pass_rate: float, gated: bool
    ) -> None:
        self.requests_total += 1
        if gated:
            self.needs_clarification_total += 1
        self.latencies_ms.append(duration_ms)
        self.coverage_vals.append(coverage)
        self.nli_pass_vals.append(nli_pass_rate)

    def summary(self) -> Dict[str, Any]:
        def _avg(xs: Deque[float]) -> float:
            return float(statistics.fmean(xs)) if xs else 0.0

        def _p95(xs: Deque[float]) -> float:
            if not xs:
                return 0.0
            data = sorted(xs)
            k = max(1, int(round(0.95 * len(data))))
            return float(data[k - 1])

        return {
            "requests_total": self.requests_total,
            "needs_clarification_total": self.needs_clarification_total,
            "p95_latency_ms": round(_p95(self.latencies_ms), 3),
            "avg_latency_ms": round(_avg(self.latencies_ms), 3),
            "avg_coverage": round(_avg(self.coverage_vals), 6),
            "avg_nli_pass_rate": round(_avg(self.nli_pass_vals), 6),
            "window_size": len(self.latencies_ms),
        }


METRICS = Metrics()


# --------- helpers ---------
def _compute_nli_pass_rate(
    nli_scores: Any, ent_thresh: float = 0.55, contra_max: float = 0.30
) -> float:
    seq = list(nli_scores or [])
    if not seq:
        return 0.0
    ok = 0
    for s in seq:
        ent = float(s.get("entailment", 0.0))
        con = float(s.get("contradiction", 1.0))
        if ent >= ent_thresh and con <= contra_max:
            ok += 1
    return ok / max(1, len(seq))


# --------- routes ---------
@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "rag-verify-svc",
        "offline": settings.offline,
        "embedding_model": settings.embedding_model,
        "mnli_model": settings.mnli_model,
        "index_dir": str(settings.rvs_index_dir),
        "k": settings.rvs_k,
        "max_sentences": settings.rvs_max_sentences,
        "gate_enabled": settings.rvs_gate_enabled,
        "coverage_threshold": settings.rvs_coverage_threshold,
    }


@app.post("/warmup")
def warmup() -> Dict[str, Any]:
    # Load both models and run tiny passes using your manager
    manager.load_embedder()
    manager.load_nli()
    e, _, _ = manager.nli_score("ESG report sets targets.", "The report sets emission targets.")
    _ = manager.embed(["hello world"])
    return {"ok": True, "device": str(manager.device), "nli_entail": e}


@app.post("/ask")
def ask(inp: AskIn) -> Dict[str, Any]:
    # Use per-request overrides if provided; else defaults from settings
    k = int(inp.k) if inp.k is not None else int(settings.rvs_k)
    max_sents = (
        int(inp.max_sentences) if inp.max_sentences is not None else int(settings.rvs_max_sentences)
    )

    try:
        result = ask_with_coverage(
            query=inp.query,
            index_dir=settings.rvs_index_dir,
            topk=k,
            max_sentences=max_sents,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=f"Missing artifact: {exc}") from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    coverage = float(result.get("coverage", 0.0))
    nli_pass = _compute_nli_pass_rate(result.get("nli_scores", []))
    elapsed_ms = float(result.get("elapsed_ms", 0.0))

    # Gate on coverage if enabled
    if settings.rvs_gate_enabled and coverage < float(settings.rvs_coverage_threshold):
        payload: Dict[str, Any] = {"needs_clarification": True, "coverage": coverage}
        METRICS.record(
            duration_ms=elapsed_ms, coverage=coverage, nli_pass_rate=nli_pass, gated=True
        )
        return payload

    METRICS.record(duration_ms=elapsed_ms, coverage=coverage, nli_pass_rate=nli_pass, gated=False)
    return result


@app.get("/metrics")
def metrics() -> Dict[str, Any]:
    return METRICS.summary()
