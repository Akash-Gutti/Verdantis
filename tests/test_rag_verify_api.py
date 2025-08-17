from __future__ import annotations

from typing import Any, Dict, Tuple

from fastapi.testclient import TestClient

from services.rag_verify_svc.main import app as real_app
from services.rag_verify_svc.main import get_engine  # type: ignore


class FakeEngine:
    def __init__(self, coverage: float, gated: bool):
        self.coverage = coverage
        self.gated = gated

    def ask(
        self, q: str, k: int | None = None, max_sentences: int | None = None
    ) -> Tuple[Dict[str, Any], float, float, bool]:
        if self.gated:
            payload = {"needs_clarification": True, "coverage": self.coverage}
            # elapsed_ms kept for metrics test
            return payload, self.coverage, 0.5, True
        payload = {
            "query": q,
            "answer": "Verified sentence.",
            "citations": [{"doc_id": "abc", "page": 1}],
            "pages": [{"doc_id": "abc", "page": 1, "text": "x", "score": 0.9}],
            "kg_nodes": [],
            "nli_scores": [
                {"sentence": "Verified sentence.", "entailment": 0.9, "contradiction": 0.05}
            ],
            "elapsed_ms": 123.0,
            "coverage": self.coverage,
        }
        return payload, self.coverage, 0.9, False


def test_ask_gated(monkeypatch) -> None:
    # Override engine with low coverage → gated
    def _fake_dep():
        return FakeEngine(coverage=0.42, gated=True)

    real_app.dependency_overrides[get_engine] = _fake_dep  # type: ignore
    client = TestClient(real_app)
    resp = client.post("/ask", json={"q": "hi"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["needs_clarification"] is True
    assert 0.0 <= body["coverage"] <= 1.0


def test_ask_ok(monkeypatch) -> None:
    # Override engine with good coverage → full payload
    def _fake_dep():
        return FakeEngine(coverage=0.88, gated=False)

    real_app.dependency_overrides[get_engine] = _fake_dep  # type: ignore
    client = TestClient(real_app)
    resp = client.post("/ask", json={"q": "hi"})
    assert resp.status_code == 200
    body = resp.json()
    for key in ("query", "answer", "citations", "pages", "kg_nodes", "nli_scores", "coverage"):
        assert key in body


def test_metrics(monkeypatch) -> None:
    # Two requests: 1 gated, 1 ok
    it = iter([FakeEngine(coverage=0.4, gated=True), FakeEngine(coverage=0.9, gated=False)])

    def _fake_dep():
        return next(it)

    real_app.dependency_overrides[get_engine] = _fake_dep  # type: ignore
    client = TestClient(real_app)
    client.post("/ask", json={"q": "one"})
    client.post("/ask", json={"q": "two"})

    m = client.get("/metrics").json()
    assert m["requests_total"] == 2
    assert m["needs_clarification_total"] == 1
    assert "p95_latency_ms" in m
    assert "avg_coverage" in m
    assert "avg_nli_pass_rate" in m
