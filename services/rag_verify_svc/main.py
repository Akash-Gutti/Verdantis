from fastapi import FastAPI
from pydantic import BaseModel

from services.common.config import settings

from .models import manager  # relative import inside the service package

app = FastAPI(title="rag-verify-svc", version="0.2.0")


class AskIn(BaseModel):
    query: str


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "rag-verify-svc",
        "offline": settings.offline,
        "embedding_model": settings.embedding_model,
        "mnli_model": settings.mnli_model,
    }


@app.post("/warmup")
def warmup():
    # Load both models and run tiny passes
    manager.load_embedder()
    manager.load_nli()
    e, n, c = manager.nli_score("ESG report sets targets.", "The report sets emission targets.")
    _ = manager.embed(["hello world"])
    return {"ok": True, "device": str(manager.device), "nli_entail": e}


@app.post("/ask")
def ask(inp: AskIn):
    # For now, just ensure models are accessible; full pipeline arrives in M4
    if not settings.offline:
        manager.load_embedder()
        manager.load_nli()
    return {
        "answer": f"(stub) You asked: {inp.query}",
        "citations": [],
        "coverage": 0.0,
        "nli_scores": [],
        "needs_clarification": True,
    }
