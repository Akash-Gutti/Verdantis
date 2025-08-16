from fastapi import FastAPI
from pydantic import BaseModel

from services.common.config import settings

app = FastAPI(title="rag-verify-svc", version="0.1.0")


class AskIn(BaseModel):
    query: str


@app.get("/health")
def health():
    return {"status": "ok", "service": "rag-verify-svc", "offline": settings.offline}


@app.post("/ask")
def ask(inp: AskIn):
    # CPU-first stub: echo answer + empty citations (we’ll wire RAG later)
    return {
        "answer": f"(stub) You asked: {inp.query}",
        "citations": [],
        "coverage": 0.0,
        "nli_scores": [],
        "needs_clarification": True,
    }
