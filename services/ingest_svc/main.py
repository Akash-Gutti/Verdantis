from fastapi import FastAPI
from pydantic import BaseModel

from services.common.bus import publish
from services.common.config import settings

app = FastAPI(title="ingest-svc", version="0.1.0")


class DocIn(BaseModel):
    doc_id: str
    title: str
    lang: str = "en"
    text: str


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "ingest-svc",
        "mode": settings.mode,
        "offline": settings.offline,
    }


@app.post("/ingest/doc")
def ingest(doc: DocIn):
    publish("DocumentIngested", {"doc_id": doc.doc_id, "lang": doc.lang})
    return {"ok": True, "doc_id": doc.doc_id}
