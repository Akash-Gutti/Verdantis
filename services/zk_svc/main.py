from fastapi import FastAPI
from pydantic import BaseModel

from services.common.config import settings

app = FastAPI(title="zk-svc", version="0.1.0")


class IssueReq(BaseModel):
    pdf_hash: str
    feature_commit: str
    score: float
    threshold: float


@app.get("/health")
def health():
    return {"status": "ok", "service": "zk-svc", "offline": settings.offline}


@app.post("/issue")
def issue(req: IssueReq):
    # signature/zk placeholder
    return {"bundle_id": f"bundle-{req.pdf_hash[:8]}", "ok": True}


class VerifyReq(BaseModel):
    bundle_id: str


@app.post("/verify")
def verify(req: VerifyReq):
    return {"bundle_id": req.bundle_id, "verified": True}
