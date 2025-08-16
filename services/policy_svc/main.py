from fastapi import FastAPI
from pydantic import BaseModel

from services.common.bus import publish
from services.common.config import settings

app = FastAPI(title="policy-svc", version="0.1.0")


class EnforceReq(BaseModel):
    rule_id: str
    asset_id: str


@app.get("/health")
def health():
    return {"status": "ok", "service": "policy-svc", "offline": settings.offline}


@app.post("/enforce")
def enforce(req: EnforceReq):
    publish("ViolationFlagged", {"rule_id": req.rule_id, "asset_id": req.asset_id})
    return {"ok": True}
