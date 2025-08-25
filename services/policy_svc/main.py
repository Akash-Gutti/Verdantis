from __future__ import annotations

from typing import Any, Dict, List, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# import our propose logic
from modules.m8.m8_3_propose import PROPOSED_DIR, propose_from_text
from services.common.bus import publish
from services.common.config import settings

app = FastAPI(title="policy-svc", version="0.2.0")


# --- existing enforce ---------------------------------------------------------
class EnforceReq(BaseModel):
    rule_id: str
    asset_id: str


@app.post("/enforce")
def enforce(req: EnforceReq):
    publish("ViolationFlagged", {"rule_id": req.rule_id, "asset_id": req.asset_id})
    return {"ok": True}


# --- new propose --------------------------------------------------------------
class ProposeRequest(BaseModel):
    text: str = Field(min_length=8)
    owner: str = Field(default="policy-team", min_length=2)
    severity: str | None = None
    id_hint: str | None = None
    save: bool = True


class ProposeCandidate(BaseModel):
    yaml: str
    rule: Dict[str, Any]


class ProposeResponse(BaseModel):
    candidates: List[ProposeCandidate]
    proposed_dir: str


@app.post("/propose_rules", response_model=ProposeResponse)
def propose_rules(req: ProposeRequest) -> ProposeResponse:
    try:
        pairs: List[Tuple[str, Dict[str, Any]]] = propose_from_text(
            text=req.text,
            owner=req.owner,
            severity=req.severity,
            id_hint=req.id_hint,
            save=req.save,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    cands = [ProposeCandidate(yaml=y, rule=r) for (y, r) in pairs]
    return ProposeResponse(candidates=cands, proposed_dir=str(PROPOSED_DIR.as_posix()))


# --- common healthcheck -------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "service": "policy-svc", "offline": settings.offline}
