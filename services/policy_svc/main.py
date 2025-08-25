from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from modules.m8.m8_3_propose import PROPOSED_DIR, propose_from_text
from modules.m8.m8_4_enforce import enforce_event
from services.common.bus import publish
from services.common.config import settings

app = FastAPI(title="policy-svc", version="0.3.0")


# ------------------------- M8.4 (compat) -------------------------------------
class EnforceReq(BaseModel):
    rule_id: str
    asset_id: str


@app.post("/enforce")
def enforce(req: EnforceReq) -> Dict[str, bool]:
    """Legacy simple endpoint: publish a violation with rule_id + asset_id."""
    publish("ViolationFlagged", {"rule_id": req.rule_id, "asset_id": req.asset_id})
    return {"ok": True}


# ------------------------- M8.3 (/propose_rules) -----------------------------
class ProposeRequest(BaseModel):
    text: str = Field(min_length=8)
    owner: str = Field(default="policy-team", min_length=2)
    severity: Optional[str] = None
    id_hint: Optional[str] = None
    save: bool = True


class ProposeCandidate(BaseModel):
    yaml: str
    rule: Dict[str, Any]


class ProposeResponse(BaseModel):
    candidates: List[ProposeCandidate]
    proposed_dir: str


@app.post("/propose_rules", response_model=ProposeResponse)
def propose_rules(req: ProposeRequest) -> ProposeResponse:
    """LLM-ready heuristic proposer → YAML + rule dict; saved under proposed/."""
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


# ------------------------- M8.4 (/enforce_v2) --------------------------------
class EnforceV2Req(BaseModel):
    asset_id: str = Field(min_length=1)
    event: Dict[str, Any]
    kg: Optional[Dict[str, Any]] = None
    rule_ids: Optional[List[str]] = None  # if None → evaluate all compiled rules
    include_proposed: bool = True


class EnforceV2Resp(BaseModel):
    violations: List[Dict[str, Any]]


@app.post("/enforce_v2", response_model=EnforceV2Resp)
def enforce_v2(req: EnforceV2Req) -> EnforceV2Resp:
    """Evaluate compiled rules against an event+KG; publish & return violations."""
    viols = enforce_event(
        asset_id=req.asset_id,
        event=req.event,
        kg=req.kg or {},
        rule_ids=req.rule_ids,
        include_proposed=req.include_proposed,
    )
    for v in viols:
        publish("ViolationFlagged", v)
    return EnforceV2Resp(violations=viols)


# ------------------------- health --------------------------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "policy-svc", "offline": settings.offline}
