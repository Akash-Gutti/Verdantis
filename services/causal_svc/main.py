from fastapi import FastAPI
from pydantic import BaseModel

from services.common.config import settings

app = FastAPI(title="causal-svc", version="0.1.0")


class EffectReq(BaseModel):
    series: list[float]
    intervention_index: int


@app.get("/health")
def health():
    return {"status": "ok", "service": "causal-svc", "offline": settings.offline}


@app.post("/effect")
def effect(req: EffectReq):
    # placeholder effect result
    return {"effect": 0.0, "p": 1.0, "counterfactual": req.series}
