from fastapi import FastAPI
from pydantic import BaseModel

from services.common.bus import publish
from services.common.config import settings

app = FastAPI(title="vision-svc", version="0.1.0")


class ChangeReq(BaseModel):
    aoi_id: str
    date_before: str
    date_after: str


@app.get("/health")
def health():
    return {"status": "ok", "service": "vision-svc", "offline": settings.offline}


@app.post("/change_score")
def change_score(req: ChangeReq):
    # NDVI pipeline to be added; publish stub event
    publish("SatelliteChangeDetected", {"aoi_id": req.aoi_id, "score": 0.0})
    return {"aoi_id": req.aoi_id, "score": 0.0}
