from fastapi import FastAPI

from services.common.config import settings

app = FastAPI(title="alerts-svc", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok", "service": "alerts-svc", "offline": settings.offline}
