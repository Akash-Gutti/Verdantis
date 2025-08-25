from __future__ import annotations

from typing import Any, Dict

from fastapi import FastAPI, HTTPException

from modules.m9.m9_2_issue_verify import IssueRequest, VerifyRequest, issue_bundle, verify_bundle
from services.common.config import settings

app = FastAPI(title="zk-svc", version="0.2.0")


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "service": "zk-svc", "offline": settings.offline}


@app.post("/issue")
def issue(req: IssueRequest) -> Dict[str, Any]:
    """
    Issue a signed proof bundle:
      input: {pdf_hash, feature_commit, score, threshold, model_id, model_version?, notes?}
      output: {ok, bundle:{...}} and file saved under data/zk/bundles/<bundle_id>.json
    """
    try:
        secret = getattr(settings, "zk_secret", None)
        bundle = issue_bundle(req, secret=secret)
        return {"ok": True, "bundle": bundle.model_dump()}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/verify")
def verify(req: VerifyRequest) -> Dict[str, Any]:
    """
    Verify a bundle without raw features:
      input: {bundle:{...}}
      output: {ok, valid, reasons, bundle_id}
    """
    try:
        secret = getattr(settings, "zk_secret", None)
        res = verify_bundle(req.bundle, secret=secret)
        return {
            "ok": True,
            "valid": res.valid,
            "reasons": res.reasons,
            "bundle_id": res.bundle_id,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
