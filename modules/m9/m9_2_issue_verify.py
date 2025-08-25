"""M9.2 — Issue & Verify (POC, HMAC signature).

- ISSUE builds a bundle from minimal fields:
    {pdf_hash, feature_commit, score, threshold, model_id, model_version?}
  Computes:
    decision, bundle_id=sha256(payload), signature=HMAC(domain||payload)
  Saves under data/zk/bundles/<bundle_id>.json.

- VERIFY recomputes bundle_id, decision, and signature from the payload using
  the same secret and returns validity + reasons.

Security notes:
- This is a Week-1 POC using HMAC. Swap HMAC for Ed25519 or a zk proof later.
- Domain separation prevents cross-protocol hash reuse.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson
from pydantic import BaseModel, Field, ValidationError

DOMAIN = b"Verdantis-M9-BundleSig-v1"
SEP = b"\x1f"

ROOT = Path(".")
BUNDLES_DIR = ROOT / "data" / "zk" / "bundles"
BUNDLES_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    utc_now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    return utc_now.isoformat()


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    # Stable bytes: no whitespace, sorted keys.
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)


def _default_secret() -> str:
    # Prefer environment variable; fall back to dev default.
    return os.environ.get("ZK_SECRET", "dev-unsafe-secret")


class IssueRequest(BaseModel):
    pdf_hash: str = Field(min_length=10)  # hex sha256 of the PDF (caller computes)
    feature_commit: str = Field(min_length=10)  # hex from M9.1
    score: float
    threshold: float
    model_id: str = Field(min_length=1)
    model_version: Optional[str] = None
    notes: Optional[str] = None


class Bundle(BaseModel):
    bundle_id: str
    pdf_hash: str
    feature_commit: str
    score: float
    threshold: float
    decision: str  # "pass" | "fail"
    model_id: str
    model_version: Optional[str] = None
    issued_at: str
    signer: str  # e.g., "hmac-sha256"
    signature: str  # hex
    notes: Optional[str] = None


def _payload_from_issue(req: IssueRequest) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "pdf_hash": req.pdf_hash,
        "feature_commit": req.feature_commit,
        "score": float(req.score),
        "threshold": float(req.threshold),
        "model_id": req.model_id,
    }
    if req.model_version is not None:
        payload["model_version"] = req.model_version
    return payload


def _compute_bundle_id(payload: Dict[str, Any]) -> str:
    pj = _canonical_json_bytes(payload)
    return hashlib.sha256(pj).hexdigest()


def _sign_payload(payload: Dict[str, Any], secret: Optional[str] = None) -> str:
    key = (secret or _default_secret()).encode("utf-8")
    pj = _canonical_json_bytes(payload)
    hm = hmac.new(key, DOMAIN + SEP + pj, hashlib.sha256).hexdigest()
    return hm


def _decision(score: float, threshold: float) -> str:
    return "pass" if float(score) >= float(threshold) else "fail"


def issue_bundle(req: IssueRequest, secret: Optional[str] = None) -> Bundle:
    payload = _payload_from_issue(req)
    bid = _compute_bundle_id(payload)
    sig = _sign_payload(payload, secret=secret)
    bundle = Bundle(
        bundle_id=bid,
        pdf_hash=payload["pdf_hash"],
        feature_commit=payload["feature_commit"],
        score=payload["score"],
        threshold=payload["threshold"],
        decision=_decision(payload["score"], payload["threshold"]),
        model_id=payload["model_id"],
        model_version=payload.get("model_version"),
        issued_at=_now_iso(),
        signer="hmac-sha256",
        signature=sig,
        notes=req.notes,
    )
    # Persist
    out = BUNDLES_DIR / f"{bundle.bundle_id}.json"
    out.write_text(bundle.model_dump_json(indent=2), encoding="utf-8")
    return bundle


class VerifyRequest(BaseModel):
    bundle: Dict[str, Any]  # raw bundle json (as dict)


class VerifyResponse(BaseModel):
    valid: bool
    reasons: List[str]
    bundle_id: str


def verify_bundle(data: Dict[str, Any], secret: Optional[str] = None) -> VerifyResponse:
    reasons: List[str] = []
    try:
        b = Bundle(**data)
    except ValidationError as exc:
        return VerifyResponse(valid=False, reasons=[f"schema: {exc}"], bundle_id="")

    # Rebuild the payload exactly as ISSUE did
    payload: Dict[str, Any] = {
        "pdf_hash": b.pdf_hash,
        "feature_commit": b.feature_commit,
        "score": float(b.score),
        "threshold": float(b.threshold),
        "model_id": b.model_id,
    }
    if b.model_version is not None:
        payload["model_version"] = b.model_version

    expected_bid = _compute_bundle_id(payload)
    if expected_bid != b.bundle_id:
        reasons.append("bundle_id mismatch (recomputed != provided)")

    expected_dec = _decision(b.score, b.threshold)
    if expected_dec != b.decision:
        reasons.append("decision mismatch (recomputed != provided)")

    expected_sig = _sign_payload(payload, secret=secret)
    if expected_sig != b.signature:
        reasons.append("signature mismatch (HMAC invalid)")

    return VerifyResponse(valid=(len(reasons) == 0), reasons=reasons, bundle_id=b.bundle_id)
