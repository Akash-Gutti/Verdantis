"""M9.1 — Commitment scheme (POC).

Compute a deterministic feature commitment:
  sha256( DOMAIN || 0x1F || canonical_json(payload) )

Payload fields:
- features: List[float] (rounded to 'precision' dp)
- model_id: str
- model_version: Optional[str]
- salt: Optional[str] (user-provided if desired)
- precision: int (default 6)

Outputs a JSON with commit, inputs (canonical), and metadata. This is the
basis for signature/zk-proof in later substeps.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson
from pydantic import BaseModel, Field, ValidationError

DOMAIN = b"Verdantis-M9-FeatureCommit-v1"
SEP = b"\x1f"

ROOT = Path(".")
ZK_DIR = ROOT / "data" / "zk" / "commits"
ZK_DIR.mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    utc_now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    return utc_now.isoformat()


class FeatureCommitInput(BaseModel):
    features: List[float] = Field(min_length=1)
    model_id: str = Field(min_length=1)
    model_version: Optional[str] = None
    salt: Optional[str] = None
    precision: int = Field(default=6, ge=0, le=12)

    def canonical_payload(self) -> Dict[str, Any]:
        # Round features deterministically to 'precision' decimal places
        rounded = [float(f"{x:.{self.precision}f}") for x in self.features]
        payload: Dict[str, Any] = {
            "features": rounded,
            "model_id": self.model_id,
            "precision": self.precision,
        }
        if self.model_version is not None:
            payload["model_version"] = self.model_version
        if self.salt is not None:
            payload["salt"] = self.salt
        return payload


class FeatureCommitOutput(BaseModel):
    created_at: str
    domain: str
    commit_sha256: str
    payload_canonical_json: str  # canonical JSON string used in the hash
    payload_hash_sha256: str  # hash of canonical payload alone (debug aid)
    inputs: FeatureCommitInput


def _canonical_json_bytes(obj: Dict[str, Any]) -> bytes:
    # orjson dumps with sorted keys and no whitespace → stable representation
    return orjson.dumps(obj, option=orjson.OPT_SORT_KEYS)


def compute_feature_commit(inp: FeatureCommitInput) -> FeatureCommitOutput:
    payload = inp.canonical_payload()
    pj = _canonical_json_bytes(payload)
    # hash of payload alone (useful for debugging / unit tests)
    payload_hash = hashlib.sha256(pj).hexdigest()
    # domain-separated commitment
    h = hashlib.sha256()
    h.update(DOMAIN)
    h.update(SEP)
    h.update(pj)
    commit = h.hexdigest()

    return FeatureCommitOutput(
        created_at=_now_iso(),
        domain=DOMAIN.decode("utf-8"),
        commit_sha256=commit,
        payload_canonical_json=pj.decode("utf-8"),
        payload_hash_sha256=payload_hash,
        inputs=inp,
    )


def save_output(out: FeatureCommitOutput, out_path: Path) -> None:
    out_json = json.loads(out.model_dump_json())
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out_json, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------- CLI glue ----------------
def cli_commit(args: Any) -> None:
    """Entry from verdctl for 'm9 commit'."""
    try:
        if args.input:
            data = json.loads(Path(args.input).read_text(encoding="utf-8"))
            inp = FeatureCommitInput(**data)
        else:
            if not args.features or not args.model_id:
                raise SystemExit("Provide --input file OR --features and --model-id")
            feats = json.loads(args.features)
            inp = FeatureCommitInput(
                features=feats,
                model_id=args.model_id,
                model_version=args.model_version,
                salt=args.salt,
                precision=int(args.precision),
            )
        out = compute_feature_commit(inp)
    except (ValidationError, json.JSONDecodeError, OSError) as exc:
        raise SystemExit(f"M9.1 commit failed: {exc}")

    if args.out:
        save_output(out, Path(args.out))
        print(f"✅ Wrote commitment → {args.out}")
    else:
        # Print minimal summary if no file requested
        print(json.dumps({"commit_sha256": out.commit_sha256}, ensure_ascii=False))


def verify() -> None:
    """No-op verifier for M9.1 (placeholder)."""
    print("M9 verify → commitment scheme ready.")
