import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    from jsonschema.validators import Draft7Validator as Validator
except Exception as e:
    raise SystemExit("Install jsonschema first: pip install jsonschema>=4.22.0") from e

ROOT = Path(__file__).resolve().parents[2]  # repo root
SCHEMA_DIR = ROOT / "configs" / "schemas"
SAMPLES_DIR = ROOT / "data" / "event_samples"
TOPICS_PATH = ROOT / "configs" / "topics.json"

_EVENT_TYPE_TO_SCHEMA = {
    "verdantis.DocumentIngested": "document_ingested.schema.json",
    "verdantis.TileFetched": "tile_fetched.schema.json",
    "verdantis.PolicyUpdated": "policy_updated.schema.json",
    "verdantis.ViolationFlagged": "violation_flagged.schema.json",
    "verdantis.ProofIssued": "proof_issued.schema.json",
    "verdantis.AlertRaised": "alert_raised.schema.json",
}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _content_hash(payload: dict) -> str:
    canon = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(canon).hexdigest()


def list_schemas() -> None:
    for p in sorted(SCHEMA_DIR.glob("*.schema.json")):
        print(f"- {p.name}")


def _load_schema_file(fname: str) -> dict:
    path = SCHEMA_DIR / fname
    if not path.exists():
        raise SystemExit(f"[m2.1] missing schema: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def validate_file(event_path: str) -> None:
    obj = json.loads(Path(event_path).read_text(encoding="utf-8"))
    et = obj.get("event_type")
    if not et:
        raise SystemExit("[m2.1] event missing 'event_type'")
    fname = _EVENT_TYPE_TO_SCHEMA.get(et)
    if not fname:
        raise SystemExit(f"[m2.1] unknown event_type: {et}")
    schema = _load_schema_file(fname)
    Validator(schema).validate(obj)
    print(f"✅ Valid: {event_path}")


def validate_dir(dir_path: str) -> None:
    d = Path(dir_path)
    if not d.exists():
        raise SystemExit(f"[m2.1] no such directory: {d}")
    count = 0
    for p in sorted(d.glob("*.json")):
        validate_file(str(p))
        count += 1
    print(f"✅ Validated {count} file(s) in {d}")


def make_samples() -> None:
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    # DocumentIngested
    p = {
        "doc_id": str(uuid.uuid4()),
        "uri": "file:///data/raw/pdfs/sample_001.pdf",
        "title": "Sample ESG Report",
        "lang": "en",
        "checksum_sha256": "0" * 64,
        "bytes": 123456,
        "mime_type": "application/pdf",
        "indexed_at": _iso_now(),
        "metadata": {"tags": ["esg", "report", "eng"]},
    }
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": "verdantis.DocumentIngested",
        "occurred_at": _iso_now(),
        "source": "m1.indexer",
        "version": "1.0.0",
        "schema_ref": "configs/schemas/document_ingested.schema.json",
        "trace_id": str(uuid.uuid4()),
        "content_hash": _content_hash(p),
        "idempotency_key": _content_hash(p),
        "payload": p,
    }
    (SAMPLES_DIR / "001_document_ingested.json").write_text(
        json.dumps(evt, indent=2), encoding="utf-8"
    )

    # TileFetched
    p = {
        "tile_id": str(uuid.uuid4()),
        "aoi_id": "AOI_001",
        "satellite": "sentinel-2",
        "sensed_at": _iso_now(),
        "bands": ["B04", "B08"],
        "url": "file:///data/raw/satellite/AOI_001/tile_20240101.tif",
        "checksum_sha256": "1" * 64,
        "cloud_cover": 0.12,
        "bbox_wgs84": [55.0, 24.0, 55.1, 24.1],
    }
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": "verdantis.TileFetched",
        "occurred_at": _iso_now(),
        "source": "m3.satellite_loader",
        "version": "1.0.0",
        "schema_ref": "configs/schemas/tile_fetched.schema.json",
        "trace_id": str(uuid.uuid4()),
        "content_hash": _content_hash(p),
        "idempotency_key": _content_hash(p),
        "payload": p,
    }
    (SAMPLES_DIR / "002_tile_fetched.json").write_text(json.dumps(evt, indent=2), encoding="utf-8")

    # PolicyUpdated
    p = {
        "policy_id": str(uuid.uuid4()),
        "name": "Energy Efficiency Disclosure",
        "version": "2025.08",
        "effective_at": _iso_now(),
        "actor": "policy-engine",
        "changed_fields": ["threshold.kwh_m2"],
        "diff_uri": "file:///data/policies/diffs/policy_energy_2025_08.diff",
    }
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": "verdantis.PolicyUpdated",
        "occurred_at": _iso_now(),
        "source": "m8.policy_engine",
        "version": "1.0.0",
        "schema_ref": "configs/schemas/policy_updated.schema.json",
        "trace_id": str(uuid.uuid4()),
        "content_hash": _content_hash(p),
        "idempotency_key": _content_hash(p),
        "payload": p,
    }
    (SAMPLES_DIR / "003_policy_updated.json").write_text(
        json.dumps(evt, indent=2), encoding="utf-8"
    )

    # ViolationFlagged
    p = {
        "violation_id": str(uuid.uuid4()),
        "policy_id": "ENERGY_POLICY_001",
        "subject_id": "ASSET_001",
        "severity": "HIGH",
        "description": "Energy usage exceeded threshold over 7 days",
        "evidence_uri": "file:///data/processed/violations/ASSET_001_2025-08-10.json",
        "detector": "model",
        "score": 0.87,
        "first_seen": _iso_now(),
    }
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": "verdantis.ViolationFlagged",
        "occurred_at": _iso_now(),
        "source": "m7.causal_detector",
        "version": "1.0.0",
        "schema_ref": "configs/schemas/violation_flagged.schema.json",
        "trace_id": str(uuid.uuid4()),
        "content_hash": _content_hash(p),
        "idempotency_key": _content_hash(p),
        "payload": p,
    }
    (SAMPLES_DIR / "004_violation_flagged.json").write_text(
        json.dumps(evt, indent=2), encoding="utf-8"
    )

    # ProofIssued
    p = {
        "proof_id": str(uuid.uuid4()),
        "subject_id": "ASSET_001",
        "policy_id": "ENERGY_POLICY_001",
        "proof_type": "zk_commitment",
        "proof_uri": "file:///data/processed/proofs/ASSET_001_commitment.json",
        "issued_at": _iso_now(),
        "issuer": "zk-service",
        "valid_until": _iso_now(),
        "hash": "2" * 64,
    }
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": "verdantis.ProofIssued",
        "occurred_at": _iso_now(),
        "source": "m9.zk_certifier",
        "version": "1.0.0",
        "schema_ref": "configs/schemas/proof_issued.schema.json",
        "trace_id": str(uuid.uuid4()),
        "content_hash": _content_hash(p),
        "idempotency_key": _content_hash(p),
        "payload": p,
    }
    (SAMPLES_DIR / "005_proof_issued.json").write_text(json.dumps(evt, indent=2), encoding="utf-8")

    # AlertRaised
    p = {
        "alert_id": str(uuid.uuid4()),
        "related_violation_id": "SOME_VIOLATION_ID",
        "title": "Critical Energy Violation",
        "message": "Asset ASSET_001 breached threshold for 3 days",
        "severity": "CRITICAL",
        "channel": "console",
        "recipients": ["ops@verdantis.local"],
        "created_at": _iso_now(),
    }
    evt = {
        "event_id": str(uuid.uuid4()),
        "event_type": "verdantis.AlertRaised",
        "occurred_at": _iso_now(),
        "source": "m10.alerts",
        "version": "1.0.0",
        "schema_ref": "configs/schemas/alert_raised.schema.json",
        "trace_id": str(uuid.uuid4()),
        "content_hash": _content_hash(p),
        "idempotency_key": _content_hash(p),
        "payload": p,
    }
    (SAMPLES_DIR / "006_alert_raised.json").write_text(json.dumps(evt, indent=2), encoding="utf-8")

    print(f"[m2.1] samples → {SAMPLES_DIR}")


def verify() -> None:
    if not TOPICS_PATH.exists():
        raise SystemExit("[m2.1] missing configs/topics.json")
    # ensure every schema exists & is a valid draft-07 schema
    for fname in _EVENT_TYPE_TO_SCHEMA.values():
        schema = _load_schema_file(fname)
        Validator.check_schema(schema)
    print("[m2.1] verify passed (topics + schema files valid)")
