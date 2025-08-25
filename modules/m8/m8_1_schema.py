"""M8.1 — Policy schema + seeding + validation + registry.

Creates JSON Schemas, seeds example YAML rules, validates all rules,
writes a registry.jsonl and an audit.log.

Design highlights:
- Rule JSON Schema enforces: meta, severity, scope, trigger, conditions, actions.
- Conditions are simple operator clauses over event/KG paths.
- Actions are generic with type+params for future expandability.
- Registry tracks id/version/status/checksum/file.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml
from jsonschema import Draft202012Validator
from pydantic import BaseModel, Field, ValidationError

# ---- Paths -----------------------------------------------------------------
ROOT = Path(".")
SCHEMAS_DIR = ROOT / "configs" / "schemas"
RULE_SCHEMA_PATH = SCHEMAS_DIR / "policy_rule.schema.json"
REGISTRY_SCHEMA_PATH = SCHEMAS_DIR / "policy_registry.schema.json"

RULES_DIR = ROOT / "data" / "rules"
REGISTRY_PATH = RULES_DIR / "registry.jsonl"
AUDIT_LOG_PATH = RULES_DIR / "audit.log"


# ---- JSON Schemas -----------------------------------------------------------
RULE_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Verdantis Policy Rule",
    "type": "object",
    "additionalProperties": False,
    "required": ["meta", "severity", "scope", "trigger", "conditions", "actions"],
    "properties": {
        "meta": {
            "type": "object",
            "additionalProperties": False,
            "required": ["id", "version", "status", "owner"],
            "properties": {
                "id": {"type": "string", "minLength": 3},
                "version": {"type": "integer", "minimum": 1},
                "status": {"type": "string", "enum": ["proposed", "active", "deprecated"]},
                "owner": {"type": "string", "minLength": 1},
                "created_at": {"type": "string", "format": "date-time"},
                "updated_at": {"type": "string", "format": "date-time"},
                "description": {"type": "string"},
            },
        },
        "severity": {"type": "string", "enum": ["info", "low", "medium", "high", "critical"]},
        "scope": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "assets": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}},
                "region": {"type": "string"},
            },
        },
        "trigger": {
            "type": "object",
            "additionalProperties": False,
            "required": ["type"],
            "properties": {
                "type": {"type": "string", "enum": ["event", "schedule"]},
                "match": {"type": "object"},  # e.g., {"event.type": "causal.effect"}
                "cron": {"type": "string"},  # for schedule-based rules
            },
        },
        "conditions": {
            "type": "object",
            "additionalProperties": False,
            "required": ["aggregator", "clauses"],
            "properties": {
                "aggregator": {"type": "string", "enum": ["all", "any"]},
                "clauses": {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["path", "op"],
                        "properties": {
                            "path": {"type": "string"},  # dotted path e.g., "event.effect.avg"
                            "op": {
                                "type": "string",
                                "enum": [
                                    "eq",
                                    "neq",
                                    "gt",
                                    "gte",
                                    "lt",
                                    "lte",
                                    "in",
                                    "regex",
                                    "exists",
                                ],
                            },
                            "value": {},
                        },
                    },
                },
            },
        },
        "actions": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["flag", "notify", "score_adjust", "open_case", "webhook"],
                    },
                    "params": {"type": "object"},
                },
            },
        },
    },
}

REGISTRY_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Verdantis Policy Registry Entry",
    "type": "object",
    "additionalProperties": False,
    "required": ["id", "version", "status", "file", "checksum_sha256"],
    "properties": {
        "id": {"type": "string"},
        "version": {"type": "integer", "minimum": 1},
        "status": {"type": "string", "enum": ["proposed", "active", "deprecated"]},
        "file": {"type": "string"},
        "checksum_sha256": {"type": "string"},
        "activated_at": {"type": "string", "format": "date-time"},
        "retired_at": {"type": "string", "format": "date-time"},
        "notes": {"type": "string"},
    },
}


# ---- Pydantic (optional, extra safety) -------------------------------------
class Meta(BaseModel):
    id: str = Field(min_length=3)
    version: int = Field(ge=1)
    status: str = Field(pattern="^(proposed|active|deprecated)$")
    owner: str
    created_at: str | None = None
    updated_at: str | None = None
    description: str | None = None


class Clause(BaseModel):
    path: str
    op: str = Field(pattern="^(eq|neq|gt|gte|lt|lte|in|regex|exists)$")
    value: Any | None = None


class Conditions(BaseModel):
    aggregator: str = Field(pattern="^(all|any)$")
    clauses: List[Clause]


class Trigger(BaseModel):
    type: str = Field(pattern="^(event|schedule)$")
    match: Dict[str, Any] | None = None
    cron: str | None = None


class Action(BaseModel):
    type: str = Field(pattern="^(flag|notify|score_adjust|open_case|webhook)$")
    params: Dict[str, Any] | None = None


class Rule(BaseModel):
    meta: Meta
    severity: str = Field(pattern="^(info|low|medium|high|critical)$")
    scope: Dict[str, Any] = Field(default_factory=dict)
    trigger: Trigger
    conditions: Conditions
    actions: List[Action]


# ---- Helpers ----------------------------------------------------------------
def _ensure_dirs() -> None:
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    RULES_DIR.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _now_iso() -> str:
    utc_now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    return utc_now.isoformat()


def write_schemas() -> None:
    """Write JSON Schemas to configs/schemas."""
    _ensure_dirs()
    _write_json(RULE_SCHEMA_PATH, RULE_JSON_SCHEMA)
    _write_json(REGISTRY_SCHEMA_PATH, REGISTRY_JSON_SCHEMA)
    print(f"✅ Wrote {RULE_SCHEMA_PATH}")
    print(f"✅ Wrote {REGISTRY_SCHEMA_PATH}")


def _sample_rules() -> List[Tuple[str, str]]:
    """Return sample (filename, yaml_text) tuples."""
    now = _now_iso()

    rule1 = f"""\
meta:
  id: rule.energy.effect.high
  version: 1
  status: proposed
  owner: policy-team
  created_at: "{now}"
  updated_at: "{now}"
  description: "Flag large energy reduction (more than 300 kWh drop) after policy date."
severity: high
scope:
  assets: ["*"]
  tags: ["energy", "policy"]
trigger:
  type: event
  match:
    event.type: "causal.effect"
    event.metric: "energy_kwh"
conditions:
  aggregator: all
  clauses:
    - path: "event.effect.avg"
      op: "lt"
      value: -300
    - path: "event.policy_date"
      op: "exists"
actions:
  - type: flag
    params:
      reason: "High energy reduction detected"
  - type: notify
    params:
      channel: "ops"
      template: "energy_reduction_alert"
"""

    rule2 = f"""\
meta:
  id: rule.sat.change.illegal_construction
  version: 1
  status: proposed
  owner: policy-team
  created_at: "{now}"
  updated_at: "{now}"
  description: "Detect vegetation loss + no permit on KG → possible illegal construction."
severity: critical
scope:
  region: "UAE"
  tags: ["satellite", "permits", "compliance"]
trigger:
  type: event
  match:
    event.type: "satellite.change"
conditions:
  aggregator: all
  clauses:
    - path: "event.ndvi_delta"
      op: "lt"
      value: -0.2
    - path: "kg.permit.exists"
      op: "eq"
      value: false
actions:
  - type: flag
    params:
      reason: "Change detected without permit"
  - type: open_case
    params:
      queue: "regulator"
      priority: "P1"
  - type: notify
    params:
      channel: "compliance"
      template: "illegal_construction_alert"
"""

    return [
        ("rule_energy_effect_high.yaml", rule1),
        ("rule_cd_illegal_construction.yaml", rule2),
    ]


def write_samples() -> List[Path]:
    _ensure_dirs()
    out: List[Path] = []
    for fname, text in _sample_rules():
        path = RULES_DIR / fname
        path.write_text(text, encoding="utf-8")  # <-- add encoding
        out.append(path)
        print(f"✅ Wrote {path}")
    return out


def _load_yaml_rules() -> List[Tuple[Path, Dict[str, Any], str]]:
    files = list(RULES_DIR.glob("*.y*ml"))
    result: List[Tuple[Path, Dict[str, Any], str]] = []
    for f in files:
        raw = f.read_text(encoding="utf-8")  # <-- add encoding
        data = yaml.safe_load(raw)
        if not isinstance(data, dict):
            raise SystemExit(f"Invalid YAML structure in {f.name}: expected a mapping/object.")
        result.append((f, data, raw))
    return result


def _validate_rules(items: List[Tuple[Path, Dict[str, Any], str]]) -> List[Dict[str, Any]]:
    """Validate each rule against JSON Schema and Pydantic."""
    v = Draft202012Validator(RULE_JSON_SCHEMA)
    good: List[Dict[str, Any]] = []
    for path, data, _raw in items:
        errs = sorted(v.iter_errors(data), key=lambda e: e.path)
        if errs:
            for e in errs:
                loc = ".".join([str(p) for p in e.path])
                print(f"❌ Schema error in {path.name} at {loc}: {e.message}", file=sys.stderr)
            raise SystemExit(f"Validation failed for {path.name}")
        try:
            Rule(**data)  # pydantic parse
        except ValidationError as exc:
            print(f"❌ Pydantic validation failed for {path.name}: {exc}", file=sys.stderr)
            raise SystemExit(f"Pydantic validation failed for {path.name}") from exc
        good.append(data)
    return good


def _append_registry(entries: List[Tuple[Path, Dict[str, Any], str]]) -> None:
    """Append/update registry.jsonl and audit.log."""
    reg_validator = Draft202012Validator(REGISTRY_JSON_SCHEMA)
    existing: Dict[Tuple[str, int], Dict[str, Any]] = {}
    if REGISTRY_PATH.exists():
        for line in REGISTRY_PATH.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = (rec.get("id", ""), int(rec.get("version", 0)))
            existing[key] = rec

    with (
        REGISTRY_PATH.open("a", encoding="utf-8") as reg,
        AUDIT_LOG_PATH.open("a", encoding="utf-8") as audit,
    ):
        for path, data, raw in entries:
            meta = data["meta"]
            checksum = _sha256_bytes(raw.encode("utf-8"))
            record = {
                "id": meta["id"],
                "version": int(meta["version"]),
                "status": meta["status"],
                "file": str(path.as_posix()),
                "checksum_sha256": checksum,
                "activated_at": _now_iso() if meta["status"] == "active" else None,
                "retired_at": None,
            }

            record = {k: v for k, v in record.items() if v is not None}

            errs = sorted(reg_validator.iter_errors(record), key=lambda e: e.path)
            if errs:
                for e in errs:
                    loc = ".".join([str(p) for p in e.path])
                    print(f"❌ Registry schema error at {loc}: {e.message}", file=sys.stderr)
                raise SystemExit("Registry entry validation failed")

            key = (record["id"], record["version"])
            # If same id/version exists with same checksum, skip duplicate line.
            if key in existing and existing[key].get("checksum_sha256") == checksum:
                continue

            reg.write(json.dumps(record, ensure_ascii=False) + "\n")
            audit.write(
                f"{_now_iso()} | added {record['id']} v{record['version']} "
                f"({record['status']}) from {record['file']} sha256={checksum}\n"
            )


def main() -> None:
    """Entry for `verdctl m8 schema`."""
    write_schemas()
    written = write_samples()
    items = _load_yaml_rules()
    _validate_rules(items)
    _append_registry(items)
    print(f"✅ M8.1 complete: {len(written)} sample rule(s), registry updated → {REGISTRY_PATH}")


def verify() -> None:
    """Used by `verdctl verify -m m8`."""
    # Ensure schemas exist and validate all rules.
    if not RULE_SCHEMA_PATH.exists() or not REGISTRY_SCHEMA_PATH.exists():
        print("Schemas missing. Run: python scripts/verdctl.py m8 schema", file=sys.stderr)
        raise SystemExit(1)
    items = _load_yaml_rules()
    _validate_rules(items)
    print(f"M8 verify → rules={len(items)} OK, registry={REGISTRY_PATH.exists()}")
