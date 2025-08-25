"""M8.2 — Policy compiler.

Reads YAML rules (validated in M8.1) and produces a normalized,
enforcement-ready JSON manifest with tokenized paths and operator checks.

Output:
  data/rules/compiled/compiled_rules.json

Design:
- Preserve provenance: source file path and SHA256 of the raw YAML.
- Normalize trigger/conditions/actions into a stable IR.
- Do not execute anything; just prepare for fast evaluation in M8.4.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel, Field, ValidationError

# Reuse models/helpers from M8.1
from .m8_1_schema import RULES_DIR, Rule, _load_yaml_rules

# -------- Paths / Constants ---------------------------------------------------
COMPILED_DIR = RULES_DIR / "compiled"
COMPILED_PATH = COMPILED_DIR / "compiled_rules.json"

VALID_OPS = {"eq", "neq", "gt", "gte", "lt", "lte", "in", "regex", "exists"}
ENGINE_VERSION = "v1"


# -------- Helper utils --------------------------------------------------------
def _now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _tokenize_path(path: str) -> List[str]:
    # Supports simple dotted paths, e.g., "event.effect.avg"
    return [p for p in path.split(".") if p]


def _validate_clause_value(op: str, value: Any) -> None:
    """Light type checks per operator to catch common mistakes early."""
    if op in {"gt", "gte", "lt", "lte"}:
        if not isinstance(value, (int, float)):
            raise ValueError(f"Operator '{op}' expects a number, got {type(value).__name__}")
    elif op == "in":
        if not isinstance(value, (list, tuple, set)):
            raise ValueError("Operator 'in' expects a list/tuple/set")
    elif op == "regex":
        if not isinstance(value, str):
            raise ValueError("Operator 'regex' expects a string pattern")
    elif op in {"eq", "neq", "exists"}:
        # flexible; 'exists' ignores value
        return
    else:
        raise ValueError(f"Unsupported operator '{op}'")


# -------- IR models -----------------------------------------------------------
class IRClause(BaseModel):
    path_tokens: List[str]
    op: str = Field(pattern="^(eq|neq|gt|gte|lt|lte|in|regex|exists)$")
    value: Any | None = None


class IRConditions(BaseModel):
    aggregator: str = Field(pattern="^(all|any)$")
    clauses: List[IRClause]


class IRTrigger(BaseModel):
    type: str = Field(pattern="^(event|schedule)$")
    match: Dict[str, Any] | None = None
    cron: str | None = None


class IRAction(BaseModel):
    type: str = Field(pattern="^(flag|notify|score_adjust|open_case|webhook)$")
    params: Dict[str, Any] | None = None


class IRRule(BaseModel):
    id: str
    version: int
    status: str
    enabled: bool
    severity: str
    scope: Dict[str, Any]
    trigger: IRTrigger
    conditions: IRConditions
    actions: List[IRAction]
    meta: Dict[str, Any]
    source_file: str
    source_checksum: str


class IRManifest(BaseModel):
    generated_at: str
    engine_version: str
    source_count: int
    compiled_count: int
    rules: List[IRRule]


# -------- Compiler ------------------------------------------------------------
def _compile_rule(rule_dict: Dict[str, Any], source_file: Path, raw_text: str) -> IRRule:
    """Parse rule via Pydantic (from M8.1), then emit normalized IR."""
    # Parse with M8.1 model (ensures it matches schema)
    try:
        model = Rule(**rule_dict)
    except ValidationError as exc:
        raise ValueError(f"Rule validation failed for {source_file.name}: {exc}") from exc

    # Normalize conditions
    ir_clauses: List[IRClause] = []
    for c in model.conditions.clauses:
        if c.op not in VALID_OPS:
            raise ValueError(f"Unknown operator '{c.op}' in {source_file.name}")
        _validate_clause_value(c.op, c.value)
        ir_clauses.append(IRClause(path_tokens=_tokenize_path(c.path), op=c.op, value=c.value))

    ir_conditions = IRConditions(aggregator=model.conditions.aggregator, clauses=ir_clauses)

    ir_trigger = IRTrigger(
        type=model.trigger.type,
        match=model.trigger.match,
        cron=model.trigger.cron,
    )

    ir_actions = [IRAction(type=a.type, params=a.params) for a in model.actions]

    enabled = model.meta.status != "deprecated"
    checksum = _sha256_bytes(raw_text.encode("utf-8"))

    ir = IRRule(
        id=model.meta.id,
        version=model.meta.version,
        status=model.meta.status,
        enabled=enabled,
        severity=model.severity,
        scope=model.scope,
        trigger=ir_trigger,
        conditions=ir_conditions,
        actions=ir_actions,
        meta={
            "owner": model.meta.owner,
            "description": model.meta.description,
            "created_at": model.meta.created_at,
            "updated_at": model.meta.updated_at,
        },
        source_file=source_file.as_posix(),
        source_checksum=checksum,
    )
    return ir


def compile_all() -> IRManifest:
    """Load all YAML rules, compile to IR, and return a manifest."""
    items: List[Tuple[Path, Dict[str, Any], str]] = _load_yaml_rules()
    ir_rules: List[IRRule] = []
    for path, data, raw in items:
        ir_rules.append(_compile_rule(data, path, raw))

    manifest = IRManifest(
        generated_at=_now_iso(),
        engine_version=ENGINE_VERSION,
        source_count=len(items),
        compiled_count=len(ir_rules),
        rules=ir_rules,
    )
    return manifest


def write_manifest(manifest: IRManifest) -> Path:
    COMPILED_DIR.mkdir(parents=True, exist_ok=True)
    payload = json.loads(manifest.model_dump_json())
    COMPILED_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return COMPILED_PATH


def main() -> None:
    manifest = compile_all()
    path = write_manifest(manifest)
    print(
        f"✅ M8.2 compiled {manifest.compiled_count}/{manifest.source_count} rule(s) "
        f"→ {path.as_posix()}"
    )
