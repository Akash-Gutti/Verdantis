"""M8.4 — Enforcement engine (evaluate compiled rules over events + KG).

- Loads compiled IR from data/rules/compiled/compiled_rules.json
- Evaluates triggers + conditions against a context: {"event": ..., "kg": ...}
- Returns violations and writes audit.jsonl; caller may publish bus events.

Usage in code:
  from modules.m8.m8_4_enforce import enforce_event

CLI is wired via verdctl (see m8 'enforce' subcommand).
"""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .m8_1_schema import RULES_DIR
from .m8_2_compiler import COMPILED_PATH

ENF_DIR = RULES_DIR / "enforcement"
ENF_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_PATH = ENF_DIR / "audit.jsonl"


def _now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _load_manifest() -> Dict[str, Any]:
    if not COMPILED_PATH.exists():
        raise SystemExit("Compiled rules not found. Run: python scripts/verdctl.py m8 compile")
    raw = COMPILED_PATH.read_text(encoding="utf-8")
    return json.loads(raw)


def _resolve(tokens: Sequence[str], ctx: Dict[str, Any]) -> Any:
    cur: Any = ctx
    for t in tokens:
        if isinstance(cur, dict) and t in cur:
            cur = cur[t]
        else:
            return None
    return cur


def _tokenize_path(path: str) -> List[str]:
    return [p for p in path.split(".") if p]


def _match_trigger(trigger: Dict[str, Any], root: Dict[str, Any]) -> bool:
    ttype = trigger.get("type")
    if ttype == "event":
        match = trigger.get("match") or {}
        for k, v in match.items():
            tokens = _tokenize_path(k)
            val = _resolve(tokens, root)
            # fallback: if not fully qualified, try under event/ and kg/
            if val is None and not k.startswith(("event.", "kg.")):
                val = _resolve(["event", *tokens], root)
                if val is None:
                    val = _resolve(["kg", *tokens], root)
            if val != v:
                return False
        return True
    if ttype == "schedule":
        return True
    return False


def _eval_op(op: str, left: Any, right: Any) -> bool:
    if op == "exists":
        return left is not None
    if op == "eq":
        return left == right
    if op == "neq":
        return left != right
    if op == "gt":
        return isinstance(left, (int, float)) and isinstance(right, (int, float)) and left > right
    if op == "gte":
        return isinstance(left, (int, float)) and isinstance(right, (int, float)) and left >= right
    if op == "lt":
        return isinstance(left, (int, float)) and isinstance(right, (int, float)) and left < right
    if op == "lte":
        return isinstance(left, (int, float)) and isinstance(right, (int, float)) and left <= right
    if op == "in":
        if isinstance(right, (list, tuple, set)):
            return left in right
        return False
    if op == "regex":
        return (
            isinstance(left, str) and isinstance(right, str) and re.search(right, left) is not None
        )
    return False


def _eval_conditions(
    conds: Dict[str, Any], root: Dict[str, Any]
) -> tuple[bool, List[Dict[str, Any]]]:
    """Return (passed, evidence_clauses)."""
    agg = conds.get("aggregator", "all")
    ev: List[Dict[str, Any]] = []
    results: List[bool] = []

    for c in conds.get("clauses", []):
        tokens = c.get("path_tokens") or _tokenize_path(c["path"])  # supports raw schema & IR
        left = _resolve(tokens, root)
        op = c["op"]
        right = c.get("value")
        ok = _eval_op(op, left, right)
        results.append(ok)
        ev.append({"path": ".".join(tokens), "op": op, "value": right, "resolved": left, "ok": ok})

    passed = all(results) if agg == "all" else any(results)
    return passed, ev


def _rule_scope_ok(scope: Dict[str, Any], asset_id: str) -> bool:
    # MVP: if 'assets' present and not wildcard, require membership
    assets = scope.get("assets")
    if isinstance(assets, list) and assets and assets != ["*"]:
        return asset_id in assets
    return True


def enforce_event(
    *,
    asset_id: str,
    event: Dict[str, Any],
    kg: Optional[Dict[str, Any]] = None,
    rule_ids: Optional[Iterable[str]] = None,
    include_proposed: bool = True,
) -> List[Dict[str, Any]]:
    """Evaluate event+kg against compiled rules and return violations list."""
    manifest = _load_manifest()
    rules = manifest.get("rules", [])

    selected = []
    ruleset = set(rule_ids) if rule_ids else None
    for r in rules:
        if ruleset and r["id"] not in ruleset:
            continue
        if not include_proposed and r.get("status") == "proposed":
            continue
        selected.append(r)

    root = {"event": event, "kg": kg or {}}
    out: List[Dict[str, Any]] = []

    for r in selected:
        if not _rule_scope_ok(r.get("scope", {}), asset_id):
            continue
        if not _match_trigger(r["trigger"], root):
            continue

        # IR produced clauses with path_tokens; rules directly from schema use 'path'
        conds = r["conditions"]
        # normalize clauses to IR shape
        if "clauses" in conds and conds["clauses"] and "path" in conds["clauses"][0]:
            norm = []
            for c in conds["clauses"]:
                norm.append(
                    {
                        "path_tokens": _tokenize_path(c["path"]),
                        "op": c["op"],
                        "value": c.get("value"),
                    }
                )
            conds = {"aggregator": conds.get("aggregator", "all"), "clauses": norm}

        passed, evidence = _eval_conditions(conds, root)
        if not passed:
            continue

        viol = {
            "ts": _now_iso(),
            "asset_id": asset_id,
            "rule_id": r["id"],
            "severity": r["severity"],
            "actions": r["actions"],
            "evidence": evidence,
            "event": {
                "type": event.get("type") or event.get("event", {}).get("type"),
                "snapshot": event,
            },
            "source": {"file": r.get("source_file"), "checksum": r.get("source_checksum")},
            "status": "violated",
        }
        out.append(viol)

    # append to audit log
    if out:
        with AUDIT_PATH.open("a", encoding="utf-8") as f:
            for v in out:
                f.write(json.dumps(v, ensure_ascii=False) + "\n")

    return out
