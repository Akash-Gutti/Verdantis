"""M8.3 — Propose rules from natural language (heuristics + templates).

This module converts a policy clause text into one or more YAML-compliant
rule dicts matching the M8.1 schema. It validates with the Rule model,
and can optionally write them under data/rules/proposed/.

Later, you can swap the heuristics with an actual LLM function, reusing
the same interface.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, List, Tuple

import yaml
from pydantic import ValidationError

from .m8_1_schema import RULES_DIR, Rule, _now_iso  # reuse utils

PROPOSED_DIR = RULES_DIR / "proposed"
PROPOSED_DIR.mkdir(parents=True, exist_ok=True)


def _slugify(text: str, max_len: int = 48) -> str:
    import re

    slug = re.sub(r"[^a-zA-Z0-9]+", ".", text.strip().lower())
    slug = re.sub(r"\.+", ".", slug).strip(".")
    if len(slug) > max_len:
        slug = slug[:max_len].rstrip(".")  # avoid trailing dot after slicing
    return slug


def _parse_threshold_kwh(text: str) -> float | None:
    # pick up phrases like "more than 250 kWh", ">=300kwh", "drop of 400 kwh"
    m = re.search(r"([<>]=?|at\s+least|more\s+than|over)\s*(\d{2,5})\s*kwh", text, re.I)
    if m:
        try:
            return float(m.group(2))
        except (ValueError, IndexError):
            return None
    m2 = re.search(r"(\d{2,5})\s*kwh", text, re.I)
    if m2:
        try:
            return float(m2.group(1))
        except (ValueError, IndexError):
            return None
    return None


def _now_iso_z() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _validate_rule(rule: Dict[str, Any]) -> None:
    try:
        Rule(**rule)
    except ValidationError as exc:
        raise ValueError(f"Proposed rule failed validation: {exc}") from exc


def _heuristic_candidates(
    text: str,
    owner: str,
    severity: str | None,
    id_hint: str | None,
) -> List[Dict[str, Any]]:
    """Very small heuristic library to produce sensible first drafts."""
    t = text.lower()
    now = _now_iso()

    # default severity
    sev = severity or "medium"

    # Candidate 1: energy consumption / kWh drops (ties to M7 causal.effects)
    if any(k in t for k in ["energy", "kwh", "consumption", "electricity", "power"]):
        thr = _parse_threshold_kwh(text) or 300.0
        rid = id_hint or f"rule.energy.effect.auto.{_slugify(text, max_len=24)}"
        rule = {
            "meta": {
                "id": rid,
                "version": 1,
                "status": "proposed",
                "owner": owner,
                "created_at": now,
                "updated_at": now,
                "description": (f"Auto-proposed: flag large energy reduction (> {int(thr)} kWh)."),
            },
            "severity": "high" if thr >= 300 else sev,
            "scope": {"assets": ["*"], "tags": ["energy"]},
            "trigger": {
                "type": "event",
                "match": {"event.type": "causal.effect", "metric": "energy_kwh"},
            },
            "conditions": {
                "aggregator": "all",
                "clauses": [
                    {"path": "event.effect.avg", "op": "lt", "value": -float(thr)},
                    {"path": "event.policy_date", "op": "exists"},
                ],
            },
            "actions": [
                {"type": "flag", "params": {"reason": "Energy reduction detected"}},
                {
                    "type": "notify",
                    "params": {"channel": "ops", "template": "energy_reduction_auto"},
                },
            ],
        }
        _validate_rule(rule)
        return [rule]

    # Candidate 2: satellite change + permits (ties to M6 + KG)
    if any(k in t for k in ["satellite", "ndvi", "vegetation", "permit", "construction"]):
        rid = id_hint or f"rule.sat.change.auto.{_slugify(text, max_len=24)}"
        rule = {
            "meta": {
                "id": rid,
                "version": 1,
                "status": "proposed",
                "owner": owner,
                "created_at": now,
                "updated_at": now,
                "description": (
                    "Auto-proposed: vegetation loss with no KG permit → possible violation."
                ),
            },
            "severity": "critical",
            "scope": {"region": "UAE", "tags": ["satellite", "permits"]},
            "trigger": {"type": "event", "match": {"event.type": "satellite.change"}},
            "conditions": {
                "aggregator": "all",
                "clauses": [
                    {"path": "event.ndvi_delta", "op": "lt", "value": -0.2},
                    {"path": "kg.permit.exists", "op": "eq", "value": False},
                ],
            },
            "actions": [
                {"type": "flag", "params": {"reason": "Change w/out permit"}},
                {"type": "open_case", "params": {"queue": "regulator", "priority": "P1"}},
                {
                    "type": "notify",
                    "params": {"channel": "compliance", "template": "illegal_construction_auto"},
                },
            ],
        }
        _validate_rule(rule)
        return [rule]

    # Fallback: generic event match skeleton for human edit
    rid = id_hint or f"rule.generic.auto.{_slugify(text, max_len=24)}"
    generic = {
        "meta": {
            "id": rid,
            "version": 1,
            "status": "proposed",
            "owner": owner,
            "created_at": now,
            "updated_at": now,
            "description": f"Auto-proposed from text: {text[:120]}",
        },
        "severity": severity or "low",
        "scope": {"assets": ["*"]},
        "trigger": {"type": "event", "match": {"event.type": "policy.signal"}},
        "conditions": {
            "aggregator": "all",
            "clauses": [{"path": "event.score", "op": "gte", "value": 0.7}],
        },
        "actions": [{"type": "flag", "params": {"reason": "Generic policy signal"}}],
    }
    _validate_rule(generic)
    return [generic]


def propose_from_text(
    text: str,
    owner: str = "policy-team",
    severity: str | None = None,
    id_hint: str | None = None,
    save: bool = True,
) -> List[Tuple[str, Dict[str, Any]]]:
    """Return a list of (yaml_text, rule_dict). Optionally save into proposed/."""
    candidates = _heuristic_candidates(text, owner, severity, id_hint)
    out: List[Tuple[str, Dict[str, Any]]] = []
    for rule in candidates:
        yaml_text = yaml.safe_dump(rule, sort_keys=False, allow_unicode=True)
        out.append((yaml_text, rule))
        if save:
            fid = rule["meta"]["id"].replace(".", "_")
            path = PROPOSED_DIR / f"{fid}.yaml"
            path.write_text(yaml_text, encoding="utf-8")
    return out
