"""M10.1 - Alerts filter engine.

Defines subscriptions ("filters") and matching logic to select material events.
Outputs matched events and simple metrics. Designed to be re-used by the alerts
microservice and CLI. Keep it dependency-light and flake8-friendly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SEVERITY_RANK: Dict[str, int] = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


@dataclass(frozen=True)
class AlertFilter:
    """Subscription for events of interest."""

    id: str
    topics: Optional[List[str]] = None
    severity_at_least: Optional[str] = None
    assets: Optional[List[str]] = None  # list of asset_ids or ["*"]
    rule_types: Optional[List[str]] = None
    aoi_ids: Optional[List[str]] = None
    min_delta: Optional[Dict[str, float]] = None  # e.g. {"ndvi": 0.2}
    suppress_if: Optional[Dict[str, Any]] = None  # simple equality checks

    def match(self, event: Dict[str, Any]) -> bool:
        """Return True if this filter matches the given event."""
        if self.topics and event.get("topic") not in set(self.topics):
            return False

        if self.severity_at_least:
            ev_sev = event.get("severity", "info")
            if SEVERITY_RANK.get(ev_sev, 0) < SEVERITY_RANK.get(self.severity_at_least, 0):
                return False

        if self.assets and "*" not in self.assets:
            if event.get("asset_id") not in set(self.assets):
                return False

        if self.rule_types:
            if event.get("rule_type") not in set(self.rule_types):
                return False

        if self.aoi_ids:
            if event.get("aoi_id") not in set(self.aoi_ids):
                return False

        if self.min_delta:
            delta = event.get("delta") or {}
            for k, v in self.min_delta.items():
                try:
                    if float(delta.get(k, 0.0)) < float(v):
                        return False
                except (TypeError, ValueError):
                    return False

        if self.suppress_if:
            # suppress event if ALL specified keys equal the given values
            for k, v in self.suppress_if.items():
                if event.get(k) != v:
                    break
            else:
                return False

        return True


def load_filters(path: Path) -> List[AlertFilter]:
    """Load filter config JSON into AlertFilter objects."""
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    subs = []
    for item in cfg.get("subscriptions", []):
        subs.append(
            AlertFilter(
                id=str(item["id"]),
                topics=item.get("topics"),
                severity_at_least=item.get("severity_at_least"),
                assets=item.get("assets"),
                rule_types=item.get("rule_types"),
                aoi_ids=item.get("aoi_ids"),
                min_delta=item.get("min_delta"),
                suppress_if=item.get("suppress_if"),
            )
        )
    return subs


def load_events(path: Path) -> List[Dict[str, Any]]:
    """Load a list of events (JSON array)."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Events JSON must be a list.")
    return [e for e in data if isinstance(e, dict)]


def apply_filters(
    events: Iterable[Dict[str, Any]], filters: List[AlertFilter]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Apply filters to events.

    Returns:
        matched: list of {"subscription_id": ..., "event": {...}}
        metrics: summary dict with counts per subscription and unmatched count
    """
    matched: List[Dict[str, Any]] = []
    counts = {flt.id: 0 for flt in filters}
    total = 0
    unmatched = 0

    flt_list = list(filters)
    for ev in events:
        total += 1
        hit_any = False
        for flt in flt_list:
            if flt.match(ev):
                matched.append({"subscription_id": flt.id, "event": ev})
                counts[flt.id] += 1
                hit_any = True
        if not hit_any:
            unmatched += 1

    metrics = {
        "total_events": total,
        "unmatched": unmatched,
        "per_subscription": counts,
    }
    return matched, metrics


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def run_filters_cli(
    events_path: Path,
    filters_path: Path,
    out_path: Path,
    metrics_path: Path,
) -> Tuple[int, int]:
    """CLI entrypoint used by modules.m10.cli."""
    filters = load_filters(filters_path)
    events = load_events(events_path)
    matched, metrics = apply_filters(events, filters)
    write_json(out_path, matched)
    write_json(metrics_path, metrics)
    return len(matched), metrics.get("unmatched", 0)
