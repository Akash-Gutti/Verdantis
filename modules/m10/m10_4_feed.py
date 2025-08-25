"""M10.4 - UI feed builder.

Reads deduped matched events from M10.3 and emits a flat alerts feed suitable
for the digital twin UI to render (cards/list). Also writes simple metrics.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Dict, Tuple

SEVERITY_RANK: Dict[str, int] = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_iso(ts: str | None) -> dt.datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(ts)
    except ValueError:
        return None


def _safe_event_id(rec: Dict[str, Any], idx: int) -> str:
    ev = rec.get("event", {})
    ev_id = ev.get("id")
    if isinstance(ev_id, str) and ev_id:
        return ev_id
    return f"ev_{idx}"


def _severity(rec: Dict[str, Any]) -> Tuple[str, int]:
    sev = str(rec.get("event", {}).get("severity", "info"))
    return sev, SEVERITY_RANK.get(sev, 0)


def _title(rec: Dict[str, Any]) -> str:
    sub_id = str(rec.get("subscription_id"))
    ev = rec.get("event", {})
    topic = str(ev.get("topic", "event"))
    sev = str(ev.get("severity", "info")).upper()
    asset = ev.get("asset_id") or ev.get("aoi_id") or "unknown"
    rt = ev.get("rule_type")
    rt_txt = f" / {rt}" if rt else ""
    return f"[{sev}] {topic}{rt_txt} @ {asset} ({sub_id})"


def _ts(rec: Dict[str, Any]) -> dt.datetime:
    ts = _parse_iso(rec.get("event", {}).get("ts"))
    return ts or dt.datetime.now(tz=dt.timezone.utc)


def _flatten(rec: Dict[str, Any], idx: int) -> Dict[str, Any]:
    ev = rec.get("event", {})
    sev, sev_rank = _severity(rec)
    item = {
        "id": _safe_event_id(rec, idx),
        "ts": (_ts(rec)).isoformat(),
        "subscription_id": rec.get("subscription_id"),
        "topic": ev.get("topic"),
        "severity": sev,
        "severity_rank": sev_rank,
        "asset_id": ev.get("asset_id"),
        "aoi_id": ev.get("aoi_id"),
        "rule_type": ev.get("rule_type"),
        "title": _title(rec),
        "payload": ev.get("payload", {}),
        # keep original in case UI wants raw
        "event": ev,
    }
    return item


def run_feed_cli(
    deduped_path: Path,
    out_path: Path,
    metrics_path: Path,
    limit: int = 100,
) -> int:
    """Build alerts feed from deduped matched events. Returns count kept."""
    data = _read_json(deduped_path)
    if not isinstance(data, list):
        raise ValueError("Deduped events JSON must be a list.")

    items = [_flatten(rec, i) for i, rec in enumerate(data) if isinstance(rec, dict)]
    # sort newest first
    items.sort(key=lambda x: x.get("ts", ""), reverse=True)
    if limit and limit > 0:
        items = items[: int(limit)]

    # metrics
    by_sev: Dict[str, int] = {}
    for it in items:
        s = str(it.get("severity", "info"))
        by_sev[s] = by_sev.get(s, 0) + 1

    _write_json(out_path, items)
    _write_json(
        metrics_path,
        {
            "count": len(items),
            "by_severity": by_sev,
            "source": str(deduped_path),
            "limit": limit,
        },
    )
    return len(items)
