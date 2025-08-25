"""M10.3 - Dedupe + Flapping Suppression.

Reads matched events from M10.1 and emits a deduped list to feed channels.
Maintains JSON state on disk to persist counters across runs.

State layout (JSON):
{
  "version": 1,
  "updated_at": "ISO-8601",
  "keys": {
    "<dedupe_key>": {
      "last_sent_ts": "ISO-8601",
      "flap_history": [["ISO-8601", "value"], ...]
    }
  }
}
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ISO_TZ = dt.timezone.utc


@dataclass(frozen=True)
class FlapCfg:
    enabled: bool
    key_fields: List[str]
    value_field: str
    window_seconds: int
    max_changes: int


@dataclass(frozen=True)
class DedupeCfg:
    ttl_seconds: int
    min_interval_seconds: int
    key_fields: List[str]
    flap: FlapCfg


def _now() -> dt.datetime:
    return dt.datetime.now(tz=ISO_TZ)


def _parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts or not isinstance(ts, str):
        return None
    # Accept "....Z" as UTC
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    try:
        return dt.datetime.fromisoformat(ts)
    except ValueError:
        return None


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_json(path: Path, obj: Any) -> None:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _get_nested(obj: Dict[str, Any], path: str) -> Any:
    """Get dot-path value; path may be 'subscription_id' or 'event.xxx'."""
    if path == "subscription_id":
        return obj.get("subscription_id")
    if not path.startswith("event."):
        return None
    cur: Any = obj.get("event", {})
    for part in path.split(".")[1:]:
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _key_from_fields(rec: Dict[str, Any], fields: Iterable[str]) -> str:
    vals = []
    for field in fields:
        vals.append(str(_get_nested(rec, field)))
    return "|".join(vals)


def _load_cfg(path: Path) -> DedupeCfg:
    raw = _read_json(path)
    flap = raw.get("flap") or {}
    return DedupeCfg(
        ttl_seconds=int(raw.get("ttl_seconds", 3600)),
        min_interval_seconds=int(raw.get("min_interval_seconds", 300)),
        key_fields=list(raw.get("key_fields", [])),
        flap=FlapCfg(
            enabled=bool(flap.get("enabled", True)),
            key_fields=list(flap.get("key_fields", [])),
            value_field=str(flap.get("value_field", "event.severity")),
            window_seconds=int(flap.get("window_seconds", 1800)),
            max_changes=int(flap.get("max_changes", 3)),
        ),
    )


def _load_matched(path: Path) -> List[Dict[str, Any]]:
    data = _read_json(path)
    if not isinstance(data, list):
        raise ValueError("Matched events JSON must be a list.")
    out = []
    for item in data:
        if isinstance(item, dict) and "subscription_id" in item and "event" in item:
            out.append(item)
    return out


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": _now().isoformat(), "keys": {}}
    try:
        obj = _read_json(path)
        if not isinstance(obj, dict):
            return {"version": 1, "updated_at": _now().isoformat(), "keys": {}}
        if "keys" not in obj:
            obj["keys"] = {}
        return obj
    except Exception:
        return {"version": 1, "updated_at": _now().isoformat(), "keys": {}}


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    state["updated_at"] = _now().isoformat()
    _write_json(path, state)


def _event_ts(rec: Dict[str, Any]) -> dt.datetime:
    ev = rec.get("event", {})
    ts = _parse_iso(ev.get("ts"))
    return ts or _now()


def _flap_value(rec: Dict[str, Any], value_field: str) -> str:
    if value_field == "subscription_id":
        val = rec.get("subscription_id")
        return str(val)
    if value_field.startswith("event."):
        return str(_get_nested(rec, value_field))
    return "None"


def _is_duplicate(
    now_dt: dt.datetime,
    last_sent_ts: Optional[str],
    ttl_seconds: int,
    min_interval_seconds: int,
) -> Tuple[bool, str]:
    if not last_sent_ts:
        return False, ""
    prev = _parse_iso(last_sent_ts)
    if not prev:
        return False, ""
    age = (now_dt - prev).total_seconds()
    if age < min_interval_seconds:
        return True, "cooldown"
    if age < ttl_seconds:
        return True, "duplicate_ttl"
    return False, ""


def _is_flapping(
    now_dt: dt.datetime,
    history: List[List[Any]],
    new_value: str,
    window_seconds: int,
    max_changes: int,
) -> bool:
    """Return True if changes within window exceed max_changes."""
    # Build a list including the new value, restricted to window
    within: List[str] = []
    cutoff = now_dt - dt.timedelta(seconds=window_seconds)
    last_val: Optional[str] = None
    # Keep only records inside window and count value transitions
    for ts_str, val in history:
        ts = _parse_iso(str(ts_str))
        if ts and ts >= cutoff:
            within.append(str(val))
    within.append(new_value)

    changes = 0
    for val in within:
        if last_val is None:
            last_val = val
            continue
        if val != last_val:
            changes += 1
            last_val = val
    return changes > max_changes


def run_dedupe_cli(
    matched_path: Path,
    cfg_path: Path,
    out_path: Path,
    metrics_path: Path,
    state_path: Path,
) -> Tuple[int, int]:
    """Apply dedupe/flapping rules. Returns (kept, suppressed)."""
    cfg = _load_cfg(cfg_path)
    matched = _load_matched(matched_path)
    state = _load_state(state_path)
    keys = state.get("keys", {})

    kept: List[Dict[str, Any]] = []
    suppressed = 0

    for idx, rec in enumerate(matched):
        now_dt = _event_ts(rec)
        key = _key_from_fields(rec, cfg.key_fields)
        key_entry: Dict[str, Any] = keys.get(key, {})
        last_sent_ts = key_entry.get("last_sent_ts")

        # Dedupe / cooldown
        is_dup, reason = _is_duplicate(
            now_dt, last_sent_ts, cfg.ttl_seconds, cfg.min_interval_seconds
        )
        if is_dup:
            suppressed += 1
            # Update flap history even for suppressed items
            if cfg.flap.enabled:
                fv = _flap_value(rec, cfg.flap.value_field)
                hist: List[List[Any]] = key_entry.get("flap_history", [])
                hist.append([now_dt.isoformat(), fv])
                key_entry["flap_history"] = hist
                keys[key] = key_entry
            continue

        # Flapping
        if cfg.flap.enabled:
            fv = _flap_value(rec, cfg.flap.value_field)
            # Flap key can be different from dedupe key
            flap_key = _key_from_fields(rec, cfg.flap.key_fields)
            fk_entry: Dict[str, Any] = keys.get(flap_key, {})
            hist: List[List[Any]] = fk_entry.get("flap_history", [])
            # prune window here when counting
            if _is_flapping(now_dt, hist, fv, cfg.flap.window_seconds, cfg.flap.max_changes):
                suppressed += 1
                # still append for future stability calculation
                hist.append([now_dt.isoformat(), fv])
                fk_entry["flap_history"] = hist
                keys[flap_key] = fk_entry
                continue
            # not flapping → update history
            hist.append([now_dt.isoformat(), fv])
            fk_entry["flap_history"] = hist
            keys[flap_key] = fk_entry

        # Keep and mark sent
        kept.append(rec)
        key_entry["last_sent_ts"] = now_dt.isoformat()
        keys[key] = key_entry

    state["keys"] = keys
    _save_state(state_path, state)

    _write_json(out_path, kept)
    _write_json(
        metrics_path,
        {
            "input": len(matched),
            "kept": len(kept),
            "suppressed": suppressed,
            "cfg": {
                "ttl_seconds": cfg.ttl_seconds,
                "min_interval_seconds": cfg.min_interval_seconds,
                "flap_enabled": cfg.flap.enabled,
            },
            "state_path": str(state_path),
        },
    )
    return len(kept), suppressed
