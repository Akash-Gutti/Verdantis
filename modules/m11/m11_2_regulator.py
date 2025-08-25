"""M11.2 - Regulator portal data builders.

- Build heatmap + open violations JSON for the UI from deduped events (M10.3).
- Record "Request audit pack" actions.
- Token-gated: requires 'regulator' role.

No external deps. Flake8-friendly.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Shared severity ladder
SEVERITY_RANK: Dict[str, int] = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}

# Risk weights (can be tuned later)
SEVERITY_WEIGHT: Dict[str, int] = {
    "low": 1,
    "medium": 2,
    "high": 4,
    "critical": 8,
}


@dataclass(frozen=True)
class InputsCfg:
    deduped_events_path: Path
    alerts_feed_path: Optional[Path] = None
    assets_geojson_path: Optional[Path] = None
    bundles_index_path: Optional[Path] = None


def _now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts or not isinstance(ts, str):
        return None
    s = ts[:-1] + "+00:00" if ts.endswith("Z") else ts
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def _safe_ts_str(ts: Optional[str]) -> str:
    return (_parse_iso(ts) or dt.datetime.now(tz=dt.timezone.utc)).isoformat()


def _read_assets_centroids(geo_path: Optional[Path]) -> Dict[str, Dict[str, float]]:
    """Return optional lat/lon per asset_id if present in GeoJSON.

    We do NOT compute polygon centroids here; we read either properties.lat/lon
    or the first coordinate from Point geometry, if available.
    """
    out: Dict[str, Dict[str, float]] = {}
    if not geo_path or not geo_path.exists():
        return out
    try:
        gj = _read_json(geo_path)
        feats = gj.get("features", []) if isinstance(gj, dict) else []
        for ft in feats:
            props = ft.get("properties", {}) if isinstance(ft, dict) else {}
            asset_id = str(props.get("asset_id") or props.get("id") or "")
            if not asset_id:
                continue
            lat = props.get("lat")
            lon = props.get("lon")
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                out[asset_id] = {"lat": float(lat), "lon": float(lon)}
                continue
            geom = ft.get("geometry", {})
            if geom and geom.get("type") == "Point":
                coords = geom.get("coordinates")
                if (
                    isinstance(coords, list)
                    and len(coords) >= 2
                    and isinstance(coords[0], (int, float))
                    and isinstance(coords[1], (int, float))
                ):
                    out[asset_id] = {"lat": float(coords[1]), "lon": float(coords[0])}
    except Exception:
        # Silent fallback: location info is optional
        return out
    return out


def _load_deduped_events(path: Path) -> List[Dict[str, Any]]:
    data = _read_json(path)
    if not isinstance(data, list):
        raise ValueError("Deduped events JSON must be a list.")
    out: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict) and "subscription_id" in item and "event" in item:
            out.append(item)
    return out


def _is_open_violation(ev: Dict[str, Any]) -> bool:
    """Heuristic: topic policy.enforcement + not acknowledged + medium+."""
    topic = str(ev.get("topic", ""))
    if topic != "policy.enforcement":
        return False
    sev = str(ev.get("severity", "info"))
    if SEVERITY_RANK.get(sev, 0) < SEVERITY_RANK.get("medium", 0):
        return False
    acknowledged = bool(ev.get("acknowledged", False))
    return not acknowledged


def _title_for_violation(sub_id: str, ev: Dict[str, Any]) -> str:
    sev = str(ev.get("severity", "info")).upper()
    topic = str(ev.get("topic", "event"))
    rt = ev.get("rule_type")
    asset = ev.get("asset_id") or ev.get("aoi_id") or "unknown"
    rt_txt = f" / {rt}" if rt else ""
    return f"[{sev}] {topic}{rt_txt} @ {asset} ({sub_id})"


def _build_open_violations(
    deduped: List[Dict[str, Any]],
    bundles_idx: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, rec in enumerate(deduped):
        sub_id = str(rec.get("subscription_id"))
        ev = rec.get("event") or {}
        if not _is_open_violation(ev):
            continue
        item = {
            "id": str(ev.get("id") or f"v_{idx}"),
            "ts": _safe_ts_str(str(ev.get("ts"))),
            "title": _title_for_violation(sub_id, ev),
            "severity": ev.get("severity"),
            "asset_id": ev.get("asset_id"),
            "aoi_id": ev.get("aoi_id"),
            "rule_type": ev.get("rule_type"),
            "topic": ev.get("topic"),
            "payload": ev.get("payload", {}),
            # Optional bundle hint if present in payload or bundle index
            "bundle_id": ev.get("payload", {}).get("bundle_id"),
        }
        out.append(item)

    # Optional enrichment from bundles index: ensure bundle_id exists in index
    if bundles_idx and isinstance(bundles_idx.get("items"), list):
        valid_ids = {str(it.get("bundle_id")) for it in bundles_idx["items"] if it}
        for it in out:
            bid = it.get("bundle_id")
            if bid and str(bid) not in valid_ids:
                it["bundle_id"] = None
    # Newest first
    out.sort(key=lambda x: x.get("ts", ""), reverse=True)
    return out


def _build_heatmap(
    deduped: List[Dict[str, Any]],
    asset_locs: Dict[str, Dict[str, float]],
) -> List[Dict[str, Any]]:
    """Aggregate severity-weighted risk per asset_id."""
    agg: Dict[str, Dict[str, Any]] = {}
    for rec in deduped:
        ev = rec.get("event") or {}
        asset = str(ev.get("asset_id") or "")
        if not asset:
            # Skip events without asset_id for heatmap
            continue
        sev = str(ev.get("severity", "info"))
        w = SEVERITY_WEIGHT.get(sev, 0)
        ent = agg.get(asset) or {"asset_id": asset, "open_count": 0, "risk_score": 0, "last_ts": ""}
        ent["risk_score"] = int(ent["risk_score"]) + int(w)
        ent["open_count"] = int(ent["open_count"]) + 1
        ts = _safe_ts_str(str(ev.get("ts")))
        if not ent["last_ts"] or ts > str(ent["last_ts"]):
            ent["last_ts"] = ts
        agg[asset] = ent

    # Attach optional lat/lon
    items: List[Dict[str, Any]] = []
    for asset, ent in agg.items():
        loc = asset_locs.get(asset, {})
        it = {
            "asset_id": asset,
            "risk_score": int(ent["risk_score"]),
            "open_count": int(ent["open_count"]),
            "last_ts": ent["last_ts"],
            "lat": loc.get("lat"),
            "lon": loc.get("lon"),
        }
        items.append(it)

    # Highest risk first
    items.sort(key=lambda x: (x.get("risk_score", 0), x.get("open_count", 0)), reverse=True)
    return items


def load_bundles_index(path: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not path or not path.exists():
        return None
    try:
        return _read_json(path)
    except Exception:
        return None


def run_regulator_build(
    inputs: InputsCfg,
    out_dir: Path,
) -> Tuple[int, int]:
    """Build open violations + heatmap. Returns (#violations, #heatmap_assets)."""
    deduped = _load_deduped_events(inputs.deduped_events_path)
    bundles_idx = load_bundles_index(inputs.bundles_index_path)
    asset_locs = _read_assets_centroids(inputs.assets_geojson_path)

    violations = _build_open_violations(deduped, bundles_idx)
    heatmap = _build_heatmap(deduped, asset_locs)

    _write_json(out_dir / "open_violations.json", violations)
    _write_json(out_dir / "heatmap.json", heatmap)
    _write_json(
        out_dir / "metrics.json",
        {
            "built_at": _now_iso(),
            "violations": len(violations),
            "heatmap_assets": len(heatmap),
            "sources": {
                "deduped_events": str(inputs.deduped_events_path),
                "assets_geojson": (
                    str(inputs.assets_geojson_path) if inputs.assets_geojson_path else None
                ),
                "bundles_index": (
                    str(inputs.bundles_index_path) if inputs.bundles_index_path else None
                ),
            },
        },
    )
    return len(violations), len(heatmap)


def run_regulator_request_audit(
    out_log: Path,
    username: str,
    role: str,
    asset_id: Optional[str],
    bundle_id: Optional[str],
    reason: Optional[str],
) -> str:
    """Append an audit request record. Returns request_id."""
    record = {
        "request_id": f"req_{int(dt.datetime.now(tz=dt.timezone.utc).timestamp())}",
        "ts": _now_iso(),
        "user": username,
        "role": role,
        "asset_id": asset_id,
        "bundle_id": bundle_id,
        "reason": reason,
        "status": "queued",
    }
    log: List[Dict[str, Any]] = []
    if out_log.exists():
        try:
            old = _read_json(out_log)
            if isinstance(old, list):
                log = old
        except Exception:
            log = []
    log.append(record)
    _write_json(out_log, log)
    return str(record["request_id"])
