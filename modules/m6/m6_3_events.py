"""M6.3 Event writer: SatelliteChangeDetected.

Reads each AOI's manifest.json + metrics.json from data/interim/m6/<aoi_id>/,
creates a normalized event row, appends to
data/processed/events/satellite_change_events.csv, and emits a JSON copy
onto the file bus at data/bus/topics/violation.flagged/.

No DB writes here; CSV + bus JSON only (idempotent via event_id reuse).
"""

from __future__ import annotations

import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

CSV_PATH = Path("data/processed/events/satellite_change_events.csv")
BUS_DIR = Path("data/bus/topics/violation.flagged")


def _load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _ensure_parents(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _severity(score: float) -> str:
    if score >= 0.05:
        return "high"
    if score >= 0.01:
        return "medium"
    return "low"


def _pick_occurred_at(manifest: Dict) -> str:
    # Prefer after_date if present; else now.
    after_date = manifest.get("after_date")
    if isinstance(after_date, str) and after_date:
        try:
            # normalize to 00:00Z if date-only string
            if "T" not in after_date:
                return f"{after_date}T00:00:00+00:00"
            return after_date
        except Exception:
            return _iso_now()
    return _iso_now()


def _diff_path_for_mode(manifest_dir: Path, mode: str) -> Optional[str]:
    cand = "ndvi_diff.tif" if mode == "ndvi" else "rgb_diff.tif"
    p = manifest_dir / cand
    return str(p.as_posix()) if p.exists() else None


def _build_event(aoi_id: str, manifest_dir: Path) -> Dict:
    manifest = _load_json(manifest_dir / "manifest.json")
    metrics = _load_json(manifest_dir / "metrics.json")

    mode = str(manifest.get("mode", "ndvi"))
    files = manifest.get("files", {})
    score = float(metrics.get("change_score", 0.0))
    occurred_at = _pick_occurred_at(manifest)
    evt_id = str(uuid.uuid4())

    evt: Dict = {
        "event_id": evt_id,
        "event_type": "SatelliteChangeDetected",
        "aoi_id": aoi_id,
        "occurred_at": occurred_at,
        "change_score": score,
        "fraction_changed": float(metrics.get("fraction_changed", score)),
        "changed_pixels": int(metrics.get("changed_pixels", 0)),
        "total_pixels": int(metrics.get("total_pixels", 0)),
        "severity": _severity(score),
        "mode": mode,
        "percentile": float(metrics.get("percentile", 0.0)),
        "threshold": float(metrics.get("threshold", 0.0)),
        "bbox": json.dumps(manifest.get("bbox")),
        "mask_path": str((manifest_dir / "change_mask.tif").as_posix()),
        "diff_path": _diff_path_for_mode(manifest_dir, mode),
        "tile_before_b4": None,
        "tile_before_b8": None,
        "tile_after_b4": None,
        "tile_after_b8": None,
        "tile_before_rgb": None,
        "tile_after_rgb": None,
    }

    if mode == "ndvi":
        evt["tile_before_b4"] = files.get("before_B4")
        evt["tile_before_b8"] = files.get("before_B8")
        evt["tile_after_b4"] = files.get("after_B4")
        evt["tile_after_b8"] = files.get("after_B8")
    else:
        evt["tile_before_rgb"] = files.get("before_rgb")
        evt["tile_after_rgb"] = files.get("after_rgb")

    return evt


def _write_bus_copy(evt: Dict) -> None:
    _ensure_parents(BUS_DIR)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    aoi = str(evt.get("aoi_id", "aoi"))
    name = f"{ts}_{aoi}.json"
    payload = {
        "topic": "violation.flagged",
        "event": evt,
    }
    with (BUS_DIR / name).open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _append_csv(rows: List[Dict]) -> None:
    _ensure_parents(CSV_PATH)
    new_cols = set()
    for r in rows:
        new_cols.update(r.keys())
    # Stable column order with sensible grouping
    preferred = [
        "event_id",
        "event_type",
        "aoi_id",
        "occurred_at",
        "severity",
        "change_score",
        "fraction_changed",
        "changed_pixels",
        "total_pixels",
        "mode",
        "percentile",
        "threshold",
        "bbox",
        "mask_path",
        "diff_path",
        "tile_before_b4",
        "tile_before_b8",
        "tile_after_b4",
        "tile_after_b8",
        "tile_before_rgb",
        "tile_after_rgb",
    ]
    cols = [c for c in preferred if c in new_cols] + [
        c for c in sorted(new_cols) if c not in preferred
    ]

    write_header = not CSV_PATH.exists()
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def run_m6_3() -> List[Dict]:
    """Generate events for all AOIs with metrics.json present."""
    index_path = Path("data/interim/m6/index.json")
    if not index_path.exists():
        raise FileNotFoundError("Run M6.1 first (missing index.json).")
    index = _load_json(index_path)

    events: List[Dict] = []
    for aoi_id, entry in index.items():
        manifest_path = Path(entry["manifest"])
        manifest_dir = manifest_path.parent
        metrics_path = manifest_dir / "metrics.json"
        if not metrics_path.exists():
            # AOI not processed in M6.2; skip quietly.
            continue
        evt = _build_event(aoi_id, manifest_dir)
        events.append(evt)
        _write_bus_copy(evt)

    if events:
        _append_csv(events)

    # Simple stdout summary
    print(f"🛈 M6.3 created {len(events)} SatelliteChangeDetected event(s).")
    return events
