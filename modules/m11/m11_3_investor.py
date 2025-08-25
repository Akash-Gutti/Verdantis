"""M11.3 - Investor portal data builders.

- Build per-asset risk trajectory from deduped events (M10.3).
- Build ESG→ROI linkage proxy from risk trend (+ optional causal series).
- Summarize news sentiment if available.
- Token-gated at CLI (role='investor').
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

SEVERITY_WEIGHT: Dict[str, int] = {
    "low": 1,
    "medium": 2,
    "high": 4,
    "critical": 8,
}


@dataclass(frozen=True)
class InvestorInputs:
    deduped_events_path: Path
    causal_series_dir: Optional[Path] = None
    news_json_path: Optional[Path] = None


# -------------------- utils --------------------


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _parse_date(ts: Optional[str]) -> Optional[str]:
    """Return YYYY-MM-DD from ISO ts; fallback None if bad."""
    if not ts or not isinstance(ts, str):
        return None
    s = ts[:-1] + "+00:00" if ts.endswith("Z") else ts
    try:
        d = dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    return d.date().isoformat()


# -------------------- risk trajectory --------------------


def _load_deduped(path: Path) -> List[Dict[str, Any]]:
    data = _read_json(path)
    if not isinstance(data, list):
        raise ValueError("Deduped events JSON must be a list.")
    out: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict) and "event" in item:
            out.append(item)
    return out


def _build_daily_scores(deduped: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    """Return dict: asset_id -> {date -> score} using severity weights."""
    agg: Dict[str, Dict[str, int]] = {}
    for rec in deduped:
        ev = rec.get("event") or {}
        asset = str(ev.get("asset_id") or "")
        if not asset:
            # investor view focuses on assets
            continue
        sev = str(ev.get("severity", "low"))
        w = SEVERITY_WEIGHT.get(sev, 0)
        date = _parse_date(str(ev.get("ts"))) or dt.date.today().isoformat()
        by_date = agg.get(asset) or {}
        by_date[date] = int(by_date.get(date, 0)) + int(w)
        agg[asset] = by_date
    return agg


def _series_sorted(by_date: Dict[str, int]) -> List[Dict[str, Any]]:
    items = [{"date": d, "risk_score": by_date[d]} for d in sorted(by_date.keys())]
    return items


def _rolling_mean(vals: List[int], window: int) -> List[float]:
    out: List[float] = []
    s = 0
    q: List[int] = []
    for v in vals:
        q.append(v)
        s += v
        if len(q) > window:
            s -= q.pop(0)
        out.append(round(s / len(q), 3))
    return out


def _risk_trajectory(deduped: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    daily = _build_daily_scores(deduped)
    result: List[Dict[str, Any]] = []
    for asset, by_date in daily.items():
        series = _series_sorted(by_date)
        vals = [int(it["risk_score"]) for it in series]
        roll7 = _rolling_mean(vals, 7) if vals else []
        for i, it in enumerate(series):
            it["risk_roll7"] = roll7[i] if i < len(roll7) else float(vals[i])
        result.append({"asset_id": asset, "series": series})

    # Sort by most recent roll7 descending (riskier first)
    def _last_roll(s: Dict[str, Any]) -> float:
        ser = s.get("series", [])
        if not ser:
            return 0.0
        return float(ser[-1].get("risk_roll7", ser[-1].get("risk_score", 0)))

    result.sort(key=_last_roll, reverse=True)
    return result


# -------------------- causal / ROI linkage --------------------


def _load_causal_series(dir_path: Optional[Path]) -> Dict[str, Dict[str, Any]]:
    """Optional: load any causal JSON files shaped like:
    {"asset_id": "...", "metric": "...", "series": {"date": [...], "y": [...]}}
    Returns asset_id -> metric -> {date, y}
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not dir_path or not dir_path.exists():
        return out
    for p in dir_path.rglob("*.json"):
        try:
            obj = _read_json(p)
        except Exception:  # noqa: BLE001
            continue
        asset = str(obj.get("asset_id") or "")
        metric = str(obj.get("metric") or "")
        series = obj.get("series") or {}
        dates = series.get("date")
        ys = series.get("y")
        if not asset or not metric or not isinstance(dates, list) or not isinstance(ys, list):
            continue
        if len(dates) != len(ys):
            continue
        out.setdefault(asset, {})
        out[asset][metric] = {"date": dates, "y": ys}
    return out


def _simple_slope(vals: List[float]) -> float:
    """Unweighted slope proxy: last - first."""
    if not vals:
        return 0.0
    return float(vals[-1]) - float(vals[0])


def _link_esg_roi(
    traj: List[Dict[str, Any]],
    causal: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Heuristic linkage:
    - Compute risk trend (Δ of roll7). ROI proxy = -risk_trend (lower risk → higher ROI).
    - If causal series exist for asset, include last known values as context.
    """
    out: List[Dict[str, Any]] = []
    for row in traj:
        asset = str(row.get("asset_id"))
        ser = row.get("series", [])
        roll = [float(it.get("risk_roll7", it.get("risk_score", 0))) for it in ser]
        trend = _simple_slope(roll)
        roi_proxy = round(-trend, 3)
        entry: Dict[str, Any] = {
            "asset_id": asset,
            "risk_trend": round(trend, 3),
            "roi_proxy": roi_proxy,
        }
        if asset in causal:
            # Include last values per metric for quick investor glance
            metrics = {}
            for m, obj in causal[asset].items():
                y = obj.get("y") or []
                if y:
                    metrics[m] = float(y[-1])
            if metrics:
                entry["causal_snapshot"] = metrics
        out.append(entry)
    # Highest ROI proxy first
    out.sort(key=lambda x: x.get("roi_proxy", 0.0), reverse=True)
    return out


# -------------------- news sentiment (optional) --------------------


def _summarize_news(news_path: Optional[Path]) -> Dict[str, Any]:
    if not news_path or not news_path.exists():
        return {"total": 0, "by_label": {}}
    try:
        items = _read_json(news_path)
    except Exception:  # noqa: BLE001
        return {"total": 0, "by_label": {}}
    if not isinstance(items, list):
        return {"total": 0, "by_label": {}}
    by_label: Dict[str, int] = {}
    for it in items:
        label = it.get("sentiment") or it.get("label") or "neutral"
        by_label[str(label)] = by_label.get(str(label), 0) + 1
    return {"total": len(items), "by_label": by_label}


# -------------------- entrypoint --------------------


def run_investor_build(
    inputs: InvestorInputs,
    out_dir: Path,
) -> Dict[str, int]:
    deduped = _load_deduped(inputs.deduped_events_path)
    traj = _risk_trajectory(deduped)
    causal = _load_causal_series(inputs.causal_series_dir)
    linkage = _link_esg_roi(traj, causal)
    news = _summarize_news(inputs.news_json_path)

    _write_json(out_dir / "risk_trajectory.json", traj)
    _write_json(out_dir / "esg_roi_linkage.json", linkage)
    _write_json(out_dir / "news_sentiment.json", news)
    _write_json(
        out_dir / "metrics.json",
        {
            "built_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "assets_with_trajectory": len(traj),
            "assets_with_causal": len(causal),
            "news_items": news.get("total", 0),
            "sources": {
                "deduped_events": str(inputs.deduped_events_path),
                "causal_series_dir": (
                    str(inputs.causal_series_dir) if inputs.causal_series_dir else None
                ),
                "news_json": str(inputs.news_json_path) if inputs.news_json_path else None,
            },
        },
    )
    return {
        "assets_with_trajectory": len(traj),
        "assets_with_causal": len(causal),
        "news_items": news.get("total", 0),
    }
