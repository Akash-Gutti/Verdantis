"""M12.1 - Observability foundations (metrics + logs).

- Collects metrics from prior modules and writes Prometheus text format.
- Writes structured JSON logs (Loki-compatible).
- Optional file ingesters to turn module outputs into logs.

No external dependencies; flake8-clean.
"""

from __future__ import annotations

import datetime as dt
import http.server
import json
import socketserver
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

# -------------------------- utils --------------------------


def _now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _safe_int(val: Any) -> int:
    try:
        return int(val)
    except Exception:  # noqa: BLE001
        return 0


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


# -------------------------- metrics --------------------------


@dataclass(frozen=True)
class MetricsSources:
    filters_metrics: Path
    dedupe_metrics: Path
    channels_metrics: Path
    feed_metrics: Optional[Path] = None
    reg_metrics: Optional[Path] = None
    inv_metrics: Optional[Path] = None
    pub_metrics: Optional[Path] = None


def _get_if_exists(p: Optional[Path]) -> Optional[Dict[str, Any]]:
    if p and p.exists():
        try:
            return _read_json(p)
        except Exception:  # noqa: BLE001
            return None
    return None


def collect_metrics(src: MetricsSources) -> Dict[str, float]:
    """Collect a flat dict of metric_name -> value."""
    out: Dict[str, float] = {}

    # M10.1 filters
    f = _get_if_exists(src.filters_metrics) or {}
    out["verdantis_events_total"] = float(_safe_int(f.get("total_events")))
    out["verdantis_events_unmatched"] = float(_safe_int(f.get("unmatched")))

    # M10.3 dedupe
    d = _get_if_exists(src.dedupe_metrics) or {}
    out["verdantis_dedupe_kept"] = float(_safe_int(d.get("kept")))
    out["verdantis_dedupe_suppressed"] = float(_safe_int(d.get("suppressed")))

    # M10.2 channels
    c = _get_if_exists(src.channels_metrics) or {}
    out["verdantis_channels_sent"] = float(_safe_int(c.get("sent")))
    out["verdantis_channels_skipped"] = float(_safe_int(c.get("skipped")))

    # M10.4 feed (optional)
    fm = _get_if_exists(src.feed_metrics) or {}
    out["verdantis_feed_items"] = float(_safe_int(fm.get("count")))

    # M11 regulator (optional)
    rm = _get_if_exists(src.reg_metrics) or {}
    out["verdantis_reg_violations"] = float(_safe_int(rm.get("violations")))
    out["verdantis_reg_heatmap_assets"] = float(_safe_int(rm.get("heatmap_assets")))

    # M11 investor (optional)
    im = _get_if_exists(src.inv_metrics) or {}
    out["verdantis_inv_assets_with_trajectory"] = float(_safe_int(im.get("assets_with_trajectory")))
    out["verdantis_inv_assets_with_causal"] = float(_safe_int(im.get("assets_with_causal")))
    out["verdantis_inv_news_items"] = float(_safe_int(im.get("news_items")))

    # M11 public (optional)
    pm = _get_if_exists(src.pub_metrics) or {}
    out["verdantis_public_items"] = float(_safe_int(pm.get("feed_items")))
    out["verdantis_public_regions"] = float(_safe_int(pm.get("regions")))

    # Build-info gauge for scrapers
    out["verdantis_build_info"] = 1.0
    return out


def _format_prometheus(metrics: Dict[str, float]) -> str:
    """Render Prometheus text exposition format."""
    # Minimal HELP/TYPE metadata for clarity
    lines = [
        "# HELP verdantis_build_info Build info marker.",
        "# TYPE verdantis_build_info gauge",
        "verdantis_build_info 1",
    ]
    help_map = {
        "verdantis_events_total": "Total input events (M10.1).",
        "verdantis_events_unmatched": "Events not matched by filters (M10.1).",
        "verdantis_dedupe_kept": "Events kept after dedupe (M10.3).",
        "verdantis_dedupe_suppressed": "Events suppressed by dedupe (M10.3).",
        "verdantis_channels_sent": "Channel sends (M10.2).",
        "verdantis_channels_skipped": "Channel skips (M10.2).",
        "verdantis_feed_items": "UI feed items (M10.4).",
        "verdantis_reg_violations": "Open violations (M11.2).",
        "verdantis_reg_heatmap_assets": "Assets on regulator heatmap (M11.2).",
        "verdantis_inv_assets_with_trajectory": "Investor assets with series (M11.3).",
        "verdantis_inv_assets_with_causal": "Investor assets with causal series (M11.3).",
        "verdantis_inv_news_items": "Investor news item count (M11.3).",
        "verdantis_public_items": "Public feed items (M11.4).",
        "verdantis_public_regions": "Regions covered in public feed (M11.4).",
    }
    type_map = {k: "gauge" for k in help_map.keys()}

    for name, value in metrics.items():
        if name == "verdantis_build_info":
            # already emitted
            continue
        if name in help_map:
            lines.append(f"# HELP {name} {help_map[name]}")
            lines.append(f"# TYPE {name} {type_map[name]}")
        lines.append(f"{name} {value:.6f}")

    # Timestamp as a comment
    lines.append(f"# scraped_at { _now_iso() }")
    return "\n".join(lines) + "\n"


def write_prometheus_textfile(out_path: Path, metrics: Dict[str, float]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    text = _format_prometheus(metrics)
    out_path.write_text(text, encoding="utf-8")


# -------------------------- logs --------------------------


def _log_path(base_dir: Path, when: Optional[dt.datetime] = None) -> Path:
    when = when or dt.datetime.now(tz=dt.timezone.utc)
    fname = f"app-{when.strftime('%Y%m%d')}.log"
    p = base_dir / fname
    _ensure_dir(p.parent)
    return p


def log_write(
    base_dir: Path,
    level: str,
    service: str,
    module: str,
    message: str,
    ctx: Optional[Dict[str, Any]] = None,
) -> Path:
    """Append one structured JSON log line."""
    line = {
        "ts": _now_iso(),
        "level": str(level).lower(),
        "service": service,
        "module": module,
        "msg": message,
        "ctx": ctx or {},
    }
    p = _log_path(base_dir)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")
    return p


def ingest_channels_results(path: Path, base_dir: Path) -> int:
    data = _read_json(path)
    if not isinstance(data, list):
        return 0
    n = 0
    for it in data:
        if not isinstance(it, dict):
            continue
        ctx = {
            "subscription_id": it.get("subscription_id"),
            "route_id": it.get("route_id"),
            "channel_id": it.get("channel_id"),
            "event_id": it.get("event_id"),
            "status": it.get("status"),
            "reason": it.get("reason"),
            "info": it.get("info"),
            "out_path": it.get("out_path"),
        }
        log_write(
            base_dir,
            "info",
            "alerts",
            "m10.channels",
            "channel_attempt",
            ctx,
        )
        n += 1
    return n


def ingest_audit_requests(path: Path, base_dir: Path) -> int:
    data = _read_json(path)
    if not isinstance(data, list):
        return 0
    n = 0
    for it in data:
        if not isinstance(it, dict):
            continue
        ctx = {
            "request_id": it.get("request_id"),
            "user": it.get("user"),
            "role": it.get("role"),
            "asset_id": it.get("asset_id"),
            "bundle_id": it.get("bundle_id"),
            "status": it.get("status"),
        }
        log_write(
            base_dir,
            "info",
            "portals",
            "m11.regulator",
            "audit_request",
            ctx,
        )
        n += 1
    return n


# -------------------------- tiny /metrics server --------------------------


class _MetricsHandler(http.server.SimpleHTTPRequestHandler):
    """Serves a single textfile at /metrics. DEV ONLY."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.metrics_file = kwargs.pop("metrics_file")
        super().__init__(*args, **kwargs)

    def do_GET(self) -> None:  # noqa: N802 (http method name)
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"not found")
            return
        try:
            txt = Path(self.metrics_file).read_text(encoding="utf-8")
        except Exception:  # noqa: BLE001
            txt = "# no metrics\n"
        payload = txt.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def serve_metrics(metrics_file: Path, port: int) -> None:
    """Blocking server; Ctrl+C to stop."""
    handler = lambda *a, **kw: _MetricsHandler(  # noqa: E731
        *a, metrics_file=str(metrics_file), **kw
    )
    with socketserver.TCPServer(("", int(port)), handler) as httpd:
        print(f"Serving /metrics from {metrics_file} on 0.0.0.0:{port}")
        httpd.serve_forever()
