"""M10.2 - Channels router (webhook/email stubs) with simple rate limits.

Reads matched events (output of M10.1) and routes them to configured channels.
Channels are stubbed: they write JSON payloads to outbox folders.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Keep severity ladder consistent with M10.1
SEVERITY_RANK: Dict[str, int] = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


@dataclass(frozen=True)
class RouteMatch:
    subscription_ids: Optional[List[str]] = None
    topics: Optional[List[str]] = None
    severity_at_least: Optional[str] = None


@dataclass(frozen=True)
class ChannelCfg:
    type: str  # "webhook" | "email"
    id: str
    outbox_dir: str
    to: Optional[List[str]] = None
    subject_prefix: Optional[str] = None
    max_per_run: Optional[int] = None  # simple limiter for this run


@dataclass(frozen=True)
class RouteCfg:
    id: str
    match: RouteMatch
    channels: List[ChannelCfg]


@dataclass
class GlobalLimits:
    max_per_run: Optional[int] = None


def _now_utc_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_config(path: Path) -> Tuple[List[RouteCfg], GlobalLimits]:
    """Load channels config file."""
    with path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    routes: List[RouteCfg] = []
    for r in cfg.get("routes", []):
        match_dict = r.get("match", {})
        match = RouteMatch(
            subscription_ids=match_dict.get("subscription_ids"),
            topics=match_dict.get("topics"),
            severity_at_least=match_dict.get("severity_at_least"),
        )
        chans = []
        for c in r.get("channels", []):
            chans.append(
                ChannelCfg(
                    type=str(c["type"]),
                    id=str(c["id"]),
                    outbox_dir=str(c["outbox_dir"]),
                    to=c.get("to"),
                    subject_prefix=c.get("subject_prefix"),
                    max_per_run=c.get("max_per_run"),
                )
            )
        routes.append(RouteCfg(id=str(r["id"]), match=match, channels=chans))

    gl = cfg.get("rate_limit", {})
    limits = GlobalLimits(max_per_run=gl.get("max_per_run"))
    return routes, limits


def load_matched_events(path: Path) -> List[Dict[str, Any]]:
    """Load matched events list from M10.1 output.

    Each item: {"subscription_id": "...", "event": {...}}.
    """
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Matched events JSON must be a list.")
    good = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if "subscription_id" in item and "event" in item:
            good.append(item)
    return good


def _route_matches(route: RouteCfg, subscription_id: str, event: Dict[str, Any]) -> bool:
    m = route.match
    if m.subscription_ids and subscription_id not in set(m.subscription_ids):
        return False
    if m.topics and event.get("topic") not in set(m.topics):
        return False
    if m.severity_at_least:
        ev_sev = event.get("severity", "info")
        if SEVERITY_RANK.get(ev_sev, 0) < SEVERITY_RANK.get(m.severity_at_least, 0):
            return False
    return True


def _format_subject(prefix: Optional[str], subscription_id: str, event: Dict[str, Any]) -> str:
    topic = str(event.get("topic", "event"))
    sev = str(event.get("severity", "info")).upper()
    base = f"[{sev}] {topic} via {subscription_id}"
    if prefix:
        return f"{prefix} {base}"
    return base


def _safe_event_id(event: Dict[str, Any], idx: int) -> str:
    ev_id = event.get("id")
    if isinstance(ev_id, str) and ev_id:
        return ev_id
    return f"ev_{idx}"


def _write_json(path: Path, obj: Any) -> None:
    _ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _send_webhook(
    cfg: ChannelCfg, subscription_id: str, event: Dict[str, Any], idx: int
) -> Tuple[bool, str, Path]:
    """Stub webhook: write a POST-like payload to outbox."""
    payload = {
        "channel_id": cfg.id,
        "type": "webhook",
        "ts": _now_utc_iso(),
        "subscription_id": subscription_id,
        "event": event,
        "meta": {"note": "stub webhook - no network call"},
    }
    fname = f"{_safe_event_id(event, idx)}__{subscription_id}.json"
    fpath = Path(cfg.outbox_dir) / fname
    _write_json(fpath, payload)
    return True, "written", fpath


def _send_email(
    cfg: ChannelCfg, subscription_id: str, event: Dict[str, Any], idx: int
) -> Tuple[bool, str, Path]:
    """Stub email: write an email-like JSON to outbox."""
    email_obj = {
        "channel_id": cfg.id,
        "type": "email",
        "ts": _now_utc_iso(),
        "to": cfg.to or [],
        "subject": _format_subject(cfg.subject_prefix, subscription_id, event),
        "body": {
            "headline": f"Alert from {subscription_id}",
            "summary": {
                "topic": event.get("topic"),
                "asset_id": event.get("asset_id"),
                "aoi_id": event.get("aoi_id"),
                "severity": event.get("severity"),
                "rule_type": event.get("rule_type"),
            },
            "event": event,
        },
    }
    fname = f"{_safe_event_id(event, idx)}__{subscription_id}.json"
    fpath = Path(cfg.outbox_dir) / fname
    _write_json(fpath, email_obj)
    return True, "written", fpath


def run_channels_cli(
    matched_path: Path,
    cfg_path: Path,
    results_path: Path,
    metrics_path: Path,
) -> Tuple[int, int]:
    """Route matched events to channels per config.

    Returns:
        sent_count: successful sends
        skipped_count: skipped (no route, rate-limited, or unknown channel)
    """
    routes, gl = load_config(cfg_path)
    matched = load_matched_events(matched_path)

    sent = 0
    skipped = 0
    global_used = 0

    per_channel_sent: Dict[str, int] = {}
    per_channel_skipped: Dict[str, int] = {}
    details: List[Dict[str, Any]] = []

    # Track per-channel counters for max_per_run
    per_chan_limit_used: Dict[str, int] = {}

    for idx, rec in enumerate(matched):
        sub_id = str(rec.get("subscription_id"))
        event = rec.get("event") or {}

        # Which routes match this (sub_id, event)?
        matched_routes = [rt for rt in routes if _route_matches(rt, sub_id, event)]
        if not matched_routes:
            details.append(
                {
                    "subscription_id": sub_id,
                    "event_id": _safe_event_id(event, idx),
                    "status": "skipped",
                    "reason": "no_route",
                }
            )
            skipped += 1
            continue

        for rt in matched_routes:
            for chan in rt.channels:
                # Global max_per_run limiter
                if gl.max_per_run is not None and global_used >= int(gl.max_per_run):
                    per_channel_skipped[chan.id] = per_channel_skipped.get(chan.id, 0) + 1
                    details.append(
                        {
                            "subscription_id": sub_id,
                            "route_id": rt.id,
                            "channel_id": chan.id,
                            "event_id": _safe_event_id(event, idx),
                            "status": "skipped",
                            "reason": "global_rate_limited",
                        }
                    )
                    skipped += 1
                    continue

                # Per-channel limiter
                used_for_chan = per_chan_limit_used.get(chan.id, 0)
                if chan.max_per_run is not None and used_for_chan >= int(chan.max_per_run):
                    per_channel_skipped[chan.id] = per_channel_skipped.get(chan.id, 0) + 1
                    details.append(
                        {
                            "subscription_id": sub_id,
                            "route_id": rt.id,
                            "channel_id": chan.id,
                            "event_id": _safe_event_id(event, idx),
                            "status": "skipped",
                            "reason": "channel_rate_limited",
                        }
                    )
                    skipped += 1
                    continue

                # Send via stub
                if chan.type == "webhook":
                    ok, info, path = _send_webhook(chan, sub_id, event, idx)
                elif chan.type == "email":
                    ok, info, path = _send_email(chan, sub_id, event, idx)
                else:
                    ok, info, path = False, f"unknown_channel_type:{chan.type}", Path("")

                if ok:
                    sent += 1
                    global_used += 1
                    per_chan_limit_used[chan.id] = used_for_chan + 1
                    per_channel_sent[chan.id] = per_channel_sent.get(chan.id, 0) + 1
                    details.append(
                        {
                            "subscription_id": sub_id,
                            "route_id": rt.id,
                            "channel_id": chan.id,
                            "event_id": _safe_event_id(event, idx),
                            "status": "sent",
                            "info": info,
                            "out_path": str(path),
                        }
                    )
                else:
                    skipped += 1
                    per_channel_skipped[chan.id] = per_channel_skipped.get(chan.id, 0) + 1
                    details.append(
                        {
                            "subscription_id": sub_id,
                            "route_id": rt.id,
                            "channel_id": chan.id,
                            "event_id": _safe_event_id(event, idx),
                            "status": "skipped",
                            "reason": info,
                        }
                    )

    metrics = {
        "sent": sent,
        "skipped": skipped,
        "per_channel_sent": per_channel_sent,
        "per_channel_skipped": per_channel_skipped,
        "global_limit_max_per_run": gl.max_per_run,
    }

    _write_json(results_path, details)
    _write_json(metrics_path, metrics)
    return sent, skipped
