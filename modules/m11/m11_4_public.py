"""M11.4 - Public portal builder (masked, high-level).

- Builds a sanitized public feed and region/severity aggregates.
- Token-gated at CLI layer (role='public').
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SEVERITY_RANK: Dict[str, int] = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


@dataclass(frozen=True)
class PublicPolicy:
    min_severity: str
    max_items: int
    visible_fields: List[str]
    anonymize_asset_id: bool
    include_asset_id_field: bool
    asset_pseudonym_prefix: str
    coords_round_decimals: int


@dataclass(frozen=True)
class Regionalization:
    aoi_to_region: Dict[str, str]
    fallback_region: str


@dataclass(frozen=True)
class PublicConfig:
    policy: PublicPolicy
    regionalization: Regionalization


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _load_config(path: Path) -> PublicConfig:
    raw = _read_json(path)
    pol = raw.get("policy", {})
    reg = raw.get("regionalization", {})
    policy = PublicPolicy(
        min_severity=str(pol.get("min_severity", "medium")),
        max_items=int(pol.get("max_items", 200)),
        visible_fields=list(
            pol.get("visible_fields", ["ts", "topic", "severity", "aoi_id", "region"])
        ),
        anonymize_asset_id=bool(pol.get("anonymize_asset_id", True)),
        include_asset_id_field=bool(pol.get("include_asset_id_field", False)),
        asset_pseudonym_prefix=str(pol.get("asset_pseudonym_prefix", "asset_")),
        coords_round_decimals=int(pol.get("coords_round_decimals", 0)),
    )
    regionalization = Regionalization(
        aoi_to_region=dict(reg.get("aoi_to_region", {})),
        fallback_region=str(reg.get("fallback_region", "Unknown")),
    )
    return PublicConfig(policy=policy, regionalization=regionalization)


def _parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts or not isinstance(ts, str):
        return None
    s = ts[:-1] + "+00:00" if ts.endswith("Z") else ts
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def _severity_at_least(ev_sev: str, min_sev: str) -> bool:
    return SEVERITY_RANK.get(ev_sev, 0) >= SEVERITY_RANK.get(min_sev, 0)


def _mask_asset(asset_id: Optional[str]) -> Optional[str]:
    if not asset_id:
        return None
    secret = os.getenv("PUBLIC_MASK_SECRET", "public-dev-secret")
    digest = hmac.new(secret.encode("utf-8"), asset_id.encode("utf-8"), hashlib.sha256).digest()
    pseudo = base64.urlsafe_b64encode(digest[:8]).decode("ascii").rstrip("=")
    return pseudo


def _to_region(aoi_id: Optional[str], regionalization: Regionalization) -> str:
    if not aoi_id:
        return regionalization.fallback_region
    return regionalization.aoi_to_region.get(str(aoi_id), regionalization.fallback_region)


def _sanitize_item(
    rec: Dict[str, Any],
    cfg: PublicConfig,
) -> Dict[str, Any]:
    """Return a sanitized, public-safe dict."""
    ev = rec.get("event", {})
    aoi_id = ev.get("aoi_id")
    region = _to_region(aoi_id, cfg.regionalization)
    item: Dict[str, Any] = {
        "ts": (_parse_iso(ev.get("ts")) or dt.datetime.now(tz=dt.timezone.utc)).isoformat(),
        "topic": ev.get("topic"),
        "severity": ev.get("severity"),
        "aoi_id": aoi_id,
        "region": region,
    }

    # Optional asset pseudonym
    if cfg.policy.include_asset_id_field:
        if cfg.policy.anonymize_asset_id:
            asset_p = _mask_asset(str(ev.get("asset_id") or ""))
            item["asset_id"] = f"{cfg.policy.asset_pseudonym_prefix}{asset_p}" if asset_p else None
        else:
            item["asset_id"] = ev.get("asset_id")

    # Drop any fields not in visible_fields
    keep = set(cfg.policy.visible_fields)
    item = {k: v for k, v in item.items() if k in keep}

    return item


def _load_deduped(path: Path) -> List[Dict[str, Any]]:
    data = _read_json(path)
    if not isinstance(data, list):
        raise ValueError("Deduped events JSON must be a list.")
    out: List[Dict[str, Any]] = []
    for it in data:
        if isinstance(it, dict) and "event" in it:
            out.append(it)
    return out


def run_public_build(
    deduped_path: Path,
    cfg_path: Path,
    out_dir: Path,
) -> Tuple[int, int]:
    """Build public feed + scores. Returns (feed_items, regions)."""
    cfg = _load_config(cfg_path)
    deduped = _load_deduped(deduped_path)

    # Filter by severity floor
    filtered: List[Dict[str, Any]] = []
    for rec in deduped:
        sev = str(rec.get("event", {}).get("severity", "info"))
        if _severity_at_least(sev, cfg.policy.min_severity):
            filtered.append(rec)

    # Sanitize & sort
    feed_items = [_sanitize_item(r, cfg) for r in filtered]
    feed_items.sort(key=lambda x: x.get("ts", ""), reverse=True)
    if cfg.policy.max_items > 0:
        feed_items = feed_items[: cfg.policy.max_items]

    # Aggregate scores by region & severity
    by_region: Dict[str, Dict[str, int]] = {}
    for it in feed_items:
        region = str(it.get("region", "Unknown"))
        sev = str(it.get("severity", "info"))
        if region not in by_region:
            by_region[region] = {}
        by_region[region][sev] = by_region[region].get(sev, 0) + 1

    # Write outputs
    _write_json(out_dir / "public_feed.json", feed_items)
    _write_json(out_dir / "public_scores.json", by_region)
    _write_json(
        out_dir / "metrics.json",
        {
            "built_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "feed_items": len(feed_items),
            "regions": len(by_region),
            "config": {
                "min_severity": cfg.policy.min_severity,
                "max_items": cfg.policy.max_items,
                "visible_fields": cfg.policy.visible_fields,
                "anonymize_asset_id": cfg.policy.anonymize_asset_id,
                "include_asset_id_field": cfg.policy.include_asset_id_field,
            },
            "source": str(deduped_path),
            "policy_path": str(cfg_path),
        },
    )
    return len(feed_items), len(by_region)
