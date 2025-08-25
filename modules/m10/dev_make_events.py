"""Generate a small set of sample events for M10.1 local testing."""

from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    out = Path("data/raw/m10/sample_events.json")
    out.parent.mkdir(parents=True, exist_ok=True)

    events = [
        {
            "id": "e1",
            "ts": "2025-08-25T09:05:00Z",
            "topic": "policy.enforcement",
            "asset_id": "asset_1",
            "rule_type": "emissions_exceedance",
            "severity": "high",
            "acknowledged": False,
            "payload": {"co2_tonnes": 14.2},
        },
        {
            "id": "e2",
            "ts": "2025-08-25T09:06:00Z",
            "topic": "policy.enforcement",
            "asset_id": "asset_2",
            "rule_type": "water_breach",
            "severity": "medium",
            "acknowledged": False,
            "payload": {"ph": 9.1},
        },
        {
            "id": "e3",
            "ts": "2025-08-25T09:07:00Z",
            "topic": "sat.change",
            "aoi_id": "aoi_2",
            "asset_id": "asset_3",
            "severity": "medium",
            "delta": {"ndvi": 0.27},
            "payload": {"note": "sudden vegetation decrease"},
        },
        {
            "id": "e4",
            "ts": "2025-08-25T09:08:00Z",
            "topic": "sat.change",
            "aoi_id": "aoi_9",
            "asset_id": "asset_4",
            "severity": "low",
            "delta": {"ndvi": 0.1},
            "payload": {"note": "minor change"},
        },
        {
            "id": "e5",
            "ts": "2025-08-25T09:09:00Z",
            "topic": "zk.verify",
            "asset_id": "asset_1",
            "severity": "low",
            "payload": {"bundle_id": "abc123", "verified": True},
        },
        {
            "id": "e6",
            "ts": "2025-08-25T09:10:00Z",
            "topic": "policy.enforcement",
            "asset_id": "asset_1",
            "rule_type": "emissions_exceedance",
            "severity": "high",
            "acknowledged": True,
            "payload": {"co2_tonnes": 15.9},
        },
    ]

    with out.open("w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)

    print(f"✓ Wrote sample events → {out}")


if __name__ == "__main__":
    main()
