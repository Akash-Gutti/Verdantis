from modules.m10.m10_1_filters import SEVERITY_RANK, AlertFilter, apply_filters


def test_apply_filters_basic():
    events = [
        {
            "id": "e1",
            "topic": "policy.enforcement",
            "severity": "high",
            "asset_id": "a1",
            "rule_type": "emissions_exceedance",
        },
        {
            "id": "e2",
            "topic": "sat.change",
            "severity": "low",
            "asset_id": "a1",
            "aoi_id": "aoi_2",
            "delta": {"ndvi": 0.15},
        },
        {"id": "e3", "topic": "zk.verify", "severity": "low", "asset_id": "a2"},
    ]
    subs = [
        AlertFilter(
            id="policy_high_plus",
            topics=["policy.enforcement"],
            severity_at_least="high",
            assets=["*"],
            rule_types=["emissions_exceedance"],
        ),
        AlertFilter(
            id="sat_ndvi_drop",
            topics=["sat.change"],
            severity_at_least="medium",
            aoi_ids=["aoi_2"],
            min_delta={"ndvi": 0.2},
        ),
        AlertFilter(
            id="zk_attest_issues",
            topics=["zk.issue", "zk.verify"],
            severity_at_least="low",
            assets=["*"],
        ),
    ]

    matched, metrics = apply_filters(events, subs)
    # e1 (policy high) matches first; e2 fails ndvi >= 0.2; e3 matches zk
    assert len(matched) == 2
    assert metrics["unmatched"] == 1
    ids = sorted([m["event"]["id"] for m in matched])
    assert ids == ["e1", "e3"]
    # severity ladder is consistent
    assert SEVERITY_RANK["high"] > SEVERITY_RANK["medium"]
