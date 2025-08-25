"""Generate small sample eval datasets for M12.3."""

from __future__ import annotations

import json
import random
from pathlib import Path


def _write(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def make_rag(path: Path) -> None:
    data = [
        {
            "id": "q1",
            "citations_pred": ["s1", "s2"],
            "citations_gold": ["s1", "s3"],
            "nli": [
                {"gold": "entails", "pred": "entails"},
                {"gold": "contradicts", "pred": "neutral"},
            ],
        },
        {
            "id": "q2",
            "citations_pred": ["s4"],
            "citations_gold": ["s5"],
            "nli": [{"gold": "neutral", "pred": "neutral"}],
        },
    ]
    _write(path, data)


def make_causal(path: Path) -> None:
    rng = random.Random(42)
    rows = []
    for asset in ("asset_1", "asset_2", "asset_3"):
        pre_true = [rng.uniform(0, 1) for _ in range(20)]
        pre_pred = [v + rng.uniform(-0.2, 0.2) for v in pre_true]
        post_true = [rng.uniform(0, 1) for _ in range(20)]
        post_pred = [v + rng.uniform(-0.1, 0.1) for v in post_true]
        placebo_true = [rng.uniform(0, 1) for _ in range(20)]
        placebo_pred = [v + rng.uniform(-0.01, 0.01) for v in placebo_true]
        rows.append(
            {
                "asset_id": asset,
                "pre": {"y_true": pre_true, "y_pred": pre_pred},
                "post": {"y_true": post_true, "y_pred": post_pred},
                "placebo": {"y_true": placebo_true, "y_pred": placebo_pred},
            }
        )
    _write(path, rows)


def make_change(path: Path) -> None:
    # 50 tiles with some positives ranked higher on average
    rng = random.Random(7)
    rows = []
    for i in range(50):
        label = 1 if i < 15 else 0  # 15 positives
        score = rng.uniform(0.6, 1.0) if label == 1 else rng.uniform(0.0, 0.8)
        rows.append({"tile_id": f"t{i+1}", "score": round(score, 3), "label": label})
    _write(path, rows)


def main() -> None:
    rag = Path("data/eval/rag_eval.json")
    causal = Path("data/eval/causal_eval.json")
    change = Path("data/eval/change_eval.json")
    make_rag(rag)
    make_causal(causal)
    make_change(change)
    print("✓ Sample eval data written under data/eval/")


if __name__ == "__main__":
    main()
