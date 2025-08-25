"""M12.3 - Evaluation harness for RAG veracity, causal fit, change detection."""

from __future__ import annotations

import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# ------------------------ utils ------------------------


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _safe_list(val: Any) -> List[Any]:
    return val if isinstance(val, list) else []


# ------------------------ RAG veracity ------------------------


@dataclass(frozen=True)
class RagEvalResult:
    items: int
    cite_precision_micro: float
    cite_recall_micro: float
    cite_f1_micro: float
    cite_precision_macro: float
    cite_recall_macro: float
    cite_f1_macro: float
    nli_pairs: int
    nli_accuracy: float


def _prf(p: float, r: float) -> float:
    if p + r == 0:
        return 0.0
    return 2 * p * r / (p + r)


def evaluate_rag(input_path: Path, out_path: Path) -> RagEvalResult:
    """Input schema: list of items
    {
      "id": "...",
      "citations_pred": ["s1","s2"],
      "citations_gold": ["s1","s3"],
      "nli": [{"gold": "entails", "pred": "entails"}, ...]
    }
    """
    data = _read_json(input_path)
    items = _safe_list(data)

    inter_sum = 0
    pred_sum = 0
    gold_sum = 0

    macro_precisions: List[float] = []
    macro_recalls: List[float] = []
    macro_f1s: List[float] = []

    nli_total = 0
    nli_ok = 0

    for it in items:
        pred = set(_safe_list(it.get("citations_pred")))
        gold = set(_safe_list(it.get("citations_gold")))
        inter = len(pred & gold)

        p_item = inter / len(pred) if pred else (1.0 if not gold else 0.0)
        r_item = inter / len(gold) if gold else (1.0 if not pred else 0.0)
        f_item = _prf(p_item, r_item)

        macro_precisions.append(p_item)
        macro_recalls.append(r_item)
        macro_f1s.append(f_item)

        inter_sum += inter
        pred_sum += len(pred)
        gold_sum += len(gold)

        for pair in _safe_list(it.get("nli")):
            gold_lbl = str(pair.get("gold", "")).lower()
            pred_lbl = str(pair.get("pred", "")).lower()
            if gold_lbl in {"entails", "neutral", "contradicts"}:
                nli_total += 1
                if gold_lbl == pred_lbl:
                    nli_ok += 1

    p_micro = inter_sum / pred_sum if pred_sum else 1.0
    r_micro = inter_sum / gold_sum if gold_sum else 1.0
    f_micro = _prf(p_micro, r_micro)

    p_macro = float(statistics.fmean(macro_precisions)) if macro_precisions else 0.0
    r_macro = float(statistics.fmean(macro_recalls)) if macro_recalls else 0.0
    f_macro = float(statistics.fmean(macro_f1s)) if macro_f1s else 0.0

    nli_acc = (nli_ok / nli_total) if nli_total else 1.0

    res = RagEvalResult(
        items=len(items),
        cite_precision_micro=round(p_micro, 4),
        cite_recall_micro=round(r_micro, 4),
        cite_f1_micro=round(f_micro, 4),
        cite_precision_macro=round(p_macro, 4),
        cite_recall_macro=round(r_macro, 4),
        cite_f1_macro=round(f_macro, 4),
        nli_pairs=nli_total,
        nli_accuracy=round(nli_acc, 4),
    )
    _write_json(out_path, res.__dict__)
    return res


# ------------------------ Causal fit ------------------------


@dataclass(frozen=True)
class CausalEvalResult:
    assets: int
    rmse_pre_mean: float
    rmse_post_mean: float
    delta_mean: float
    placebo_delta_mean: float
    improved_frac: float


def _rmse(y_true: List[float], y_pred: List[float]) -> float:
    n = min(len(y_true), len(y_pred))
    if n == 0:
        return float("nan")
    s = 0.0
    for i in range(n):
        try:
            diff = float(y_true[i]) - float(y_pred[i])
        except Exception:  # noqa: BLE001
            diff = 0.0
        s += diff * diff
    return math.sqrt(s / n)


def evaluate_causal(input_path: Path, out_path: Path) -> CausalEvalResult:
    """Input schema: list of assets
    {
      "asset_id": "...",
      "pre": {"y_true": [...], "y_pred": [...]},
      "post": {"y_true": [...], "y_pred": [...]},
      "placebo": {"y_true": [...], "y_pred": [...]}  # optional
    }
    """
    data = _read_json(input_path)
    rows = _safe_list(data)

    rmse_pre: List[float] = []
    rmse_post: List[float] = []
    deltas: List[float] = []
    placebo_deltas: List[float] = []
    improved = 0
    total = 0

    for it in rows:
        pre = it.get("pre") or {}
        post = it.get("post") or {}
        r_pre = _rmse(_safe_list(pre.get("y_true")), _safe_list(pre.get("y_pred")))
        r_post = _rmse(_safe_list(post.get("y_true")), _safe_list(post.get("y_pred")))
        if not math.isnan(r_pre) and not math.isnan(r_post):
            total += 1
            rmse_pre.append(r_pre)
            rmse_post.append(r_post)
            deltas.append(r_pre - r_post)
            if r_post < r_pre:
                improved += 1
        plc = it.get("placebo") or {}
        if plc:
            r_plc_pre = _rmse(_safe_list(plc.get("y_true")), _safe_list(plc.get("y_pred")))
            r_plc_post = r_plc_pre  # placebo: assume no change; dataset can reflect otherwise
            placebo_deltas.append(r_plc_pre - r_plc_post)

    def _mean(xs: List[float]) -> float:
        xs2 = [x for x in xs if not math.isnan(x)]
        return float(statistics.fmean(xs2)) if xs2 else float("nan")

    res = CausalEvalResult(
        assets=total,
        rmse_pre_mean=round(_mean(rmse_pre), 4),
        rmse_post_mean=round(_mean(rmse_post), 4),
        delta_mean=round(_mean(deltas), 4),
        placebo_delta_mean=round(_mean(placebo_deltas), 4),
        improved_frac=round((improved / total) if total else 0.0, 4),
    )
    _write_json(out_path, res.__dict__)
    return res


# ------------------------ Change detection ------------------------


@dataclass(frozen=True)
class ChangeEvalResult:
    items: int
    precision_at_k: Dict[str, float]
    precision_at_frac: Dict[str, float]


def _precision_at_k(labels_sorted: List[int], k: int) -> float:
    if k <= 0:
        return 0.0
    k_eff = min(k, len(labels_sorted))
    if k_eff == 0:
        return 0.0
    top = labels_sorted[:k_eff]
    return sum(1 for v in top if int(v) == 1) / float(k_eff)


def _precision_at_frac(labels_sorted: List[int], frac: float) -> float:
    if frac <= 0:
        return 0.0
    k = max(1, int(round(frac * len(labels_sorted))))
    return _precision_at_k(labels_sorted, k)


def evaluate_change(
    input_path: Path,
    out_path: Path,
    k_list: Optional[List[int]] = None,
    frac_list: Optional[List[float]] = None,
) -> ChangeEvalResult:
    """Input schema: list of items {"tile_id": "...", "score": float, "label": 0/1}"""
    k_list = k_list or [1, 5, 10, 20]
    frac_list = frac_list or [0.1, 0.2]

    data = _read_json(input_path)
    rows = _safe_list(data)
    # Sort by score descending
    rows.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    labels = [int(r.get("label", 0)) for r in rows]

    p_at_k: Dict[str, float] = {}
    for k in k_list:
        p_at_k[str(k)] = round(_precision_at_k(labels, int(k)), 4)

    p_at_frac: Dict[str, float] = {}
    for frac in frac_list:
        key = f"{frac:.2f}"
        p_at_frac[key] = round(_precision_at_frac(labels, float(frac)), 4)

    res = ChangeEvalResult(items=len(rows), precision_at_k=p_at_k, precision_at_frac=p_at_frac)
    _write_json(
        out_path,
        {
            "items": res.items,
            "precision_at_k": res.precision_at_k,
            "precision_at_frac": res.precision_at_frac,
        },
    )
    return res
