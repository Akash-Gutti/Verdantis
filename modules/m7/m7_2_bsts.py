"""M7.2 BSTS-like causal effect via UnobservedComponents (robust)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from math import sqrt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("pip install matplotlib") from exc

try:
    from statsmodels.tsa.statespace.structural import UnobservedComponents
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("pip install statsmodels") from exc

EFFECTS_DIR = Path("data/processed/causal/effects")
PLOTS_DIR = Path("data/processed/causal/plots")
OUT_SUMMARY = Path("data/processed/causal/effects_summary.json")


@dataclass
class BstsConfig:
    metric: str = "energy_kwh"
    seasonal_period: int = 7
    alpha: float = 0.05
    min_pre_days: int = 30
    asset_filter: Optional[List[str]] = None


def _ensure_dirs() -> None:
    EFFECTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def _load_inputs() -> Tuple[pd.DataFrame, pd.DataFrame]:
    ts_pq = Path("data/processed/causal/ts_daily.parquet")
    ts_csv = Path("data/processed/causal/ts_daily.csv")
    if ts_pq.exists():
        ts = pd.read_parquet(ts_pq)
    elif ts_csv.exists():
        ts = pd.read_csv(ts_csv)
    else:
        raise FileNotFoundError("Missing ts_daily (run m7 prep).")
    ts["date"] = pd.to_datetime(ts["date"])

    pol = pd.read_csv(Path("data/processed/causal/policy_table.csv"))
    return ts, pol


def _load_config(path: str) -> Dict:
    p = Path(path)
    if not p.exists():
        return {"defaults": {"policy_date": None}}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _build_design_matrix(df: pd.DataFrame) -> pd.DataFrame:
    x = pd.DataFrame(index=df.index)
    if "temp_c" in df.columns:
        x["temp_c"] = pd.to_numeric(df["temp_c"], errors="coerce")
    if "weekday" in df.columns:
        w = pd.get_dummies(df["weekday"].astype(int), prefix="wd")
        if w.shape[1] >= 2:
            w = w.drop(columns=w.columns[-1])
        x = pd.concat([x, w], axis=1)
    if x.empty:
        x["const_exog"] = 1.0
    return x.ffill().bfill().fillna(0.0)


def _ucm_forecast(
    y_pre: pd.Series,
    x_pre: pd.DataFrame,
    x_full: pd.DataFrame,
    seasonal_period: int,
    alpha: float,
) -> Tuple[pd.Series, pd.DataFrame]:
    model = UnobservedComponents(
        endog=y_pre,
        level="local level",
        seasonal=seasonal_period if seasonal_period and seasonal_period > 1 else None,
        exog=x_pre,
    )
    res = model.fit(disp=False)
    pred = res.get_prediction(
        start=x_full.index[0],
        end=x_full.index[-1],
        exog=x_full,
    )
    yhat = pred.predicted_mean
    try:
        ci = pred.conf_int(alpha=alpha).rename(columns={0: "lower", 1: "upper"})
    except Exception:
        ci = pd.DataFrame({"lower": yhat * np.nan, "upper": yhat * np.nan}, index=yhat.index)
    return yhat, ci


def _p_value_from_cumulative(cum_eff: float, var_sum: float) -> float:
    if not np.isfinite(var_sum) or var_sum <= 0.0:
        return float("nan")
    z = float(cum_eff) / float(sqrt(var_sum))
    return 2.0 * (1.0 - float(norm.cdf(abs(z))))


def _variance_from_ci(ci: pd.DataFrame, alpha: float) -> pd.Series:
    if "lower" not in ci.columns or "upper" not in ci.columns:
        return pd.Series(np.nan, index=ci.index)
    z = float(norm.ppf(1.0 - alpha / 2.0))
    half = (ci["upper"] - ci["lower"]) / 2.0
    return (half / z) ** 2


def _fallback_counterfactual(
    y: pd.Series, pre_mask: pd.Series
) -> Tuple[pd.Series, pd.DataFrame, bool]:
    mu = float(y.loc[pre_mask].mean())
    yhat = pd.Series(mu, index=y.index)
    ci = pd.DataFrame({"lower": yhat * np.nan, "upper": yhat * np.nan}, index=y.index)
    return yhat, ci, True


def _plot_asset(
    asset_id: str,
    metric: str,
    y: pd.Series,
    yhat: pd.Series,
    ci: pd.DataFrame,
    policy_date: pd.Timestamp,
) -> None:
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(y.index, y.values, label="Observed")
    ax.plot(yhat.index, yhat.values, label="Counterfactual")
    if "lower" in ci.columns and "upper" in ci.columns:
        ax.fill_between(ci.index, ci["lower"], ci["upper"], alpha=0.2, label="95% CI")
    ax.axvline(policy_date, linestyle="--", label="Policy start")
    ax.set_title(f"{asset_id} – {metric}")
    ax.set_xlabel("Date")
    ax.set_ylabel(metric)
    ax.legend(loc="best")
    out = PLOTS_DIR / f"{asset_id}_{metric}.png"
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)


def _pick_policy_date_for_asset(
    df_a: pd.DataFrame,
    pol: pd.DataFrame,
    aid: str,
    default_policy_date: Optional[str],
) -> Tuple[Optional[pd.Timestamp], bool]:
    row = pol.loc[pol["asset_id"].astype(str) == str(aid)]
    if not row.empty:
        pdate = pd.to_datetime(row["policy_start_date"].iloc[0], errors="coerce")
        if pd.notna(pdate):
            return pdate, False
    if default_policy_date:
        pdate = pd.to_datetime(default_policy_date, errors="coerce")
        if pd.notna(pdate):
            return pdate, True
    dates = pd.to_datetime(df_a["date"], errors="coerce").dropna().sort_values()
    if dates.empty:
        return None, True
    return dates.iloc[len(dates) // 2], True


def _process_asset(
    df_asset: pd.DataFrame,
    policy_date: pd.Timestamp,
    cfg: BstsConfig,
) -> Dict:
    df = df_asset.sort_values("date").copy()
    df = df.set_index("date")

    metric = cfg.metric
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not in data columns.")
    y_all = pd.to_numeric(df[metric], errors="coerce").ffill()
    x_all = _build_design_matrix(df)

    pre_mask = y_all.index < policy_date
    post_mask = y_all.index >= policy_date
    if int(pre_mask.sum()) < cfg.min_pre_days:
        return {
            "asset_id": str(df_asset["asset_id"].iloc[0]),
            "status": "skipped_insufficient_pre",
            "pre_days": int(pre_mask.sum()),
            "post_days": int(post_mask.sum()),
        }

    y_pre = y_all.loc[pre_mask]
    x_pre = x_all.loc[pre_mask]
    try:
        yhat, ci = _ucm_forecast(
            y_pre=y_pre,
            x_pre=x_pre,
            x_full=x_all,
            seasonal_period=cfg.seasonal_period,
            alpha=cfg.alpha,
        )
        used_fallback = False
    except Exception:
        yhat, ci, used_fallback = _fallback_counterfactual(y_all, pre_mask)

    df_out = pd.DataFrame(
        {"y": y_all, "yhat": yhat, "lower": ci.get("lower"), "upper": ci.get("upper")}
    )
    df_out["effect"] = df_out["y"] - df_out["yhat"]

    var_series = _variance_from_ci(ci, alpha=cfg.alpha).reindex(df_out.index)
    var_sum = float(var_series.loc[post_mask].sum(skipna=True))

    cum_eff = float(df_out.loc[post_mask, "effect"].sum())
    avg_eff = float(df_out.loc[post_mask, "effect"].mean())
    pval = _p_value_from_cumulative(cum_eff, var_sum)

    _ensure_dirs()
    asset_id = str(df_asset["asset_id"].iloc[0])
    csv_path = EFFECTS_DIR / f"{asset_id}_{metric}.csv"
    df_out.reset_index().rename(columns={"date": "timestamp"}).to_csv(csv_path, index=False)
    _plot_asset(asset_id, metric, y_all, yhat, ci, policy_date)

    return {
        "asset_id": asset_id,
        "metric": metric,
        "policy_date": str(policy_date.date()),
        "pre_days": int(pre_mask.sum()),
        "post_days": int(post_mask.sum()),
        "avg_effect": avg_eff,
        "cum_effect": cum_eff,
        "p_value": pval,
        "used_fallback": used_fallback,
        "csv_path": str(csv_path.as_posix()),
        "plot_path": str((PLOTS_DIR / f"{asset_id}_{metric}.png").as_posix()),
    }


def run_m7_2(
    metric: str = "energy_kwh",
    seasonal_period: int = 7,
    alpha: float = 0.05,
    min_pre_days: int = 30,
    asset: Optional[str] = None,
    config_path: str = "configs/m7_causal.json",
) -> Dict:
    ts, pol = _load_inputs()
    cfg_json = _load_config(config_path)
    default_policy_date = cfg_json.get("defaults", {}).get("policy_date")

    cfg = BstsConfig(
        metric=metric,
        seasonal_period=seasonal_period,
        alpha=alpha,
        min_pre_days=min_pre_days,
        asset_filter=[asset] if asset else None,
    )

    pol["policy_start_date"] = pd.to_datetime(pol.get("policy_start_date"), errors="coerce")
    assets = [asset] if asset else sorted(ts["asset_id"].astype(str).dropna().unique())

    results: List[Dict] = []
    for aid in assets:
        df_a = ts.loc[ts["asset_id"].astype(str) == str(aid)].copy()
        if df_a.empty:
            continue
        pdate, inferred = _pick_policy_date_for_asset(df_a, pol, aid, default_policy_date)
        if pdate is None or pd.isna(pdate):
            results.append(
                {
                    "asset_id": str(aid),
                    "metric": metric,
                    "status": "skipped_no_policy_date",
                }
            )
            continue
        try:
            res = _process_asset(df_a, policy_date=pdate, cfg=cfg)
            res["policy_inferred"] = bool(inferred)
            results.append(res)
        except Exception as exc:
            results.append({"asset_id": str(aid), "metric": metric, "error": str(exc)})

    eff = [r for r in results if "cum_effect" in r and np.isfinite(r["cum_effect"])]
    agg: Dict[str, object] = {
        "num_assets": len(assets),
        "processed": len(results),
        "with_effects": len(eff),
        "metric": metric,
        "alpha": alpha,
        "seasonal_period": seasonal_period,
    }
    if eff:
        agg["total_cum_effect"] = float(sum(r["cum_effect"] for r in eff))
        agg["avg_of_avg_effects"] = float(
            np.mean([r["avg_effect"] for r in eff if np.isfinite(r["avg_effect"])])
        )

    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        json.dump({"assets": results, "aggregate": agg}, f, indent=2)

    print(f"🛈 M7.2 processed {len(results)} asset(s).")
    return {"assets": results, "aggregate": agg}
