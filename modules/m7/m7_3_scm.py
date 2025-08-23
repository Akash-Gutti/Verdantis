"""M7.3 SCM what-if engine (policy → retrofit → energy → CO2).

We fit a simple linear SEM per asset:

  energy_kwh = α + β_temp * temp_c + β_wend * is_weekend + γ * retrofit + ε
  retrofit   = policy (binary), optionally scaled by --retrofit-scale
  co2_kg     = emission_factor * energy_kwh

Counterfactuals:
- policy=off → set policy=0 (retrofit=0)
- policy=on with --retrofit-scale s → keep policy=1 but scale γ by s

Outputs per asset:
- data/processed/causal/scm/<asset>_scm.csv        (observed vs CF + bands)
- data/processed/causal/plots/<asset>_scm.png      (plot)
- JSON rollup: data/processed/causal/scm_summary.json
- DAG+assumptions: data/processed/causal/scm_dag.json

Notes:
- OLS is fit on ALL days using design X=[1, temp_c, is_weekend, policy].
- 95% bands: yhat ± z * sqrt(σ² * (1 + leverage_cf)), leverage_cf = x·(X'X)^-1·x^T
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import norm

try:
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("matplotlib not installed. Run: pip install matplotlib") from exc


SCM_DIR = Path("data/processed/causal/scm")
PLOTS_DIR = Path("data/processed/causal/plots")
SUMMARY_JSON = Path("data/processed/causal/scm_summary.json")
DAG_JSON = Path("data/processed/causal/scm_dag.json")


@dataclass
class ScmConfig:
    metric: str = "energy_kwh"  # model is fit on energy_kwh; co2 derived
    policy: str = "off"  # "off" or "on"
    retrofit_scale: float = 1.0
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    config_path: str = "configs/m7_causal.json"


def _ensure_dirs() -> None:
    SCM_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def _load_ts_and_policy() -> Tuple[pd.DataFrame, pd.DataFrame]:
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
    pol["policy_start_date"] = pd.to_datetime(pol["policy_start_date"], errors="coerce")
    return ts, pol


def _load_cfg(config_path: str) -> Dict:
    p = Path(config_path)
    if not p.exists():
        return {"defaults": {"emission_factor_kg_per_kwh": 0.4}}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _design_matrix(df: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
    """Return design matrix (const, temp, is_weekend, policy) and y."""
    x = pd.DataFrame(index=df.index)
    x["const"] = 1.0
    x["temp_c"] = pd.to_numeric(df.get("temp_c"), errors="coerce")
    if "is_weekend" in df.columns:
        x["is_weekend"] = df["is_weekend"].astype(int)
    else:
        w = df["date"].dt.weekday
        x["is_weekend"] = (w >= 5).astype(int)
    x["policy"] = df["policy"].astype(int)

    # Fill small gaps conservatively
    x["temp_c"] = x["temp_c"].ffill().bfill()
    y = pd.to_numeric(df["energy_kwh"], errors="coerce").ffill()
    return x, y.values.astype(float)


def _fit_ols(x: pd.DataFrame, y: np.ndarray) -> Tuple[np.ndarray, np.ndarray, float]:
    """Return (beta, invXtX, sigma2). Uses pseudo-inverse if needed."""
    X = x.values.astype(float)
    XtX = X.T @ X
    try:
        invXtX = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        invXtX = np.linalg.pinv(XtX)
    beta = invXtX @ X.T @ y
    resid = y - X @ beta
    dof = max(1, X.shape[0] - X.shape[1])
    sigma2 = float((resid @ resid) / dof)
    return beta, invXtX, sigma2


def _predict_with_bands(
    x: pd.DataFrame,
    invXtX: np.ndarray,
    beta: np.ndarray,
    sigma2: float,
    alpha: float = 0.05,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (yhat, lower, upper) with 1-α bands."""
    X = x.values.astype(float)
    yhat = X @ beta
    # leverage = diag(X @ invXtX @ X^T) computed efficiently
    # (X @ invXtX) ⊙ X, sum across columns
    mid = X @ invXtX
    leverage = np.einsum("ij,ij->i", mid, X)
    z = float(norm.ppf(1.0 - alpha / 2.0))
    se = np.sqrt(sigma2 * (1.0 + leverage))
    lower = yhat - z * se
    upper = yhat + z * se
    return yhat, lower, upper


def _make_policy_indicator(df: pd.DataFrame, pdate: Optional[pd.Timestamp]) -> pd.Series:
    if pdate is None or pd.isna(pdate):
        return pd.Series(0, index=df.index, dtype=int)
    return (df["date"] >= pdate).astype(int)


def _per_asset(
    df_a: pd.DataFrame,
    policy_date: Optional[pd.Timestamp],
    cfg: ScmConfig,
    ef_kg_per_kwh: float,
) -> Dict:
    """Fit OLS, simulate CF, and write artifacts for a single asset."""
    df = df_a.sort_values("date").copy()
    # Build policy indicator from policy_date
    df["policy"] = _make_policy_indicator(df, policy_date)

    # Restrict date window if provided
    if cfg.start_date:
        df = df[df["date"] >= pd.to_datetime(cfg.start_date)]
    if cfg.end_date:
        df = df[df["date"] <= pd.to_datetime(cfg.end_date)]
    if df.empty:
        return {"asset_id": str(df_a["asset_id"].iloc[0]), "status": "no_data_window"}

    x_obs, y = _design_matrix(df)
    beta, invXtX, sigma2 = _fit_ols(x_obs, y)

    # Build counterfactual design:
    x_cf = x_obs.copy()
    # Override policy per scenario
    if cfg.policy.lower() == "off":
        x_cf["policy"] = 0
    else:
        x_cf["policy"] = 1

    # Scale retrofit effect by scaling the policy coefficient γ
    beta_cf = beta.copy()
    # Column order: const, temp_c, is_weekend, policy
    policy_idx = list(x_obs.columns).index("policy")
    beta_cf[policy_idx] = beta_cf[policy_idx] * float(cfg.retrofit_scale)

    # Predictions with 95% bands
    yhat_obs, lo_obs, hi_obs = _predict_with_bands(x_obs, invXtX, beta, sigma2)
    yhat_cf, lo_cf, hi_cf = _predict_with_bands(x_cf, invXtX, beta_cf, sigma2)

    delta = yhat_obs - yhat_cf  # positive -> observed greater than CF
    # Energy & CO2 totals
    eff_energy = float(np.nansum(delta))
    eff_co2 = float(eff_energy * ef_kg_per_kwh)

    # Persist per-asset CSV
    out_csv = SCM_DIR / f"{df['asset_id'].iloc[0]}_scm.csv"
    pd.DataFrame(
        {
            "date": df["date"].values,
            "energy_obs": y,
            "energy_fit": yhat_obs,
            "energy_cf": yhat_cf,
            "delta_energy": delta,
            "fit_lower": lo_obs,
            "fit_upper": hi_obs,
            "cf_lower": lo_cf,
            "cf_upper": hi_cf,
        }
    ).to_csv(out_csv, index=False)

    # Plot
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(df["date"], y, label="Observed")
    ax.plot(
        df["date"],
        yhat_cf,
        label=f"Counterfactual (policy {cfg.policy}, scale={cfg.retrofit_scale})",
    )  # noqa: E501
    ax.fill_between(df["date"], lo_cf, hi_cf, alpha=0.2, label="95% CF band")
    ax.set_title(f"{df['asset_id'].iloc[0]} – SCM what-if")
    ax.set_xlabel("Date")
    ax.set_ylabel("Energy (kWh)")
    ax.legend(loc="best")
    out_png = PLOTS_DIR / f"{df['asset_id'].iloc[0]}_scm.png"
    fig.tight_layout()
    fig.savefig(out_png, dpi=120)
    plt.close(fig)

    return {
        "asset_id": str(df["asset_id"].iloc[0]),
        "rows": int(len(df)),
        "policy_date": None if policy_date is None else str(policy_date.date()),
        "scenario_policy": cfg.policy,
        "retrofit_scale": float(cfg.retrofit_scale),
        "effect_energy_kwh": eff_energy,
        "effect_co2_kg": eff_co2,
        "csv_path": str(out_csv.as_posix()),
        "plot_path": str(out_png.as_posix()),
    }


def run_m7_3(
    metric: str = "energy_kwh",
    policy: str = "off",
    retrofit_scale: float = 1.0,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    asset: Optional[str] = None,
    config_path: str = "configs/m7_causal.json",
) -> Dict:
    """Run SCM what-if for all (or a single) asset."""
    _ensure_dirs()
    cfg = ScmConfig(
        metric=metric,
        policy=policy,
        retrofit_scale=retrofit_scale,
        start_date=start_date,
        end_date=end_date,
        config_path=config_path,
    )

    ts, pol = _load_ts_and_policy()
    cfg_json = _load_cfg(config_path)
    ef = float(cfg_json.get("defaults", {}).get("emission_factor_kg_per_kwh", 0.4))

    assets = [asset] if asset else sorted(ts["asset_id"].astype(str).unique())
    results: List[Dict] = []

    for aid in assets:
        df_a = ts.loc[ts["asset_id"].astype(str) == str(aid)].copy()
        if df_a.empty:
            continue
        row = pol.loc[pol["asset_id"].astype(str) == str(aid)]
        pdate = row["policy_start_date"].iloc[0] if not row.empty else pd.NaT
        res = _per_asset(df_a, pdate, cfg, ef)
        results.append(res)

    # Aggregate summary
    agg = {
        "num_assets": len(assets),
        "processed": len(results),
        "total_effect_energy_kwh": float(
            np.nansum([r.get("effect_energy_kwh", 0.0) for r in results])
        ),
        "total_effect_co2_kg": float(np.nansum([r.get("effect_co2_kg", 0.0) for r in results])),
        "scenario_policy": cfg.policy,
        "retrofit_scale": float(cfg.retrofit_scale),
        "metric_modeled": "energy_kwh",
    }

    with SUMMARY_JSON.open("w", encoding="utf-8") as f:
        json.dump({"assets": results, "aggregate": agg}, f, indent=2)

    # DAG & assumptions
    dag = {
        "nodes": ["policy", "retrofit", "temp_c", "is_weekend", "energy_kwh", "co2_kg"],
        "edges": [
            ["policy", "retrofit"],
            ["retrofit", "energy_kwh"],
            ["temp_c", "energy_kwh"],
            ["is_weekend", "energy_kwh"],
            ["energy_kwh", "co2_kg"],
        ],
        "assumptions": [
            "Linear additive effects; stationarity within study window.",
            "Retrofit is policy-mediated; modeled via policy indicator.",
            "Emission factor is constant over time.",
            "No unobserved confounders affecting both policy and energy, beyond temp/weekend.",  # noqa: E501
        ],
        "equations": {
            "energy_kwh": "α + β_temp·temp_c + β_wend·is_weekend + γ·retrofit + ε",
            "retrofit": "policy (scaled by --retrofit-scale at query time)",
            "co2_kg": "emission_factor * energy_kwh",
        },
    }
    with DAG_JSON.open("w", encoding="utf-8") as f:
        json.dump(dag, f, indent=2)

    print(f"🛈 M7.3 SCM simulated {len(results)} asset(s).")
    return {"assets": results, "aggregate": agg}
