"""M7.4 Causal API: /effect → effect, p, counterfactual, bands + PNG."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple, Union

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from scipy.stats import norm

# --- Use non-interactive backend to avoid GUI/thread warnings ---
try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("pip install matplotlib") from exc

try:
    from statsmodels.tsa.statespace.structural import UnobservedComponents
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("pip install statsmodels") from exc

API_PLOTS = Path("data/processed/causal/api/plots")
API_PLOTS.mkdir(parents=True, exist_ok=True)


@dataclass
class EffectConfig:
    metric: str = "energy_kwh"
    seasonal_period: int = 7
    alpha: float = 0.05
    min_pre_days: int = 7
    config_path: str = "configs/m7_causal.json"


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
    z = float(cum_eff) / float(np.sqrt(var_sum))
    return 2.0 * (1.0 - float(norm.cdf(abs(z))))


def _variance_from_ci(ci: pd.DataFrame, alpha: float) -> pd.Series:
    if "lower" not in ci.columns or "upper" not in ci.columns:
        return pd.Series(np.nan, index=ci.index)
    z = float(norm.ppf(1.0 - alpha / 2.0))
    half = (ci["upper"] - ci["lower"]) / 2.0
    return (half / z) ** 2


def _pick_policy_date(
    df_a: pd.DataFrame,
    pol: pd.DataFrame,
    asset_id: str,
    cfg_json: Dict,
) -> Optional[pd.Timestamp]:
    row = pol.loc[pol["asset_id"].astype(str) == str(asset_id)]
    if not row.empty:
        pdate = pd.to_datetime(row["policy_start_date"].iloc[0], errors="coerce")
        if pd.notna(pdate):
            return pdate
    default_policy = cfg_json.get("defaults", {}).get("policy_date")
    if default_policy:
        pdate = pd.to_datetime(default_policy, errors="coerce")
        if pd.notna(pdate):
            return pdate
    dates = pd.to_datetime(df_a["date"], errors="coerce").dropna().sort_values()
    if dates.empty:
        return None
    return dates.iloc[len(dates) // 2]


def _plot_png(
    asset_id: str,
    metric: str,
    y: pd.Series,
    yhat: pd.Series,
    ci: pd.DataFrame,
    policy_date: Optional[pd.Timestamp],
) -> Path:
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(y.index, y.values, label="Observed")
    ax.plot(yhat.index, yhat.values, label="Counterfactual")
    if "lower" in ci.columns and "upper" in ci.columns:
        ax.fill_between(ci.index, ci["lower"], ci["upper"], alpha=0.2, label="95% CI")
    if policy_date is not None and pd.notna(policy_date):
        ax.axvline(policy_date, linestyle="--", label="Policy")
    ax.set_title(f"{asset_id} – {metric}")
    ax.set_xlabel("Date")
    ax.set_ylabel(metric)
    ax.legend(loc="best")
    out = API_PLOTS / f"effect_{asset_id}_{metric}.png"
    fig.tight_layout()
    fig.savefig(out, dpi=120)
    plt.close(fig)
    return out


# ---------- JSON safety helpers (replace NaN/Inf with None) ----------


def _clean_number(x: object) -> Optional[float]:
    try:
        xf = float(x)  # type: ignore[arg-type]
    except Exception:
        return None
    return xf if np.isfinite(xf) else None


def _to_clean_list(arr: Union[np.ndarray, List[float]]) -> List[Optional[float]]:
    a = np.asarray(arr, dtype=float)
    out: List[Optional[float]] = []
    for v in a.flat:
        out.append(float(v) if np.isfinite(v) else None)
    return out


# Series values include nullable floats now
SeriesDict = Dict[str, Union[List[Optional[float]], List[str]]]


class EffectRequest(BaseModel):
    asset_id: str = Field(..., description="Asset ID, e.g., 'asset_1'")
    metric: Literal["energy_kwh", "co2_kg"] = "energy_kwh"
    alpha: float = 0.05
    seasonal_period: int = 7
    min_pre_days: int = 7
    config_path: str = "configs/m7_causal.json"
    return_series: bool = True
    return_png_base64: bool = False
    policy_date: Optional[str] = None  # override (YYYY-MM-DD)


class EffectResponse(BaseModel):
    asset_id: str
    metric: str
    policy_date: Optional[str]
    avg_effect: Optional[float]
    cum_effect: Optional[float]
    p_value: Optional[float]
    used_fallback: bool
    plot_path: Optional[str]
    png_base64: Optional[str] = None
    series: Optional[SeriesDict] = None


def build_app() -> FastAPI:
    app = FastAPI(title="Verdantis Causal Service", version="0.1.0")

    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post("/effect", response_model=EffectResponse)
    def effect(req: EffectRequest) -> EffectResponse:
        ts, pol = _load_ts_and_policy()
        cfgj = _load_config(req.config_path)

        df_a = ts.loc[ts["asset_id"].astype(str) == str(req.asset_id)].copy()
        if df_a.empty:
            raise HTTPException(status_code=404, detail="asset_id not found in ts_daily")

        if req.metric not in df_a.columns:
            raise HTTPException(status_code=400, detail="unknown metric for asset")

        df_a["date"] = pd.to_datetime(df_a["date"], errors="coerce")
        dates = df_a["date"]
        y_vals = pd.to_numeric(df_a[req.metric], errors="coerce")
        y_all = pd.Series(y_vals.values, index=dates).ffill()
        x_all = _build_design_matrix(df_a.set_index("date"))

        pdate = None
        if req.policy_date:
            pdate = pd.to_datetime(req.policy_date, errors="coerce")
            if pd.isna(pdate):
                raise HTTPException(status_code=400, detail="invalid policy_date (YYYY-MM-DD)")
        if pdate is None:
            pdate = _pick_policy_date(df_a, pol, req.asset_id, cfgj)
        if pdate is None or pd.isna(pdate):
            raise HTTPException(status_code=400, detail="could not determine policy_date")

        pre_mask = y_all.index < pdate
        pre_days = int(pre_mask.sum())
        if pre_days < int(req.min_pre_days):
            raise HTTPException(
                status_code=400,
                detail=f"insufficient pre-policy days: have {pre_days}, "
                f"need {req.min_pre_days}",
            )

        y_pre = y_all.loc[pre_mask]
        x_pre = x_all.loc[pre_mask]
        used_fallback = False
        try:
            yhat, ci = _ucm_forecast(
                y_pre=y_pre,
                x_pre=x_pre,
                x_full=x_all,
                seasonal_period=req.seasonal_period,
                alpha=req.alpha,
            )
        except Exception:
            mu = float(y_pre.mean())
            yhat = pd.Series(mu, index=y_all.index)
            ci = pd.DataFrame({"lower": yhat * np.nan, "upper": yhat * np.nan}, index=yhat.index)
            used_fallback = True

        eff_series = y_all - yhat
        var_series = _variance_from_ci(ci, alpha=req.alpha).reindex(y_all.index)
        var_sum = float(var_series.loc[~pre_mask].sum(skipna=True))

        avg_eff_raw = float(eff_series.loc[~pre_mask].mean())
        cum_eff_raw = float(eff_series.loc[~pre_mask].sum())
        pval_raw = _p_value_from_cumulative(cum_eff_raw, var_sum)

        # Plot first (disk write), independent of JSON cleaning
        png_path = _plot_png(req.asset_id, req.metric, y_all, yhat, ci, pdate)

        payload = EffectResponse(
            asset_id=req.asset_id,
            metric=req.metric,
            policy_date=str(pdate.date()) if pd.notna(pdate) else None,
            avg_effect=_clean_number(avg_eff_raw),
            cum_effect=_clean_number(cum_eff_raw),
            p_value=_clean_number(pval_raw),
            used_fallback=used_fallback,
            plot_path=str(png_path.as_posix()),
        )

        if req.return_series:
            ser = pd.DataFrame(
                {
                    "date": y_all.index.astype("datetime64[ns]"),
                    "y": y_all.values,
                    "yhat": yhat.reindex(y_all.index).values,
                    "lower": (
                        ci.get("lower").reindex(y_all.index).values
                        if "lower" in ci.columns
                        else np.full_like(y_all.values, np.nan, dtype=float)
                    ),
                    "upper": (
                        ci.get("upper").reindex(y_all.index).values
                        if "upper" in ci.columns
                        else np.full_like(y_all.values, np.nan, dtype=float)
                    ),
                }
            )
            payload.series = {
                "date": [pd.Timestamp(d).strftime("%Y-%m-%d") for d in ser["date"]],
                "y": _to_clean_list(ser["y"].values),
                "yhat": _to_clean_list(ser["yhat"].values),
                "lower": _to_clean_list(ser["lower"].values),
                "upper": _to_clean_list(ser["upper"].values),
            }

        if req.return_png_base64:
            with open(png_path, "rb") as f:
                payload.png_base64 = base64.b64encode(f.read()).decode("ascii")

        return payload

    return app


def run_m7_4_api(host: str = "127.0.0.1", port: int = 8009) -> None:
    try:
        import uvicorn  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise SystemExit("pip install uvicorn") from exc
    app = build_app()
    uvicorn.run(app, host=host, port=int(port))
