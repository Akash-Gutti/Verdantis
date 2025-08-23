"""M7.1 Data prep: IoT -> daily series + policy table.

Steps:
1) Load IoT hourly CSV (schema-agnostic).
2) Normalize columns to: timestamp | asset_id | energy_kwh | temp_c
3) Aggregate to daily per asset (sum energy, mean temp).
4) Compute co2_kg using emission factor from config (default 0.4 kg/kWh).
5) Build policy_table.csv preferring DAILY ASSETS (fallback to KG only if empty).
6) Write outputs under data/processed/causal/.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

OUT_DIR = Path("data/processed/causal")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _load_config(path: str) -> Dict:
    p = Path(path)
    if not p.exists():
        return {"defaults": {"emission_factor_kg_per_kwh": 0.4, "policy_date": None}}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _read_iot(iot_path: str) -> pd.DataFrame:
    p = Path(iot_path)
    if not p.exists():
        raise FileNotFoundError(
            f"IoT file not found: {iot_path}. Expected at data/raw/iot/iot_hourly.csv"
        )
    df = pd.read_csv(p)
    if df.empty:
        raise ValueError("IoT file is empty.")
    return df


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    # Timestamp candidates
    ts_col = None
    for cand in ["timestamp", "time", "datetime", "ts"]:
        if cand in cols:
            ts_col = cols[cand]
            break
    if not ts_col:
        raise ValueError("No timestamp column found (tried: timestamp/time/datetime/ts).")

    # Asset id candidates
    asset_col = None
    for cand in ["asset_id", "asset", "site_id", "site"]:
        if cand in cols:
            asset_col = cols[cand]
            break

    # Wide schema candidates
    wide_energy = None
    for cand in ["energy_kwh", "kwh", "energy"]:
        if cand in cols:
            wide_energy = cols[cand]
            break
    wide_temp = None
    for cand in ["temp_c", "temperature_c", "temperature"]:
        if cand in cols:
            wide_temp = cols[cand]
            break

    # Long schema detection: metric/value
    has_metric = "metric" in cols and "value" in cols

    if wide_energy or has_metric:
        base = df[[ts_col]].copy()
        base["asset_id"] = df[asset_col].astype(str) if asset_col else "asset_01"
        base["timestamp"] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)

        if has_metric:
            piv = df[
                [ts_col, cols["metric"], cols["value"]] + ([asset_col] if asset_col else [])
            ].rename(  # noqa: E501
                columns={
                    ts_col: "timestamp",
                    cols["metric"]: "metric",
                    cols["value"]: "value",
                    asset_col: "asset_id" if asset_col else None,
                }
            )
            if "asset_id" not in piv.columns:
                piv["asset_id"] = "asset_01"
            piv["timestamp"] = pd.to_datetime(
                piv["timestamp"], errors="coerce", utc=True
            )  # noqa: E501
            w = piv.pivot_table(
                index=["timestamp", "asset_id"],
                columns="metric",
                values="value",
                aggfunc="mean",
            ).reset_index()
            if "kwh" in w.columns and "energy_kwh" not in w.columns:
                w = w.rename(columns={"kwh": "energy_kwh"})
            if "temperature" in w.columns and "temp_c" not in w.columns:
                w = w.rename(columns={"temperature": "temp_c"})
            if "temperature_c" in w.columns and "temp_c" not in w.columns:
                w = w.rename(columns={"temperature_c": "temp_c"})
            out = w
        else:
            out = base
            if wide_energy:
                out["energy_kwh"] = pd.to_numeric(df[wide_energy], errors="coerce")
            if wide_temp:
                out["temp_c"] = pd.to_numeric(df[wide_temp], errors="coerce")

        for col in ["energy_kwh", "temp_c"]:
            if col not in out.columns:
                out[col] = pd.NA

        out = out[["timestamp", "asset_id", "energy_kwh", "temp_c"]]
        return out

    raise ValueError(
        "Could not detect schema. Provide either wide columns "
        "(energy_kwh/temp_c) or long columns (metric/value)."
    )


def _aggregate_daily(df: pd.DataFrame, tz: str) -> pd.DataFrame:
    df = df.dropna(subset=["timestamp"]).copy()
    if df.empty:
        raise ValueError("No valid timestamps after parsing.")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert(tz) if tz else df["timestamp"]
    df["date"] = df["timestamp"].dt.date

    grp = df.groupby(["asset_id", "date"], dropna=False, as_index=False).agg(
        energy_kwh=("energy_kwh", "sum"),
        temp_c=("temp_c", "mean"),
        obs=("timestamp", "count"),
    )
    grp["energy_kwh"] = pd.to_numeric(grp["energy_kwh"], errors="coerce").fillna(0.0)
    grp["temp_c"] = pd.to_numeric(grp["temp_c"], errors="coerce")
    grp["weekday"] = pd.to_datetime(grp["date"]).dt.weekday
    grp["is_weekend"] = (grp["weekday"] >= 5).astype(int)
    return grp


def _append_co2(grp: pd.DataFrame, ef_kg_per_kwh: float) -> pd.DataFrame:
    grp = grp.copy()
    grp["co2_kg"] = grp["energy_kwh"].astype(float) * float(ef_kg_per_kwh)
    return grp


def _load_assets_from_kg() -> List[str]:
    p = Path("data/processed/kg/csv/asset.csv")
    if not p.exists():
        return []
    try:
        df = pd.read_csv(p)
        cols = {c.lower(): c for c in df.columns}
        for cand in ["asset_id", "id", "asset"]:
            if cand in cols:
                col = cols[cand]
                vals = df[col].astype(str).dropna().unique().tolist()
                return vals
    except Exception:
        return []
    return []


def _build_policy_table(
    assets: List[str],
    defaults: Dict,
    asset_policies: List[Dict],
) -> pd.DataFrame:
    default_date = defaults.get("policy_date")
    rows: List[Tuple[str, str]] = []
    per_asset = {str(x.get("asset_id")): x.get("policy_date") for x in asset_policies}

    if not assets:
        assets = ["asset_01"]

    for aid in assets:
        date = per_asset.get(aid, default_date)
        rows.append((aid, date if date else ""))

    return pd.DataFrame(rows, columns=["asset_id", "policy_start_date"])


def _save_outputs(ts_daily: pd.DataFrame, policy: pd.DataFrame, meta: Dict) -> None:
    _ensure_dir(OUT_DIR)
    try:
        ts_daily.to_parquet(OUT_DIR / "ts_daily.parquet", index=False)
    except Exception:
        ts_daily.to_csv(OUT_DIR / "ts_daily.csv", index=False)

    policy.to_csv(OUT_DIR / "policy_table.csv", index=False)

    with (OUT_DIR / "meta.json").open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


def run_m7_1(config_path: str, iot_path: str) -> Dict:
    """Entry point for M7.1 data prep."""
    cfg = _load_config(config_path)
    defaults = cfg.get("defaults", {})
    ef = float(defaults.get("emission_factor_kg_per_kwh", 0.4))
    tz = str(defaults.get("timezone", "UTC"))
    asset_policies = cfg.get("asset_policies", [])

    raw = _read_iot(iot_path)
    norm = _normalize_columns(raw)
    daily = _aggregate_daily(norm, tz=tz)
    daily = _append_co2(daily, ef_kg_per_kwh=ef)

    # FIX: prefer assets from daily (actual modeling set), fallback to KG
    assets_daily = sorted(daily["asset_id"].astype(str).unique())
    assets_kg = _load_assets_from_kg()
    assets = assets_daily if assets_daily else assets_kg

    policy = _build_policy_table(assets, defaults, asset_policies)

    meta = {
        "assets": assets,
        "rows_daily": int(len(daily)),
        "emission_factor_kg_per_kwh": ef,
        "timezone": tz,
        "policy_assets_source": "daily" if assets == assets_daily else "kg",
        "has_parquet": True,
    }

    _save_outputs(daily, policy, meta)
    return meta
