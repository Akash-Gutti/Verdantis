"""CLI registrar for Module 7 (Causal Impact & Counterfactuals)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Callable, Dict

from .m7_1_prep import run_m7_1
from .m7_2_bsts import run_m7_2


def register(subparsers: argparse._SubParsersAction, verifiers: Dict[str, Callable]):
    """Register m7 commands with verdctl."""
    m7 = subparsers.add_parser("m7", help="Module 7: Causal impact & counterfactuals")
    m7_sub = m7.add_subparsers(dest="m7_cmd", required=True)

    # m7 prep
    p_prep = m7_sub.add_parser("prep", help="M7.1 prepare daily time series + policy")
    p_prep.add_argument(
        "--config",
        default="configs/m7_causal.json",
        help="Path to m7 config (default: configs/m7_causal.json)",
    )
    p_prep.add_argument(
        "--iot-path",
        default="data/raw/iot/iot_hourly.csv",
        help="IoT hourly CSV path (default: data/raw/iot/iot_hourly.csv)",
    )
    p_prep.set_defaults(func=_cmd_m7_prep)

    # m7 bsts
    p_bsts = m7_sub.add_parser("bsts", help="M7.2 causal effect via UCM (CPU-first)")
    p_bsts.add_argument(
        "--metric", choices=["energy_kwh", "co2_kg"], default="energy_kwh"
    )  # noqa: E501
    p_bsts.add_argument("--asset", default=None, help="Single asset_id (optional)")
    p_bsts.add_argument(
        "--seasonal-period", type=int, default=7, dest="seasonal_period"
    )  # noqa: E501
    p_bsts.add_argument("--alpha", type=float, default=0.05)
    p_bsts.add_argument("--min-pre-days", type=int, default=30, dest="min_pre_days")
    p_bsts.add_argument(
        "--config",
        default="configs/m7_causal.json",
        help="Path to m7 config for defaults (policy_date, etc.)",
    )
    p_bsts.set_defaults(func=_cmd_m7_bsts)

    # register verifier
    verifiers["m7"] = verify


def _cmd_m7_prep(args: argparse.Namespace) -> int:
    run_m7_1(config_path=args.config, iot_path=args.iot_path)
    print("✅ M7.1 data prep complete.")
    return 0


def _cmd_m7_bsts(args: argparse.Namespace) -> int:
    run_m7_2(
        metric=args.metric,
        seasonal_period=args.seasonal_period,
        alpha=args.alpha,
        min_pre_days=args.min_pre_days,
        asset=args.asset,
        config_path=args.config,
    )
    print("✅ M7.2 causal impact complete.")
    return 0


def verify() -> bool:
    """Verifier for M7: prefer effects summary; else prep outputs (with warning for 0 processed)."""  # noqa: E501
    effects_summary = Path("data/processed/causal/effects_summary.json")
    if effects_summary.exists():
        try:
            data = json.loads(effects_summary.read_text(encoding="utf-8"))
            processed = int(data.get("aggregate", {}).get("processed", 0))
            if processed > 0:
                print("M7 verify → effects summary present.")
                return True
            print(
                "M7 verify → effects summary exists but 0 processed. "
                "Check policy coverage or rerun 'm7 prep' and 'm7 bsts'."
            )  # noqa: E501
            return False
        except Exception:
            print("M7 verify → effects summary present (could not parse).")
            return True

    ts_parquet = Path("data/processed/causal/ts_daily.parquet")
    ts_csv = Path("data/processed/causal/ts_daily.csv")
    policy_csv = Path("data/processed/causal/policy_table.csv")
    ts_ok = ts_parquet.exists() or ts_csv.exists()

    if ts_ok and policy_csv.exists():
        print("M7 verify → daily series + policy table present (run m7 bsts).")
        return False

    if not ts_ok:
        print("M7 verify → ts_daily missing (run m7 prep).")
    if not policy_csv.exists():
        print("M7 verify → policy_table.csv missing (run m7 prep).")
    return False
