"""CLI registrar for Module 7 (Causal Impact & Counterfactuals)."""

from __future__ import annotations

import argparse
from typing import Callable, Dict

from .m7_1_prep import run_m7_1


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

    # register verifier
    verifiers["m7"] = verify


def _cmd_m7_prep(args: argparse.Namespace) -> int:
    run_m7_1(config_path=args.config, iot_path=args.iot_path)
    print("✅ M7.1 data prep complete.")
    return 0


def verify() -> bool:
    """Verifier for M7: require ts_daily + policy table."""
    from pathlib import Path

    ts_parquet = Path("data/processed/causal/ts_daily.parquet")
    ts_csv = Path("data/processed/causal/ts_daily.csv")
    policy_csv = Path("data/processed/causal/policy_table.csv")

    ts_ok = ts_parquet.exists() or ts_csv.exists()
    if ts_ok and policy_csv.exists():
        print("M7 verify → daily series + policy table present.")
        return True

    if not ts_ok:
        print("M7 verify → ts_daily missing (run m7 prep).")
    if not policy_csv.exists():
        print("M7 verify → policy_table.csv missing (run m7 prep).")
    return False
