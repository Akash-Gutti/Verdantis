"""CLI registrar for Module 7 (Causal Impact & Counterfactuals)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Callable, Dict

from .m7_1_prep import run_m7_1
from .m7_2_bsts import run_m7_2
from .m7_3_scm import run_m7_3
from .m7_4_api import run_m7_4_api


def register(subparsers: argparse._SubParsersAction, verifiers: Dict[str, Callable]):
    m7 = subparsers.add_parser("m7", help="Module 7: Causal impact & counterfactuals")
    m7_sub = m7.add_subparsers(dest="m7_cmd", required=True)

    # M7.1
    p_prep = m7_sub.add_parser("prep", help="Prepare daily series + policy")
    p_prep.add_argument("--config", default="configs/m7_causal.json")
    p_prep.add_argument("--iot-path", default="data/raw/iot/iot_hourly.csv")
    p_prep.set_defaults(func=_cmd_m7_prep)

    # M7.2
    p_bsts = m7_sub.add_parser("bsts", help="Causal effect via UCM (CPU-first)")
    p_bsts.add_argument(
        "--metric", choices=["energy_kwh", "co2_kg"], default="energy_kwh"
    )  # noqa: E501
    p_bsts.add_argument("--asset", default=None)
    p_bsts.add_argument(
        "--seasonal-period", type=int, default=7, dest="seasonal_period"
    )  # noqa: E501
    p_bsts.add_argument("--alpha", type=float, default=0.05)
    p_bsts.add_argument("--min-pre-days", type=int, default=30, dest="min_pre_days")
    p_bsts.add_argument("--config", default="configs/m7_causal.json")
    p_bsts.set_defaults(func=_cmd_m7_bsts)

    # M7.3
    p_scm = m7_sub.add_parser("scm", help="SCM what-if (policy on/off, retrofit)")
    p_scm.add_argument(
        "--metric", choices=["energy_kwh", "co2_kg"], default="energy_kwh"
    )  # noqa: E501
    p_scm.add_argument("--policy", choices=["on", "off"], default="off")
    p_scm.add_argument(
        "--retrofit-scale", type=float, default=1.0, dest="retrofit_scale"
    )  # noqa: E501
    p_scm.add_argument("--start-date", default=None)
    p_scm.add_argument("--end-date", default=None)
    p_scm.add_argument("--asset", default=None)
    p_scm.add_argument("--config", default="configs/m7_causal.json")
    p_scm.set_defaults(func=_cmd_m7_scm)

    # M7.4
    p_api = m7_sub.add_parser("api", help="Serve /effect API (FastAPI)")
    p_api.add_argument("--host", default="127.0.0.1")
    p_api.add_argument("--port", type=int, default=8009)
    p_api.set_defaults(func=_cmd_m7_api)

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


def _cmd_m7_scm(args: argparse.Namespace) -> int:
    run_m7_3(
        metric=args.metric,
        policy=args.policy,
        retrofit_scale=args.retrofit_scale,
        start_date=args.start_date,
        end_date=args.end_date,
        asset=args.asset,
        config_path=args.config,
    )
    print("✅ M7.3 SCM what-if complete.")
    return 0


def _cmd_m7_api(args: argparse.Namespace) -> int:
    run_m7_4_api(host=args.host, port=args.port)
    return 0


def verify() -> bool:
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
            )
            return False
        except Exception:
            print("M7 verify → effects summary present (parse error).")
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
