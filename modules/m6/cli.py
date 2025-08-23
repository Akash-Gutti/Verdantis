"""CLI registrar for Module 6 (Change Detection)."""

from __future__ import annotations

import argparse
from typing import Callable, Dict

from .m6_1_stac_fetch import run_m6_1


def register(subparsers: argparse._SubParsersAction, verifiers: Dict[str, Callable]):
    """Register m6 commands with verdctl."""
    m6 = subparsers.add_parser("m6", help="Module 6: Satellite change detection")
    m6_sub = m6.add_subparsers(dest="m6_cmd", required=True)

    # m6 stac --config configs/m6_satellite.json
    p_stac = m6_sub.add_parser("stac", help="M6.1 STAC-like local resolve")
    p_stac.add_argument(
        "--config",
        default="configs/m6_satellite.json",
        help="Path to m6 config (default: configs/m6_satellite.json)",
    )
    p_stac.set_defaults(func=_cmd_m6_stac)

    # register module-level verifier hook
    verifiers["m6"] = verify


def _cmd_m6_stac(args: argparse.Namespace) -> int:
    run_m6_1(args.config)
    print("✅ M6.1 STAC resolve complete.")
    return 0


def verify() -> bool:
    """Lightweight verifier for M6.1 onwards.

    For now (after M6.1), we just check that data/interim/m6/index.json exists.
    Later steps (M6.2–M6.4) will extend this.
    """
    from pathlib import Path

    idx = Path("data/interim/m6/index.json")
    if idx.exists():
        print("M6 verify → interim index present.")
        return True
    print("M6 verify → missing data/interim/m6/index.json")
    return False
