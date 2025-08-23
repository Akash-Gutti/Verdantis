"""CLI registrar for Module 6 (Change Detection)."""

from __future__ import annotations

import argparse
from typing import Callable, Dict

from .m6_1_stac_fetch import run_m6_1
from .m6_2_diff import run_m6_2


def register(subparsers: argparse._SubParsersAction, verifiers: Dict[str, Callable]):
    """Register m6 commands with verdctl."""
    m6 = subparsers.add_parser("m6", help="Module 6: Satellite change detection")
    m6_sub = m6.add_subparsers(dest="m6_cmd", required=True)

    # m6 stac
    p_stac = m6_sub.add_parser("stac", help="M6.1 STAC-like local resolve")
    p_stac.add_argument(
        "--config",
        default="configs/m6_satellite.json",
        help="Path to m6 config (default: configs/m6_satellite.json)",
    )
    p_stac.set_defaults(func=_cmd_m6_stac)

    # m6 diff
    p_diff = m6_sub.add_parser("diff", help="M6.2 NDVI/RGB differencing")
    p_diff.add_argument(
        "--config",
        default="configs/m6_satellite.json",
        help="Path to m6 config (for defaults)",
    )
    p_diff.add_argument("--percentile", type=float, default=None)
    p_diff.add_argument("--min-area", type=int, default=None, dest="min_area")
    p_diff.add_argument("--open-iters", type=int, default=None, dest="open_iters")
    p_diff.add_argument("--close-iters", type=int, default=None, dest="close_iters")
    p_diff.add_argument(
        "--mode",
        choices=["abs", "neg", "pos"],
        default=None,
        help="NDVI diff mode: abs|neg|pos (default from config or 'abs')",
    )
    p_diff.set_defaults(func=_cmd_m6_diff)

    verifiers["m6"] = verify


def _cmd_m6_stac(args: argparse.Namespace) -> int:
    run_m6_1(args.config)
    print("✅ M6.1 STAC resolve complete.")
    return 0


def _cmd_m6_diff(args: argparse.Namespace) -> int:
    run_m6_2(
        config_path=args.config,
        percentile=args.percentile,
        min_area=args.min_area,
        open_iters=args.open_iters,
        close_iters=args.close_iters,
        mode=args.mode,
    )
    print("✅ M6.2 differencing complete.")
    return 0


def verify() -> bool:
    """Verifier for M6."""
    from pathlib import Path

    idx = Path("data/interim/m6/index.json")
    if not idx.exists():
        print("M6 verify → missing data/interim/m6/index.json")
        return False

    # If any AOI has change_mask.tif, we consider M6.2 passed for that AOI.
    interim = Path("data/interim/m6")
    masks = list(interim.glob("*/change_mask.tif"))
    if masks:
        print(f"M6 verify → masks present: {len(masks)} AOI(s).")
        return True

    print("M6 verify → no change masks yet (run m6 diff).")
    return True
