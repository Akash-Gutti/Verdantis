"""CLI registrar for Module 10 (Streaming Alerts) - M10.1 filters."""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Dict, Tuple

from .m10_1_filters import run_filters_cli
from .m10_2_channels import run_channels_cli
from .m10_3_dedupe import run_dedupe_cli


def _cmd_filters(args: Namespace) -> int:
    matched, unmatched = run_filters_cli(
        events_path=Path(args.events),
        filters_path=Path(args.filters),
        out_path=Path(args.out),
        metrics_path=Path(args.metrics),
    )
    print(
        f"M10.1 filters → matched={matched}, unmatched={unmatched} " f"→ {args.out}, {args.metrics}"
    )
    return 0


def _cmd_channels(args: Namespace) -> int:  # NEW
    sent, skipped = run_channels_cli(
        matched_path=Path(args.matched),
        cfg_path=Path(args.config),
        results_path=Path(args.results),
        metrics_path=Path(args.metrics),
    )
    print(f"M10.2 channels → sent={sent}, skipped={skipped} " f"→ {args.results}, {args.metrics}")
    return 0


def _cmd_dedupe(args: Namespace) -> int:
    kept, suppressed = run_dedupe_cli(
        matched_path=Path(args.matched),
        cfg_path=Path(args.config),
        out_path=Path(args.out),
        metrics_path=Path(args.metrics),
        state_path=Path(args.state),
    )
    print(
        f"M10.3 dedupe → kept={kept}, suppressed={suppressed} "
        f"→ {args.out}, {args.metrics} (state: {args.state})"
    )
    return 0


def verify_m10() -> Tuple[bool, str]:
    """Lightweight verify: ensure metrics exist and are well-formed."""
    metrics_path = Path("data/processed/m10/filters_metrics.json")
    if not metrics_path.exists():
        msg = "M10 verify: metrics file not found."
        print(msg)
        return False, msg
    try:
        import json  # local import to keep top clean

        with metrics_path.open("r", encoding="utf-8") as f:
            metrics: Dict[str, Any] = json.load(f)
        _ = int(metrics.get("total_events", 0))
        _ = int(metrics.get("unmatched", 0))
        if "per_subscription" not in metrics:
            msg = "M10 verify: per_subscription missing."
            print(msg)
            return False, msg
        print("M10 verify OK.")
        return True, "M10 verify OK."
    except Exception as exc:  # noqa: BLE001
        msg = f"M10 verify: failed to parse metrics ({exc})."
        print(msg)
        return False, msg


def register(subparsers: ArgumentParser, verifiers: Dict[str, Any]) -> None:
    """Register M10 commands with the root verdctl."""
    p = subparsers.add_parser("m10", help="Module 10 - Streaming Alerts")
    sp = p.add_subparsers(dest="m10_cmd")

    p_filters = sp.add_parser("filters", help="Run M10.1 filters on events")
    p_filters.add_argument("--events", required=True, help="Path to events JSON list")
    p_filters.add_argument(
        "--filters", required=True, help="Path to subscriptions filter config JSON"
    )
    p_filters.add_argument(
        "--out",
        default="data/processed/m10/filtered_events.json",
        help="Output file for matched events",
    )
    p_filters.add_argument(
        "--metrics",
        default="data/processed/m10/filters_metrics.json",
        help="Output file for metrics",
    )
    p_filters.set_defaults(func=_cmd_filters)

    # NEW: channels subcommand
    p_channels = sp.add_parser("channels", help="Run M10.2 channel routing on matched events")
    p_channels.add_argument(
        "--matched",
        default="data/processed/m10/filtered_events.json",
        help="Input: matched events from M10.1",
    )
    p_channels.add_argument(
        "--config",
        default="configs/m10_channels.json",
        help="Channels routing config JSON",
    )
    p_channels.add_argument(
        "--results",
        default="data/processed/m10/channels_results.json",
        help="Output file for per-attempt results",
    )
    p_channels.add_argument(
        "--metrics",
        default="data/processed/m10/channels_metrics.json",
        help="Output file for channel metrics",
    )
    p_channels.set_defaults(func=_cmd_channels)

    # NEW: dedupe subcommand (M10.3)
    p_dedupe = sp.add_parser(
        "dedupe", help="Run M10.3 dedupe + flapping suppression on matched events"
    )
    p_dedupe.add_argument(
        "--matched",
        default="data/processed/m10/filtered_events.json",
        help="Input: matched events from M10.1",
    )
    p_dedupe.add_argument(
        "--config",
        default="configs/m10_dedupe.json",
        help="Dedupe/flap config JSON",
    )
    p_dedupe.add_argument(
        "--out",
        default="data/processed/m10/filtered_events_deduped.json",
        help="Output: deduped matched events for channels",
    )
    p_dedupe.add_argument(
        "--metrics",
        default="data/processed/m10/dedupe_metrics.json",
        help="Metrics JSON",
    )
    p_dedupe.add_argument(
        "--state",
        default="data/processed/m10/state/dedupe_state.json",
        help="Persistent state JSON",
    )
    p_dedupe.set_defaults(func=_cmd_dedupe)

    # attach verifier for `scripts/verdctl.py verify -m m10`
    verifiers["m10"] = verify_m10
