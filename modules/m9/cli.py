"""CLI registrar for Module 9 (zk-Attested Compliance)."""

from __future__ import annotations

from argparse import _SubParsersAction
from typing import Callable, Dict

from . import m9_1_commit


def register(subparsers: _SubParsersAction, verifiers: Dict[str, Callable[[], None]]) -> None:
    """Register M9 subcommands with verdctl."""
    parser = subparsers.add_parser("m9", help="Module 9: zk-Attested Compliance")
    sp = parser.add_subparsers(dest="m9_cmd")

    # m9 commit
    p_commit = sp.add_parser("commit", help="Compute feature commitment (M9.1)")
    p_commit.add_argument("--features", help='Inline JSON list, e.g. "[0.1, 5, 9.2]"')
    p_commit.add_argument(
        "--input", help="Path to JSON with {features, model_id, model_version?, salt?, precision?}"
    )
    p_commit.add_argument("--model-id", required=False)
    p_commit.add_argument("--model-version", default=None)
    p_commit.add_argument("--salt", default=None)
    p_commit.add_argument("--precision", type=int, default=6)
    p_commit.add_argument(
        "--out", default=None, help="Output path (JSON). If omitted, prints only."
    )
    p_commit.set_defaults(func=lambda args: m9_1_commit.cli_commit(args))

    # Verifier hook (no checks yet for M9.1)
    verifiers["m9"] = m9_1_commit.verify
