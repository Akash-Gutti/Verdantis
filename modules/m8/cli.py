"""CLI registrar for Module 8 (Policy Engine).

Registers 'm8' commands into verdctl:
- m8 schema  → create schemas, write sample rules, validate, register.
"""

from __future__ import annotations

from argparse import _SubParsersAction
from typing import Callable, Dict

from . import m8_1_schema


def register(subparsers: _SubParsersAction, verifiers: Dict[str, Callable[[], None]]) -> None:
    """Register M8 subcommands and verifier with verdctl."""
    parser = subparsers.add_parser("m8", help="Module 8: Policy Engine")
    sp = parser.add_subparsers(dest="m8_cmd")

    # m8 schema
    p_schema = sp.add_parser("schema", help="Create/validate rule schemas & seed samples")
    p_schema.set_defaults(func=lambda args: m8_1_schema.main())

    # Hook verifier
    verifiers["m8"] = m8_1_schema.verify
