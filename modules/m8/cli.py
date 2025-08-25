"""CLI registrar for Module 8 (Policy Engine).

Registers 'm8' commands into verdctl:
- m8 schema   → create schemas, write sample rules, validate, register.
- m8 compile  → compile YAML rules into normalized IR for enforcement.
"""

from __future__ import annotations

from argparse import _SubParsersAction
from typing import Callable, Dict

from . import m8_1_schema, m8_2_compiler, m8_3_propose


def register(subparsers: _SubParsersAction, verifiers: Dict[str, Callable[[], None]]) -> None:
    """Register M8 subcommands and verifier with verdctl."""
    parser = subparsers.add_parser("m8", help="Module 8: Policy Engine")
    sp = parser.add_subparsers(dest="m8_cmd")

    # m8 schema
    p_schema = sp.add_parser("schema", help="Create/validate rule schemas & seed samples")
    p_schema.set_defaults(func=lambda args: m8_1_schema.main())

    # m8 compile
    p_compile = sp.add_parser("compile", help="Compile YAML rules into normalized IR")
    p_compile.set_defaults(func=lambda args: m8_2_compiler.main())

    # m8 propose
    p_prop = sp.add_parser("propose", help="Propose rule(s) from text and save to proposed/")
    p_prop.add_argument("--text", required=True, help="Policy clause text")
    p_prop.add_argument("--owner", default="policy-team")
    p_prop.add_argument("--severity", default=None)
    p_prop.add_argument("--id-hint", dest="id_hint", default=None)
    p_prop.add_argument("--dry-run", action="store_true")

    def _run_prop(a):
        pairs = m8_3_propose.propose_from_text(
            text=a.text,
            owner=a.owner,
            severity=a.severity if a.severity else None,
            id_hint=a.id_hint,
            save=not a.dry_run,
        )
        print(f"Generated {len(pairs)} candidate(s).")
        for yml, rule in pairs:
            print(f"- {rule['meta']['id']}")
            if a.dry_run:
                print(yml)

    p_prop.set_defaults(func=_run_prop)

    # Hook verifier
    verifiers["m8"] = m8_1_schema.verify
