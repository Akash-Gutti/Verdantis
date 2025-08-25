"""CLI registrar for Module 8 (Policy Engine).

Registers 'm8' commands into verdctl:
- m8 schema   → create schemas, write sample rules, validate, register.
- m8 compile  → compile YAML rules into normalized IR for enforcement.
"""

from __future__ import annotations

from argparse import _SubParsersAction
from typing import Callable, Dict

from . import m8_1_schema, m8_2_compiler, m8_3_propose, m8_4_enforce


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

    p_enf = sp.add_parser(
        "enforce", help="Evaluate a JSON event (and optional KG) via compiled rules"
    )
    p_enf.add_argument("--asset-id", required=True)
    p_enf.add_argument("--event", required=True, help="Path to event JSON file")
    p_enf.add_argument("--kg", default=None, help="Path to KG JSON file (optional)")
    p_enf.add_argument("--rule-id", action="append", dest="rule_ids", default=None)
    p_enf.add_argument("--only-active", action="store_true")

    def _run_enf(a):
        import json
        from pathlib import Path

        ev = json.loads(Path(a.event).read_text(encoding="utf-8"))
        kg = json.loads(Path(a.kg).read_text(encoding="utf-8")) if a.kg else {}
        res = m8_4_enforce.enforce_event(
            asset_id=a.asset_id,
            event=ev,
            kg=kg,
            rule_ids=a.rule_ids,
            include_proposed=not a.only_active,
        )
        print(json.dumps(res, indent=2, ensure_ascii=False))

    p_enf.set_defaults(func=_run_enf)

    # Hook verifier
    verifiers["m8"] = m8_1_schema.verify
