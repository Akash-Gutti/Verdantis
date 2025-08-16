# modules/m1/cli.py
from __future__ import annotations

import argparse

from .m1_1_schema import schema
from .m1_2_constraints import constraints
from .m1_3_seed import seed
from .m1_3b_link_docs import link_docs
from .m1_4_export import export
from .verify import verify as verify_all


def register(sub: argparse._SubParsersAction, verifiers: dict) -> None:
    sp = sub.add_parser("m1.schema", help="Run M1.1 schema migration")
    sp.set_defaults(func=lambda _: schema())

    sp = sub.add_parser("m1.constraints", help="Run M1.2 constraints/indexes")
    sp.set_defaults(func=lambda _: constraints())

    sp = sub.add_parser("m1.seed", help="Run M1.3 seeding (assets+permits)")
    sp.set_defaults(func=lambda _: seed())

    sp = sub.add_parser("m1.link_docs", help="Run M1.3b link PDFs to documents")
    sp.set_defaults(func=lambda _: link_docs())

    sp = sub.add_parser("m1.export", help="Run M1.4 KG export (CSV+GraphML)")
    sp.set_defaults(func=lambda _: export())

    # Register M1’s verifier into the shared dict
    verifiers["m1"] = verify_all
