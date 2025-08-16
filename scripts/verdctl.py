# scripts/verdctl.py
import argparse
import os
import sys

from modules import m0, m1

# Add Verdantis parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def main():
    p = argparse.ArgumentParser(description="Verdantis control CLI (modules m0..m11)")
    sub = p.add_subparsers(dest="cmd")

    # --- module 1 actions ---
    sub.add_parser("m1.schema", help="Run M1.1 schema migration")
    sub.add_parser("m1.constraints", help="Run M1.2 constraints/indexes")
    sub.add_parser("m1.seed", help="Run M1.3 seeding (assets+permits)")
    sub.add_parser("m1.link_docs", help="Run M1.3b link PDFs to documents")
    sub.add_parser("m1.export", help="Run M1.4 KG export (CSV+GraphML)")

    # verify
    v = sub.add_parser("verify", help="Run verifiers")
    v.add_argument(
        "--module",
        "-m",
        default="all",
        help="Which module to verify: m0, m1 or 'all'",
    )

    args = p.parse_args()

    if args.cmd == "m1.schema":
        m1.schema()
        print("M1.1 done.")
    elif args.cmd == "m1.constraints":
        m1.constraints()
        print("M1.2 done.")
    elif args.cmd == "m1.seed":
        m1.seed()
        print("M1.3 done.")
    elif args.cmd == "m1.link_docs":
        m1.link_docs()
        print("M1.3b done.")
    elif args.cmd == "m1.export":
        m1.export()
        print("M1.4 done.")
    elif args.cmd == "verify":
        if args.module == "all":
            print("=== verify m0 ===")
            m0.verify()
            print("=== verify m1 ===")
            m1.verify()
            print("ALL VERIFICATIONS PASSED")
        elif args.module == "m0":
            m0.verify()
        elif args.module == "m1":
            m1.verify()
        else:
            raise SystemExit("Unknown module. Use m0, m1, or all.")
    else:
        p.print_help()


if __name__ == "__main__":
    main()
