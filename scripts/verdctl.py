# scripts/verdctl.py
import argparse
import os
import sys


def main():
    # Make 'modules' importable when running this script directly
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # Local imports after sys.path adjustment (flake8-friendly)
    from modules import m0, m1
    from modules.m2 import schemas as m2s

    p = argparse.ArgumentParser(description="Verdantis control CLI (modules m0..m11)")
    sub = p.add_subparsers(dest="cmd")

    # --- module 1 actions ---
    sub.add_parser("m1.schema", help="Run M1.1 schema migration")
    sub.add_parser("m1.constraints", help="Run M1.2 constraints/indexes")
    sub.add_parser("m1.seed", help="Run M1.3 seeding (assets+permits)")
    sub.add_parser("m1.link_docs", help="Run M1.3b link PDFs to documents")
    sub.add_parser("m1.export", help="Run M1.4 KG export (CSV+GraphML)")

    # --- module 2 actions (M2.1) ---
    sub.add_parser("m2.schemas", help="(M2.1) check schemas in configs/schemas/")
    p_m2v = sub.add_parser("m2.validate", help="Validate event JSON(s) against schemas")
    grp = p_m2v.add_mutually_exclusive_group(required=True)
    grp.add_argument("--file", type=str, help="Path to a single event JSON")
    grp.add_argument("--dir", type=str, help="Directory of *.json events")
    sub.add_parser("m2.ls", help="List available schema contracts")
    sub.add_parser("m2.samples", help="Generate sample events into data/event_samples")

    # verify
    v = sub.add_parser("verify", help="Run verifiers")
    v.add_argument(
        "--module",
        "-m",
        default="all",
        help="Which module to verify: m0, m1, m2 or 'all'",
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

    # ---- M2.1 commands ----
    elif args.cmd == "m2.schemas":
        m2s.verify()  # verify presence/validity of checked-in schema files
        print("M2.1 schemas OK.")
    elif args.cmd == "m2.validate":
        if getattr(args, "file", None):
            m2s.validate_file(args.file)
        else:
            m2s.validate_dir(args.dir)
    elif args.cmd == "m2.ls":
        m2s.list_schemas()
    elif args.cmd == "m2.samples":
        m2s.make_samples()

    elif args.cmd == "verify":
        if args.module == "all":
            print("=== verify m0 ===")
            m0.verify()
            print("=== verify m1 ===")
            m1.verify()
            print("=== verify m2 ===")
            m2s.verify()
            print("ALL VERIFICATIONS PASSED")
        elif args.module == "m0":
            m0.verify()
        elif args.module == "m1":
            m1.verify()
        elif args.module == "m2":
            m2s.verify()
        else:
            raise SystemExit("Unknown module. Use m0, m1, m2, or all.")
    else:
        p.print_help()


if __name__ == "__main__":
    main()
