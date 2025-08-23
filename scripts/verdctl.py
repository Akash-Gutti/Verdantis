import argparse
import os
import sys


def main():
    # make 'modules' importable when running directly
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # base modules
    from modules import m0  # m0 is simple for now

    # dynamic subcommand registry and module verifiers map
    verifiers = {"m0": m0.verify}

    parser = argparse.ArgumentParser(prog="verdctl", description="Verdantis CLI")
    sub = parser.add_subparsers(dest="cmd")

    # global verify command (modules plug into 'verifiers' below)
    p_verify = sub.add_parser("verify", help="Run verifiers")
    p_verify.add_argument(
        "--module", "-m", default="all", help="Module to verify: m0, m1, m2, ... or 'all'"
    )

    # === modules register their own CLI here ===
    from modules.m1 import cli as m1cli

    m1cli.register(sub, verifiers)

    from modules.m2 import cli as m2cli

    m2cli.register(sub, verifiers)

    from modules.m3 import cli as m3cli

    m3cli.register(sub, verifiers)

    from modules.m4 import cli as m4cli

    m4cli.register(sub, verifiers)

    from modules.m5 import cli as m5cli

    m5cli.register(sub, verifiers)

    from modules.m6 import cli as m6cli

    m6cli.register(sub, verifiers)

    args = parser.parse_args()

    if args.cmd == "verify":
        if args.module == "all":
            for key in sorted(verifiers.keys()):
                print(f"=== verify {key} ===")
                verifiers[key]()  # call the module's verify
            print("ALL VERIFICATIONS PASSED")
            return 0
        if args.module not in verifiers:
            raise SystemExit(
                f"Unknown module '{args.module}'. Known: {', '.join(sorted(verifiers))} or 'all'."
            )
        verifiers[args.module]()
        return 0

    # generic dispatch: every subcommand sets args.func
    if hasattr(args, "func"):
        return args.func(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
