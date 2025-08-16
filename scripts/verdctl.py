# scripts/verdctl.py
import argparse
import os
import sys
from pathlib import Path


def main():
    # Make 'modules' importable when running this script directly
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    # Local imports after sys.path adjustment
    from modules import m0, m1
    from modules.m2 import agents as m2a
    from modules.m2 import flow as m2f
    from modules.m2 import replay as m2r
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

    # --- module 2 actions (M2.2) ---
    p_m2a = sub.add_parser(
        "m2.agent.process", help="Process event file(s) into DB with idempotency"
    )
    grp2 = p_m2a.add_mutually_exclusive_group(required=True)
    grp2.add_argument("--file", type=str, help="Path to a single event JSON")
    grp2.add_argument("--dir", type=str, help="Directory of *.json events")
    sub.add_parser("m2.agent.verify", help="Verify agent dedupe gate (process same event twice)")

    # --- module 2 actions (M2.3) ---
    sub.add_parser(
        "m2.flow.init", help="Create file-bus directories (inbox/processing/done/dup/err/topics)"
    )
    p_flow = sub.add_parser("m2.flow.dispatch", help="Run file-bus dispatcher")
    p_flow.add_argument(
        "--dir", type=str, default="data/bus", help="Bus root directory (default: data/bus)"
    )
    p_flow.add_argument("--loop", action="store_true", help="Keep running (polling)")
    p_flow.add_argument(
        "--interval", type=float, default=1.0, help="Polling interval seconds (default: 1.0)"
    )
    sub.add_parser("m2.flow.verify", help="Verify publish→consume loop and dedupe")

    # --- module 2 actions (M2.4) ---
    p_rt = sub.add_parser("m2.replay.topics", help="Replay mirrored events by topic/time window")
    p_rt.add_argument(
        "--topic", type=str, default="all", help="Topic name (e.g., doc.ingested) or 'all'"
    )
    p_rt.add_argument(
        "--from", dest="since", type=str, default=None, help="Start ISO datetime (inclusive)"
    )
    p_rt.add_argument(
        "--to", dest="until", type=str, default=None, help="End ISO datetime (exclusive)"
    )
    p_rt.add_argument("--limit", type=int, default=None, help="Max events to replay")
    p_rt.add_argument("--sleep", type=float, default=0.0, help="Delay between emits (seconds)")
    p_rt.add_argument("--dry-run", action="store_true", help="Print what would be enqueued")

    p_rd = sub.add_parser("m2.replay.db", help="Replay events from DB by time window")
    p_rd.add_argument(
        "--event-type",
        action="append",
        dest="etypes",
        help="Repeatable. e.g., --event-type verdantis.DocumentIngested",
    )
    p_rd.add_argument(
        "--from", dest="since", type=str, default=None, help="Start ISO datetime (inclusive)"
    )
    p_rd.add_argument(
        "--to", dest="until", type=str, default=None, help="End ISO datetime (exclusive)"
    )
    p_rd.add_argument("--limit", type=int, default=None, help="Max events to replay")
    p_rd.add_argument("--sleep", type=float, default=0.0, help="Delay between emits (seconds)")
    p_rd.add_argument("--dry-run", action="store_true", help="Print what would be enqueued")

    sub.add_parser("m2.replay.verify", help="Verify replay preserves idempotency and lands in dup/")

    # verify (global)
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
        m2s.verify()
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

    # ---- M2.2 commands ----
    elif args.cmd == "m2.agent.process":
        if getattr(args, "file", None):
            m2a.agent_process_file(args.file)
        else:
            m2a.agent_process_dir(args.dir)
    elif args.cmd == "m2.agent.verify":
        m2a.verify()

    # ---- M2.3 commands ----
    elif args.cmd == "m2.flow.init":
        # just ensuring directories exists
        from modules.m2.flow import _ensure_dirs  # local import to keep namespace lean

        _ensure_dirs()
        print("M2.3 file-bus directories ready at data/bus")
    elif args.cmd == "m2.flow.dispatch":
        disp = m2f.FileBusDispatcher(agent_name="bus-consumer")
        # override root if requested (supports custom dirs)
        if args.dir and Path(args.dir).resolve() != (Path("data") / "bus").resolve():
            print("Note: custom --dir set; create it manually to avoid surprises.")
        if args.loop:
            disp.dispatch_loop(interval=args.interval)
        else:
            results = disp.dispatch_once()
            for r in results:
                print(f"{r.status.upper()}: {r.path.name} key={r.idempotency_key} id={r.event_id}")
    elif args.cmd == "m2.flow.verify":
        disp = m2f.FileBusDispatcher(agent_name="bus-consumer")
        disp.verify()

    # ---- M2.4 commands ----
    elif args.cmd == "m2.replay.topics":
        m2r.replay_from_topics(
            topic=args.topic,
            start_iso=args.since,
            end_iso=args.until,
            limit=args.limit,
            sleep=args.sleep,
            dry_run=args.dry_run,
        )
    elif args.cmd == "m2.replay.db":
        m2r.replay_from_db(
            event_types=args.etypes,
            start_iso=args.since,
            end_iso=args.until,
            limit=args.limit,
            sleep=args.sleep,
            dry_run=args.dry_run,
        )
    elif args.cmd == "m2.replay.verify":
        m2r.verify()

    elif args.cmd == "verify":
        if args.module == "all":
            print("=== verify m0 ===")
            m0.verify()
            print("=== verify m1 ===")
            m1.verify()
            print("=== verify m2 (schemas) ===")
            m2s.verify()
            print("=== verify m2 (agent) ===")
            m2a.verify()
            print("=== verify m2 (flow) ===")
            m2f.FileBusDispatcher().verify()
            print("=== verify m2 (replay) ===")
            m2r.verify()
            print("ALL VERIFICATIONS PASSED")
        elif args.module == "m0":
            m0.verify()
        elif args.module == "m1":
            m1.verify()
        elif args.module == "m2":
            m2s.verify()
            m2a.verify()
            m2f.FileBusDispatcher().verify()
            m2r.verify()
        else:
            raise SystemExit("Unknown module. Use m0, m1, m2, or all.")
    else:
        p.print_help()


if __name__ == "__main__":
    main()
