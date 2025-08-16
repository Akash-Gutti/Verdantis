# modules/m2/cli.py
from __future__ import annotations

import argparse

from . import agents as m2a
from . import flow as m2f
from . import replay as m2r
from . import schemas as m2s


# ---- handlers ----
def _handle_m2_schemas(_: argparse.Namespace) -> None:
    m2s.verify()
    print("M2.1 schemas OK.")


def _handle_m2_validate(args: argparse.Namespace) -> None:
    if getattr(args, "file", None):
        m2s.validate_file(args.file)
    else:
        m2s.validate_dir(args.dir)


def _handle_m2_ls(_: argparse.Namespace) -> None:
    m2s.list_schemas()


def _handle_m2_samples(_: argparse.Namespace) -> None:
    m2s.make_samples()


def _handle_m2_agent_process(args: argparse.Namespace) -> None:
    if getattr(args, "file", None):
        m2a.agent_process_file(args.file)
    else:
        m2a.agent_process_dir(args.dir)


def _handle_m2_agent_verify(_: argparse.Namespace) -> None:
    m2a.verify()


def _handle_m2_flow_init(_: argparse.Namespace) -> None:
    # create bus dirs
    from .flow import _ensure_dirs  # local import keeps namespace tidy

    _ensure_dirs()
    print("M2.3 file-bus directories ready at data/bus")


def _handle_m2_flow_dispatch(args: argparse.Namespace) -> None:
    disp = m2f.FileBusDispatcher(agent_name="bus-consumer")
    if getattr(args, "loop", False):
        disp.dispatch_loop(interval=args.interval)
    else:
        results = disp.dispatch_once()
        for r in results:
            print(f"{r.status.upper()}: {r.path.name} key={r.idempotency_key} id={r.event_id}")


def _handle_m2_flow_verify(_: argparse.Namespace) -> None:
    m2f.FileBusDispatcher(agent_name="bus-consumer").verify()


def _handle_m2_replay_topics(args: argparse.Namespace) -> None:
    m2r.replay_from_topics(
        topic=args.topic,
        start_iso=args.since,
        end_iso=args.until,
        limit=args.limit,
        sleep=args.sleep,
        dry_run=args.dry_run,
    )


def _handle_m2_replay_db(args: argparse.Namespace) -> None:
    m2r.replay_from_db(
        event_types=args.etypes,
        start_iso=args.since,
        end_iso=args.until,
        limit=args.limit,
        sleep=args.sleep,
        dry_run=args.dry_run,
    )


def _handle_m2_replay_verify(_: argparse.Namespace) -> None:
    m2r.verify()


def _verify_all() -> None:
    """Aggregate verifier for the whole of Module 2."""
    m2s.verify()
    m2a.verify()
    m2f.FileBusDispatcher(agent_name="bus-consumer").verify()
    m2r.verify()


# ---- registrar ----
def register(sub: argparse._SubParsersAction, verifiers: dict) -> None:
    # M2.1
    sp = sub.add_parser("m2.schemas", help="(M2.1) check schemas in configs/schemas/")
    sp.set_defaults(func=_handle_m2_schemas)

    sp = sub.add_parser("m2.validate", help="Validate event JSON(s) against schemas")
    grp = sp.add_mutually_exclusive_group(required=True)
    grp.add_argument("--file", type=str, help="Path to a single event JSON")
    grp.add_argument("--dir", type=str, help="Directory of *.json events")
    sp.set_defaults(func=_handle_m2_validate)

    sp = sub.add_parser("m2.ls", help="List available schema contracts")
    sp.set_defaults(func=_handle_m2_ls)

    sp = sub.add_parser("m2.samples", help="Generate sample events into data/event_samples")
    sp.set_defaults(func=_handle_m2_samples)

    # M2.2
    sp = sub.add_parser("m2.agent.process", help="Process event file(s) into DB with idempotency")
    grp = sp.add_mutually_exclusive_group(required=True)
    grp.add_argument("--file", type=str, help="Path to a single event JSON")
    grp.add_argument("--dir", type=str, help="Directory of *.json events")
    sp.set_defaults(func=_handle_m2_agent_process)

    sp = sub.add_parser("m2.agent.verify", help="Verify agent dedupe gate")
    sp.set_defaults(func=_handle_m2_agent_verify)

    # M2.3
    sp = sub.add_parser("m2.flow.init", help="Create file-bus directories")
    sp.set_defaults(func=_handle_m2_flow_init)

    sp = sub.add_parser("m2.flow.dispatch", help="Run file-bus dispatcher")
    sp.add_argument(
        "--dir", type=str, default="data/bus", help="Bus root (default: data/bus)"
    )  # informative
    sp.add_argument("--loop", action="store_true", help="Keep running (polling)")
    sp.add_argument(
        "--interval", type=float, default=1.0, help="Polling interval seconds (default: 1.0)"
    )
    sp.set_defaults(func=_handle_m2_flow_dispatch)

    sp = sub.add_parser("m2.flow.verify", help="Verify publish→consume loop and dedupe")
    sp.set_defaults(func=_handle_m2_flow_verify)

    # M2.4
    sp = sub.add_parser("m2.replay.topics", help="Replay mirrored events by topic/time window")
    sp.add_argument(
        "--topic", type=str, default="all", help="Topic name (e.g., doc.ingested) or 'all'"
    )
    sp.add_argument(
        "--from", dest="since", type=str, default=None, help="Start ISO datetime (inclusive)"
    )
    sp.add_argument(
        "--to", dest="until", type=str, default=None, help="End ISO datetime (exclusive)"
    )
    sp.add_argument("--limit", type=int, default=None, help="Max events to replay")
    sp.add_argument("--sleep", type=float, default=0.0, help="Delay between emits (seconds)")
    sp.add_argument("--dry-run", action="store_true", help="Print what would be enqueued")
    sp.set_defaults(func=_handle_m2_replay_topics)

    sp = sub.add_parser("m2.replay.db", help="Replay events from DB by time window")
    sp.add_argument(
        "--event-type",
        action="append",
        dest="etypes",
        help="Repeatable. e.g., --event-type verdantis.DocumentIngested",
    )
    sp.add_argument(
        "--from", dest="since", type=str, default=None, help="Start ISO datetime (inclusive)"
    )
    sp.add_argument(
        "--to", dest="until", type=str, default=None, help="End ISO datetime (exclusive)"
    )
    sp.add_argument("--limit", type=int, default=None, help="Max events to replay")
    sp.add_argument("--sleep", type=float, default=0.0, help="Delay between emits (seconds)")
    sp.add_argument("--dry-run", action="store_true", help="Print what would be enqueued")
    sp.set_defaults(func=_handle_m2_replay_db)

    sp = sub.add_parser(
        "m2.replay.verify", help="Verify replay preserves idempotency and lands in dup/"
    )
    sp.set_defaults(func=_handle_m2_replay_verify)

    # register combined verifier
    verifiers["m2"] = _verify_all
