"""CLI registrar for Module 12 (Observability) - M12.1 metrics & logs."""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Dict, Tuple

from .m12_1_obs import (
    MetricsSources,
    collect_metrics,
    ingest_audit_requests,
    ingest_channels_results,
    log_write,
    serve_metrics,
    write_prometheus_textfile,
)


def _cmd_metrics_export(args: Namespace) -> int:
    src = MetricsSources(
        filters_metrics=Path(args.filters_metrics),
        dedupe_metrics=Path(args.dedupe_metrics),
        channels_metrics=Path(args.channels_metrics),
        feed_metrics=Path(args.feed_metrics) if args.feed_metrics else None,
        reg_metrics=Path(args.reg_metrics) if args.reg_metrics else None,
        inv_metrics=Path(args.inv_metrics) if args.inv_metrics else None,
        pub_metrics=Path(args.pub_metrics) if args.pub_metrics else None,
    )
    m = collect_metrics(src)
    out_path = Path(args.out)
    write_prometheus_textfile(out_path, m)
    print(f"M12.1 metrics-export → series={len(m)} → {out_path}")
    return 0


def _cmd_logs_demo(args: Namespace) -> int:
    base = Path(args.dir)
    n = int(args.n)
    for i in range(n):
        log_write(
            base,
            "info",
            "demo",
            "m12.demo",
            "hello",
            {"i": i},
        )
    print(f"M12.1 logs-demo → wrote {n} lines → {base}")
    return 0


def _cmd_logs_ingest(args: Namespace) -> int:
    base = Path(args.dir)
    src = Path(args.path)
    if args.type == "channels_results":
        n = ingest_channels_results(src, base)
    elif args.type == "audit_requests":
        n = ingest_audit_requests(src, base)
    else:
        print(f"M12.1 logs-ingest → unknown type: {args.type}")
        return 2
    print(f"M12.1 logs-ingest → ingested={n} ({args.type}) → {base}")
    return 0


def _cmd_serve_metrics(args: Namespace) -> int:
    serve_metrics(metrics_file=Path(args.file), port=int(args.port))
    return 0


def verify_m12() -> Tuple[bool, str]:
    """Lightweight verify: ensure metrics.prom exists,
    is non-empty, and has at least one metric line."""
    p = Path("data/observability/metrics.prom")
    if not p.exists():
        msg = "M12 verify: metrics.prom not found. Run metrics-export."
        print(msg)
        return False, msg
    try:
        txt = p.read_text(encoding="utf-8")
        if not txt.strip():
            msg = "M12 verify: metrics.prom is empty."
            print(msg)
            return False, msg

        # Basic sanity: ensure there is at least one non-comment line (i.e., an actual metric)
        has_metric = any(
            line.strip() and not line.lstrip().startswith("#") for line in txt.splitlines()
        )
        if not has_metric:
            msg = "M12 verify: only comments found; no metrics emitted."
            print(msg)
            return False, msg

        print("M12 verify OK.")
        return True, "M12 verify OK."
    except Exception as exc:  # noqa: BLE001
        msg = f"M12 verify: cannot read metrics ({exc})."
        print(msg)
        return False, msg


def register(subparsers: ArgumentParser, verifiers: Dict[str, Any]) -> None:
    p = subparsers.add_parser("m12", help="Module 12 - Observability")

    sp = p.add_subparsers(dest="m12_cmd")

    p_me = sp.add_parser("metrics-export", help="Export Prometheus metrics textfile")
    p_me.add_argument("--filters-metrics", default="data/processed/m10/filters_metrics.json")
    p_me.add_argument("--dedupe-metrics", default="data/processed/m10/dedupe_metrics.json")
    p_me.add_argument("--channels-metrics", default="data/processed/m10/channels_metrics.json")
    p_me.add_argument("--feed-metrics", default="data/processed/m10/ui/alerts_feed_metrics.json")
    p_me.add_argument("--reg-metrics", default="data/processed/m11/portals/regulator/metrics.json")
    p_me.add_argument("--inv-metrics", default="data/processed/m11/portals/investor/metrics.json")
    p_me.add_argument("--pub-metrics", default="data/processed/m11/portals/public/metrics.json")
    p_me.add_argument("--out", default="data/observability/metrics.prom")
    p_me.set_defaults(func=_cmd_metrics_export)

    p_ld = sp.add_parser("logs-demo", help="Write N demo JSON log lines")
    p_ld.add_argument("--dir", default="data/observability/logs")
    p_ld.add_argument("--n", default="5")
    p_ld.set_defaults(func=_cmd_logs_demo)

    p_li = sp.add_parser("logs-ingest", help="Ingest module outputs into logs")
    p_li.add_argument("--type", required=True, choices=["channels_results", "audit_requests"])
    p_li.add_argument("--path", required=True, help="Source JSON path")
    p_li.add_argument("--dir", default="data/observability/logs")
    p_li.set_defaults(func=_cmd_logs_ingest)

    p_srv = sp.add_parser("serve-metrics", help="Serve /metrics from a textfile (dev)")
    p_srv.add_argument("--file", default="data/observability/metrics.prom")
    p_srv.add_argument("--port", default="9300")
    p_srv.set_defaults(func=_cmd_serve_metrics)

    verifiers["m12"] = verify_m12
