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
from .m12_2_ci import run_ci_cli
from .m12_3_eval import evaluate_causal, evaluate_change, evaluate_rag
from .m12_4_cards import CardInputs, run_cards_build


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


def _cmd_ci_run(args: Namespace) -> int:
    ok, report = run_ci_cli(
        report_path=Path(args.report),
        bundle_path=Path(args.bundle),
    )
    status = "OK" if ok else "FAILED"
    print(
        f"M12.2 ci-run → {status} (lint={report['lint']['ok']}, "
        f"tests={report['tests']['ok']}, bundle={report['bundle']['ok']}) "
        f"→ {args.report}, {args.bundle}"
    )
    return 0 if ok else 1


def _cmd_eval_rag(args: Namespace) -> int:
    res = evaluate_rag(input_path=Path(args.input), out_path=Path(args.out))
    print(
        "M12.3 eval-rag → "
        f"items={res.items} cite_f1_micro={res.cite_f1_micro} nli_acc={res.nli_accuracy} → {args.out}"
    )
    return 0


def _cmd_eval_causal(args: Namespace) -> int:
    res = evaluate_causal(input_path=Path(args.input), out_path=Path(args.out))
    print(
        "M12.3 eval-causal → "
        f"assets={res.assets} ΔRMSE_mean={res.delta_mean} placeboΔ={res.placebo_delta_mean} → {args.out}"
    )
    return 0


def _cmd_eval_change(args: Namespace) -> int:
    ks = [int(k) for k in (args.k or "").split(",") if k.strip().isdigit()] if args.k else None
    fracs = [float(x) for x in (args.frac or "").split(",") if x.strip()] if args.frac else None
    res = evaluate_change(
        input_path=Path(args.input),
        out_path=Path(args.out),
        k_list=ks,
        frac_list=fracs,
    )
    print(
        "M12.3 eval-change → "
        f"items={res.items} p@k={res.precision_at_k} p@frac={res.precision_at_frac} → {args.out}"
    )
    return 0


def _cmd_eval_all(args: Namespace) -> int:
    base = Path(args.dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    # RAG
    evaluate_rag(base / "rag_eval.json", out / "rag_report.json")
    # Causal
    evaluate_causal(base / "causal_eval.json", out / "causal_report.json")
    # Change
    evaluate_change(base / "change_eval.json", out / "change_report.json")
    print(f"M12.3 eval-all → read {base} → wrote reports to {out}")
    return 0


def _cmd_cards_build(args: Namespace) -> int:
    files = run_cards_build(
        inputs=CardInputs(
            metrics_prom=Path(args.metrics_prom),
            rag_report=Path(args.rag_report),
            causal_report=Path(args.causal_report),
            change_report=Path(args.change_report),
            ci_report=Path(args.ci_report) if args.ci_report else None,
        ),
        out_dir=Path(args.out_dir),
    )
    print(f"M12.4 cards-build → wrote {len(files)} files → {args.out_dir}")
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

    # CI commands
    p_ci = sp.add_parser("ci-run", help="Run CI locally: flake8, pytest, bundle, report")
    p_ci.add_argument("--report", default="data/observability/ci/ci_report.json")
    p_ci.add_argument("--bundle", default="dist/verdantis_bundle.zip")
    p_ci.set_defaults(func=_cmd_ci_run)

    # --- M12.3 eval commands ---
    p_er = sp.add_parser("eval-rag", help="Evaluate RAG veracity (citations + NLI)")
    p_er.add_argument("--input", default="data/eval/rag_eval.json")
    p_er.add_argument("--out", default="data/eval/reports/rag_report.json")
    p_er.set_defaults(func=_cmd_eval_rag)

    p_ec = sp.add_parser("eval-causal", help="Evaluate causal fit (pre/post RMSE + placebo)")
    p_ec.add_argument("--input", default="data/eval/causal_eval.json")
    p_ec.add_argument("--out", default="data/eval/reports/causal_report.json")
    p_ec.set_defaults(func=_cmd_eval_causal)

    p_eg = sp.add_parser("eval-change", help="Evaluate change detection (precision@K)")
    p_eg.add_argument("--input", default="data/eval/change_eval.json")
    p_eg.add_argument("--out", default="data/eval/reports/change_report.json")
    p_eg.add_argument("--k", default="1,5,10,20", help="Comma-separated Ks")
    p_eg.add_argument("--frac", default="0.1,0.2", help="Comma-separated fractions")
    p_eg.set_defaults(func=_cmd_eval_change)

    p_ea = sp.add_parser("eval-all", help="Run all evals from a directory")
    p_ea.add_argument("--dir", default="data/eval")
    p_ea.add_argument("--out-dir", default="data/eval/reports")
    p_ea.set_defaults(func=_cmd_eval_all)

    # -- M12.4 cards commands ---
    p_cards = sp.add_parser(
        "cards-build", help="Build model & data cards (Markdown) from artifacts"
    )
    p_cards.add_argument("--metrics-prom", default="data/observability/metrics.prom")
    p_cards.add_argument("--rag-report", default="data/eval/reports/rag_report.json")
    p_cards.add_argument("--causal-report", default="data/eval/reports/causal_report.json")
    p_cards.add_argument("--change-report", default="data/eval/reports/change_report.json")
    p_cards.add_argument("--ci-report", default="data/observability/ci/ci_report.json")
    p_cards.add_argument("--out-dir", default="docs/cards")
    p_cards.set_defaults(func=_cmd_cards_build)

    verifiers["m12"] = verify_m12
