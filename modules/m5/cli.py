# modules/m5/cli.py
from __future__ import annotations

import argparse
import os
import subprocess
from typing import Callable, Dict

from .m5_1_views import (
    create_assets_source_from_table,
    create_doc_index_norm_from_table,
    create_views,
    create_views_after_bind,
    verify,
)
from .m5_3_views import create_m53_views, verify_m53
from .m5_seed_assets import seed_from_geojson


def _cmd_map(_: argparse.Namespace) -> None:
    app_path = "services/map_app/app.py"
    if not os.path.exists(app_path):
        raise SystemExit("Map app not found at services/map_app/app.py")

    cmd = [
        "streamlit",
        "run",
        app_path,
        "--server.address",
        "127.0.0.1",
        "--server.port",
        "8501",
    ]

    env = os.environ.copy()
    # Ensure project root is on the module search path
    env["PYTHONPATH"] = os.pathsep.join([os.getcwd(), env.get("PYTHONPATH", "")]).rstrip(os.pathsep)

    print("🚀 Launching Streamlit:", " ".join(cmd))
    subprocess.run(cmd, check=False, env=env)


def _cmd_views(_: argparse.Namespace) -> None:
    create_views()
    print(
        "✅ M5.1 views created: vw_assets_source, vw_assets_basic, "
        "vw_doc_index_norm, vw_doc_citation_counts, vw_docs_with_assets, "
        "vw_asset_events_current, vw_asset_overlays"
    )


def _cmd_verify(_: argparse.Namespace) -> None:
    assets, events, assets_with_min = verify()
    print(
        "M5 verify → "
        f"assets={assets}, events(last60d)={events}, "
        f"assets_with_min_citations={assets_with_min}"
    )


def _cmd_seed(args: argparse.Namespace) -> None:
    inserted, _ = seed_from_geojson(args.file, args.table)
    print(f"✅ Seeded table '{args.table}' from {args.file}: " f"rows_upserted={inserted}")


def _split_schema_table(qualified: str) -> tuple[str, str]:
    if "." in qualified:
        s, t = qualified.split(".", 1)
        return s, t
    return "public", qualified


def _cmd_bind(args: argparse.Namespace) -> None:
    schema, table = _split_schema_table(args.table)
    mapping = {
        "id": args.id,
        "name": args.name,
        "geom": args.geom,
        "asset_type": args.asset_type or "",
        "city": args.city or "",
        "country": args.country or "",
    }
    create_assets_source_from_table(schema, table, mapping)
    create_views_after_bind()
    print("✅ Bound assets from " f"{schema}.{table} → vw_assets_source and built M5.1 views.")


def _cmd_bind_docs(args: argparse.Namespace) -> None:
    schema, table = _split_schema_table(args.table)
    mapping = {
        "doc_sha256": args.doc,
        "asset_id": args.asset,
        "title": args.title or "",
        "source": args.source or "",
        "url": args.url or "",
        "lang": args.lang or "",
        "published_list": args.published or "",
    }
    create_doc_index_norm_from_table(schema, table, mapping)
    create_views_after_bind()
    print("✅ Bound docs from " f"{schema}.{table} → vw_doc_index_norm and built M5.1 views.")


def register(
    subparsers: argparse._SubParsersAction,
    verifiers: Dict[str, Callable[[], None]],
) -> None:
    m5 = subparsers.add_parser("m5", help="Module 5 commands")
    m5_sub = m5.add_subparsers(dest="m5_cmd")

    p_views = m5_sub.add_parser("views", help="Auto-detect & create M5.1 views")
    p_views.set_defaults(func=_cmd_views)

    p_verify = m5_sub.add_parser("verify", help="Verify M5 health")
    p_verify.set_defaults(func=_cmd_verify)

    p_seed = m5_sub.add_parser("seed-assets", help="Seed minimal assets table " "from GeoJSON")
    p_seed.add_argument(
        "--file",
        default="data/raw/assets/assets.geojson",
        help="Path to assets GeoJSON",
    )
    p_seed.add_argument(
        "--table",
        default="assets",
        help="Target table name (default: assets)",
    )
    p_seed.set_defaults(func=_cmd_seed)

    p_bind = m5_sub.add_parser("bind", help="Bind an existing assets table")
    p_bind.add_argument("--table", required=True, help="schema.table or table")
    p_bind.add_argument("--id", required=True, help="ID column")
    p_bind.add_argument("--name", required=True, help="Name column")
    p_bind.add_argument("--geom", required=True, help="Geometry column")
    p_bind.add_argument("--asset-type", dest="asset_type", help="Asset type col")
    p_bind.add_argument("--city", help="City column")
    p_bind.add_argument("--country", help="Country column")
    p_bind.set_defaults(func=_cmd_bind)

    p_bindd = m5_sub.add_parser(
        "bind-docs", help="Bind an existing docs table to vw_doc_index_norm"
    )
    p_bindd.add_argument("--table", required=True, help="schema.table or table")
    p_bindd.add_argument("--doc", required=True, help="doc id/sha column")
    p_bindd.add_argument("--asset", required=True, help="asset_id column")
    p_bindd.add_argument(
        "--published",
        help="comma-separated publish columns "
        "(e.g. published_at,published,date,created_at,ingested_at)",
    )
    p_bindd.add_argument("--title", help="title column")
    p_bindd.add_argument("--source", help="source/publisher column")
    p_bindd.add_argument("--url", help="url/link column")
    p_bindd.add_argument("--lang", help="lang column")
    p_bindd.set_defaults(func=_cmd_bind_docs)

    p_map = m5_sub.add_parser("map", help="Run M5.2 Streamlit map")
    p_map.set_defaults(func=_cmd_map)

    verifiers["m5"] = lambda: _cmd_verify(argparse.Namespace())

    p_ev = m5_sub.add_parser("evidence-views", help="Create/refresh M5.3 evidence views")
    p_ev.set_defaults(func=lambda _: (create_m53_views(), print("✅ M5.3 views created.")))

    p_ev_v = m5_sub.add_parser("evidence-verify", help="Verify M5.3 evidence views")
    p_ev_v.set_defaults(
        func=lambda _: (
            lambda c, e, b: print(f"M5.3 verify → citations={c}, edges={e}, bundles={b}")
        )(*verify_m53())
    )
