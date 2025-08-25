"""CLI registrar for Module 11 (Role-Based Portals) - M11.1 Auth & Roles."""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Dict, Tuple

from .m11_1_auth import (
    build_user_store,
    load_auth_config,
    load_user_store,
    login_and_issue_token,
    save_user_store,
    verify_token,
)
from .m11_2_regulator import InputsCfg, run_regulator_build, run_regulator_request_audit


def _cmd_auth_build(args: Namespace) -> int:
    cfg = load_auth_config(Path(args.config))
    store = build_user_store(cfg)
    save_user_store(Path(args.out), store)
    print(f"M11.1 auth-build → wrote {args.out} (users={len(store.get('users', []))})")
    return 0


def _cmd_auth_login(args: Namespace) -> int:
    store = load_user_store(Path(args.store))
    ok, msg, token = login_and_issue_token(
        store=store,
        username=str(args.username),
        password=str(args.password),
        ttl_seconds=int(args.ttl) if args.ttl else None,
        issuer=args.issuer,
    )
    if not ok:
        print(f"M11.1 auth-login → FAILED: {msg}")
        return 1
    outp = Path(args.token_out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(token or "", encoding="utf-8")
    print(f"M11.1 auth-login → OK: token saved to {outp}")
    return 0


def _cmd_auth_verify(args: Namespace) -> int:
    token: str
    if args.token:
        token = str(args.token)
    else:
        token = Path(args.token_file).read_text(encoding="utf-8").strip()
    ok, msg, payload = verify_token(token)
    if ok:
        print(f"M11.1 auth-verify → OK: role={payload.get('role')} sub={payload.get('sub')}")
        return 0
    print(f"M11.1 auth-verify → FAILED: {msg}")
    return 1


def _require_regulator(token: str) -> Tuple[bool, str, Dict[str, Any]]:
    ok, msg, payload = verify_token(token)
    if not ok:
        return False, f"auth_failed:{msg}", {}
    role = str(payload.get("role", ""))
    if role != "regulator":
        return False, f"forbidden_role:{role}", {}
    return True, "ok", payload


def _cmd_reg_build(args: Namespace) -> int:
    token = Path(args.token_file).read_text(encoding="utf-8").strip()
    ok, msg, payload = _require_regulator(token)
    if not ok:
        print(f"M11.2 reg-build → {msg}")
        return 1

    inputs = InputsCfg(
        deduped_events_path=Path(args.deduped),
        alerts_feed_path=Path(args.feed) if args.feed else None,
        assets_geojson_path=Path(args.assets_geojson) if args.assets_geojson else None,
        bundles_index_path=Path(args.bundles_index) if args.bundles_index else None,
    )
    out_dir = Path(args.out_dir)
    vio, hm = run_regulator_build(inputs, out_dir)
    print(f"M11.2 reg-build → violations={vio}, heatmap_assets={hm} → {out_dir}")
    return 0


def _cmd_reg_request_audit(args: Namespace) -> int:
    token = Path(args.token_file).read_text(encoding="utf-8").strip()
    ok, msg, payload = _require_regulator(token)
    if not ok:
        print(f"M11.2 reg-request-audit → {msg}")
        return 1
    username = str(payload.get("sub", "unknown"))
    role = str(payload.get("role", "regulator"))
    req_id = run_regulator_request_audit(
        out_log=Path(args.out_log),
        username=username,
        role=role,
        asset_id=args.asset_id,
        bundle_id=args.bundle_id,
        reason=args.reason,
    )
    print(
        f"M11.2 reg-request-audit → queued request_id={req_id} "
        f"(asset_id={args.asset_id}, bundle_id={args.bundle_id}) → {args.out_log}"
    )
    return 0


def verify_m11() -> Tuple[bool, str]:
    """Lightweight verify: ensure user store exists and is well-formed."""
    store_path = Path("data/processed/m11/auth/users_store.json")
    if not store_path.exists():
        msg = "M11 verify: users_store.json not found. Run auth-build."
        print(msg)
        return False, msg
    try:
        import json  # local import to keep top clean

        with store_path.open("r", encoding="utf-8") as f:
            store: Dict[str, Any] = json.load(f)

        if "users" not in store or not isinstance(store["users"], list):
            msg = "M11 verify: invalid store format (missing 'users' list)."
            print(msg)
            return False, msg

        # Basic element check to mirror m10-style sanity validation
        _ = int(len(store["users"]))  # ensure iterable/list-like

        print("M11 verify OK.")
        return True, "M11 verify OK."
    except Exception as exc:  # noqa: BLE001
        msg = f"M11 verify: cannot read store ({exc})."
        print(msg)
        return False, msg


def register(subparsers: ArgumentParser, verifiers: Dict[str, Any]) -> None:
    p = subparsers.add_parser("m11", help="Module 11 - Role-Based Portals")
    sp = p.add_subparsers(dest="m11_cmd")

    p_build = sp.add_parser("auth-build", help="Build hashed user store from config")
    p_build.add_argument("--config", default="configs/m11_auth.json")
    p_build.add_argument("--out", default="data/processed/m11/auth/users_store.json")
    p_build.set_defaults(func=_cmd_auth_build)

    p_login = sp.add_parser("auth-login", help="Login with username/password and issue token")
    p_login.add_argument("--store", default="data/processed/m11/auth/users_store.json")
    p_login.add_argument("--username", required=True)
    p_login.add_argument("--password", required=True)
    p_login.add_argument("--ttl", default=None, help="Token TTL seconds (optional)")
    p_login.add_argument("--issuer", default=None, help="Override issuer (optional)")
    p_login.add_argument("--token-out", default="data/processed/m11/auth/tokens/demo.jwt")
    p_login.set_defaults(func=_cmd_auth_login)

    p_verify = sp.add_parser("auth-verify", help="Verify a token (string or file)")
    group = p_verify.add_mutually_exclusive_group(required=True)
    group.add_argument("--token", default=None, help="Token string")
    group.add_argument("--token-file", default=None, help="Path to token file")
    p_verify.set_defaults(func=_cmd_auth_verify)

    # Regulator portal subcommands (M11.2)
    p_reg_build = sp.add_parser(
        "reg-build", help="Build regulator portal data (heatmap + open violations)"
    )
    p_reg_build.add_argument("--token-file", required=True, help="JWT from m11 auth-login")
    p_reg_build.add_argument(
        "--deduped",
        default="data/processed/m10/filtered_events_deduped.json",
        help="M10.3 deduped events JSON",
    )
    p_reg_build.add_argument(
        "--feed",
        default="data/processed/m10/ui/alerts_feed.json",
        help="Optional alerts feed JSON (unused for now, reserved)",
    )
    p_reg_build.add_argument(
        "--assets-geojson",
        default="data/raw/assets/assets.geojson",
        help="Optional assets GeoJSON for lat/lon enrichment",
    )
    p_reg_build.add_argument(
        "--bundles-index",
        default="data/processed/m9/index.json",
        help="Optional zk bundle index for validation",
    )
    p_reg_build.add_argument(
        "--out-dir",
        default="data/processed/m11/portals/regulator",
        help="Output directory for portal JSON files",
    )
    p_reg_build.set_defaults(func=_cmd_reg_build)

    p_req = sp.add_parser("reg-request-audit", help="Create an audit-pack request (queued)")
    p_req.add_argument("--token-file", required=True, help="JWT from m11 auth-login")
    p_req.add_argument("--asset-id", default=None, help="Target asset ID (optional)")
    p_req.add_argument("--bundle-id", default=None, help="Target bundle ID (optional)")
    p_req.add_argument("--reason", default=None, help="Reason/context (optional)")
    p_req.add_argument(
        "--out-log",
        default="data/processed/m11/portals/regulator/audit_requests.json",
        help="Append-only request log JSON",
    )
    p_req.set_defaults(func=_cmd_reg_request_audit)

    verifiers["m11"] = verify_m11
