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

    verifiers["m11"] = verify_m11
