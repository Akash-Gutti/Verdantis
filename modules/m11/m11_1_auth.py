"""M11.1 - Auth & Roles.

- Build a hashed user store from a demo config.
- Issue/verify HMAC-signed tokens with role claims.
- Pure-Python, flake8-friendly; no external deps.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# -------------------- Config & Store --------------------


@dataclass(frozen=True)
class UserEntry:
    username: str
    role: str
    salt: str
    pw_hash: str  # hex


@dataclass(frozen=True)
class AuthConfig:
    issuer: str
    default_ttl_seconds: int
    users: List[Dict[str, str]]
    roles: Dict[str, Dict[str, Any]]


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def load_auth_config(path: Path) -> AuthConfig:
    raw = _read_json(path)
    return AuthConfig(
        issuer=str(raw.get("issuer", "verdantis")),
        default_ttl_seconds=int(raw.get("default_ttl_seconds", 86400)),
        users=list(raw.get("users", [])),
        roles=dict(raw.get("roles", {})),
    )


# -------------------- Password hashing --------------------


def _hash_password(password: str, salt: Optional[bytes] = None) -> Tuple[str, str]:
    """Return (salt_hex, hash_hex) using PBKDF2-HMAC-SHA256."""
    if salt is None:
        salt = secrets.token_bytes(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return salt.hex(), hashed.hex()


def _verify_password(password: str, salt_hex: str, hash_hex: str) -> bool:
    salt = bytes.fromhex(salt_hex)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    return hmac.compare_digest(hashed.hex(), hash_hex)


# -------------------- Token signing (JWT-ish) --------------------


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_json(obj: Dict[str, Any]) -> str:
    return _b64url(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _sign(msg: bytes, secret: bytes) -> str:
    sig = hmac.new(secret, msg, hashlib.sha256).digest()
    return _b64url(sig)


def _get_secret() -> bytes:
    # DEV secret; override with env var for safety
    secret = os.getenv("PORTALS_AUTH_SECRET", "dev-secret-change-me")
    return secret.encode("utf-8")


def issue_token(username: str, role: str, issuer: str, ttl_seconds: int) -> str:
    """Create a compact token: header.payload.signature (HMAC-SHA256)."""
    header = {"alg": "HS256", "typ": "JWT"}
    now = int(time.time())
    payload = {
        "iss": issuer,
        "sub": username,
        "role": role,
        "iat": now,
        "exp": now + int(ttl_seconds),
    }
    h = _b64url_json(header)
    p = _b64url_json(payload)
    msg = f"{h}.{p}".encode("ascii")
    sig = _sign(msg, _get_secret())
    return f"{h}.{p}.{sig}"


def verify_token(token: str) -> Tuple[bool, str, Dict[str, Any]]:
    """Verify token and return (ok, msg, payload)."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return False, "malformed", {}
        h_b64, p_b64, sig = parts
        msg = f"{h_b64}.{p_b64}".encode("ascii")
        exp_sig = _sign(msg, _get_secret())
        if not hmac.compare_digest(exp_sig, sig):
            return False, "bad_signature", {}
        # Decode payload
        pad = "=" * (-len(p_b64) % 4)
        payload_bytes = base64.urlsafe_b64decode(p_b64 + pad)
        payload = json.loads(payload_bytes.decode("utf-8"))
        now = int(time.time())
        if int(payload.get("exp", 0)) < now:
            return False, "expired", payload
        return True, "ok", payload
    except Exception as exc:  # noqa: BLE001
        return False, f"verify_error:{exc}", {}


# -------------------- Store build & login --------------------


def build_user_store(cfg: AuthConfig) -> Dict[str, Any]:
    """Hash passwords from config into a user store dict."""
    entries: List[UserEntry] = []
    for u in cfg.users:
        username = str(u["username"])
        role = str(u["role"])
        salt_hex, hash_hex = _hash_password(str(u["password"]))
        entries.append(UserEntry(username=username, role=role, salt=salt_hex, pw_hash=hash_hex))
    store = {
        "issuer": cfg.issuer,
        "roles": cfg.roles,
        "users": [
            {"username": e.username, "role": e.role, "salt": e.salt, "pw_hash": e.pw_hash}
            for e in entries
        ],
        "created_at": int(time.time()),
        "version": 1,
    }
    return store


def save_user_store(path: Path, store: Dict[str, Any]) -> None:
    _write_json(path, store)


def load_user_store(path: Path) -> Dict[str, Any]:
    return _read_json(path)


def login_and_issue_token(
    store: Dict[str, Any],
    username: str,
    password: str,
    ttl_seconds: Optional[int],
    issuer: Optional[str],
) -> Tuple[bool, str, Optional[str]]:
    """Validate credentials, issue token if ok."""
    issuer_eff = issuer or str(store.get("issuer", "verdantis"))
    ttl_eff = int(ttl_seconds) if ttl_seconds else 86400

    users = store.get("users", [])
    rec = next((u for u in users if u.get("username") == username), None)
    if not rec:
        return False, "unknown_user", None
    if not _verify_password(password, str(rec["salt"]), str(rec["pw_hash"])):
        return False, "invalid_password", None
    role = str(rec.get("role", "public"))
    token = issue_token(username, role, issuer_eff, ttl_eff)
    return True, "ok", token
