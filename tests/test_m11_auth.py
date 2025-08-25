from modules.m11.m11_1_auth import AuthConfig, build_user_store, login_and_issue_token, verify_token


def test_login_and_verify_happy_path():
    cfg = AuthConfig(
        issuer="verdantis",
        default_ttl_seconds=600,
        users=[{"username": "alice", "password": "A!a12345", "role": "investor"}],
        roles={"investor": {"can": ["view_risk_trajectory"]}},
    )
    store = build_user_store(cfg)
    ok, msg, token = login_and_issue_token(
        store=store,
        username="alice",
        password="A!a12345",
        ttl_seconds=120,
        issuer=None,
    )
    assert ok and token and msg == "ok"
    vok, vmsg, payload = verify_token(token)
    assert vok and vmsg == "ok"
    assert payload.get("sub") == "alice"
    assert payload.get("role") == "investor"
