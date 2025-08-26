# scripts/00b_smoke.py
import json
import sys
import time
from pathlib import Path

import httpx

SERVICES = {
    "ingest": "http://127.0.0.1:8001/health",
    "rag": "http://127.0.0.1:8002/health",
    "vision": "http://127.0.0.1:8003/health",
    "causal": "http://127.0.0.1:8004/health",
    "zk": "http://127.0.0.1:8005/health",
    "policy": "http://127.0.0.1:8006/health",
    "alerts": "http://127.0.0.1:8007/health",
}

REPORT = Path("data/processed/00b_smoke_report.json")
EVENTS = Path("data/processed/events.log")


def check_health():
    out = {}
    with httpx.Client(timeout=5) as c:
        for name, url in SERVICES.items():
            try:
                r = c.get(url)
                out[name] = {
                    "ok": r.status_code == 200,
                    "status": r.status_code,
                    "body": r.json() if r.status_code == 200 else r.text,
                }
            except Exception as e:
                out[name] = {"ok": False, "error": repr(e)}
    return out


def warmup():
    with httpx.Client(timeout=60) as c:
        try:
            r = c.post("http://127.0.0.1:8002/warmup")
            return {
                "ok": r.status_code == 200,
                "status": r.status_code,
                "body": r.json() if r.status_code == 200 else r.text,
            }
        except Exception as e:
            return {"ok": False, "error": repr(e)}


def roundtrip():
    with httpx.Client(timeout=60) as c:
        try:
            r1 = c.post(
                "http://127.0.0.1:8001/ingest/doc",
                json={
                    "doc_id": "smoke-doc",
                    "title": "Smoke ESG",
                    "lang": "en",
                    "text": "This is a smoke test document with emissions targets.",
                },
            )
            r2 = c.post("http://127.0.0.1:8002/ask", json={"query": "What are emissions targets?"})
            return {
                "ingest": {"ok": r1.status_code == 200, "status": r1.status_code, "body": r1.text},
                "ask": {"ok": r2.status_code == 200, "status": r2.status_code, "body": r2.text},
            }
        except Exception as e:
            return {"error": repr(e)}


def check_index():
    p = Path("data/processed/index/INDEX_READY")
    return p.exists()


def check_events():
    if not EVENTS.exists():
        return 0
    try:
        return len(EVENTS.read_text(encoding="utf-8").strip().splitlines())
    except Exception:
        return -1


if __name__ == "__main__":
    health = check_health()
    warm = warmup()
    rt = roundtrip()
    time.sleep(0.5)
    ev = check_events()
    idx = check_index()

    ok_health = all(v.get("ok") for v in health.values())
    ok_warm = warm.get("ok", False)
    ok_rt = isinstance(rt, dict) and rt.get("ingest", {}).get("ok") and rt.get("ask", {}).get("ok")
    ok_idx = idx is True

    result = {
        "health": health,
        "warmup": warm,
        "roundtrip": rt,
        "events_lines": ev,
        "index_ready": ok_idx,
        "pass": ok_health and ok_warm and ok_rt,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print("=== 00b_smoke summary ===")
    print(json.dumps({k: (v if k == "pass" else "…") for k, v in result.items()}, indent=2))
    if not result["pass"]:
        print("\n--- DETAILS ---")
        print(json.dumps(result, indent=2))
        sys.exit(1)
    sys.exit(0)
