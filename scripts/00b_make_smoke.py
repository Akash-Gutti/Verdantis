import json
import time
from pathlib import Path

import httpx

SERVICES = {
    "ingest": ("http://127.0.0.1:8001/health", None),
    "rag": ("http://127.0.0.1:8002/health", None),
    "vision": ("http://127.0.0.1:8003/health", None),
    "causal": ("http://127.0.0.1:8004/health", None),
    "zk": ("http://127.0.0.1:8005/health", None),
    "policy": ("http://127.0.0.1:8006/health", None),
}

REPORT = Path("data/processed/smoke_report.json")
EVENTS = Path("data/processed/events.log")


def check_health():
    rs = {}
    with httpx.Client(timeout=5) as client:
        for name, (url, _) in SERVICES.items():
            r = client.get(url)
            rs[name] = {
                "ok": r.status_code == 200,
                "resp": r.json() if r.status_code == 200 else None,
            }
    return rs


def do_roundtrip():
    with httpx.Client(timeout=5) as client:
        r1 = client.post(
            "http://127.0.0.1:8001/ingest/doc",
            json={
                "doc_id": "smoke-doc",
                "title": "Smoke ESG",
                "lang": "en",
                "text": "This is a smoke test document with emissions targets.",
            },
        )
        r2 = client.post("http://127.0.0.1:8002/ask", json={"query": "What are emissions targets?"})
        return {
            "ingest_status": r1.status_code,
            "ask_status": r2.status_code,
            "ask_json": r2.json() if r2.status_code == 200 else None,
        }


def check_events():
    if not EVENTS.exists():
        return 0
    lines = EVENTS.read_text(encoding="utf-8").strip().splitlines()
    return len(lines)


if __name__ == "__main__":
    health = check_health()
    rt = do_roundtrip()
    time.sleep(0.5)
    ev_count = check_events()
    result = {
        "health": health,
        "roundtrip": rt,
        "events_lines": ev_count,
        "pass": all(v["ok"] for v in health.values())
        and rt["ingest_status"] == 200
        and rt["ask_status"] == 200,
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print("Smoke:", "PASS" if result["pass"] else "FAIL", "- see", REPORT)
