import json
from pathlib import Path


def verify() -> None:
    """Verify Module 0 (smoke)."""
    path = Path("data/processed/smoke_report.json")
    if not path.exists():
        raise SystemExit("[m0] MISSING: data/processed/smoke_report.json")
    data = json.loads(path.read_text(encoding="utf-8"))
    ok = bool(data.get("pass"))
    print(f"[m0] smoke_report.json pass={ok}")
    if not ok:
        raise SystemExit("[m0] FAILED")
    print("[m0] PASSED")
