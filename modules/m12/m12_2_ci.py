"""M12.2 - CI runner helpers: lint, test, bundle, report."""

from __future__ import annotations

import json
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class CiResult:
    ok: bool
    rc: int
    seconds: float
    stdout: str
    stderr: str


def _run_cmd(args: List[str], cwd: Path | None = None) -> CiResult:
    t0 = time.time()
    p = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        text=True,
        shell=False,
    )
    dt = round(time.time() - t0, 3)
    return CiResult(
        ok=(p.returncode == 0),
        rc=int(p.returncode),
        seconds=dt,
        stdout=p.stdout.strip(),
        stderr=p.stderr.strip(),
    )


def _bundle_zip(out_zip: Path, include: List[Path]) -> Tuple[bool, int]:
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in include:
            if p.is_dir():
                for sub in p.rglob("*"):
                    if sub.is_file():
                        zf.write(sub, sub.as_posix())
                        count += 1
            elif p.is_file():
                zf.write(p, p.as_posix())
                count += 1
    return True, count


def run_ci_cli(report_path: Path, bundle_path: Path) -> Tuple[bool, Dict[str, Any]]:
    """Run flake8, pytest (scoped), and build a bundle zip. Return (ok, report)."""
    python = sys.executable or "python"

    flake8_args = [
        python,
        "-m",
        "flake8",
        ".",
        "--exclude",
        ".git,__pycache__,venv,.venv,env,build,dist,data",
        "--max-line-length",
        "120",
        "--extend-ignore",
        "E203,W503",
    ]
    lint = _run_cmd(flake8_args)

    tests_to_run = [
        "tests/test_m10_filters.py",
        "tests/test_m11_auth.py",
    ]
    existing = [p for p in tests_to_run if Path(p).exists()]
    pytest_args = [python, "-m", "pytest", "-q"] + existing
    tests = _run_cmd(pytest_args)

    include = [
        Path("modules"),
        Path("configs"),
        Path("scripts/verdctl.py"),
        Path("README.md"),
    ]
    bundle_ok, bundle_count = _bundle_zip(bundle_path, include)

    report: Dict[str, Any] = {
        "lint": {
            "ok": lint.ok,
            "rc": lint.rc,
            "seconds": lint.seconds,
            "stdout": lint.stdout,
            "stderr": lint.stderr,
        },
        "tests": {
            "ok": tests.ok,
            "rc": tests.rc,
            "seconds": tests.seconds,
            "stdout": tests.stdout,
            "stderr": tests.stderr,
        },
        "bundle": {
            "ok": bool(bundle_ok),
            "path": str(bundle_path),
            "files": int(bundle_count),
        },
        "summary_ok": bool(lint.ok and tests.ok and bundle_ok),
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return bool(report["summary_ok"]), report
