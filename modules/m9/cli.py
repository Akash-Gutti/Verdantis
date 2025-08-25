"""CLI registrar for Module 9 (zk-Attested Compliance)."""

from __future__ import annotations

import json
from argparse import _SubParsersAction
from pathlib import Path
from typing import Callable, Dict

from . import m9_1_commit
from .m9_2_issue_verify import IssueRequest, VerifyRequest, issue_bundle, verify_bundle


def register(subparsers: _SubParsersAction, verifiers: Dict[str, Callable[[], None]]) -> None:
    """Register M9 subcommands with verdctl."""
    parser = subparsers.add_parser("m9", help="Module 9: zk-Attested Compliance")
    sp = parser.add_subparsers(dest="m9_cmd")

    # m9 commit
    p_commit = sp.add_parser("commit", help="Compute feature commitment (M9.1)")
    p_commit.add_argument("--features", help='Inline JSON list, e.g. "[0.1, 5, 9.2]"')
    p_commit.add_argument(
        "--input", help="Path to JSON with {features, model_id, model_version?, salt?, precision?}"
    )
    p_commit.add_argument("--model-id", required=False)
    p_commit.add_argument("--model-version", default=None)
    p_commit.add_argument("--salt", default=None)
    p_commit.add_argument("--precision", type=int, default=6)
    p_commit.add_argument(
        "--out", default=None, help="Output path (JSON). If omitted, prints only."
    )
    p_commit.set_defaults(func=lambda args: m9_1_commit.cli_commit(args))

    # m9 issue (M9.2)
    p_issue = sp.add_parser("issue", help="Issue a signed proof bundle")
    p_issue.add_argument("--pdf-hash", required=True)
    p_issue.add_argument("--feature-commit", required=True)
    p_issue.add_argument("--score", required=True, type=float)
    p_issue.add_argument("--threshold", required=True, type=float)
    p_issue.add_argument("--model-id", required=True)
    p_issue.add_argument("--model-version", default=None)
    p_issue.add_argument("--notes", default=None)
    p_issue.add_argument("--out", default=None, help="Output bundle path (JSON).")

    def _run_issue(a):
        req = IssueRequest(
            pdf_hash=a.pdf_hash,
            feature_commit=a.feature_commit,
            score=float(a.score),
            threshold=float(a.threshold),
            model_id=a.model_id,
            model_version=a.model_version,
            notes=a.notes,
        )
        b = issue_bundle(req)
        if a.out:
            Path(a.out).write_text(json.dumps(b.model_dump(), indent=2), encoding="utf-8")
            print(f"✅ Wrote bundle → {a.out}")
        else:
            print(json.dumps({"bundle_id": b.bundle_id}, ensure_ascii=False))

    p_issue.set_defaults(func=_run_issue)

    # m9 verify (M9.2)
    p_verify = sp.add_parser("verify", help="Verify a proof bundle from file")
    p_verify.add_argument("--input", required=True, help="Path to bundle JSON")

    def _run_verify(a):
        data = json.loads(Path(a.input).read_text(encoding="utf-8"))
        res = verify_bundle(VerifyRequest(bundle=data).bundle)
        print(
            json.dumps(
                {"valid": res.valid, "reasons": res.reasons, "bundle_id": res.bundle_id},
                ensure_ascii=False,
            )
        )

    p_verify.set_defaults(func=_run_verify)

    # Verifier hook (no checks yet for M9.1)
    verifiers["m9"] = m9_1_commit.verify
