"""M12.4 - Model & Data cards generator.

Reads previously produced metrics/eval/CI artifacts and renders Markdown cards
under docs/cards/*. Also writes a small index manifest for traceability.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class CardInputs:
    metrics_prom: Path
    rag_report: Path
    causal_report: Path
    change_report: Path
    ci_report: Optional[Path] = None


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _maybe_json(path: Optional[Path]) -> Dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        return _read_json(path)
    except Exception:  # noqa: BLE001
        return {}


def _read_prom_metrics(path: Path) -> Dict[str, float]:
    """Parse simple 'name value' lines from Prometheus text format."""
    out: Dict[str, float] = {}
    if not path.exists():
        return out
    txt = path.read_text(encoding="utf-8").splitlines()
    for line in txt:
        if not line or line.startswith("#"):
            continue
        parts = line.strip().split()
        if len(parts) >= 2:
            name, val = parts[0], parts[1]
            try:
                out[name] = float(val)
            except Exception:  # noqa: BLE001
                continue
    return out


def _git_info() -> Dict[str, str]:
    """Return commit/branch if Git available, otherwise empty."""

    def _run(args: List[str]) -> Optional[str]:
        try:
            res = subprocess.run(args, capture_output=True, text=True, check=False)
            out = (res.stdout or "").strip()
            return out or None
        except Exception:  # noqa: BLE001
            return None

    commit = _run(["git", "rev-parse", "--short", "HEAD"]) or ""
    branch = _run(["git", "rev-parse", "--abbrev-ref", "HEAD"]) or ""
    return {"commit": commit, "branch": branch}


def _now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _md_header(title: str) -> str:
    return f"# {title}\n\n_Generated: {_now_iso()}Z_\n\n"


def _format_table(rows: List[Tuple[str, str]]) -> str:
    if not rows:
        return ""
    head = "| Metric | Value |\n|---|---|\n"
    body = "".join([f"| {k} | {v} |\n" for k, v in rows])
    return head + body + "\n"


def render_model_card_alerts(
    out_path: Path,
    prom: Dict[str, float],
    rag: Dict[str, Any],
    causal: Dict[str, Any],
    change: Dict[str, Any],
    ci: Dict[str, Any],
    gitmeta: Dict[str, str],
) -> None:
    title = "Verdantis Alerts — Model Card (Streaming & Scoring)"
    rows = [
        ("Build info", "verdantis_build_info" if "verdantis_build_info" in prom else "n/a"),
        ("Events (total)", f"{int(prom.get('verdantis_events_total', 0))}"),
        ("Events (unmatched)", f"{int(prom.get('verdantis_events_unmatched', 0))}"),
        ("Dedupe kept", f"{int(prom.get('verdantis_dedupe_kept', 0))}"),
        ("Dedupe suppressed", f"{int(prom.get('verdantis_dedupe_suppressed', 0))}"),
        ("Channels sent", f"{int(prom.get('verdantis_channels_sent', 0))}"),
        ("Channels skipped", f"{int(prom.get('verdantis_channels_skipped', 0))}"),
    ]

    # Eval snippets
    rows += [
        ("RAG micro F1", f"{rag.get('cite_f1_micro', 'n/a')}"),
        ("RAG NLI acc", f"{rag.get('nli_accuracy', 'n/a')}"),
        ("Causal ΔRMSE mean", f"{causal.get('delta_mean', 'n/a')}"),
        ("Change p@10", f"{(change.get('precision_at_k') or {}).get('10', 'n/a')}"),
    ]

    # CI summary (optional)
    if ci:
        rows += [
            ("CI lint ok", f"{ci.get('lint', {}).get('ok', False)}"),
            ("CI tests ok", f"{ci.get('tests', {}).get('ok', False)}"),
            ("Bundle files", f"{ci.get('bundle', {}).get('files', 0)}"),
        ]

    # Git meta
    rows += [
        ("Git commit", gitmeta.get("commit") or "n/a"),
        ("Git branch", gitmeta.get("branch") or "n/a"),
    ]

    md = _md_header(title)
    md += "## Overview\n"
    md += (
        "Streaming alerts ingest events, filter and deduplicate them (M10), and "
        "publish to channels and role portals (M11). This card summarizes current metrics "
        "and evaluation signals (M12.3).\n\n"
    )
    md += "## Key Metrics\n"
    md += _format_table(rows)
    md += "## Intended Use & Limitations\n"
    md += (
        "- **Use**: Operational monitoring of material events and risk signals.\n"
        "- **Limits**: Sample datasets; stubs for channels; evaluation sizes are small.\n"
        "- **Safety**: PII is masked in Public portal; tokens gate role data.\n"
    )
    _write(out_path, md)


def render_data_card_evals(
    out_path: Path,
    rag_in: Path,
    causal_in: Path,
    change_in: Path,
    rag: Dict[str, Any],
    causal: Dict[str, Any],
    change: Dict[str, Any],
) -> None:
    title = "Verdantis — Data Card (Evaluation Sets)"
    md = _md_header(title)
    md += "## Datasets\n"
    md += (
        f"- RAG eval: `{rag_in.as_posix()}`\n"
        f"- Causal eval: `{causal_in.as_posix()}`\n"
        f"- Change eval: `{change_in.as_posix()}`\n\n"
    )
    rows = [
        ("RAG items", f"{rag.get('items', 'n/a')}"),
        ("RAG micro F1", f"{rag.get('cite_f1_micro', 'n/a')}"),
        ("RAG NLI accuracy", f"{rag.get('nli_accuracy', 'n/a')}"),
        ("Causal assets", f"{causal.get('assets', 'n/a')}"),
        ("Causal ΔRMSE mean", f"{causal.get('delta_mean', 'n/a')}"),
        ("Change items", f"{change.get('items', 'n/a')}"),
        ("Change p@5", f"{(change.get('precision_at_k') or {}).get('5', 'n/a')}"),
        ("Change p@10", f"{(change.get('precision_at_k') or {}).get('10', 'n/a')}"),
    ]
    md += "## Summary Stats\n"
    md += _format_table(rows)
    md += "## Provenance & Ethics\n"
    md += (
        "Sample evaluation data is synthetic and for demonstration only. No personal data.\n"
        "Ensure real datasets are documented with source, consent, and masking policies.\n"
    )
    _write(out_path, md)


def run_cards_build(
    inputs: CardInputs,
    out_dir: Path,
) -> List[str]:
    """Render model & data cards; return list of written files."""
    prom = _read_prom_metrics(inputs.metrics_prom)
    rag = _maybe_json(inputs.rag_report)
    causal = _maybe_json(inputs.causal_report)
    change = _maybe_json(inputs.change_report)
    ci = _maybe_json(inputs.ci_report)
    gitmeta = _git_info()

    files: List[str] = []
    out_dir.mkdir(parents=True, exist_ok=True)

    model_md = out_dir / "model_card_alerts.md"
    render_model_card_alerts(model_md, prom, rag, causal, change, ci, gitmeta)
    files.append(str(model_md))

    data_md = out_dir / "data_card_eval_sets.md"
    render_data_card_evals(
        data_md,
        inputs.rag_report,
        inputs.causal_report,
        inputs.change_report,
        rag,
        causal,
        change,
    )
    files.append(str(data_md))

    # Index manifest
    index = {
        "generated_at": _now_iso(),
        "files": files,
        "sources": {
            "metrics_prom": str(inputs.metrics_prom),
            "rag_report": str(inputs.rag_report),
            "causal_report": str(inputs.causal_report),
            "change_report": str(inputs.change_report),
            "ci_report": str(inputs.ci_report) if inputs.ci_report else None,
        },
        "git": gitmeta,
    }
    index_path = out_dir / "cards_index.json"
    _write(index_path, json.dumps(index, ensure_ascii=False, indent=2))
    files.append(str(index_path))

    return files
