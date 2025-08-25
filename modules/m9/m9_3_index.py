"""M9.3 — Bundle Storage & Indexing.

Maintains a compact JSONL index of issued bundles for fast listing/filtering.
- Index path: data/zk/index/bundles.index.jsonl
- Records are derived from bundle JSON files in data/zk/bundles/*.json

Exports:
- rebuild_index()            → rebuilds index from all bundle files
- ensure_index_exists()      → rebuild if index missing
- upsert_index_record(bundle)→ insert/update a single record
- list_index(...)            → filter/paginate records
- read_bundle_file(bundle_id)→ load full bundle JSON by id
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel, Field, ValidationError

# Reuse Bundle model for shape validation
from .m9_2_issue_verify import BUNDLES_DIR, Bundle  # type: ignore

ROOT = Path(".")
INDEX_DIR = ROOT / "data" / "zk" / "index"
INDEX_PATH = INDEX_DIR / "bundles.index.jsonl"


class IndexRecord(BaseModel):
    bundle_id: str = Field(min_length=8)
    issued_at: str
    decision: str
    model_id: str
    model_version: str | None = None
    score: float
    threshold: float
    pdf_hash_prefix: str
    feature_commit_prefix: str
    signer: str
    signature_prefix: str
    file: str


def _prefix(s: str, n: int = 16) -> str:
    return s[:n] if len(s) >= n else s


def _record_from_bundle_dict(bdict: Dict[str, Any], file_path: Path) -> IndexRecord:
    try:
        b = Bundle(**bdict)
    except ValidationError as exc:
        raise SystemExit(f"Invalid bundle at {file_path.name}: {exc}")  # fail fast

    rec = IndexRecord(
        bundle_id=b.bundle_id,
        issued_at=b.issued_at,
        decision=b.decision,
        model_id=b.model_id,
        model_version=b.model_version,
        score=float(b.score),
        threshold=float(b.threshold),
        pdf_hash_prefix=_prefix(b.pdf_hash, 16),
        feature_commit_prefix=_prefix(b.feature_commit, 16),
        signer=b.signer,
        signature_prefix=_prefix(b.signature, 16),
        file=str(file_path.as_posix()),
    )
    return rec


def _load_index_map() -> Dict[str, IndexRecord]:
    m: Dict[str, IndexRecord] = {}
    if not INDEX_PATH.exists():
        return m
    for line in INDEX_PATH.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            d = json.loads(line)
            rec = IndexRecord(**d)
        except Exception:
            continue
        m[rec.bundle_id] = rec
    return m


def _write_index_map(m: Dict[str, IndexRecord]) -> None:
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    lines = []
    # Sort: newest first by issued_at, then bundle_id
    for rec in sorted(m.values(), key=lambda r: (r.issued_at, r.bundle_id), reverse=True):
        lines.append(json.dumps(rec.model_dump(), ensure_ascii=False))
    INDEX_PATH.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def rebuild_index() -> Tuple[int, int]:
    """Scan bundle files and rebuild the JSONL index. Returns (files_scanned, indexed)."""
    m: Dict[str, IndexRecord] = {}
    files = list(BUNDLES_DIR.glob("*.json"))
    for fp in files:
        try:
            d = json.loads(fp.read_text(encoding="utf-8"))
            rec = _record_from_bundle_dict(d, fp)
            m[rec.bundle_id] = rec
        except SystemExit:
            raise
        except Exception:
            # skip unreadable files but keep going
            continue
    _write_index_map(m)
    return len(files), len(m)


def ensure_index_exists() -> None:
    if not INDEX_PATH.exists():
        rebuild_index()


def upsert_index_record(bundle: Bundle) -> None:
    """Insert/update a single record in the index."""
    m = _load_index_map()
    rec = _record_from_bundle_dict(
        json.loads(bundle.model_dump_json()), Path(f"{bundle.bundle_id}.json")
    )
    # Replace file path with canonical bundles location
    rec.file = str((BUNDLES_DIR / f"{bundle.bundle_id}.json").as_posix())
    m[rec.bundle_id] = rec
    _write_index_map(m)


def list_index(
    *,
    model_id: str | None = None,
    decision: str | None = None,
    q: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> Tuple[int, List[Dict[str, Any]]]:
    """Return (total, records) after filtering, paginated."""
    ensure_index_exists()
    m = _load_index_map()
    recs = list(m.values())

    def _match(rec: IndexRecord) -> bool:
        if model_id and rec.model_id != model_id:
            return False
        if decision and rec.decision != decision:
            return False
        if q:
            ql = q.lower()
            hay = (
                rec.bundle_id.lower()
                + rec.pdf_hash_prefix.lower()
                + rec.feature_commit_prefix.lower()
                + (rec.model_version or "").lower()
            )
            if ql not in hay:
                return False
        return True

    filtered = [r for r in recs if _match(r)]
    # already sorted at write time; safe to slice
    total = len(filtered)
    page = filtered[offset : offset + limit]
    return total, [r.model_dump() for r in page]


def read_bundle_file(bundle_id: str) -> Dict[str, Any]:
    """Load full bundle JSON by id."""
    path = BUNDLES_DIR / f"{bundle_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Bundle not found: {bundle_id}")
    return json.loads(path.read_text(encoding="utf-8"))
