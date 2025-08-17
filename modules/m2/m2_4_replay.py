# modules/m2/replay.py
from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import psycopg
from dotenv import load_dotenv

from . import m2_1_schemas as m2s  # for event_type -> schema filename map & validation
from .m2_1_schemas import _content_hash  # reuse canonical hashing

# Local imports (no circular: replay -> flow -> schemas)
from .m2_3_flow import (  # type: ignore
    INBOX,
    TOPICS_DIR,
    TOPICS_PATH,
    FileBusDispatcher,
    _ensure_dirs,
)

ROOT = Path(__file__).resolve().parents[2]  # repo root
BUS_ROOT = ROOT / "data" / "bus"

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL not set (check your .env)")


# ---------- helpers ----------
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    s = ts.strip()
    # support trailing 'Z'
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception as e:
        raise SystemExit(f"[m2.4] invalid ISO datetime: {ts}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_topics() -> dict:
    if not TOPICS_PATH.exists():
        return {}
    return json.loads(TOPICS_PATH.read_text(encoding="utf-8"))


def _iter_topic_files(selected_topics: Iterable[str]) -> Iterable[Path]:
    for topic in selected_topics:
        tdir = TOPICS_DIR / topic
        if not tdir.exists():
            continue
        for p in sorted(tdir.glob("*.json")):
            yield p


def _read_event_occurred_at(p: Path) -> Optional[datetime]:
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        ts = obj.get("occurred_at")
        return _parse_iso(ts) if ts else None
    except Exception:
        return None


def _within(dt: Optional[datetime], start: Optional[datetime], end: Optional[datetime]) -> bool:
    if dt is None:
        return False
    if start and dt < start:
        return False
    if end and dt >= end:
        return False
    return True


def _emit_to_inbox(
    events: List[dict], prefix: str = "replay", sleep: float = 0.0, dry_run: bool = False
) -> int:
    _ensure_dirs()
    count = 0
    for i, evt in enumerate(events, start=1):
        fname = f"{prefix}_{i:04d}.json"
        path = INBOX / fname
        if dry_run:
            print(f"[m2.4] (dry-run) would write {path}")
        else:
            path.write_text(json.dumps(evt, indent=2), encoding="utf-8")
        count += 1
        if sleep and not dry_run:
            time.sleep(sleep)
    return count


# ---------- public API: Replay from topics ----------
def replay_from_topics(
    topic: Optional[str] = None,
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    limit: Optional[int] = None,
    sleep: float = 0.0,
    dry_run: bool = False,
) -> int:
    """
    Select mirrored events under data/bus/topics/<topic>/ by time window and
    re-emit them back into data/bus/inbox/ (preserving idempotency keys).
    """
    _ensure_dirs()
    topics_map = _load_topics()
    # resolve selector: allow either a topic name (e.g., "doc.ingested") or an event_type
    if not topic or topic.lower() in {"all", "*"}:
        selected = sorted(set(topics_map.values()))
    elif topic.startswith("verdantis."):
        selected = [topics_map.get(topic, topic)]  # map event_type -> topic
    else:
        selected = [topic]

    start = _parse_iso(start_iso)
    end = _parse_iso(end_iso)

    picked: List[dict] = []
    for p in _iter_topic_files(selected):
        if limit and len(picked) >= limit:
            break
        dt = _read_event_occurred_at(p)
        if not _within(dt, start, end):
            continue
        try:
            evt = json.loads(p.read_text(encoding="utf-8"))
            # sanity-check: ensure idempotency_key exists & schema_ref set
            if not evt.get("idempotency_key"):
                # reconstruct from payload if missing (rare)
                idem = _content_hash(evt.get("payload") or {})
                evt["idempotency_key"] = idem
                evt["content_hash"] = idem
            if not evt.get("schema_ref"):
                fname = m2s._EVENT_TYPE_TO_SCHEMA.get(evt.get("event_type"), "")
                if fname:
                    evt["schema_ref"] = f"configs/schemas/{fname}"
            picked.append(evt)
        except Exception:
            continue

    count = _emit_to_inbox(picked, prefix="replay.topics", sleep=sleep, dry_run=dry_run)
    print(
        f"[m2.4] replay_from_topics enqueued={count} "
        f"(topics={selected}, window={start_iso}..{end_iso})"
    )
    return count


# ---------- public API: Replay from DB ----------
def replay_from_db(
    event_types: Optional[List[str]] = None,
    start_iso: Optional[str] = None,
    end_iso: Optional[str] = None,
    limit: Optional[int] = None,
    sleep: float = 0.0,
    dry_run: bool = False,
) -> int:
    """
    Pull events from DB by time window (and optional event_type filter),
    reconstruct a full envelope, and emit to inbox.
    Preserves original event_key as idempotency_key to keep DB dedupe intact.
    """
    start = _parse_iso(start_iso)
    end = _parse_iso(end_iso)

    clauses = []
    params: List[object] = []

    if start:
        clauses.append("occurred_at >= %s")
        params.append(start)
    if end:
        clauses.append("occurred_at < %s")
        params.append(end)
    if event_types:
        placeholders = ", ".join(["%s"] * len(event_types))
        clauses.append(f"event_type IN ({placeholders})")
        params.extend(event_types)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    lim = f" LIMIT {int(limit)}" if limit else ""

    sql = (
        "SELECT id::text, event_key, event_type, occurred_at, payload::text "
        f"FROM event{where} ORDER BY occurred_at ASC{lim};"
    )

    rows: List[Tuple[str, str, str, datetime, str]] = []
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        for r in cur.fetchall():
            rows.append((r[0], r[1], r[2], r[3], r[4]))

    events: List[dict] = []
    for _, event_key, etype, occurred_at, payload_text in rows:
        try:
            payload = json.loads(payload_text)
        except Exception:
            payload = {}
        # schema filename for envelope
        schema_fname = m2s._EVENT_TYPE_TO_SCHEMA.get(etype)
        schema_ref = f"configs/schemas/{schema_fname}" if schema_fname else None
        # hash based on payload (best-effort; align with our idempotency approach)
        canon = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
        h = hashlib.sha256(canon).hexdigest()
        evt = {
            "event_id": f"replay-{_iso_now()}",
            "event_type": etype,
            "occurred_at": occurred_at.astimezone(timezone.utc).isoformat(),
            "source": "replay.db",
            "version": "1.0.0",
            "schema_ref": schema_ref,
            "trace_id": f"trace-{_iso_now()}",
            "content_hash": h,
            "idempotency_key": event_key,  # preserve dedupe
            "payload": payload,
        }
        events.append(evt)

    count = _emit_to_inbox(events, prefix="replay.db", sleep=sleep, dry_run=dry_run)
    print(
        f"[m2.4] replay_from_db enqueued={count} "
        f"(types={event_types or 'ALL'}, window={start_iso}..{end_iso})"
    )
    return count


# ---------- verifier ----------
def verify() -> None:
    """
    M2.4 gate:
      - ensure there is at least 1 topic file available;
      if not, synthesize by running M2.3 verify path
      - replay 1 from topics, consume once, assert DB
      row count unchanged and file lands in DUP
    """
    _ensure_dirs()
    # ensure there is a topic file
    any_topic_files = list(TOPICS_DIR.glob("*/*.json"))
    if not any_topic_files:
        # run M2.3 verify to seed a DONE + DUP and mirror into topics
        disp = FileBusDispatcher(agent_name="bus-consumer")
        disp.verify()  # this will generate/mirror topic files as a side-effect

    # pick newest topic file
    files = sorted(TOPICS_DIR.glob("*/*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise SystemExit("[m2.4] no topic files to replay")

    f = files[0]
    obj = json.loads(f.read_text(encoding="utf-8"))
    key = obj.get("idempotency_key")
    if not key:
        raise SystemExit("[m2.4] selected topic file missing idempotency_key")

    # record current DB count for this key
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM event WHERE event_key=%s;", (key,))
        before = cur.fetchone()[0]

    # replay exactly this one (preserving key), then dispatch once
    _ = replay_from_topics(
        topic=f.parent.name, start_iso=None, end_iso=None, limit=1, dry_run=False
    )
    FileBusDispatcher(agent_name="bus-consumer").dispatch_once()

    # check count again
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM event WHERE event_key=%s;", (key,))
        after = cur.fetchone()[0]

    if before != after:
        raise SystemExit("[m2.4] FAILED: DB row count changed after replay (should be deduped)")

    # confirm the replayed file ended as DUP (by name prefix)
    dup_candidates = list((BUS_ROOT / "dup").glob("replay.*.json"))
    if not dup_candidates:
        raise SystemExit("[m2.4] FAILED: no replayed files found in dup/")

    print(
        f"[m2.4] PASSED: replay preserved idempotency "
        f"(before={before}, after={after}) and landed in dup/"
    )
