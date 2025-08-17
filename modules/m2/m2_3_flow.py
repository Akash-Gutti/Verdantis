from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg
from dotenv import load_dotenv

from .m2_1_schemas import _content_hash  # used to synthesize fresh idempotency keys

# Local imports
from .m2_2_agents import BaseAgent  # validates & writes with idempotency

ROOT = Path(__file__).resolve().parents[2]  # repo root (…/modules/m2 -> …/)
BUS_ROOT = ROOT / "data" / "bus"
INBOX = BUS_ROOT / "inbox"
PROCESSING = BUS_ROOT / "processing"
DONE = BUS_ROOT / "done"
DUP = BUS_ROOT / "dup"
ERR = BUS_ROOT / "err"
TOPICS_DIR = BUS_ROOT / "topics"
TOPICS_PATH = ROOT / "configs" / "topics.json"

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL not set (check your .env)")


def _load_topics() -> dict[str, str]:
    if not TOPICS_PATH.exists():
        return {}
    return json.loads(TOPICS_PATH.read_text(encoding="utf-8"))


def _ensure_dirs() -> None:
    for d in (BUS_ROOT, INBOX, PROCESSING, DONE, DUP, ERR, TOPICS_DIR):
        d.mkdir(parents=True, exist_ok=True)
    topics = _load_topics()
    for topic in sorted(set(topics.values())):
        (TOPICS_DIR / topic).mkdir(parents=True, exist_ok=True)


@dataclass
class DispatchResult:
    path: Path
    status: str  # "done" | "dup" | "err"
    event_id: Optional[str]
    idempotency_key: Optional[str]


class FileBusDispatcher:
    """
    Minimal file-bus dispatcher:
      - scans INBOX for *.json
      - moves to PROCESSING/<name>
      - feeds each event to BaseAgent (validation + idempotent insert)
      - moves file to DONE/, DUP/, or ERR/
      - mirrors a copy to topics/<topic>/ for audit/debug (if topics.json present)
    """

    def __init__(self, agent_name: str = "bus-consumer") -> None:
        _ensure_dirs()
        self.agent = BaseAgent(name=agent_name)
        self.topics = _load_topics()

    def _topic_for_event_type(self, event_type: str) -> Optional[str]:
        return self.topics.get(event_type)

    def _mirror_to_topic(self, src_file: Path, event_type: str) -> None:
        topic = self._topic_for_event_type(event_type)
        if not topic:
            return
        target_dir = TOPICS_DIR / topic
        target_dir.mkdir(parents=True, exist_ok=True)
        dst = target_dir / src_file.name
        if dst.exists():
            dst = target_dir / f"{src_file.stem}.copy{src_file.suffix}"
        try:
            shutil.copy2(src_file, dst)
        except Exception:
            pass  # best-effort

    def _process_file(self, inbox_file: Path) -> DispatchResult:
        # move to processing
        proc_file = PROCESSING / inbox_file.name
        try:
            inbox_file.replace(proc_file)
        except FileNotFoundError:
            # already taken by another run
            return DispatchResult(inbox_file, "err", None, None)

        # read json
        try:
            data = json.loads(proc_file.read_text(encoding="utf-8"))
            event_type = data.get("event_type") or "UNKNOWN"
        except Exception:
            dst = ERR / proc_file.name
            proc_file.replace(dst)
            return DispatchResult(dst, "err", None, None)

        # process via BaseAgent
        try:
            idem, event_id = self.agent.process_event_dict(data)
            self._mirror_to_topic(proc_file, event_type)
            if event_id:  # inserted
                dst = DONE / proc_file.name
                proc_file.replace(dst)
                return DispatchResult(dst, "done", event_id, idem)
            else:  # duplicate
                dst = DUP / proc_file.name
                proc_file.replace(dst)
                return DispatchResult(dst, "dup", None, idem)
        except Exception:
            dst = ERR / proc_file.name
            proc_file.replace(dst)
            return DispatchResult(dst, "err", None, None)

    # ------------- public API -------------
    def dispatch_once(self) -> list[DispatchResult]:
        results: list[DispatchResult] = []
        for f in sorted(INBOX.glob("*.json")):
            results.append(self._process_file(f))
        return results

    def dispatch_loop(self, interval: float = 1.0) -> None:
        try:
            while True:
                self.dispatch_once()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[m2.3] dispatcher stopped by user")

    # ------------- verifier helpers -------------
    def _key_exists_in_db(self, event_key: str) -> bool:
        with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 FROM event WHERE event_key=%s LIMIT 1;", (event_key,))
            return cur.fetchone() is not None

    def _prepare_fresh_pair(self, sample_path: Path) -> tuple[dict, dict, str, str, str]:
        """
        Create TWO event objects with the SAME fresh idempotency_key,
        using the sample as a base. Returns (evtA, evtB, key, fnA, fnB).
        """
        base = json.loads(sample_path.read_text(encoding="utf-8"))

        # tweak payload to get a new hash/key
        payload = dict(base.get("payload") or {})
        title = payload.get("title", "")
        payload["title"] = f"{title}-verify-{int(time.time() * 1000)}"

        # recompute keys
        new_key = _content_hash(payload)
        # ensure uniqueness of the key in DB
        nonce = 0
        while self._key_exists_in_db(new_key):
            nonce += 1
            payload["title"] = f"{payload['title']}-{nonce}"
            new_key = _content_hash(payload)

        def mk_event() -> dict:
            evt = dict(base)
            evt["event_id"] = str(uuid.uuid4())
            evt["occurred_at"] = base.get("occurred_at") or datetime.now(timezone.utc).isoformat()
            evt["payload"] = payload
            evt["content_hash"] = new_key
            evt["idempotency_key"] = new_key
            return evt

        evtA = mk_event()
        evtB = mk_event()

        fnA = "verify_doc_ingested_A.json"
        fnB = "verify_doc_ingested_B.json"
        return evtA, evtB, new_key, fnA, fnB

    # ------------- verifier -------------
    def verify(self) -> None:
        """
        Gate: publish→consume loop proven; duplicate submit doesn't duplicate DB rows.
        - Make a fresh pair of events with the same idempotency_key
        - Put both into INBOX
        - Run dispatcher twice
        - DB must have exactly one row for that event_key
        - Exactly one file in DONE and the other in DUP
        """
        sample = ROOT / "data" / "event_samples" / "001_document_ingested.json"
        if not sample.exists():
            raise SystemExit("[m2.3] sample missing. Run: python scripts/verdctl.py m2.samples")

        _ensure_dirs()

        evtA, evtB, key, fnA, fnB = self._prepare_fresh_pair(sample)
        (INBOX / fnA).write_text(json.dumps(evtA, indent=2), encoding="utf-8")
        (INBOX / fnB).write_text(json.dumps(evtB, indent=2), encoding="utf-8")

        # run dispatcher twice
        self.dispatch_once()
        self.dispatch_once()

        # DB row count for key
        with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM event WHERE event_key=%s;", (key,))
            cnt = cur.fetchone()[0]

        # file outcomes
        doneA = (DONE / fnA).exists()
        doneB = (DONE / fnB).exists()
        dupA = (DUP / fnA).exists()
        dupB = (DUP / fnB).exists()

        print(
            f"[m2.3] key={key} db_count={cnt} | "
            f"A: done={doneA} dup={dupA} | B: done={doneB} dup={dupB}"
        )

        ok_files = (doneA and dupB) or (doneB and dupA)
        if cnt != 1 or not ok_files:
            raise SystemExit("[m2.3] FAILED: dispatch loop or dedupe not satisfied")

        print("[m2.3] PASSED: publish→consume proven and duplicates handled")
