from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

try:
    from jsonschema.validators import Draft7Validator as Validator
except Exception as e:
    raise SystemExit("Install jsonschema first: pip install jsonschema>=4.22.0") from e

import psycopg
from psycopg.types.json import Json

# ---------- paths & env ----------
ROOT = Path(__file__).resolve().parents[2]  # repo root
SCHEMA_DIR = ROOT / "configs" / "schemas"
TOPICS_PATH = ROOT / "configs" / "topics.json"
AUDIT_DIR = ROOT / "data" / "logs" / "agents"

_EVENT_TYPE_TO_SCHEMA = {
    "verdantis.DocumentIngested": "document_ingested.schema.json",
    "verdantis.TileFetched": "tile_fetched.schema.json",
    "verdantis.PolicyUpdated": "policy_updated.schema.json",
    "verdantis.ViolationFlagged": "violation_flagged.schema.json",
    "verdantis.ProofIssued": "proof_issued.schema.json",
    "verdantis.AlertRaised": "alert_raised.schema.json",
}

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise SystemExit("DATABASE_URL not set (check your .env)")


# ---------- helpers ----------
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_schema_for_event_type(event_type: str) -> dict:
    fname = _EVENT_TYPE_TO_SCHEMA.get(event_type)
    if not fname:
        raise SystemExit(f"[m2.2] unknown event_type: {event_type}")
    path = SCHEMA_DIR / fname
    if not path.exists():
        raise SystemExit(f"[m2.2] missing schema: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_event(obj: dict) -> None:
    et = obj.get("event_type")
    if not et:
        raise SystemExit("[m2.2] event missing 'event_type'")
    schema = _load_schema_for_event_type(et)
    Validator(schema).validate(obj)


def _ensure_dirs() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)


# ---------- BaseAgent ----------
class BaseAgent:
    """
    Minimal event agent:
      - validates event against JSON Schema
      - computes idempotency key (uses envelope.idempotency_key)
      - writes to DB with ON CONFLICT DO NOTHING (exactly-once-ish)
      - retries with exponential backoff
      - writes a JSONL audit log per agent
    """

    def __init__(
        self,
        name: str,
        max_retries: int = 5,
        base_delay: float = 0.25,
        max_delay: float = 2.0,
    ) -> None:
        self.name = name
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        _ensure_dirs()
        self.audit_path = AUDIT_DIR / f"{self.name}.jsonl"

    # ---- audit ----
    def _audit(self, action: str, status: str, **fields) -> None:
        rec = {"ts": _iso_now(), "agent": self.name, "action": action, "status": status}
        rec.update(fields)
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")

    # ---- db ----
    def _connect(self):
        return psycopg.connect(DATABASE_URL)

    # ---- core ----
    def process_event_dict(self, obj: dict) -> tuple[str, Optional[str]]:
        """
        Validate & write event to DB.
        Returns (idempotency_key, inserted_event_id_or_None_if_duplicate)
        """
        _validate_event(obj)
        idem = obj.get("idempotency_key")
        if not idem:
            raise SystemExit("[m2.2] event missing idempotency_key")

        def attempt():
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO event (event_key, event_type, occurred_at, payload)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_key) DO NOTHING
                    RETURNING id::text;
                    """,
                    (
                        idem,
                        obj["event_type"],
                        obj.get("occurred_at") or _iso_now(),
                        Json(obj.get("payload") or {}),
                    ),
                )
                row = cur.fetchone()
                conn.commit()
                return row[0] if row else None  # None → duplicate

        # retries with backoff for transient failures
        tries = 0
        while True:
            try:
                event_id = attempt()
                if event_id:
                    self._audit("insert", "accepted", idempotency_key=idem, event_id=event_id)
                else:
                    self._audit("insert", "duplicate", idempotency_key=idem)
                return idem, event_id
            except Exception as e:  # transient or unexpected
                tries += 1
                if tries > self.max_retries:
                    self._audit("insert", "failed", error=str(e))
                    raise
                delay = min(self.max_delay, self.base_delay * (2 ** (tries - 1)))
                delay += random.uniform(0, delay * 0.2)  # jitter
                time.sleep(delay)

    def process_path(self, path: str) -> tuple[str, Optional[str]]:
        p = Path(path)
        obj = json.loads(p.read_text(encoding="utf-8"))
        return self.process_event_dict(obj)

    def process_dir(self, dir_path: str) -> int:
        d = Path(dir_path)
        if not d.exists():
            raise SystemExit(f"[m2.2] no such directory: {d}")
        processed = 0
        for p in sorted(d.glob("*.json")):
            self.process_path(str(p))
            processed += 1
        self._audit("dir", "completed", processed=processed, dir=str(d))
        print(f"[m2.2] processed {processed} file(s) from {d}")
        return processed


# ---------- simple façade functions for CLI ----------
def agent_process_file(path: str, name: str = "event-writer") -> None:
    agent = BaseAgent(name=name)
    idem, eid = agent.process_path(path)
    if eid:
        print(f"✅ inserted event (id={eid}) key={idem}")
    else:
        print(f"🟨 duplicate ignored key={idem}")


def agent_process_dir(dir_path: str, name: str = "event-writer") -> None:
    agent = BaseAgent(name=name)
    agent.process_dir(dir_path)


def verify() -> None:
    """
    M2.2 gate: duplicate submit doesn’t duplicate DB rows.
    Steps:
      - pick (or require) a sample event file
      - process it twice
      - assert event row count for its idempotency key == 1
    """
    sample = ROOT / "data" / "event_samples" / "001_document_ingested.json"
    if not sample.exists():
        raise SystemExit("[m2.2] missing sample event. Run: python scripts/verdctl.py m2.samples")

    obj = json.loads(sample.read_text(encoding="utf-8"))
    key = obj.get("idempotency_key")
    if not key:
        raise SystemExit("[m2.2] sample missing idempotency_key")

    agent = BaseAgent(name="verify-agent")
    # twice on purpose
    agent.process_event_dict(obj)
    agent.process_event_dict(obj)

    # check DB count
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM event WHERE event_key=%s;", (key,))
        cnt = cur.fetchone()[0]
    print(f"[m2.2] event_key={key} count={cnt}")
    if cnt != 1:
        raise SystemExit("[m2.2] FAILED: duplicate insert occurred")
    print("[m2.2] PASSED: idempotent insert verified")
