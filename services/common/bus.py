import json
import pathlib
import time
from typing import Any, Dict

from .config import settings

try:
    import redis as _redis
except Exception:  # redis is optional
    _redis = None

_events_path = pathlib.Path("data/processed/events.log")
_events_path.parent.mkdir(parents=True, exist_ok=True)


def publish(topic: str, payload: Dict[str, Any]) -> None:
    """Minimal event publisher: file backend by default, Redis Streams if configured."""
    record = {"ts": time.time(), "topic": topic, **payload}
    if settings.bus_backend == "redis" and _redis:
        r = _redis.from_url(settings.redis_url)
        r.xadd(topic, {"data": json.dumps(record, ensure_ascii=False)})
    else:
        with _events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
