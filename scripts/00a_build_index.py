import json
from pathlib import Path

import numpy as np

from services.common.config import settings
from services.rag_verify_svc.models import manager

NEWS = Path("data/raw/news/news.jsonl")
OUT_DIR = Path("data/processed/index")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_samples(n=50):
    items = []
    if NEWS.exists():
        with NEWS.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                obj = json.loads(line)
                body = (
                    obj.get("body")
                    or obj.get("text")
                    or obj.get("content")
                    or obj.get("description")
                    or ""
                )
                title = obj.get("title", "")
                lang = obj.get("lang") or obj.get("language") or "en"
                text = (title + "\n" + body).strip()
                if len(text.split()) >= 60:
                    items.append({"id": f"news-{i:03d}", "text": text, "lang": lang})
    return items


if __name__ == "__main__":
    if settings.offline:
        # Ensure models are loaded even in offline mode
        manager.load_embedder()
    docs = load_samples()
    texts = [d["text"] for d in docs]
    embs = manager.embed(texts)
    np.savez(OUT_DIR / "embeddings.npz", embeddings=embs)
    with (OUT_DIR / "meta.json").open("w", encoding="utf-8") as f:
        json.dump(
            {"count": len(docs), "ids": [d["id"] for d in docs], "model": settings.embedding_model},
            f,
            ensure_ascii=False,
            indent=2,
        )
    (OUT_DIR / "INDEX_READY").write_text("ok", encoding="utf-8")
    print(f"Index built: {len(docs)} docs, model={settings.embedding_model}")
