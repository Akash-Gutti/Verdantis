from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from services.common.config import settings


@dataclass
class ModelManager:
    embedder: Optional[SentenceTransformer] = None
    nli_model: Optional[AutoModelForSequenceClassification] = None
    nli_tokenizer: Optional[AutoTokenizer] = None
    device: torch.device = torch.device("cpu")

    def __post_init__(self):
        self.device = self._select_device()

    def embed(self, texts: list[str]):
        """Encode texts with the configured embedding model."""
        emb = self.load_embedder()
        return emb.encode(texts, convert_to_numpy=True, normalize_embeddings=True)

    def _select_device(self) -> torch.device:
        if settings.force_device:
            try:
                dev = torch.device(settings.force_device)
                if "cuda" in dev.type and not torch.cuda.is_available():
                    return torch.device("cpu")
                return dev
            except Exception:
                return torch.device("cpu")
        return torch.device("cpu")

    def load_embedder(self):
        if self.embedder is None:
            self.embedder = SentenceTransformer(settings.embedding_model, device=str(self.device))
        return self.embedder

    def load_nli(self):
        if self.nli_model is None or self.nli_tokenizer is None:
            tok = AutoTokenizer.from_pretrained(settings.mnli_model, use_fast=False)
            mdl = AutoModelForSequenceClassification.from_pretrained(settings.mnli_model)
            mdl.to(self.device)
            mdl.eval()
            self.nli_tokenizer = tok
            self.nli_model = mdl
        return self.nli_tokenizer, self.nli_model

    @torch.inference_mode()
    def nli_score(self, premise: str, hypothesis: str):
        tok, mdl = self.load_nli()
        inputs = tok(premise, hypothesis, return_tensors="pt", truncation=True, max_length=512).to(
            self.device
        )
        logits = mdl(**inputs).logits
        probs = logits.softmax(dim=-1).detach().cpu().numpy()[0]

        # Build label index map robustly
        id2label = getattr(mdl.config, "id2label", None) or {
            0: "contradiction",
            1: "neutral",
            2: "entailment",
        }
        label2id = {v.lower(): int(k) for k, v in id2label.items()}
        idx_ent = label2id.get("entailment", 2)
        idx_neu = label2id.get("neutral", 1)
        idx_con = label2id.get("contradiction", 0)

        return float(probs[idx_ent]), float(probs[idx_neu]), float(probs[idx_con])


manager = ModelManager()
