from __future__ import annotations

import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

try:
    import PyPDF2
except Exception:  # pragma: no cover
    PyPDF2 = None  # type: ignore

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None  # type: ignore

try:
    from langdetect import detect as langdetect_detect  # optional
except Exception:  # pragma: no cover
    langdetect_detect = None  # type: ignore

try:
    import pytesseract  # optional
    from PIL import Image
except Exception:  # pragma: no cover
    pytesseract = None  # type: ignore
    Image = None  # type: ignore


ARABIC_BLOCK = re.compile(r"[\u0600-\u06FF]")

NEWS_DIRS = [
    Path("data/raw/news"),
    Path("data/raw/news_json"),
]
NEWS_FILES = [Path("data/raw/news.json")]

PDF_DIR_SPECS = {
    "esg": Path("data/raw/pdfs"),
    "permit": Path("data/raw/permits"),
    "tender": Path("data/raw/tenders"),
}


@dataclass
class PageRecord:
    doc_id: str
    doc_path: str
    source: str
    page: int
    text: str
    lang: str
    doc_sha256: str
    words: int
    extractor: str


def ensure_dirs() -> None:
    Path("data/interim").mkdir(parents=True, exist_ok=True)


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def detect_lang(text: str) -> str:
    """Fast heuristic 'ar' vs 'en', with optional langdetect fallback."""
    if not text:
        return "en"
    # Arabic script heuristic first (robust & offline)
    ar_chars = len(ARABIC_BLOCK.findall(text))
    total_chars = max(len(text), 1)
    if ar_chars / total_chars >= 0.10:
        return "ar"
    # Optional langdetect for edge cases
    if langdetect_detect:
        try:
            code = langdetect_detect(text)
            return "ar" if code.startswith("ar") else "en"
        except Exception:
            pass
    return "en"


def sha256_of_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def iter_news_records() -> Iterable[Tuple[str, str]]:
    """
    Yields (doc_id, full_text) for news JSONs.
    Accepts:
      - directory of *.json (array or single object per file)
      - single file data/raw/news.json   (array or JSONL)
    Tries keys: body|text|content|description
    """
    # Directories of per-article JSON
    for d in NEWS_DIRS:
        if d.exists():
            for fp in sorted(d.glob("*.json")):
                for text in _extract_texts_from_json_file(fp):
                    yield (fp.stem, text)

    # Single combined file
    for f in NEWS_FILES:
        if f.exists():
            # Could be array JSON or JSONL
            with f.open("r", encoding="utf-8") as fh:
                first = fh.read(1)
                fh.seek(0)
                if first == "[":
                    items = json.load(fh)
                    for i, obj in enumerate(items):
                        text = _pick_news_text(obj)
                        if text:
                            yield (f"{f.stem}_{i}", text)
                else:
                    for i, line in enumerate(fh):
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        text = _pick_news_text(obj)
                        if text:
                            yield (f"{f.stem}_{i}", text)


def _extract_texts_from_json_file(fp: Path) -> Iterable[str]:
    try:
        obj = json.loads(fp.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            text = _pick_news_text(obj)
            if text:
                yield text
        elif isinstance(obj, list):
            for item in obj:
                text = _pick_news_text(item)
                if text:
                    yield text
    except Exception:
        # Try line-delimited fallback
        try:
            with fp.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        item = json.loads(line)
                        text = _pick_news_text(item)
                        if text:
                            yield text
                    except Exception:
                        continue
        except Exception:
            return


def _pick_news_text(obj: Dict) -> Optional[str]:
    for k in ("body", "text", "content", "description"):
        val = obj.get(k)
        if isinstance(val, str) and val.strip():
            return normalize_ws(val)
    # Join title + snippet if present
    title = obj.get("title")
    snippet = obj.get("snippet") or obj.get("summary")
    parts = [p for p in [title, snippet] if isinstance(p, str) and p.strip()]
    if parts:
        return normalize_ws(". ".join(parts))
    return None


def extract_pdf_pages(path: Path) -> List[Tuple[int, str, str]]:
    """
    Returns list of (page_number, text, extractor_name).
    Fallback chain: PyPDF2 -> PyMuPDF -> OCR (if pytesseract available)
    """
    pages: List[Tuple[int, str, str]] = []

    # Try PyPDF2 first (fast, pure Python)
    if PyPDF2 is not None:
        try:
            with path.open("rb") as fh:
                reader = PyPDF2.PdfReader(fh)
                for i, page in enumerate(reader.pages, start=1):
                    try:
                        txt = page.extract_text() or ""
                        txt = normalize_ws(txt)
                        if txt:
                            pages.append((i, txt, "pypdf2"))
                        else:
                            pages.append((i, "", "pypdf2_empty"))
                    except Exception:
                        pages.append((i, "", "pypdf2_error"))
        except Exception:
            pass

    # If any pages empty/errored, try PyMuPDF per page
    need_upgrade = not pages or any(
        src.endswith("_empty") or src.endswith("_error") for _, _, src in pages
    )
    if need_upgrade and fitz is not None:
        try:
            with fitz.open(path) as doc:
                upgraded: List[Tuple[int, str, str]] = []
                n_pages = doc.page_count
                for i in range(1, n_pages + 1):
                    text_prev = ""
                    src_prev = "missing"
                    if pages and i <= len(pages):
                        text_prev, src_prev = pages[i - 1][1], pages[i - 1][2]
                    try:
                        page = doc.load_page(i - 1)
                        txt = normalize_ws(page.get_text() or "")
                        if txt and (
                            not text_prev
                            or src_prev.endswith("_empty")
                            or src_prev.endswith("_error")
                        ):
                            upgraded.append((i, txt, "pymupdf"))
                        else:
                            upgraded.append((i, text_prev, src_prev))
                    except Exception:
                        upgraded.append((i, text_prev, src_prev if text_prev else "pymupdf_error"))
                pages = upgraded
        except Exception:
            pass

    # OCR only if still too many empty pages & pytesseract available
    if pytesseract is not None and fitz is not None:
        try:
            empties = [i for i, (_, txt, _) in enumerate(pages, start=1) if not txt]
            if empties:
                with fitz.open(path) as doc:
                    for i in empties:
                        try:
                            page = doc.load_page(i - 1)
                            pix = page.get_pixmap(dpi=200, alpha=False)
                            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                            ocr_text = normalize_ws(pytesseract.image_to_string(img) or "")
                            if ocr_text:
                                pages[i - 1] = (i, ocr_text, "ocr")
                        except Exception:
                            # leave as-is
                            pass
        except Exception:
            pass

    return pages


def collect_inputs() -> Dict[str, List[Path]]:
    inputs: Dict[str, List[Path]] = {"news": [], "esg": [], "permit": [], "tender": []}
    # PDFs
    for source, dpath in PDF_DIR_SPECS.items():
        if dpath.exists():
            inputs[source] = sorted(dpath.glob("*.pdf"))
    # News
    news_paths: List[Path] = []
    for d in NEWS_DIRS:
        if d.exists():
            news_paths.extend(sorted(d.glob("*.json")))
    for f in NEWS_FILES:
        if f.exists():
            news_paths.append(f)
    inputs["news"] = news_paths
    return inputs


def run_loader() -> int:
    ensure_dirs()
    inputs = collect_inputs()

    records: List[PageRecord] = []
    failures: List[Dict[str, str]] = []
    ocr_pages = 0

    # Process PDFs
    for source in ("esg", "permit", "tender"):
        for pdf_path in inputs[source]:
            try:
                pages = extract_pdf_pages(pdf_path)
                # Build doc-level id from path stem and sha
                doc_text_concat = " ".join([t for _, t, _ in pages if t])
                doc_sha = sha256_of_text(doc_text_concat or pdf_path.as_posix())
                base_id = pdf_path.stem

                for page_no, text, extractor in pages:
                    text_norm = normalize_ws(text)
                    lang = detect_lang(text_norm)
                    rec = PageRecord(
                        doc_id=f"{base_id}",
                        doc_path=str(pdf_path),
                        source=source,
                        page=page_no,
                        text=text_norm,
                        lang=lang,
                        doc_sha256=doc_sha,
                        words=len(text_norm.split()) if text_norm else 0,
                        extractor=extractor,
                    )
                    records.append(rec)
                    if extractor == "ocr":
                        ocr_pages += 1
            except Exception as e:
                failures.append({"path": str(pdf_path), "error": repr(e)})

    # Process news JSON → treat as single-page docs
    for news_path in inputs["news"]:
        try:
            for idx, (doc_id, text) in enumerate(iter_news_records_for_path(news_path), start=1):
                text_norm = normalize_ws(text)
                lang = detect_lang(text_norm)
                doc_sha = sha256_of_text(text_norm or str(news_path))
                rec = PageRecord(
                    doc_id=doc_id,
                    doc_path=str(news_path),
                    source="news",
                    page=1,
                    text=text_norm,
                    lang=lang,
                    doc_sha256=doc_sha,
                    words=len(text_norm.split()) if text_norm else 0,
                    extractor="json",
                )
                records.append(rec)
        except Exception as e:
            failures.append({"path": str(news_path), "error": repr(e)})

    # Build DataFrame
    df = pd.DataFrame([r.__dict__ for r in records])

    # Metrics & gates
    pages_total = int(df.shape[0]) if not df.empty else 0
    words_total = int(df["words"].sum()) if pages_total else 0
    empty_pages = int((df["words"] == 0).sum()) if pages_total else 0
    sources_count = df["source"].value_counts().to_dict() if pages_total else {}
    extractors_count = df["extractor"].value_counts().to_dict() if pages_total else {}

    metrics = {
        "pages_total": pages_total,
        "words_total": words_total,
        "empty_pages": empty_pages,
        "ocr_pages": ocr_pages,
        "by_source": sources_count,
        "by_extractor": extractors_count,
        "failures": failures,
    }

    # Write outputs
    df.to_parquet("data/interim/doc_pages.parquet", index=False)
    Path("data/interim/load_metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    if failures:
        with Path("data/interim/failed_docs.jsonl").open("w", encoding="utf-8") as fh:
            for f in failures:
                fh.write(json.dumps(f, ensure_ascii=False) + "\n")

    # Gate: hard fail if no pages extracted
    if pages_total == 0:
        print("❌ M3.1 Loader: extracted 0 pages — failing fast. Check inputs.", file=sys.stderr)
        return 1

    print(
        f"✅ M3.1 Loader: pages={pages_total}, empty={empty_pages}, ocr={ocr_pages}, "
        f"sources={sources_count}"
    )
    print("→ data/interim/doc_pages.parquet")
    print("→ data/interim/load_metrics.json")
    if failures:
        print(f"⚠️ {len(failures)} inputs failed. See data/interim/failed_docs.jsonl")
    return 0


def iter_news_records_for_path(path: Path) -> Iterable[Tuple[str, str]]:
    """Restrict to a specific file/dir for deterministic iteration."""
    if path.is_dir():
        for fp in sorted(path.glob("*.json")):
            for text in _extract_texts_from_json_file(fp):
                yield (fp.stem, text)
    else:
        # Single file: array JSON or JSONL
        with path.open("r", encoding="utf-8") as fh:
            first = fh.read(1)
            fh.seek(0)
            if first == "[":
                items = json.load(fh)
                for i, obj in enumerate(items):
                    text = _pick_news_text(obj)
                    if text:
                        yield (f"{path.stem}_{i}", text)
            else:
                for i, line in enumerate(fh):
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    text = _pick_news_text(obj)
                    if text:
                        yield (f"{path.stem}_{i}", text)
