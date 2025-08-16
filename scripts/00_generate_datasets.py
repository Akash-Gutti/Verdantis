import datetime
import json
import random
import textwrap
from pathlib import Path

from dateutil.relativedelta import relativedelta
from fpdf import FPDF

# ----- CONFIG -----
BASE = Path("data/raw")

ESG_COUNT = 35
ESG_MIN_PAGES = 50
ESG_WORDS_PER_PAGE = (180, 230)

PERMIT_COUNT = 18
PERMIT_MIN_WORDS = 600

TENDER_COUNT = 12
TENDER_MIN_WORDS = 700

NEWS_COUNT = 100
NEWS_MIN_WORDS = 220

SAT_EXPECTED_MAP = {
    "before_red": "_before_B4.tif",
    "before_nir": "_before_B8.tif",
    "after_red": "_after_B4.tif",
    "after_nir": "_after_B8.tif",
}

# Helpers for safe text
SAFE_MAP = {"—": "-", "–": "-", "’": "'", "‘": "'", "“": '"', "”": '"', "•": "*", "\u00A0": " "}


def to_latin1(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    s = "".join(SAFE_MAP.get(ch, ch) for ch in s)
    return s.encode("latin-1", "ignore").decode("latin-1")


# ----- UTILS -----
def ensure_dirs():
    (BASE / "pdfs").mkdir(parents=True, exist_ok=True)
    (BASE / "permits").mkdir(parents=True, exist_ok=True)
    (BASE / "tenders").mkdir(parents=True, exist_ok=True)
    (BASE / "news").mkdir(parents=True, exist_ok=True)
    (BASE / "satellite" / "aoi_real").mkdir(parents=True, exist_ok=True)


def words(n):
    topics = [
        "emissions reduction",
        "energy efficiency",
        "water stewardship",
        "waste diversion",
        "circular economy",
        "biodiversity protection",
        "renewable procurement",
        "supply-chain decarbonization",
        "occupational safety",
        "board oversight",
        "ethics & compliance",
        "community engagement",
        "green financing",
        "ISO 14001 systems",
        "climate risk & TCFD",
        "sustainable procurement",
        "mangrove restoration",
        "wetland conservation",
        "solar deployment",
        "waste-to-energy",
        "hazardous materials control",
        "marine protection",
        "NDVI monitoring",
        "satellite change detection",
        "UAE Net Zero",
        "MENA policy alignment",
    ]
    out = []
    while len(" ".join(out).split()) < n:
        topics_count = min(random.randint(2, 5), len(topics))
        t = random.sample(topics, k=topics_count)
        para = (
            "This section details "
            + ", ".join(t[:-1])
            + (", and " + t[-1] if len(t) > 1 else "")
            + ", including baselines, targets, governance, and verification mechanisms. "
            "It explains methodology, assumptions, known limitations, and control procedures, "
            "illustrating how data quality is protected and how corrective actions are triggered."
        )
        out.append(para)
    return " ".join(out)


def wrap(txt, width=92):
    return "\n".join(textwrap.fill(line, width=width) for line in txt.split("\n"))


class LongPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", size=10)
        self.set_text_color(80)
        self.cell(0, 8, to_latin1(self.title), ln=1, align="R")
        self.ln(2)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", size=9)
        self.set_text_color(120)
        self.cell(0, 8, f"Page {self.page_no()}", align="C")


def pdf_paragraph(pdf: LongPDF, text: str, size=11, ln=6):
    pdf.set_font("Helvetica", size=size)
    pdf.set_text_color(20)
    pdf.multi_cell(0, 6, wrap(to_latin1(text), 110))
    pdf.ln(ln)


def make_long_pdf(path: Path, title: str, pages: int, words_per_page=(180, 230), sections=5):
    pdf = LongPDF()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.title = to_latin1(title)

    pdf.add_page()
    pdf.set_font("Helvetica", size=22)
    pdf.cell(0, 14, to_latin1(title), ln=1)
    pdf.ln(4)
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(
        0,
        7,
        wrap(
            to_latin1(
                "This document is auto-generated for Verdantis R2 demonstration. "
                "It follows a realistic structure with governance, strategy, risk, "
                "metrics & targets, data quality controls, and assurance narrative. "
                "The content is synthetic but domain-aligned."
            )
        ),
    )
    pdf.ln(6)

    for p in range(pages):
        pdf.add_page()
        for s in range(sections):
            pdf.set_font("Helvetica", size=14)
            pdf.cell(0, 8, to_latin1(f"Section {p+1}.{s+1} - Strategy & Performance"), ln=1)
            body_words = random.randint(*words_per_page) // sections
            pdf_paragraph(pdf, words(body_words), size=11, ln=4)

    pdf.output(str(path))


# ----- ESG PDFs -----
def generate_esg_pdfs(count=ESG_COUNT, min_pages=ESG_MIN_PAGES):
    existing = {p.name for p in (BASE / "pdfs").glob("*.pdf")}
    for i in range(1, count + 1):
        fname = f"ESG_Report_synth_{i:03d}.pdf"
        if fname in existing:
            continue
        path = BASE / "pdfs" / fname
        title = f"Corporate Sustainability & ESG Report {i:03d} - 2024/25"
        make_long_pdf(path, title, pages=min_pages, words_per_page=ESG_WORDS_PER_PAGE, sections=5)


# ----- Permits -----
def generate_permits(count=PERMIT_COUNT):
    for i in range(1, count + 1):
        path = BASE / "permits" / f"Permit_synth_{i:03d}.pdf"
        make_long_pdf(
            path,
            f"Environmental Operating Permit #{i:03d}",
            pages=3,
            words_per_page=(600, 800),
            sections=3,
        )


# ----- Tenders -----
def generate_tenders(count=TENDER_COUNT):
    for i in range(1, count + 1):
        path = BASE / "tenders" / f"Tender_synth_{i:03d}.pdf"
        make_long_pdf(
            path,
            f"Tender Notice #{i:03d} - Environmental Works",
            pages=4,
            words_per_page=(700, 900),
            sections=3,
        )


# ----- News -----
def random_date_within(days=180):
    today = datetime.date.today()
    start = today - relativedelta(days=days)
    dt = start + relativedelta(days=random.randint(0, days))
    return dt.isoformat()


def news_item(idx: int):
    sources = [
        "LocalENNews",
        "LocalArabicNews",
        "MENAEnvReview",
        "GulfSustainability",
        "PolicyWatchUAE",
    ]
    titles = [
        "City unveils integrated emissions and water strategy for 2030",
        "New biodiversity roadmap links mangrove restoration to blue carbon credits",
        "Waste-to-energy milestone announced amid circular economy targets",
        "Industrial decarbonization program expands supplier verification",
        "Coastal resilience plan pairs satellites with surveys",
    ]
    title = random.choice(titles)
    if random.random() < 0.15:
        title = to_latin1("تحديث شامل لاستراتيجية الانبعاثات والمياه حتى عام 2030")
    body = words(max(NEWS_MIN_WORDS + 30, 230))
    return {
        "id": f"news_{idx}",
        "title": to_latin1(title),
        "body": to_latin1(body),
        "date": random_date_within(180),
        "source": random.choice(sources),
        "url": f"https://example.org/{idx}",
    }


def generate_news(n=NEWS_COUNT):
    path = BASE / "news" / "news.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        for i in range(1, n + 1):
            json.dump(news_item(i), f, ensure_ascii=False)
            f.write("\n")


# ----- Satellite conversion (unchanged) -----
def convert_tif_to_csv():
    try:
        import numpy as np
        import rasterio
    except Exception:
        print("⚠ rasterio not available. Skipping.")
        return
    sat_root = BASE / "satellite"
    aoi_real = sat_root / "aoi_real"
    tifs = list(sat_root.glob("**/*_before_B4.tif"))
    if not tifs:
        print("⚠ No *_before_B4.tif found under data/raw/satellite.")
        return
    used_master = False
    for b4_before_tif in tifs:
        site_dir = b4_before_tif.parent
        slug = site_dir.name

        def find(rel_suffix):
            p = list(site_dir.glob(f"*{rel_suffix}"))
            if p:
                return p[0]
            global_p = list(sat_root.glob(f"**/*{rel_suffix}"))
            return global_p[0] if global_p else None

        pairs = {
            "before_red": b4_before_tif,
            "before_nir": find(SAT_EXPECTED_MAP["before_nir"]),
            "after_red": find(SAT_EXPECTED_MAP["after_red"]),
            "after_nir": find(SAT_EXPECTED_MAP["after_nir"]),
        }
        if any(v is None or not Path(v).exists() for v in pairs.values()):
            print(f"⚠ Missing TIFFs for {slug}; skipping site.")
            continue

        def write_csv(tif_path: Path, out_csv: Path):
            with rasterio.open(tif_path) as src:
                arr = src.read(1)
            rows, cols = arr.shape
            grid = np.stack(
                np.meshgrid(np.arange(rows), np.arange(cols), indexing="ij"), axis=-1
            ).reshape(-1, 2)
            values = arr.reshape(-1)
            import pandas as pd

            df = pd.DataFrame(grid, columns=["row", "col"])
            df["value"] = values
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out_csv, index=False)

        site_csv_dir = site_dir / "csv"
        for key, tifp in pairs.items():
            write_csv(Path(tifp), site_csv_dir / f"{key}.csv")
        if not used_master:
            for key, tifp in pairs.items():
                write_csv(Path(tifp), aoi_real / f"{key}.csv")
            used_master = True
            print(f"✓ Wrote canonical aoi_real CSVs from site: {slug}")


# ----- MAIN -----
if __name__ == "__main__":
    ensure_dirs()
    print("→ Generating ESG PDFs…")
    generate_esg_pdfs()
    print("→ Generating Permits…")
    generate_permits()
    print("→ Generating Tenders…")
    generate_tenders()
    print("→ Generating News…")
    generate_news()
    print("→ Converting satellite TIFFs to CSV…")
    convert_tif_to_csv()
    print("✓ Dataset generation complete.")
