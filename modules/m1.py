import hashlib
import json
import os
from datetime import date, datetime, timedelta
from glob import glob
from pathlib import Path
from typing import Dict, List, Tuple

import networkx as nx
import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL not set in .env"

# -------- PATH LOCKS (match your repo) --------
ASSETS_PATH = Path("data/raw/assets/assets.geojson")
PERMITS_PDF_GLOBS = [
    "data/raw/permits/Permit_synth_*.pdf",
    "data/raw/permits/Permit_real_*.pdf",
]
CSV_OUT_DIR = Path("data/processed/kg/csv")
GRAPH_DIR = Path("data/processed/kg/graph")
GRAPH_PATH = GRAPH_DIR / "verdantis_kg.graphml"
META_PATH = GRAPH_DIR / "metadata.json"
# ----------------------------------------------


# --------------- helpers ----------------
def _run_sql_file(conn: psycopg.Connection, path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------- M1.1 -------------------
def schema() -> None:
    """Run M1.1 schema migration."""
    path = Path("db/migrations/0001_create_core_schema.sql")
    if not path.exists():
        raise SystemExit(f"[m1.1] missing {path}")
    with psycopg.connect(DATABASE_URL) as conn:
        _run_sql_file(conn, path)
    print("[m1.1] schema migration applied")


# --------------- M1.2 -------------------
def constraints() -> None:
    """Run M1.2 constraints + indexes migration."""
    path = Path("db/migrations/0002_constraints_indexes.sql")
    if not path.exists():
        raise SystemExit(f"[m1.2] missing {path}")
    with psycopg.connect(DATABASE_URL) as conn:
        _run_sql_file(conn, path)
    print("[m1.2] constraints migration applied")


# --------------- M1.3 -------------------
def _normalize_org_key(name: str | None, cc: str | None) -> tuple[str, str]:
    return (name or "").strip().lower(), (cc or "").strip().upper()


def _upsert_org(cur, name: str | None, org_type: str | None, cc: str | None):
    if not name:
        return None
    key_name, key_cc = _normalize_org_key(name, cc)
    cur.execute(
        """
        SELECT id FROM organization
        WHERE lower(name)=%s AND COALESCE(country_code,'')=COALESCE(%s,'');
        """,
        (key_name, key_cc or None),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        """
        INSERT INTO organization (name, org_type, country_code)
        VALUES (%s, %s, %s)
        RETURNING id;
        """,
        (name.strip(), org_type, (cc or None)),
    )
    return cur.fetchone()[0]


def _load_assets() -> List[dict]:
    if not ASSETS_PATH.exists():
        raise FileNotFoundError("Assets GeoJSON not found at data/raw/assets/assets.geojson")
    gj = json.loads(ASSETS_PATH.read_text(encoding="utf-8"))
    feats = gj.get("features", [])
    if not feats:
        raise ValueError("No features in assets.geojson")
    print(f"[m1.3] assets from {ASSETS_PATH} (features={len(feats)})")
    return feats


def _build_permit_manifest(asset_names: List[str]) -> pd.DataFrame:
    pdfs = sorted({p for pat in PERMITS_PDF_GLOBS for p in glob(pat)})
    if not pdfs:
        raise FileNotFoundError(
            "No permit PDFs found. Expected:\n  " + "\n  ".join(PERMITS_PDF_GLOBS)
        )
    rows = []
    names = sorted(asset_names)
    today = date.today()
    for i, pdf in enumerate(pdfs):
        stem = Path(pdf).stem
        rows.append(
            {
                "asset_name": names[i % len(names)],
                "org_name": "Regulatory Authority",
                "permit_type": "Environmental Permit",
                "status": "active",
                "issue_date": (today - timedelta(days=120 + i * 2)).isoformat(),
                "expiry_date": (today + timedelta(days=365)).isoformat(),
                "reference_id": stem,
                "country_code": "AE",
                "geom_geojson": "",
            }
        )
    print(f"[m1.3] permit manifest from PDFs → rows={len(rows)}")
    return pd.DataFrame(rows)


def seed() -> None:
    """M1.3: load assets+permits (overwrite previous demo rows)."""
    features = _load_assets()
    asset_names = [
        f.get("properties", {}).get("name", f"asset_{i}") for i, f in enumerate(features)
    ]
    permits_df = _build_permit_manifest(asset_names)

    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        print("[m1.3] RESET: TRUNCATE permit, asset (CASCADE)")
        cur.execute("TRUNCATE TABLE permit RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE asset  RESTART IDENTITY CASCADE;")

        name_to_id: Dict[str, str] = {}

        # assets
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry")
            gjson = json.dumps(geom) if geom else None

            owner_name = (
                props.get("owner_org") or props.get("owner") or props.get("operator") or None
            )
            owner_id = _upsert_org(cur, owner_name, "owner", props.get("country_code"))

            cur.execute(
                """
                WITH g AS (
                  SELECT ST_SetSRID(
                           ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))),
                           4326
                         ) AS fp
                )
                INSERT INTO asset
                  (name, kind, status, owner_org_id,
                   centroid, footprint, address, city, region, country_code)
                SELECT
                  %s, %s, %s, %s,
                  ST_Centroid(g.fp), g.fp,
                  %s, %s, %s, %s
                FROM g
                RETURNING id;
                """,
                (
                    gjson,
                    props.get("name"),
                    props.get("kind"),
                    props.get("status") or "active",
                    owner_id,
                    props.get("address"),
                    props.get("city"),
                    props.get("region"),
                    props.get("country_code"),
                ),
            )
            aid = cur.fetchone()[0]
            name_to_id[props.get("name")] = aid

        # permits
        for _, row in permits_df.iterrows():
            aname = str(row.get("asset_name"))
            if aname not in name_to_id:
                raise RuntimeError(f"Unknown asset in manifest: {aname}")

            asset_id = name_to_id[aname]
            org_id = _upsert_org(
                cur, row.get("org_name"), "regulator/issuer", row.get("country_code")
            )
            gjson = row.get("geom_geojson")
            gjson = None if (pd.isna(gjson) or str(gjson).strip() == "") else str(gjson)

            cur.execute(
                """
                WITH g AS (
                  SELECT ST_SetSRID(
                           ST_Multi(ST_MakeValid(ST_GeomFromGeoJSON(%s::text))),
                           4326
                         ) AS gm
                )
                INSERT INTO permit
                  (asset_id, org_id, permit_type, status,
                   issue_date, expiry_date, document_id, reference_id, geom)
                SELECT
                  %s, %s, %s, %s,
                  %s::date, %s::date, NULL, %s, g.gm
                FROM g;
                """,
                (
                    gjson,
                    asset_id,
                    org_id,
                    row.get("permit_type"),
                    row.get("status"),
                    row.get("issue_date"),
                    row.get("expiry_date"),
                    row.get("reference_id"),
                ),
            )

        conn.commit()

    print("[m1.3] seeding complete")


# --------------- M1.3b ------------------
def link_docs() -> None:
    """Ingest permit PDFs into document table and link permits."""
    pdfs = sorted({Path(p) for pat in PERMITS_PDF_GLOBS for p in glob(pat)})
    if not pdfs:
        raise SystemExit("No permit PDFs found to ingest.")
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        # clean prior local sources to avoid cruft
        cur.execute("DELETE FROM document WHERE source='permits_local';")

        docs = links = 0
        for p in pdfs:
            stem = p.stem
            digest = _sha256_file(p)
            doc_date = datetime.fromtimestamp(p.stat().st_mtime).date()

            cur.execute(
                """
                INSERT INTO document
                    (title, source, lang, url, storage_path, doc_sha256, doc_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (doc_sha256) DO UPDATE
                  SET storage_path = EXCLUDED.storage_path
                RETURNING id;
                """,
                (stem, "permits_local", "en", None, str(p), digest, doc_date),
            )
            doc_id = cur.fetchone()[0]
            docs += 1

            cur.execute(
                "UPDATE permit SET document_id=%s WHERE reference_id=%s;",
                (doc_id, stem),
            )
            links += cur.rowcount

        conn.commit()
    print(f"[m1.3b] documents upserted={docs}, permits linked={links}")


# --------------- M1.4 -------------------
_TABLE_QUERIES: Dict[str, str] = {
    "organization": (
        "SELECT id::text, name, org_type, country_code, created_at "
        "FROM organization ORDER BY created_at;"
    ),
    "asset": (
        "SELECT id::text, name, kind, status, owner_org_id::text, "
        "ST_AsText(centroid) AS centroid_wkt, "
        "ST_AsText(footprint) AS footprint_wkt, "
        "address, city, region, country_code, created_at, updated_at "
        "FROM asset ORDER BY created_at;"
    ),
    "permit": (
        "SELECT id::text, asset_id::text, org_id::text, permit_type, status, "
        "issue_date, expiry_date, document_id::text, reference_id, "
        "ST_AsText(geom) AS geom_wkt "
        "FROM permit ORDER BY issue_date NULLS LAST, id;"
    ),
    "document": (
        "SELECT id::text, title, source, lang, url, storage_path, doc_sha256, "
        "doc_date, ingested_at FROM document "
        "ORDER BY ingested_at NULLS LAST, id;"
    ),
    "policy_clause": (
        "SELECT id::text, document_id::text, clause_ref, jurisdiction, theme, "
        "clause_hash FROM policy_clause ORDER BY id;"
    ),
    "satellite_tile": (
        "SELECT id::text, asset_id::text, aoi_name, capture_date, sensor, path, "
        "cloud_cover, ST_AsText(footprint) AS footprint_wkt "
        "FROM satellite_tile ORDER BY capture_date NULLS LAST, id;"
    ),
    "iot_stream": (
        "SELECT id::text, asset_id::text, stream_name, unit, started_at, ended_at, "
        "ST_AsText(location) AS location_wkt FROM iot_stream "
        "ORDER BY started_at NULLS LAST, id;"
    ),
    "event": (
        "SELECT id::text, event_key, event_type, asset_id::text, "
        "related_document_id::text, occurred_at, payload::text "
        "FROM event ORDER BY occurred_at NULLS LAST, id;"
    ),
    "rule": (
        "SELECT id::text, rule_code, name, description, severity, version, "
        "definition::text, target_selector::text, created_at "
        "FROM rule ORDER BY created_at;"
    ),
    "proof_bundle": (
        "SELECT id::text, asset_id::text, rule_id::text, event_id::text, "
        "document_id::text, status, proof_hash, evidence_url, meta::text, "
        "created_at FROM proof_bundle ORDER BY created_at;"
    ),
}

_RELATIONS: List[Tuple[str, str, str, str]] = [
    ("asset", "owner_org_id", "organization", "owned_by"),
    ("permit", "asset_id", "asset", "applies_to"),
    ("permit", "org_id", "organization", "issued_by"),
    ("permit", "document_id", "document", "document"),
    ("satellite_tile", "asset_id", "asset", "observes"),
    ("iot_stream", "asset_id", "asset", "monitors"),
    ("event", "asset_id", "asset", "about"),
    ("event", "related_document_id", "document", "refers_to"),
    ("proof_bundle", "asset_id", "asset", "proof_for"),
    ("proof_bundle", "rule_id", "rule", "attests_rule"),
    ("proof_bundle", "event_id", "event", "for_event"),
    ("proof_bundle", "document_id", "document", "evidence"),
    ("policy_clause", "document_id", "document", "part_of"),
]

_NODE_NAME_COL = {
    "organization": "name",
    "asset": "name",
    "permit": "reference_id",
    "document": "title",
    "policy_clause": "clause_ref",
    "satellite_tile": "aoi_name",
    "iot_stream": "stream_name",
    "event": "event_key",
    "rule": "rule_code",
    "proof_bundle": "proof_hash",
}


def export() -> None:
    """M1.4: export CSV per table + GraphML + metadata."""
    CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
    GRAPH_DIR.mkdir(parents=True, exist_ok=True)

    dfs: Dict[str, pd.DataFrame] = {}
    with psycopg.connect(DATABASE_URL) as conn:
        for tbl, qry in _TABLE_QUERIES.items():
            df = pd.read_sql_query(qry, conn)
            out = CSV_OUT_DIR / f"{tbl}.csv"
            df.to_csv(out, index=False)
            dfs[tbl] = df
            print(f"[m1.4] CSV wrote: {out} (rows={len(df)})")

    G = nx.DiGraph()
    # nodes
    for tbl, df in dfs.items():
        if "id" not in df.columns:
            continue
        name_col = _NODE_NAME_COL.get(tbl)
        for _, row in df.iterrows():
            key = f"{tbl}:{row['id']}"
            attrs = {"entity": tbl}
            if name_col and name_col in df.columns:
                val = row[name_col]
                attrs["name"] = "" if pd.isna(val) else str(val)
            G.add_node(key, **attrs)
    # edges
    for s_tbl, fk_col, d_tbl, etype in _RELATIONS:
        s_df = dfs.get(s_tbl)
        if s_df is None or fk_col not in s_df.columns:
            continue
        for _, row in s_df.iterrows():
            fk = row[fk_col]
            if pd.isna(fk) or fk is None or str(fk).strip() == "":
                continue
            src = f"{s_tbl}:{row['id']}"
            dst = f"{d_tbl}:{fk}"
            if G.has_node(src) and G.has_node(dst):
                G.add_edge(src, dst, rel=etype)

    nx.write_graphml(G, GRAPH_PATH)

    by_entity: Dict[str, int] = {}
    for t, df_t in dfs.items():
        by_entity[t] = int(df_t.shape[0])

    meta = {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "by_entity": by_entity,
        "relations": [{"src": a, "fk": b, "dst": c, "type": d} for a, b, c, d in _RELATIONS],
        "graph_path": str(GRAPH_PATH),
    }
    META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[m1.4] GraphML wrote: {GRAPH_PATH} " f"(nodes={meta['nodes']}, edges={meta['edges']})")
    print(f"[m1.4] Metadata wrote: {META_PATH}")


# --------------- M1.verify --------------
def verify() -> None:
    """Aggregate verifier for Module 1."""
    # constraints present?
    with psycopg.connect(DATABASE_URL) as conn, conn.cursor() as cur:
        for tbl, con in [
            ("document", "uq_document_sha256"),
            ("rule", "uq_rule_code"),
            ("event", "uq_event_key"),
        ]:
            cur.execute(
                """
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                WHERE c.conname=%s AND t.relname=%s AND n.nspname='public';
                """,
                (con, tbl),
            )
            ok = cur.fetchone() is not None
            print(f"[m1.2] unique {tbl}.{con}: {'OK' if ok else 'MISSING'}")
            if not ok:
                raise SystemExit("[m1] FAILED: missing uniqueness")

        # counts & geom validity
        cur.execute("SELECT COUNT(*) FROM asset;")
        assets = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM permit;")
        permits = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM organization;")
        orgs = cur.fetchone()[0]
        print(f"[m1.3] counts asset={assets}, permit={permits}, org={orgs}")
        if not (assets >= 3 and permits >= 5 and orgs >= 2):
            raise SystemExit("[m1] FAILED: counts outside expected")

        for tbl, col in [
            ("asset", "footprint"),
            ("asset", "centroid"),
            ("permit", "geom"),
        ]:
            cur.execute(
                f"SELECT COUNT(*) FROM {tbl} " f"WHERE {col} IS NOT NULL AND NOT ST_IsValid({col});"
            )
            bad = cur.fetchone()[0]
            print(f"[m1.3] geom {tbl}.{col}: invalid={bad}")
            if bad != 0:
                raise SystemExit("[m1] FAILED: invalid geometries")

    # artifacts exist?
    csv_ok = all((CSV_OUT_DIR / f"{t}.csv").exists() for t in _TABLE_QUERIES)
    graph_ok = GRAPH_PATH.exists() and META_PATH.exists()
    print(f"[m1.4] csv_all={'OK' if csv_ok else 'MISSING'}")
    print(f"[m1.4] graphml={'OK' if graph_ok else 'MISSING'}")
    if not (csv_ok and graph_ok):
        raise SystemExit("[m1] FAILED: export artifacts missing")

    print("[m1] PASSED")
