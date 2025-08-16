import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

import networkx as nx
import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL not set in .env"

OUT_CSV_DIR = Path("data/processed/kg/csv")
OUT_GRAPH_DIR = Path("data/processed/kg/graph")
GRAPH_PATH = OUT_GRAPH_DIR / "verdantis_kg.graphml"
META_PATH = OUT_GRAPH_DIR / "metadata.json"

TABLE_QUERIES: Dict[str, str] = {
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
        "SELECT id::text, title, source, lang, url, storage_path, "
        "doc_sha256, doc_date, ingested_at "
        "FROM document ORDER BY ingested_at NULLS LAST, id;"
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
        "ST_AsText(location) AS location_wkt "
        "FROM iot_stream ORDER BY started_at NULLS LAST, id;"
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

# (src_table, src_fk_col, dst_table, edge_type)
RELATIONS: List[Tuple[str, str, str, str]] = [
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

NODE_NAME_COL = {
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


def export_tables(conn) -> Dict[str, pd.DataFrame]:
    OUT_CSV_DIR.mkdir(parents=True, exist_ok=True)
    dataframes: Dict[str, pd.DataFrame] = {}
    for tbl, qry in TABLE_QUERIES.items():
        df = pd.read_sql_query(qry, conn)
        out_path = OUT_CSV_DIR / f"{tbl}.csv"
        df.to_csv(out_path, index=False)
        dataframes[tbl] = df
        print(f"✓ CSV wrote: {out_path} (rows={len(df)})")
    return dataframes


def build_graph(dfs: Dict[str, pd.DataFrame]) -> None:
    OUT_GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    G = nx.DiGraph()

    # nodes
    for tbl, df in dfs.items():
        if "id" not in df.columns:
            continue
        name_col = NODE_NAME_COL.get(tbl)
        for _, row in df.iterrows():
            node_key = f"{tbl}:{row['id']}"
            attrs = {"entity": tbl}
            if name_col and name_col in df.columns:
                val = row[name_col]
                attrs["name"] = "" if pd.isna(val) else str(val)
            G.add_node(node_key, **attrs)

    # edges
    for src_tbl, fk_col, dst_tbl, etype in RELATIONS:
        s_df = dfs.get(src_tbl)
        if s_df is None or fk_col not in s_df.columns:
            continue
        for _, row in s_df.iterrows():
            fk = row[fk_col]
            if pd.isna(fk) or fk is None or str(fk).strip() == "":
                continue
            src = f"{src_tbl}:{row['id']}"
            dst = f"{dst_tbl}:{fk}"
            if G.has_node(src) and G.has_node(dst):
                G.add_edge(src, dst, rel=etype)

    # write graphml
    nx.write_graphml(G, GRAPH_PATH)

    # metadata (avoid DataFrame truthiness)
    by_entity: Dict[str, int] = {}
    for t in TABLE_QUERIES:
        df_t = dfs.get(t)
        by_entity[t] = int(df_t.shape[0]) if df_t is not None else 0

    meta = {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "by_entity": by_entity,
        "relations": [{"src": a, "fk": b, "dst": c, "type": d} for a, b, c, d in RELATIONS],
        "graph_path": str(GRAPH_PATH),
    }
    META_PATH.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"✓ GraphML wrote: {GRAPH_PATH} " f"(nodes={meta['nodes']}, edges={meta['edges']})")
    print(f"✓ Metadata wrote: {META_PATH}")


def main() -> None:
    with psycopg.connect(DATABASE_URL) as conn:
        dfs = export_tables(conn)
    build_graph(dfs)


if __name__ == "__main__":
    main()
