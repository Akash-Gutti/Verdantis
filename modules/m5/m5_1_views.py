# modules/m5/m5_1_views.py
from __future__ import annotations

import os
from typing import Dict, List, Optional, Tuple

import psycopg


def _dsn() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "verdantis")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def _exec_sql(cur: psycopg.Cursor, sql: str) -> None:
    cur.execute(sql)


def _list_tables(conn: psycopg.Connection) -> List[Tuple[str, str]]:
    sql = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema NOT IN ('pg_catalog', 'information_schema');
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [(str(s), str(t)) for (s, t) in rows]


def _list_columns(conn: psycopg.Connection, schema: str, table: str) -> List[Tuple[str, str]]:
    sql = """
    SELECT column_name, udt_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        rows = cur.fetchall()
    return [(str(c), str(t)) for (c, t) in rows]


def _quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


# ---------- ASSETS DISCOVERY / BIND ----------


def _find_asset_table_and_mapping(
    conn: psycopg.Connection,
) -> Tuple[str, str, Dict[str, str]]:
    candidates = {
        "id": ["id", "asset_id"],
        "name": ["name", "asset_name", "project_name", "site_name", "title"],
        "asset_type": ["asset_type", "type", "category"],
        "city": ["city", "town"],
        "country": ["country", "nation"],
        "geom": ["geom", "geometry", "wkb_geometry", "the_geom"],
    }
    geometry_udts = {"geometry", "geography"}
    best: Optional[Tuple[str, str, Dict[str, str]]] = None

    for schema, table in _list_tables(conn):
        cols = _list_columns(conn, schema, table)
        colnames = {c for (c, _) in cols}
        udt = {c: u for (c, u) in cols}

        found: Dict[str, Optional[str]] = {
            k: None for k in ["id", "name", "asset_type", "city", "country", "geom"]
        }

        for cand in candidates["geom"]:
            if cand in colnames and udt.get(cand) in geometry_udts:
                found["geom"] = cand
                break
        if not found["geom"]:
            continue

        for key in ("id", "name", "asset_type", "city", "country"):
            for cand in candidates[key]:
                if cand in colnames:
                    found[key] = cand
                    break

        if found["id"] and found["name"]:
            mapping = {k: v or "" for (k, v) in found.items()}
            best = (schema, table, mapping)
            if table in {"assets", "assets_geo", "m1_assets"}:
                break

    if not best:
        raise SystemExit(
            "No assets-like table found (needs geometry + id + name). "
            "Run 'verdctl m5 seed-assets' or 'verdctl m5 bind' first."
        )
    return best


def _create_assets_source_view(
    cur: psycopg.Cursor, schema: str, table: str, m: Dict[str, str]
) -> None:
    qschema = _quote_ident(schema)
    qtable = _quote_ident(table)
    q_id = _quote_ident(m["id"])
    q_name = _quote_ident(m["name"])
    q_geom = _quote_ident(m["geom"])

    sel_asset_type = _quote_ident(m["asset_type"]) if m["asset_type"] else "NULL::text"
    sel_city = _quote_ident(m["city"]) if m["city"] else "NULL::text"
    sel_country = _quote_ident(m["country"]) if m["country"] else "NULL::text"

    sql = f"""
    DROP VIEW IF EXISTS vw_assets_source CASCADE;
    CREATE VIEW vw_assets_source AS
    SELECT
        {q_id}::text AS asset_id,
        {q_name}::text AS name,
        {sel_asset_type}::text AS asset_type,
        {sel_city}::text AS city,
        {sel_country}::text AS country,
        {q_geom} AS geom
    FROM {qschema}.{qtable}
    WHERE {q_geom} IS NOT NULL;
    """
    _exec_sql(cur, sql)


def create_assets_source_from_table(
    schema: str,
    table: str,
    mapping: Dict[str, str],
) -> None:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        _create_assets_source_view(cur, schema, table, mapping)
        conn.commit()


# ---------- DOC INDEX DISCOVERY / BIND ----------


def _find_doc_index_table_and_mapping(
    conn: psycopg.Connection,
) -> Tuple[str, str, Dict[str, str]]:
    """
    Find a docs-like table with doc + asset linkage.
    Required: doc_sha256 & asset_id
    Optional: title, source, url, lang, any published_* timestamp.
    """
    must_doc = ["doc_sha256", "sha256", "doc_id", "id"]
    must_asset = ["asset_id", "asset", "site_id", "project_id", "asset_code"]
    title_c = ["title", "doc_title", "name"]
    source_c = ["source", "publisher"]
    url_c = ["url", "link"]
    lang_c = ["lang", "language"]
    published_c = [
        "published_at",
        "published",
        "date",
        "created_at",
        "ingested_at",
    ]

    prefer_tables = {"doc_index", "documents", "articles", "news"}

    best: Optional[Tuple[str, str, Dict[str, str]]] = None

    for schema, table in _list_tables(conn):
        cols = _list_columns(conn, schema, table)
        colnames = {c for (c, _) in cols}

        doc_col = next((c for c in must_doc if c in colnames), None)
        asset_col = next((c for c in must_asset if c in colnames), None)
        if not doc_col or not asset_col:
            continue

        def _opt(keys: List[str]) -> str:
            for k in keys:
                if k in colnames:
                    return k
            return ""

        mapping = {
            "doc_sha256": doc_col,
            "asset_id": asset_col,
            "title": _opt(title_c),
            "source": _opt(source_c),
            "url": _opt(url_c),
            "lang": _opt(lang_c),
        }

        # gather available published candidates in table
        mapping["published_list"] = ",".join([c for c in published_c if c in colnames])

        best = (schema, table, mapping)
        if table in prefer_tables:
            break

    if not best:
        raise SystemExit(
            "No docs-like table with doc↔asset link found. "
            "Run 'verdctl m5 bind-docs --table ... --doc ... --asset ...' first."
        )
    return best


def _create_doc_index_norm_view(
    cur: psycopg.Cursor, schema: str, table: str, m: Dict[str, str]
) -> None:
    qschema = _quote_ident(schema)
    qtable = _quote_ident(table)

    q_doc = _quote_ident(m["doc_sha256"])
    q_asset = _quote_ident(m["asset_id"])

    q_title = _quote_ident(m["title"]) if m["title"] else "NULL::text"
    q_source = _quote_ident(m["source"]) if m["source"] else "NULL::text"
    q_url = _quote_ident(m["url"]) if m["url"] else "NULL::text"
    q_lang = _quote_ident(m["lang"]) if m["lang"] else "NULL::text"

    pub_cols = [c for c in (m.get("published_list") or "").split(",") if c]
    if pub_cols:
        pub_expr = "COALESCE(" + ", ".join(_quote_ident(c) for c in pub_cols) + ")"
    else:
        pub_expr = "NULL::timestamp"

    sql = f"""
    DROP VIEW IF EXISTS vw_doc_index_norm CASCADE;
    CREATE VIEW vw_doc_index_norm AS
    SELECT
        {q_doc}::text AS doc_sha256,
        {q_title}::text AS title,
        ({pub_expr})::timestamp AS published_at,
        {q_source}::text AS source,
        {q_url}::text AS url,
        {q_lang}::text AS lang,
        {q_asset}::text AS asset_id
    FROM {qschema}.{qtable}
    WHERE {q_asset} IS NOT NULL;
    """
    _exec_sql(cur, sql)


def create_doc_index_norm_from_table(
    schema: str,
    table: str,
    mapping: Dict[str, str],
) -> None:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        _create_doc_index_norm_view(cur, schema, table, mapping)
        conn.commit()


# ---------- DOWNSTREAM VIEWS & PUBLIC API ----------


def _create_downstream_views(cur: psycopg.Cursor) -> None:
    _exec_sql(
        cur,
        """
        DROP VIEW IF EXISTS vw_assets_basic CASCADE;
        CREATE VIEW vw_assets_basic AS
        SELECT
            s.asset_id,
            s.name,
            s.asset_type,
            s.city,
            s.country,
            ST_Transform(s.geom, 4326) AS geom
        FROM vw_assets_source s
        WHERE ST_IsValid(s.geom);
        """,
    )
    _exec_sql(
        cur,
        """
        DROP VIEW IF EXISTS vw_doc_citation_counts CASCADE;
        CREATE VIEW vw_doc_citation_counts AS
        SELECT
            dc.doc_sha256,
            COUNT(*)::int AS clause_count
        FROM doc_clauses dc
        GROUP BY dc.doc_sha256;
        """,
    )
    _exec_sql(
        cur,
        """
        DROP VIEW IF EXISTS vw_docs_with_assets CASCADE;
        CREATE VIEW vw_docs_with_assets AS
        SELECT
            d.doc_sha256,
            d.title,
            d.published_at,
            d.source,
            d.url,
            d.lang,
            d.asset_id
        FROM vw_doc_index_norm d
        WHERE d.asset_id IS NOT NULL;
        """,
    )
    _exec_sql(
        cur,
        """
        DROP VIEW IF EXISTS vw_asset_events_current CASCADE;
        CREATE VIEW vw_asset_events_current AS
        SELECT
            a.asset_id,
            a.name,
            d.doc_sha256,
            d.title,
            d.published_at,
            d.source,
            d.url,
            COALESCE(c.clause_count, 0) AS citation_count,
            (COALESCE(c.clause_count, 0) >= 2) AS has_min_citations
        FROM vw_assets_basic a
        JOIN vw_docs_with_assets d
          ON d.asset_id = a.asset_id
        LEFT JOIN vw_doc_citation_counts c
          ON c.doc_sha256 = d.doc_sha256
        WHERE d.published_at >= (CURRENT_DATE - INTERVAL '60 days')
           OR d.published_at IS NULL
        ORDER BY a.asset_id, d.published_at DESC NULLS LAST;
        """,
    )
    _exec_sql(
        cur,
        """
        DROP VIEW IF EXISTS vw_asset_overlays CASCADE;
        CREATE VIEW vw_asset_overlays AS
        SELECT
            a.asset_id,
            a.name,
            'proximity_250m'::text AS overlay_type,
            ST_Transform(
                ST_Buffer(
                    ST_Transform(a.geom, 3857),
                    250.0
                ),
                4326
            ) AS geom
        FROM vw_assets_basic a;
        """,
    )


def create_views() -> None:
    with psycopg.connect(_dsn()) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        with conn.cursor() as cur:
            # assets
            a_schema, a_table, a_map = _find_asset_table_and_mapping(conn)
            print("🔎 Detected assets table:", f"{a_schema}.{a_table} → {a_map}")
            _create_assets_source_view(cur, a_schema, a_table, a_map)

            # docs
            d_schema, d_table, d_map = _find_doc_index_table_and_mapping(conn)
            print("🔎 Detected docs table:", f"{d_schema}.{d_table} → {d_map}")
            _create_doc_index_norm_view(cur, d_schema, d_table, d_map)

            # downstream
            _create_downstream_views(cur)
        conn.commit()


def create_views_after_bind() -> None:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        _create_downstream_views(cur)
        conn.commit()


def verify() -> Tuple[int, int, int]:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vw_assets_basic;")
        assets = int(cur.fetchone()[0])

        cur.execute("SELECT COUNT(*) FROM vw_asset_events_current;")
        events = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT COUNT(DISTINCT asset_id)
            FROM vw_asset_events_current
            WHERE has_min_citations = true;
            """
        )
        assets_with_min = int(cur.fetchone()[0])
    return assets, events, assets_with_min
