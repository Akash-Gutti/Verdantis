-- sql/m5_full_reset.sql
-- Module 5: full (re)build of all views used by the app (M5.1 + M5.3)

-- 0) Prereqs ---------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgis;

-- Optional helpful indexes (no-op if exist)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='assets') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_assets_geom ON public.assets USING GIST(geom)';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='doc_index') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_doc_index_asset ON public.doc_index(asset_id)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_doc_index_pub ON public.doc_index(published_at)';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables
             WHERE table_schema='public' AND table_name='doc_clauses') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_doc_clauses_sha ON public.doc_clauses(doc_sha256)';
  END IF;
END $$;

-- 1) M5.1: Assets ----------------------------------------------------------

-- Base source view from public.assets (columns: id, name, asset_type, city, country, geom)
DROP VIEW IF EXISTS vw_assets_source CASCADE;
CREATE VIEW vw_assets_source AS
SELECT
  a.id::text        AS asset_id,
  a.name::text      AS name,
  a.asset_type::text AS asset_type,
  a.city::text      AS city,
  a.country::text   AS country,
  /* normalize to 4326, force 2D */
  ST_Force2D(
    CASE WHEN ST_SRID(a.geom) = 4326
         THEN a.geom
         ELSE ST_Transform(a.geom, 4326)
    END
  )                 AS geom
FROM public.assets a;

DROP VIEW IF EXISTS vw_assets_basic CASCADE;
CREATE VIEW vw_assets_basic AS
SELECT asset_id, name, asset_type, city, country, geom
FROM vw_assets_source;

-- 2) M5.1: Documents (normalized) -----------------------------------------

-- Expect table public.doc_index with: doc_sha256, asset_id, title, source, url, lang, published_at
DROP VIEW IF EXISTS vw_doc_index_norm CASCADE;
CREATE VIEW vw_doc_index_norm AS
SELECT
  d.doc_sha256::text   AS doc_sha256,
  d.asset_id::text     AS asset_id,
  COALESCE(d.title,'')::text   AS title,
  COALESCE(d.source,'')::text  AS source,
  COALESCE(d.url,'')::text     AS url,
  COALESCE(d.lang,'')::text    AS lang,
  /* published_at may be null */
  d.published_at::timestamp    AS published_at
FROM public.doc_index d;

-- 3) M5.1: Citation counts (override-aware) -------------------------------

CREATE TABLE IF NOT EXISTS public.doc_citation_override (
  doc_sha256 TEXT PRIMARY KEY,
  citation_count INT NOT NULL CHECK (citation_count >= 0)
);

DROP VIEW IF EXISTS vw_doc_citation_counts CASCADE;
CREATE VIEW vw_doc_citation_counts AS
WITH c AS (
  SELECT dc.doc_sha256, COUNT(*)::int AS clause_count
  FROM public.doc_clauses dc
  GROUP BY dc.doc_sha256
)
SELECT d.doc_sha256,
       COALESCE(o.citation_count, c.clause_count, 0) AS clause_count
FROM vw_doc_index_norm d
LEFT JOIN c ON c.doc_sha256 = d.doc_sha256
LEFT JOIN public.doc_citation_override o ON o.doc_sha256 = d.doc_sha256;

-- 4) M5.1: Docs joined to assets -----------------------------------------

DROP VIEW IF EXISTS vw_docs_with_assets CASCADE;
CREATE VIEW vw_docs_with_assets AS
SELECT
  d.doc_sha256,
  d.asset_id,
  a.name,
  d.title,
  d.source,
  d.url,
  d.lang,
  d.published_at
FROM vw_doc_index_norm d
JOIN vw_assets_basic a ON a.asset_id = d.asset_id;

-- 5) M5.1: Current events (last 60 days; used by m5 verify) ---------------

DROP VIEW IF EXISTS vw_asset_events_current CASCADE;
CREATE VIEW vw_asset_events_current AS
SELECT
  d.asset_id,
  a.name,
  d.doc_sha256,
  d.title,
  d.published_at,
  d.source,
  d.url,
  COALESCE(c.clause_count, 0) AS citation_count,
  (COALESCE(c.clause_count, 0) >= 2) AS has_min_citations
FROM vw_docs_with_assets d
LEFT JOIN vw_doc_citation_counts c ON c.doc_sha256 = d.doc_sha256
JOIN vw_assets_basic a ON a.asset_id = d.asset_id
WHERE d.published_at >= CURRENT_DATE - INTERVAL '60 days'
ORDER BY d.asset_id, d.published_at DESC;

-- 6) M5.1: Overlays (250 m buffer around assets) --------------------------

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

-- 7) M5.3: Evidence+ views -------------------------------------------------

CREATE TABLE IF NOT EXISTS public.proof_bundle_override(
  doc_sha256 TEXT PRIMARY KEY,
  bundle_id  TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP VIEW IF EXISTS vw_doc_citations_detail CASCADE;
CREATE VIEW vw_doc_citations_detail AS
SELECT
  dc.doc_sha256,
  COALESCE(dc.page, 1)        AS page,
  COALESCE(dc.clause_type,'') AS clause_type,
  LEFT(COALESCE(dc.text,''), 300) AS snippet
FROM public.doc_clauses dc;

DROP VIEW IF EXISTS vw_kg_edges_from_clauses CASCADE;
CREATE VIEW vw_kg_edges_from_clauses AS
SELECT
  md5(dc.doc_sha256 || ':' || COALESCE(dc.clause_type,'') || ':' || COALESCE(dc.page::text,'')) AS edge_id,
  'asset'::text AS src_type,
  d.asset_id::text AS src_id,
  CASE
    WHEN lower(COALESCE(dc.clause_type,'')) = 'metric'      THEN 'metric'
    WHEN lower(COALESCE(dc.clause_type,'')) = 'permit'      THEN 'permit'
    WHEN lower(COALESCE(dc.clause_type,'')) = 'operational' THEN 'operation'
    ELSE 'policy'
  END AS dst_type,
  md5(COALESCE(dc.clause_type,'') || ':' || COALESCE(dc.text,'')) AS dst_id,
  COALESCE(dc.clause_type,'') AS label,
  0.7::float AS weight,
  dc.doc_sha256
FROM public.doc_clauses dc
JOIN vw_doc_index_norm d ON d.doc_sha256 = dc.doc_sha256;

DROP VIEW IF EXISTS vw_doc_proof_bundle CASCADE;
CREATE VIEW vw_doc_proof_bundle AS
SELECT
  d.doc_sha256,
  COALESCE(pbo.bundle_id, 'pb-' || substr(md5(d.doc_sha256), 1, 12)) AS bundle_id
FROM vw_doc_index_norm d
LEFT JOIN public.proof_bundle_override pbo ON pbo.doc_sha256 = d.doc_sha256;
