-- Enable extensions (id generation + GIS)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- for gen_random_uuid()

-- =============== ORGANIZATION ===============
CREATE TABLE IF NOT EXISTS organization (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    org_type        TEXT,                  -- owner, operator, regulator, supplier, other
    country_code    CHAR(2),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============== ASSET ===============
CREATE TABLE IF NOT EXISTS asset (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name             TEXT NOT NULL,
    kind             TEXT,                 -- building, plant, solar_farm, wind_farm, pipeline, other
    status           TEXT,                 -- active, inactive, planned, decommissioned
    owner_org_id     UUID REFERENCES organization(id) ON DELETE SET NULL,   -- [1 relation]
    centroid         geometry(Point, 4326),
    footprint        geometry(MultiPolygon, 4326),
    address          TEXT,
    city             TEXT,
    region           TEXT,
    country_code     CHAR(2),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Spatial indexes for fast geo queries
CREATE INDEX IF NOT EXISTS idx_asset_centroid_gist  ON asset USING GIST (centroid);
CREATE INDEX IF NOT EXISTS idx_asset_footprint_gist ON asset USING GIST (footprint);

-- =============== DOCUMENT ===============
CREATE TABLE IF NOT EXISTS document (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title          TEXT NOT NULL,
    source         TEXT,
    lang           VARCHAR(5),
    url            TEXT,
    storage_path   TEXT,           -- e.g., MinIO path
    doc_sha256     TEXT NOT NULL,  -- unique to be enforced in M1.2
    doc_date       DATE,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============== POLICY_CLAUSE ===============
CREATE TABLE IF NOT EXISTS policy_clause (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id   UUID REFERENCES document(id) ON DELETE CASCADE,    -- [2]
    clause_ref    TEXT,               -- e.g., "Sec 5.3"
    jurisdiction  TEXT,
    theme         TEXT,               -- emissions, water, labor, safety, etc.
    clause_text   TEXT NOT NULL,
    clause_hash   TEXT NOT NULL       -- stable hash of the clause text
);

-- =============== PERMIT ===============
CREATE TABLE IF NOT EXISTS permit (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id       UUID REFERENCES asset(id) ON DELETE CASCADE,      -- [3]
    org_id         UUID REFERENCES organization(id) ON DELETE SET NULL,      -- issuing or holder org [4]
    permit_type    TEXT,               -- e.g., "Air Emissions Permit"
    status         TEXT,               -- requested, active, expired, revoked
    issue_date     DATE,
    expiry_date    DATE,
    document_id    UUID REFERENCES document(id) ON DELETE SET NULL,  -- [5]
    reference_id   TEXT,
    geom           geometry(MultiPolygon, 4326)
);
CREATE INDEX IF NOT EXISTS idx_permit_geom_gist ON permit USING GIST (geom);

-- =============== SATELLITE_TILE ===============
CREATE TABLE IF NOT EXISTS satellite_tile (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id      UUID REFERENCES asset(id) ON DELETE SET NULL,      -- [6]
    aoi_name      TEXT,
    capture_date  DATE,
    sensor        TEXT,               -- Sentinel-2, etc.
    path          TEXT,               -- storage path to the tile/raster
    cloud_cover   REAL,
    footprint     geometry(Polygon, 4326),
    band_summary  JSONB               -- quick stats per band
);
CREATE INDEX IF NOT EXISTS idx_satellite_tile_footprint_gist ON satellite_tile USING GIST (footprint);

-- =============== IOT_STREAM ===============
CREATE TABLE IF NOT EXISTS iot_stream (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id    UUID REFERENCES asset(id) ON DELETE CASCADE,         -- [7]
    stream_name TEXT NOT NULL,       -- e.g., "energy_kwh_hourly"
    unit        TEXT,                -- kWh, °C, ppm, etc.
    started_at  TIMESTAMPTZ,
    ended_at    TIMESTAMPTZ,
    location    geometry(Point, 4326),
    meta        JSONB                -- sensor metadata
);
CREATE INDEX IF NOT EXISTS idx_iot_stream_location_gist ON iot_stream USING GIST (location);

-- =============== EVENT ===============
CREATE TABLE IF NOT EXISTS event (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_key           TEXT NOT NULL,     -- unique to be enforced in M1.2
    event_type          TEXT,              -- ingestion, alert, non-compliance, maintenance, etc.
    asset_id            UUID REFERENCES asset(id) ON DELETE SET NULL,          -- [8]
    related_document_id UUID REFERENCES document(id) ON DELETE SET NULL,       -- [9]
    occurred_at         TIMESTAMPTZ NOT NULL,
    payload             JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============== RULE ===============
CREATE TABLE IF NOT EXISTS rule (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_code       TEXT NOT NULL,         -- unique to be enforced in M1.2
    name            TEXT,
    description     TEXT,
    severity        SMALLINT CHECK (severity BETWEEN 1 AND 5),
    version         TEXT,
    definition      JSONB,                 -- machine-evaluable rule spec
    target_selector JSONB,                 -- selector for targets (by asset kind, region, etc.)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============== PROOF_BUNDLE ===============
CREATE TABLE IF NOT EXISTS proof_bundle (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id     UUID REFERENCES asset(id) ON DELETE SET NULL,       -- [10]
    rule_id      UUID REFERENCES rule(id) ON DELETE CASCADE,         -- [11]
    event_id     UUID REFERENCES event(id) ON DELETE SET NULL,       -- [12]
    document_id  UUID REFERENCES document(id) ON DELETE SET NULL,    -- [13]
    status       TEXT,                 -- draft, attested, rejected, revoked
    proof_hash   TEXT NOT NULL,        -- content-addressed proof artifact
    evidence_url TEXT,                 -- where to fetch the bundle
    meta         JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Basic helper indexes (non-unique; uniqueness will be added in M1.2)
CREATE INDEX IF NOT EXISTS idx_document_sha256 ON document (doc_sha256);
CREATE INDEX IF NOT EXISTS idx_rule_code ON rule (rule_code);
CREATE INDEX IF NOT EXISTS idx_event_key ON event (event_key);
