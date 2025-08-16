BEGIN;

-- 1) Replace non-unique helper indexes with proper UNIQUE constraints
DROP INDEX IF EXISTS idx_document_sha256;
ALTER TABLE document
  ADD CONSTRAINT uq_document_sha256 UNIQUE (doc_sha256);

DROP INDEX IF EXISTS idx_rule_code;
ALTER TABLE rule
  ADD CONSTRAINT uq_rule_code UNIQUE (rule_code);

DROP INDEX IF EXISTS idx_event_key;
ALTER TABLE event
  ADD CONSTRAINT uq_event_key UNIQUE (event_key);

-- 2) Geometry validity gates (allow NULL, enforce validity when present)
ALTER TABLE asset
  ADD CONSTRAINT ck_asset_centroid_valid  CHECK (centroid  IS NULL OR ST_IsValid(centroid)),
  ADD CONSTRAINT ck_asset_footprint_valid CHECK (footprint IS NULL OR ST_IsValid(footprint));

ALTER TABLE permit
  ADD CONSTRAINT ck_permit_geom_valid CHECK (geom IS NULL OR ST_IsValid(geom));

ALTER TABLE satellite_tile
  ADD CONSTRAINT ck_sat_tile_footprint_valid CHECK (footprint IS NULL OR ST_IsValid(footprint));

ALTER TABLE iot_stream
  ADD CONSTRAINT ck_iot_stream_location_valid CHECK (location IS NULL OR ST_IsValid(location));

-- 3) Targeted indexes (FKs and time filters)
CREATE INDEX IF NOT EXISTS idx_asset_owner_org_id        ON asset(owner_org_id);
CREATE INDEX IF NOT EXISTS idx_permit_asset_id           ON permit(asset_id);
CREATE INDEX IF NOT EXISTS idx_permit_org_id             ON permit(org_id);
CREATE INDEX IF NOT EXISTS idx_satellite_tile_asset_id   ON satellite_tile(asset_id);
CREATE INDEX IF NOT EXISTS idx_iot_stream_asset_id       ON iot_stream(asset_id);
CREATE INDEX IF NOT EXISTS idx_event_asset_id            ON event(asset_id);
CREATE INDEX IF NOT EXISTS idx_event_occurred_at         ON event(occurred_at);
CREATE INDEX IF NOT EXISTS idx_document_doc_date         ON document(doc_date);
CREATE INDEX IF NOT EXISTS idx_proof_bundle_asset_id     ON proof_bundle(asset_id);
CREATE INDEX IF NOT EXISTS idx_proof_bundle_rule_id      ON proof_bundle(rule_id);
CREATE INDEX IF NOT EXISTS idx_proof_bundle_event_id     ON proof_bundle(event_id);
CREATE INDEX IF NOT EXISTS idx_proof_bundle_document_id  ON proof_bundle(document_id);

-- 4) Optional documentation (helps future devs)
COMMENT ON CONSTRAINT uq_document_sha256 ON document IS 'Deduplicate documents by content hash.';
COMMENT ON CONSTRAINT uq_rule_code       ON rule     IS 'Unique code+version identity for a rule.';
COMMENT ON CONSTRAINT uq_event_key       ON event    IS 'Idempotent event identity (no duplicates).';

COMMIT;
