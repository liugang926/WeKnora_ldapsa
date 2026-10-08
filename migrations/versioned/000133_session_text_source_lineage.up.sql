ALTER TABLE sessions ADD COLUMN IF NOT EXISTS title_source_lineage JSONB;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS description_source_lineage JSONB;
COMMENT ON COLUMN sessions.title_source_lineage IS 'Private original title input lineage; NULL is unknown and must not be backfilled from missing source evidence';
COMMENT ON COLUMN sessions.description_source_lineage IS 'Private original description input lineage; NULL is unknown';
