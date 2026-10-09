ALTER TABLE resources DROP CONSTRAINT IF EXISTS chk_resources_source_provenance;
ALTER TABLE resources DROP COLUMN IF EXISTS source_provenance;
