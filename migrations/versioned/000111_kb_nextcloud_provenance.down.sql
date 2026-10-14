DROP TRIGGER IF EXISTS trg_preserve_kb_nextcloud_provenance ON knowledge_bases;
DROP FUNCTION IF EXISTS preserve_kb_nextcloud_provenance();
ALTER TABLE knowledge_bases DROP COLUMN IF EXISTS ever_had_nextcloud_source;
