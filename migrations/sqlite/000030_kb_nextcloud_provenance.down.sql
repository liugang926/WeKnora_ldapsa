DROP TRIGGER IF EXISTS trg_preserve_kb_nextcloud_provenance;
ALTER TABLE knowledge_bases DROP COLUMN ever_had_nextcloud_source;
