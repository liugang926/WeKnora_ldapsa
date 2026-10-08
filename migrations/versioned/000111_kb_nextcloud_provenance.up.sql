-- Persist provenance independently of source and knowledge row retention.
ALTER TABLE knowledge_bases
    ADD COLUMN IF NOT EXISTS ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE knowledge_bases AS kb
SET ever_had_nextcloud_source = TRUE
WHERE EXISTS (
    SELECT 1 FROM data_sources AS ds
    WHERE ds.knowledge_base_id = kb.id AND ds.tenant_id = kb.tenant_id
      AND ds.type = 'nextcloud'
)
OR EXISTS (
    SELECT 1 FROM knowledges AS k
    WHERE k.knowledge_base_id = kb.id AND k.tenant_id = kb.tenant_id
      AND (k.channel = 'nextcloud'
           OR k.metadata::text LIKE '%"nextcloud_instance_id"%'
           OR k.metadata::text LIKE '%"nextcloud_binding_id"%'
           OR k.metadata::text LIKE '%"nextcloud_file_id"%')
);

CREATE OR REPLACE FUNCTION preserve_kb_nextcloud_provenance() RETURNS trigger AS $$
BEGIN
    IF OLD.ever_had_nextcloud_source AND NOT NEW.ever_had_nextcloud_source THEN
        RAISE EXCEPTION 'Nextcloud knowledge-base provenance cannot be cleared';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_preserve_kb_nextcloud_provenance
    BEFORE UPDATE OF ever_had_nextcloud_source ON knowledge_bases
    FOR EACH ROW EXECUTE FUNCTION preserve_kb_nextcloud_provenance();
