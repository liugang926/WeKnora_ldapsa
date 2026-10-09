ALTER TABLE knowledge_bases
    ADD COLUMN ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 0;

UPDATE knowledge_bases
SET ever_had_nextcloud_source = 1
WHERE EXISTS (
    SELECT 1 FROM data_sources AS ds
    WHERE ds.knowledge_base_id = knowledge_bases.id
      AND ds.tenant_id = knowledge_bases.tenant_id
      AND ds.type = 'nextcloud'
)
OR EXISTS (
    SELECT 1 FROM knowledges AS k
    WHERE k.knowledge_base_id = knowledge_bases.id
      AND k.tenant_id = knowledge_bases.tenant_id
      AND (k.channel = 'nextcloud'
           OR k.metadata LIKE '%"nextcloud_instance_id"%'
           OR k.metadata LIKE '%"nextcloud_binding_id"%'
           OR k.metadata LIKE '%"nextcloud_file_id"%')
);

CREATE TRIGGER trg_preserve_kb_nextcloud_provenance
    BEFORE UPDATE OF ever_had_nextcloud_source ON knowledge_bases
    WHEN OLD.ever_had_nextcloud_source = 1 AND NEW.ever_had_nextcloud_source = 0
BEGIN
    SELECT RAISE(ABORT, 'Nextcloud knowledge-base provenance cannot be cleared');
END;
