ALTER TABLE resources ADD COLUMN source_provenance VARCHAR(16) NOT NULL DEFAULT 'unknown';

UPDATE resources AS r
SET source_provenance = 'nextcloud'
WHERE EXISTS (
    SELECT 1 FROM resource_bindings AS b
    JOIN knowledges AS k ON k.id = b.owner_id AND k.tenant_id = b.tenant_id
    WHERE b.resource_id = r.id AND b.tenant_id = r.tenant_id
      AND b.owner_type = 'knowledge'
      AND (k.channel = 'nextcloud'
           OR k.metadata LIKE '%"nextcloud_instance_id"%'
           OR k.metadata LIKE '%"nextcloud_binding_id"%'
           OR k.metadata LIKE '%"nextcloud_file_id"%')
)
OR EXISTS (
    SELECT 1 FROM knowledges AS k
    WHERE k.tenant_id = r.tenant_id
      AND (k.file_path = 'resource://' || r.handle OR k.file_path = r.physical_path)
      AND (k.channel = 'nextcloud'
           OR k.metadata LIKE '%"nextcloud_instance_id"%'
           OR k.metadata LIKE '%"nextcloud_binding_id"%'
           OR k.metadata LIKE '%"nextcloud_file_id"%')
);
