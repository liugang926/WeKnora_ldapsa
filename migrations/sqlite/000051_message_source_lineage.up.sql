-- Additive foundation only. Migration execution holds SQLite's write
-- transaction across backfill and trigger installation. Legacy answers stay NULL.
ALTER TABLE messages ADD COLUMN source_lineage TEXT CHECK (
    source_lineage IS NULL OR CASE WHEN json_valid(source_lineage) THEN
        json_type(source_lineage) IS 'object'
        AND json_type(source_lineage, '$.version') IS 'integer'
        AND json_extract(source_lineage, '$.version') IS 1
        AND json_type(source_lineage, '$.state') IS 'text'
        AND json_extract(source_lineage, '$.state') IN ('complete', 'unknown')
        AND json_type(source_lineage, '$.sources') IS 'array'
        AND length(CAST(source_lineage AS BLOB)) <= 262144
    ELSE 0 END);

-- Only positive facts are recorded. A missing row is unknown, never a never-source proof.
CREATE TABLE nextcloud_source_tombstones (
    tenant_id INTEGER NOT NULL CHECK (tenant_id > 0),
    scope_type TEXT NOT NULL CHECK (scope_type IN ('tenant', 'knowledge_base', 'datasource', 'pair')),
    scope_id TEXT NOT NULL CHECK (length(trim(scope_id)) > 0),
    recorded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, scope_type, scope_id),
    CHECK (scope_type <> 'tenant' OR scope_id = CAST(tenant_id AS TEXT))
);

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM knowledge_bases
    WHERE (ever_had_nextcloud_source = 1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', id FROM knowledge_bases
    WHERE (ever_had_nextcloud_source = 1) AND tenant_id > 0 AND length(trim(id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM data_sources
    WHERE (type = 'nextcloud') AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM data_sources
    WHERE (type = 'nextcloud') AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', id FROM data_sources
    WHERE (type = 'nextcloud') AND tenant_id > 0 AND length(trim(id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM knowledges
    WHERE (channel = 'nextcloud' OR metadata LIKE '%"nextcloud_instance_id"%' OR metadata LIKE '%"nextcloud_binding_id"%' OR metadata LIKE '%"nextcloud_file_id"%') AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM knowledges
    WHERE (channel = 'nextcloud' OR metadata LIKE '%"nextcloud_instance_id"%' OR metadata LIKE '%"nextcloud_binding_id"%' OR metadata LIKE '%"nextcloud_file_id"%') AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', CASE WHEN json_valid(metadata) THEN json_extract(metadata, '$.datasource_id') ELSE '' END FROM knowledges
    WHERE (channel = 'nextcloud' OR metadata LIKE '%"nextcloud_instance_id"%' OR metadata LIKE '%"nextcloud_binding_id"%' OR metadata LIKE '%"nextcloud_file_id"%') AND tenant_id > 0 AND length(trim(CASE WHEN json_valid(metadata) THEN json_extract(metadata, '$.datasource_id') ELSE '' END)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_source_versions
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_source_versions
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_source_versions
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_source_revisions
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_source_revisions
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_source_revisions
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_source_pairings
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_source_pairings
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_source_pairings
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'pair', operation_id FROM nextcloud_source_pairings
    WHERE (1) AND tenant_id > 0 AND length(trim(operation_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_source_rotations
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_source_rotations
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_source_rotations
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'pair', pair_operation_id FROM nextcloud_source_rotations
    WHERE (1) AND tenant_id > 0 AND length(trim(pair_operation_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_source_pairing_aborts
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_source_pairing_aborts
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_source_pairing_aborts
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'pair', operation_id FROM nextcloud_source_pairing_aborts
    WHERE (1) AND tenant_id > 0 AND length(trim(operation_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_source_decommissions
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_source_decommissions
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_source_decommissions
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'pair', pair_operation_id FROM nextcloud_source_decommissions
    WHERE (1) AND tenant_id > 0 AND length(trim(pair_operation_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_indexed_withdrawals
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_indexed_withdrawals
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_indexed_withdrawals
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'pair', pair_operation_id FROM nextcloud_indexed_withdrawals
    WHERE (1) AND tenant_id > 0 AND length(trim(pair_operation_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_event_connections
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_event_connections
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_event_connections
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;

    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'tenant', CAST(tenant_id AS TEXT) FROM nextcloud_gc_jobs
    WHERE (1) AND tenant_id > 0 AND length(trim(CAST(tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'knowledge_base', knowledge_base_id FROM nextcloud_gc_jobs
    WHERE (1) AND tenant_id > 0 AND length(trim(knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT tenant_id, 'datasource', datasource_id FROM nextcloud_gc_jobs
    WHERE (1) AND tenant_id > 0 AND length(trim(datasource_id)) > 0
    ON CONFLICT DO NOTHING;

CREATE TRIGGER nextcloud_source_tombstones_knowledge_bases_insert
AFTER INSERT ON knowledge_bases
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (NEW.ever_had_nextcloud_source = 1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.id
    WHERE (NEW.ever_had_nextcloud_source = 1) AND NEW.tenant_id > 0 AND length(trim(NEW.id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_knowledge_bases_update
AFTER UPDATE ON knowledge_bases
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'tenant', CAST(OLD.tenant_id AS TEXT)
    WHERE (OLD.ever_had_nextcloud_source = 1) AND OLD.tenant_id > 0 AND length(trim(CAST(OLD.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'knowledge_base', OLD.id
    WHERE (OLD.ever_had_nextcloud_source = 1) AND OLD.tenant_id > 0 AND length(trim(OLD.id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (NEW.ever_had_nextcloud_source = 1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.id
    WHERE (NEW.ever_had_nextcloud_source = 1) AND NEW.tenant_id > 0 AND length(trim(NEW.id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_data_sources_insert
AFTER INSERT ON data_sources
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (NEW.type = 'nextcloud') AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (NEW.type = 'nextcloud') AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.id
    WHERE (NEW.type = 'nextcloud') AND NEW.tenant_id > 0 AND length(trim(NEW.id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_data_sources_update
AFTER UPDATE ON data_sources
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'tenant', CAST(OLD.tenant_id AS TEXT)
    WHERE (OLD.type = 'nextcloud') AND OLD.tenant_id > 0 AND length(trim(CAST(OLD.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'knowledge_base', OLD.knowledge_base_id
    WHERE (OLD.type = 'nextcloud') AND OLD.tenant_id > 0 AND length(trim(OLD.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'datasource', OLD.id
    WHERE (OLD.type = 'nextcloud') AND OLD.tenant_id > 0 AND length(trim(OLD.id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (NEW.type = 'nextcloud') AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (NEW.type = 'nextcloud') AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.id
    WHERE (NEW.type = 'nextcloud') AND NEW.tenant_id > 0 AND length(trim(NEW.id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_knowledges_insert
AFTER INSERT ON knowledges
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (NEW.channel = 'nextcloud' OR NEW.metadata LIKE '%"nextcloud_instance_id"%' OR NEW.metadata LIKE '%"nextcloud_binding_id"%' OR NEW.metadata LIKE '%"nextcloud_file_id"%') AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (NEW.channel = 'nextcloud' OR NEW.metadata LIKE '%"nextcloud_instance_id"%' OR NEW.metadata LIKE '%"nextcloud_binding_id"%' OR NEW.metadata LIKE '%"nextcloud_file_id"%') AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', CASE WHEN json_valid(NEW.metadata) THEN json_extract(NEW.metadata, '$.datasource_id') ELSE '' END
    WHERE (NEW.channel = 'nextcloud' OR NEW.metadata LIKE '%"nextcloud_instance_id"%' OR NEW.metadata LIKE '%"nextcloud_binding_id"%' OR NEW.metadata LIKE '%"nextcloud_file_id"%') AND NEW.tenant_id > 0 AND length(trim(CASE WHEN json_valid(NEW.metadata) THEN json_extract(NEW.metadata, '$.datasource_id') ELSE '' END)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_knowledges_update
AFTER UPDATE ON knowledges
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'tenant', CAST(OLD.tenant_id AS TEXT)
    WHERE (OLD.channel = 'nextcloud' OR OLD.metadata LIKE '%"nextcloud_instance_id"%' OR OLD.metadata LIKE '%"nextcloud_binding_id"%' OR OLD.metadata LIKE '%"nextcloud_file_id"%') AND OLD.tenant_id > 0 AND length(trim(CAST(OLD.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'knowledge_base', OLD.knowledge_base_id
    WHERE (OLD.channel = 'nextcloud' OR OLD.metadata LIKE '%"nextcloud_instance_id"%' OR OLD.metadata LIKE '%"nextcloud_binding_id"%' OR OLD.metadata LIKE '%"nextcloud_file_id"%') AND OLD.tenant_id > 0 AND length(trim(OLD.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT OLD.tenant_id, 'datasource', CASE WHEN json_valid(OLD.metadata) THEN json_extract(OLD.metadata, '$.datasource_id') ELSE '' END
    WHERE (OLD.channel = 'nextcloud' OR OLD.metadata LIKE '%"nextcloud_instance_id"%' OR OLD.metadata LIKE '%"nextcloud_binding_id"%' OR OLD.metadata LIKE '%"nextcloud_file_id"%') AND OLD.tenant_id > 0 AND length(trim(CASE WHEN json_valid(OLD.metadata) THEN json_extract(OLD.metadata, '$.datasource_id') ELSE '' END)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (NEW.channel = 'nextcloud' OR NEW.metadata LIKE '%"nextcloud_instance_id"%' OR NEW.metadata LIKE '%"nextcloud_binding_id"%' OR NEW.metadata LIKE '%"nextcloud_file_id"%') AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (NEW.channel = 'nextcloud' OR NEW.metadata LIKE '%"nextcloud_instance_id"%' OR NEW.metadata LIKE '%"nextcloud_binding_id"%' OR NEW.metadata LIKE '%"nextcloud_file_id"%') AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', CASE WHEN json_valid(NEW.metadata) THEN json_extract(NEW.metadata, '$.datasource_id') ELSE '' END
    WHERE (NEW.channel = 'nextcloud' OR NEW.metadata LIKE '%"nextcloud_instance_id"%' OR NEW.metadata LIKE '%"nextcloud_binding_id"%' OR NEW.metadata LIKE '%"nextcloud_file_id"%') AND NEW.tenant_id > 0 AND length(trim(CASE WHEN json_valid(NEW.metadata) THEN json_extract(NEW.metadata, '$.datasource_id') ELSE '' END)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_versions_insert
AFTER INSERT ON nextcloud_source_versions
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_revisions_insert
AFTER INSERT ON nextcloud_source_revisions
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_pairings_insert
AFTER INSERT ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'pair', NEW.operation_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.operation_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_rotations_insert
AFTER INSERT ON nextcloud_source_rotations
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'pair', NEW.pair_operation_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.pair_operation_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_pairing_aborts_insert
AFTER INSERT ON nextcloud_source_pairing_aborts
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'pair', NEW.operation_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.operation_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_source_decommissions_insert
AFTER INSERT ON nextcloud_source_decommissions
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'pair', NEW.pair_operation_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.pair_operation_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_indexed_withdrawals_insert
AFTER INSERT ON nextcloud_indexed_withdrawals
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'pair', NEW.pair_operation_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.pair_operation_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_event_connections_insert
AFTER INSERT ON nextcloud_event_connections
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_nextcloud_gc_jobs_insert
AFTER INSERT ON nextcloud_gc_jobs
BEGIN
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'tenant', CAST(NEW.tenant_id AS TEXT)
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(CAST(NEW.tenant_id AS TEXT))) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'knowledge_base', NEW.knowledge_base_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.knowledge_base_id)) > 0
    ON CONFLICT DO NOTHING;
    INSERT INTO nextcloud_source_tombstones(tenant_id, scope_type, scope_id)
    SELECT NEW.tenant_id, 'datasource', NEW.datasource_id
    WHERE (1) AND NEW.tenant_id > 0 AND length(trim(NEW.datasource_id)) > 0
    ON CONFLICT DO NOTHING;
END;

CREATE TRIGGER nextcloud_source_tombstones_no_update
BEFORE UPDATE ON nextcloud_source_tombstones
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_tombstones_immutable');
END;
-- REPLACE normally deletes a conflicting row without firing delete triggers
-- when recursive_triggers is off. Preserve the first fact for that key too.
CREATE TRIGGER nextcloud_source_tombstones_existing_insert
BEFORE INSERT ON nextcloud_source_tombstones
WHEN EXISTS (SELECT 1 FROM nextcloud_source_tombstones
    WHERE tenant_id = NEW.tenant_id AND scope_type = NEW.scope_type AND scope_id = NEW.scope_id)
BEGIN
    SELECT RAISE(IGNORE);
END;
CREATE TRIGGER nextcloud_source_tombstones_no_delete
BEFORE DELETE ON nextcloud_source_tombstones
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_tombstones_immutable');
END;
