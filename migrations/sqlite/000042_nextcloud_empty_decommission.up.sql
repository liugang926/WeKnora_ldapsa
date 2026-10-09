-- Existing pairs have no virgin row and cannot claim an empty historical inventory.
CREATE TABLE nextcloud_source_virgin (
    pair_operation_id TEXT PRIMARY KEY REFERENCES nextcloud_source_pairings(operation_id) ON DELETE CASCADE,
    ever_touched INTEGER NOT NULL DEFAULT 0 CHECK (ever_touched IN (0, 1))
);
CREATE TABLE nextcloud_source_decommissions (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL UNIQUE REFERENCES nextcloud_source_pairings(operation_id) ON DELETE RESTRICT,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL UNIQUE,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    publication_epoch INTEGER NOT NULL CHECK (publication_epoch >= 0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('prepared', 'acknowledged')),
    inventory_sha256 TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nextcloud_instance_id, binding_id)
);
CREATE TRIGGER nextcloud_virgin_pair_insert AFTER INSERT ON nextcloud_source_pairings
BEGIN
    INSERT INTO nextcloud_source_virgin(pair_operation_id) VALUES (NEW.operation_id);
END;

CREATE TRIGGER nextcloud_virgin_sync_log BEFORE INSERT ON sync_logs
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.data_source_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.data_source_id);
END;
CREATE TRIGGER nextcloud_virgin_knowledge BEFORE INSERT ON knowledges
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE knowledge_base_id = NEW.knowledge_base_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE knowledge_base_id = NEW.knowledge_base_id);
END;
CREATE TRIGGER nextcloud_virgin_chunk BEFORE INSERT ON chunks
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE knowledge_base_id = NEW.knowledge_base_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE knowledge_base_id = NEW.knowledge_base_id);
END;
CREATE TRIGGER nextcloud_virgin_version BEFORE INSERT ON nextcloud_source_versions
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.datasource_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.datasource_id);
END;
CREATE TRIGGER nextcloud_virgin_event_connection BEFORE INSERT ON nextcloud_event_connections
BEGIN
    SELECT RAISE(ABORT, 'nextcloud_source_decommissioned')
    WHERE EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.datasource_id);
    UPDATE nextcloud_source_virgin SET ever_touched = 1
    WHERE pair_operation_id = (SELECT operation_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.datasource_id);
END;
CREATE TRIGGER nextcloud_decommission_resume_guard BEFORE UPDATE OF status ON data_sources
WHEN NEW.status <> 'paused' AND EXISTS (
    SELECT 1 FROM nextcloud_source_decommissions WHERE datasource_id = NEW.id)
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_decommissioned'); END;
CREATE TRIGGER nextcloud_decommission_row_guard_update
BEFORE UPDATE ON nextcloud_source_decommissions
WHEN NEW.operation_id IS NOT OLD.operation_id
 OR NEW.pair_operation_id IS NOT OLD.pair_operation_id
 OR NEW.tenant_id IS NOT OLD.tenant_id
 OR NEW.knowledge_base_id IS NOT OLD.knowledge_base_id
 OR NEW.datasource_id IS NOT OLD.datasource_id
 OR NEW.nextcloud_instance_id IS NOT OLD.nextcloud_instance_id
 OR NEW.binding_id IS NOT OLD.binding_id
 OR NEW.publication_epoch IS NOT OLD.publication_epoch
 OR NEW.key_id IS NOT OLD.key_id
 OR (OLD.state <> 'prepared' AND NEW.state IS NOT OLD.state)
 OR (NEW.state = 'prepared' AND NEW.inventory_sha256 <> '')
 OR (NEW.state = 'acknowledged' AND NEW.inventory_sha256 <>
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
 OR (OLD.state = 'acknowledged' AND NEW.inventory_sha256 IS NOT OLD.inventory_sha256)
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_decommission_immutable'); END;
CREATE TRIGGER nextcloud_decommission_row_guard_delete
BEFORE DELETE ON nextcloud_source_decommissions
BEGIN SELECT RAISE(ABORT, 'nextcloud_source_decommission_immutable'); END;
CREATE TRIGGER nextcloud_virgin_row_guard
BEFORE UPDATE ON nextcloud_source_virgin
WHEN NEW.pair_operation_id IS NOT OLD.pair_operation_id
 OR (OLD.ever_touched = 1 AND NEW.ever_touched <> 1)
BEGIN SELECT RAISE(ABORT, 'nextcloud_virgin_proof_immutable'); END;
