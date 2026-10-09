-- An indexed source can be withdrawn before its historical copy inventory is
-- provably complete. This row is a permanent access/write fence, not a GC ACK.
CREATE TABLE nextcloud_indexed_withdrawals (
    operation_id TEXT PRIMARY KEY,
    pair_operation_id TEXT NOT NULL UNIQUE REFERENCES nextcloud_source_pairings(operation_id) ON DELETE RESTRICT,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id TEXT NOT NULL UNIQUE,
    datasource_id TEXT NOT NULL UNIQUE,
    nextcloud_instance_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    publication_epoch BIGINT NOT NULL CHECK (publication_epoch >= 0),
    key_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'withdrawn_inventory_incomplete'
        CHECK (state = 'withdrawn_inventory_incomplete'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nextcloud_instance_id, binding_id)
);

-- Only observed local SQL identities are stored here. The table must never be
-- interpreted as a complete inventory of old external vector, graph or Wiki
-- copies. Paths remain private; public status exposes counts only.
CREATE TABLE nextcloud_indexed_withdrawal_items (
    operation_id TEXT NOT NULL REFERENCES nextcloud_indexed_withdrawals(operation_id) ON DELETE RESTRICT,
    kind TEXT NOT NULL CHECK (kind IN ('knowledge', 'source_file', 'extracted_image',
        'invalid_image_inventory', 'missing_local_embedding_table',
        'unverified_external_index', 'chunk',
        'postgres_embedding', 'gc_job', 'gc_item', 'source_version', 'sync_log')),
    object_ref TEXT NOT NULL,
    knowledge_id TEXT NOT NULL DEFAULT '',
    observed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (operation_id, kind, object_ref)
);

CREATE FUNCTION nextcloud_indexed_withdrawal_immutable() RETURNS trigger
LANGUAGE plpgsql AS $guard$
BEGIN
    RAISE EXCEPTION 'nextcloud_indexed_withdrawal_immutable' USING ERRCODE = '23514';
END
$guard$;
CREATE TRIGGER nextcloud_indexed_withdrawal_immutable
BEFORE UPDATE OR DELETE ON nextcloud_indexed_withdrawals FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_immutable();
CREATE TRIGGER nextcloud_indexed_withdrawal_item_immutable
BEFORE UPDATE OR DELETE ON nextcloud_indexed_withdrawal_items FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_immutable();

CREATE FUNCTION nextcloud_indexed_excludes_empty_ack() RETURNS trigger
LANGUAGE plpgsql AS $guard$
BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_indexed_withdrawals
        WHERE pair_operation_id = NEW.pair_operation_id) THEN
        RAISE EXCEPTION 'nextcloud_indexed_source_cannot_ack_empty'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$guard$;
CREATE TRIGGER nextcloud_indexed_excludes_empty_ack
BEFORE INSERT ON nextcloud_source_decommissions FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_excludes_empty_ack();

-- The KB row lock serializes withdrawal with every supported SQL content
-- writer. A worker that started before withdrawal may finish an external
-- request later; inventory stays incomplete and no release is authorized.
CREATE FUNCTION nextcloud_indexed_withdrawal_write_guard() RETURNS trigger
LANGUAGE plpgsql AS $guard$
DECLARE
    kb_id TEXT;
    prior_kb TEXT;
    next_kb TEXT;
    safe_event_revoke BOOLEAN := FALSE;
BEGIN
    IF TG_TABLE_NAME = 'nextcloud_event_connections' AND TG_OP = 'UPDATE' THEN
        -- Emergency revoke remains possible after withdrawal. No identity,
        -- active key or secret may change; previous-key grace must be cleared.
        safe_event_revoke := OLD.status = 'active' AND NEW.status = 'revoked'
            AND NEW.connection_id IS NOT DISTINCT FROM OLD.connection_id
            AND NEW.tenant_id IS NOT DISTINCT FROM OLD.tenant_id
            AND NEW.knowledge_base_id IS NOT DISTINCT FROM OLD.knowledge_base_id
            AND NEW.datasource_id IS NOT DISTINCT FROM OLD.datasource_id
            AND NEW.nextcloud_instance_id IS NOT DISTINCT FROM OLD.nextcloud_instance_id
            AND NEW.binding_id IS NOT DISTINCT FROM OLD.binding_id
            AND NEW.datasource_base_url IS NOT DISTINCT FROM OLD.datasource_base_url
            AND NEW.datasource_config_sha256 IS NOT DISTINCT FROM OLD.datasource_config_sha256
            AND NEW.current_key_id IS NOT DISTINCT FROM OLD.current_key_id
            AND NEW.current_secret_ciphertext IS NOT DISTINCT FROM OLD.current_secret_ciphertext
            AND NEW.created_at IS NOT DISTINCT FROM OLD.created_at
            AND NEW.previous_key_id IS NULL
            AND NEW.previous_secret_ciphertext IS NULL
            AND NEW.previous_valid_until IS NULL;
    END IF;
    IF TG_TABLE_NAME = 'sync_logs' THEN
        IF TG_OP <> 'INSERT' THEN
            SELECT p.knowledge_base_id INTO prior_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = OLD.data_source_id;
        END IF;
        IF TG_OP <> 'DELETE' THEN
            SELECT p.knowledge_base_id INTO next_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = NEW.data_source_id;
        END IF;
    ELSIF TG_TABLE_NAME IN ('nextcloud_source_versions', 'nextcloud_event_connections') THEN
        IF TG_OP <> 'INSERT' THEN
            SELECT p.knowledge_base_id INTO prior_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = OLD.datasource_id;
        END IF;
        IF TG_OP <> 'DELETE' THEN
            SELECT p.knowledge_base_id INTO next_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = NEW.datasource_id;
        END IF;
    ELSE
        IF TG_OP <> 'INSERT' THEN
            prior_kb := OLD.knowledge_base_id;
        END IF;
        IF TG_OP <> 'DELETE' THEN
            next_kb := NEW.knowledge_base_id;
        END IF;
    END IF;
    -- Lock both sides of a move in stable order. Otherwise an UPDATE could
    -- move a copy out of a withdrawn source and bypass the old-scope fence.
    FOR kb_id IN SELECT DISTINCT scope FROM (VALUES (prior_kb), (next_kb)) AS scopes(scope)
                 WHERE scope IS NOT NULL ORDER BY scope LOOP
        PERFORM 1 FROM knowledge_bases WHERE id = kb_id FOR UPDATE;
        IF NOT safe_event_revoke AND EXISTS (
            SELECT 1 FROM nextcloud_indexed_withdrawals WHERE knowledge_base_id = kb_id) THEN
            RAISE EXCEPTION 'nextcloud_indexed_source_withdrawn' USING ERRCODE = '23514';
        END IF;
    END LOOP;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$guard$;
CREATE TRIGGER nextcloud_indexed_knowledge_guard
BEFORE INSERT OR UPDATE OR DELETE ON knowledges FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_write_guard();
CREATE TRIGGER nextcloud_indexed_chunk_guard
BEFORE INSERT OR UPDATE OR DELETE ON chunks FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_write_guard();
CREATE TRIGGER nextcloud_indexed_version_guard
BEFORE INSERT OR UPDATE OR DELETE ON nextcloud_source_versions FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_write_guard();
CREATE TRIGGER nextcloud_indexed_event_guard
BEFORE INSERT OR UPDATE OR DELETE ON nextcloud_event_connections FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_write_guard();
CREATE TRIGGER nextcloud_indexed_sync_guard
BEFORE INSERT OR UPDATE OR DELETE ON sync_logs FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_write_guard();
DO $$ BEGIN
    IF to_regclass('embeddings') IS NOT NULL THEN
        CREATE TRIGGER nextcloud_indexed_embedding_guard
        BEFORE INSERT OR UPDATE OR DELETE ON embeddings FOR EACH ROW
        EXECUTE FUNCTION nextcloud_indexed_withdrawal_write_guard();
    END IF;
END $$;

CREATE FUNCTION nextcloud_indexed_withdrawal_resume_guard() RETURNS trigger
LANGUAGE plpgsql AS $guard$
BEGIN
    IF NEW.status <> 'paused' AND EXISTS (
        SELECT 1 FROM nextcloud_indexed_withdrawals WHERE datasource_id = NEW.id
    ) THEN
        RAISE EXCEPTION 'nextcloud_indexed_source_withdrawn' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$guard$;
CREATE TRIGGER nextcloud_indexed_withdrawal_resume_guard
BEFORE UPDATE OF status ON data_sources FOR EACH ROW
EXECUTE FUNCTION nextcloud_indexed_withdrawal_resume_guard();
