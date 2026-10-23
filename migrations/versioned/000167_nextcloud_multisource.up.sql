-- Contract v3 lets independent Nextcloud bindings publish into one dedicated
-- knowledge base. Pairing and target-closure rows remain unique per binding,
-- while the knowledge base may own several independently enforced sources.
ALTER TABLE IF EXISTS nextcloud_source_pairings
    DROP CONSTRAINT IF EXISTS nextcloud_source_pairings_knowledge_base_id_key;
DO $pairing_index$
BEGIN
    IF to_regclass('nextcloud_source_pairings') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS nextcloud_pairing_kb_state
            ON nextcloud_source_pairings (tenant_id, knowledge_base_id, state);
    END IF;
END
$pairing_index$;

ALTER TABLE IF EXISTS nextcloud_onboarding_operations
    DROP CONSTRAINT IF EXISTS nextcloud_onboarding_operations_tenant_id_knowledge_base_id_key;
DO $operation_index$
BEGIN
    IF to_regclass('nextcloud_onboarding_operations') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS nextcloud_onboarding_operation_kb_state
            ON nextcloud_onboarding_operations (tenant_id, knowledge_base_id, state);
    END IF;
END
$operation_index$;

ALTER TABLE IF EXISTS nextcloud_onboarding_target_closures
    DROP CONSTRAINT IF EXISTS nextcloud_onboarding_target_clo_tenant_id_knowledge_base_id_key;
DO $target_index$
BEGIN
    IF to_regclass('nextcloud_onboarding_target_closures') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS nextcloud_target_closure_kb_operation
            ON nextcloud_onboarding_target_closures (tenant_id, knowledge_base_id, operation_id);
    END IF;
END
$target_index$;

ALTER TABLE IF EXISTS nextcloud_source_decommissions
    DROP CONSTRAINT IF EXISTS nextcloud_source_decommissions_knowledge_base_id_key;
DO $decommission_index$
BEGIN
    IF to_regclass('nextcloud_source_decommissions') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS nextcloud_source_decommission_kb_state
            ON nextcloud_source_decommissions (tenant_id, knowledge_base_id, state);
    END IF;
END
$decommission_index$;

ALTER TABLE IF EXISTS nextcloud_indexed_withdrawals
    DROP CONSTRAINT IF EXISTS nextcloud_indexed_withdrawals_knowledge_base_id_key;
DO $withdrawal_index$
BEGIN
    IF to_regclass('nextcloud_indexed_withdrawals') IS NOT NULL THEN
        CREATE INDEX IF NOT EXISTS nextcloud_indexed_withdrawal_kb_state
            ON nextcloud_indexed_withdrawals (tenant_id, knowledge_base_id, state);
    END IF;
END
$withdrawal_index$;

-- Soft deletion is the entry point for exact target closure. The repository
-- must retire each source fence and persist its closure before commit. Hard
-- deletion remains blocked by nextcloud_paired_kb_hard_delete_guard.
DROP TRIGGER IF EXISTS nextcloud_paired_kb_delete_guard ON knowledge_bases;

-- The v2 guard compared a source against the single operation for its KB.
-- A shared KB now owns one operation per binding, so admit a source when its
-- configured operation belongs to any operation reserved for that KB.
CREATE OR REPLACE FUNCTION nextcloud_onboarding_content_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_source
 WHERE id=NEW.knowledge_base_id AND tenant_id=NEW.tenant_id;
 IF EXISTS(SELECT 1 FROM nextcloud_onboarding_operations o
           WHERE o.tenant_id=NEW.tenant_id AND o.knowledge_base_id=NEW.knowledge_base_id) THEN
  IF TG_TABLE_NAME='data_sources' THEN
   IF NEW.type <> 'nextcloud' OR NOT EXISTS(
     SELECT 1 FROM nextcloud_onboarding_operations owned
     WHERE owned.tenant_id=NEW.tenant_id
       AND owned.knowledge_base_id=NEW.knowledge_base_id
       AND owned.operation_id IS NOT DISTINCT FROM
           (NEW.config::jsonb->'settings'->>'onboarding_operation_id')
   ) THEN RAISE EXCEPTION 'nextcloud_kb_reserved'; END IF;
  ELSIF EXISTS(SELECT 1 FROM nextcloud_onboarding_operations pending
               WHERE pending.tenant_id=NEW.tenant_id
                 AND pending.knowledge_base_id=NEW.knowledge_base_id
                 AND pending.state <> 'active') THEN
   RAISE EXCEPTION 'nextcloud_kb_reserved';
  ELSIF TG_TABLE_NAME='knowledges' THEN
   IF NEW.channel <> 'nextcloud' THEN RAISE EXCEPTION 'nextcloud_kb_reserved'; END IF;
  ELSIF TG_TABLE_NAME='wiki_pages' THEN
   RAISE EXCEPTION 'nextcloud_kb_reserved';
  END IF;
 END IF;
 UPDATE nextcloud_kb_blank_witnesses SET virgin=FALSE
 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id;
 RETURN NEW;
END $$;

-- Virgin proof and decommission admission are source-scoped. In a shared KB,
-- content from a healthy binding must not burn or block another binding.
CREATE OR REPLACE FUNCTION nextcloud_virgin_touch() RETURNS trigger
LANGUAGE plpgsql AS $nextcloud_virgin_touch$
DECLARE
    pair_id TEXT;
    touched BOOLEAN;
    source_id TEXT;
BEGIN
    IF TG_TABLE_NAME = 'sync_logs' THEN
        SELECT operation_id INTO pair_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.data_source_id;
    ELSIF TG_TABLE_NAME IN ('nextcloud_source_versions', 'nextcloud_event_connections') THEN
        SELECT operation_id INTO pair_id FROM nextcloud_source_pairings
        WHERE datasource_id = NEW.datasource_id;
    ELSIF TG_TABLE_NAME = 'knowledges' THEN
        source_id := NEW.metadata->>'datasource_id';
        SELECT operation_id INTO pair_id FROM nextcloud_source_pairings
        WHERE tenant_id = NEW.tenant_id AND knowledge_base_id = NEW.knowledge_base_id
          AND datasource_id = source_id;
    ELSIF TG_TABLE_NAME = 'chunks' THEN
        SELECT k.metadata->>'datasource_id' INTO source_id
        FROM knowledges k WHERE k.id = NEW.knowledge_id
          AND k.tenant_id = NEW.tenant_id
          AND k.knowledge_base_id = NEW.knowledge_base_id;
        SELECT operation_id INTO pair_id FROM nextcloud_source_pairings
        WHERE tenant_id = NEW.tenant_id AND knowledge_base_id = NEW.knowledge_base_id
          AND datasource_id = source_id;
    END IF;
    IF pair_id IS NULL AND COALESCE(source_id, '') = '' AND
       EXISTS (SELECT 1 FROM nextcloud_source_pairings p
               WHERE p.tenant_id = NEW.tenant_id
                 AND p.knowledge_base_id = NEW.knowledge_base_id) THEN
        IF EXISTS (SELECT 1 FROM nextcloud_source_decommissions d
                   JOIN nextcloud_source_pairings p ON p.operation_id=d.pair_operation_id
                   WHERE p.tenant_id=NEW.tenant_id
                     AND p.knowledge_base_id=NEW.knowledge_base_id) THEN
            RAISE EXCEPTION 'nextcloud_source_decommissioned' USING ERRCODE = '23514';
        END IF;
        UPDATE nextcloud_source_virgin v SET ever_touched=TRUE
        FROM nextcloud_source_pairings p
        WHERE p.operation_id=v.pair_operation_id
          AND p.tenant_id=NEW.tenant_id
          AND p.knowledge_base_id=NEW.knowledge_base_id;
        RETURN NEW;
    END IF;
    IF pair_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT ever_touched INTO touched FROM nextcloud_source_virgin
    WHERE pair_operation_id = pair_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM nextcloud_source_decommissions WHERE pair_operation_id = pair_id) THEN
        RAISE EXCEPTION 'nextcloud_source_decommissioned' USING ERRCODE = '23514';
    END IF;
    IF touched IS NOT NULL THEN
        UPDATE nextcloud_source_virgin SET ever_touched = TRUE WHERE pair_operation_id = pair_id;
    END IF;
    RETURN NEW;
END
$nextcloud_virgin_touch$;

-- Withdrawal fences are also source-scoped. A move still checks both the old
-- and new KB/source, while an unattributable Nextcloud row continues to fail
-- closed against any withdrawal in its KB.
CREATE OR REPLACE FUNCTION nextcloud_indexed_withdrawal_write_guard() RETURNS trigger
LANGUAGE plpgsql AS $guard$
DECLARE
    kb_id TEXT;
    prior_kb TEXT;
    next_kb TEXT;
    prior_source TEXT;
    next_source TEXT;
    safe_event_revoke BOOLEAN := FALSE;
BEGIN
    IF TG_TABLE_NAME = 'nextcloud_event_connections' AND TG_OP = 'UPDATE' THEN
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
            prior_source := OLD.data_source_id;
            SELECT p.knowledge_base_id INTO prior_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = OLD.data_source_id;
        END IF;
        IF TG_OP <> 'DELETE' THEN
            next_source := NEW.data_source_id;
            SELECT p.knowledge_base_id INTO next_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = NEW.data_source_id;
        END IF;
    ELSIF TG_TABLE_NAME IN ('nextcloud_source_versions', 'nextcloud_event_connections') THEN
        IF TG_OP <> 'INSERT' THEN
            prior_source := OLD.datasource_id;
            SELECT p.knowledge_base_id INTO prior_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = OLD.datasource_id;
        END IF;
        IF TG_OP <> 'DELETE' THEN
            next_source := NEW.datasource_id;
            SELECT p.knowledge_base_id INTO next_kb FROM nextcloud_source_pairings p
            WHERE p.datasource_id = NEW.datasource_id;
        END IF;
    ELSIF TG_TABLE_NAME = 'knowledges' THEN
        IF TG_OP <> 'INSERT' THEN
            prior_kb := OLD.knowledge_base_id;
            prior_source := OLD.metadata->>'datasource_id';
        END IF;
        IF TG_OP <> 'DELETE' THEN
            next_kb := NEW.knowledge_base_id;
            next_source := NEW.metadata->>'datasource_id';
        END IF;
    ELSE
        IF TG_OP <> 'INSERT' THEN
            prior_kb := OLD.knowledge_base_id;
            SELECT k.metadata->>'datasource_id' INTO prior_source
            FROM knowledges k WHERE k.id = OLD.knowledge_id;
        END IF;
        IF TG_OP <> 'DELETE' THEN
            next_kb := NEW.knowledge_base_id;
            SELECT k.metadata->>'datasource_id' INTO next_source
            FROM knowledges k WHERE k.id = NEW.knowledge_id;
        END IF;
    END IF;
    FOR kb_id IN SELECT DISTINCT scope FROM (VALUES (prior_kb), (next_kb)) AS scopes(scope)
                 WHERE scope IS NOT NULL ORDER BY scope LOOP
        PERFORM 1 FROM knowledge_bases WHERE id = kb_id FOR UPDATE;
    END LOOP;
    IF NOT safe_event_revoke AND (
        (COALESCE(prior_kb, '') <> '' AND COALESCE(prior_source, '') <> '' AND EXISTS (
            SELECT 1 FROM nextcloud_indexed_withdrawals w
            WHERE w.knowledge_base_id = prior_kb AND w.datasource_id = prior_source
        ))
        OR (COALESCE(next_kb, '') <> '' AND COALESCE(next_source, '') <> '' AND EXISTS (
            SELECT 1 FROM nextcloud_indexed_withdrawals w
            WHERE w.knowledge_base_id = next_kb AND w.datasource_id = next_source
        ))
        OR (COALESCE(prior_source, '') = '' AND COALESCE(next_source, '') = '' AND EXISTS (
            SELECT 1 FROM nextcloud_indexed_withdrawals w
            WHERE w.knowledge_base_id IN (prior_kb, next_kb)
        ))
    ) THEN
        RAISE EXCEPTION 'nextcloud_indexed_source_withdrawn' USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$guard$;
