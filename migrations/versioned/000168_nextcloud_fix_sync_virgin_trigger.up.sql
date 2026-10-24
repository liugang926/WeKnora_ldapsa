-- Migration 000167 installed a source-scoped virgin trigger that referenced
-- NEW.knowledge_base_id even for sync_logs. PostgreSQL must resolve every
-- subexpression of an IF condition, so the otherwise unreachable branch made
-- all Nextcloud sync_log inserts fail with SQLSTATE 42703.
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

    -- Only content tables carry knowledge_base_id. Keep the fallback inside
    -- this table-specific branch so sync_logs never evaluates that field.
    IF TG_TABLE_NAME IN ('knowledges', 'chunks') THEN
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
