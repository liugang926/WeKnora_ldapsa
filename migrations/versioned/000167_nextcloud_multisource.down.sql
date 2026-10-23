DO $rollback$
BEGIN
    IF EXISTS (
        SELECT 1 FROM nextcloud_source_pairings
        GROUP BY tenant_id, knowledge_base_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot roll back multi-source pairings while shared knowledge bases exist';
    END IF;
    IF EXISTS (
        SELECT 1 FROM nextcloud_onboarding_operations
        GROUP BY tenant_id, knowledge_base_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot roll back multi-source operations while shared knowledge bases exist';
    END IF;
    IF EXISTS (
        SELECT 1 FROM nextcloud_onboarding_target_closures
        GROUP BY tenant_id, knowledge_base_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot roll back multi-source target closures while shared knowledge bases exist';
    END IF;
    IF EXISTS (
        SELECT 1 FROM nextcloud_source_decommissions
        GROUP BY tenant_id, knowledge_base_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot roll back multi-source decommissions while shared knowledge bases exist';
    END IF;
    IF EXISTS (
        SELECT 1 FROM nextcloud_indexed_withdrawals
        GROUP BY tenant_id, knowledge_base_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot roll back multi-source withdrawals while shared knowledge bases exist';
    END IF;
END
$rollback$;

-- Restore the v2 single-operation guard before reintroducing the constraint.
CREATE OR REPLACE FUNCTION nextcloud_onboarding_content_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE op nextcloud_onboarding_operations; BEGIN
 UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_source
 WHERE id=NEW.knowledge_base_id AND tenant_id=NEW.tenant_id;
 SELECT * INTO op FROM nextcloud_onboarding_operations
 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id;
 IF FOUND THEN
  IF TG_TABLE_NAME='data_sources' THEN
   IF NEW.type <> 'nextcloud' OR (NEW.config::jsonb->'settings'->>'onboarding_operation_id') IS DISTINCT FROM op.operation_id THEN RAISE EXCEPTION 'nextcloud_kb_reserved'; END IF;
  ELSIF op.state <> 'active' THEN RAISE EXCEPTION 'nextcloud_kb_reserved';
  ELSIF TG_TABLE_NAME='knowledges' THEN IF NEW.channel <> 'nextcloud' THEN RAISE EXCEPTION 'nextcloud_kb_reserved'; END IF;
  ELSIF TG_TABLE_NAME='wiki_pages' THEN RAISE EXCEPTION 'nextcloud_wiki_unsupported';
  END IF;
 END IF;
 UPDATE nextcloud_kb_blank_witnesses SET virgin=FALSE
 WHERE tenant_id=NEW.tenant_id AND knowledge_base_id=NEW.knowledge_base_id;
 RETURN NEW; END $$;

DROP INDEX IF EXISTS nextcloud_target_closure_kb_operation;
ALTER TABLE nextcloud_onboarding_target_closures
    ADD CONSTRAINT nextcloud_onboarding_target_clo_tenant_id_knowledge_base_id_key
    UNIQUE (tenant_id, knowledge_base_id);

DROP INDEX IF EXISTS nextcloud_source_decommission_kb_state;
ALTER TABLE nextcloud_source_decommissions
    ADD CONSTRAINT nextcloud_source_decommissions_knowledge_base_id_key
    UNIQUE (knowledge_base_id);

DROP INDEX IF EXISTS nextcloud_indexed_withdrawal_kb_state;
ALTER TABLE nextcloud_indexed_withdrawals
    ADD CONSTRAINT nextcloud_indexed_withdrawals_knowledge_base_id_key
    UNIQUE (knowledge_base_id);

CREATE TRIGGER nextcloud_paired_kb_delete_guard
BEFORE UPDATE OF deleted_at ON knowledge_bases FOR EACH ROW
EXECUTE FUNCTION nextcloud_paired_kb_delete_guard();

DROP INDEX IF EXISTS nextcloud_onboarding_operation_kb_state;
ALTER TABLE nextcloud_onboarding_operations
    ADD CONSTRAINT nextcloud_onboarding_operations_tenant_id_knowledge_base_id_key
    UNIQUE (tenant_id, knowledge_base_id);

DROP INDEX IF EXISTS nextcloud_pairing_kb_state;
ALTER TABLE nextcloud_source_pairings
    ADD CONSTRAINT nextcloud_source_pairings_knowledge_base_id_key
    UNIQUE (knowledge_base_id);
