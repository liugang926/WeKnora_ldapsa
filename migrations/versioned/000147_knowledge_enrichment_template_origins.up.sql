-- Explicit new templates participate in the existing immutable original
-- configuration origin and monotonic actual-control counter.
ALTER TABLE knowledge_bases ADD COLUMN enrichment_templates JSONB;
CREATE FUNCTION knowledge_enrichment_template_control_version() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.enrichment_templates IS NOT DISTINCT FROM NEW.enrichment_templates THEN RETURN NEW; END IF; INSERT INTO knowledge_configuration_control_versions(knowledge_base_id,revision) VALUES(NEW.id,1) ON CONFLICT(knowledge_base_id) DO UPDATE SET revision=knowledge_configuration_control_versions.revision+1; RETURN NEW; END; $$;
CREATE TRIGGER knowledge_enrichment_template_control_version AFTER UPDATE ON knowledge_bases FOR EACH ROW EXECUTE FUNCTION knowledge_enrichment_template_control_version();
CREATE TABLE knowledge_enrichment_template_inputs (
 id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL,
 purpose TEXT NOT NULL CHECK(purpose IN ('summary','question')),
 model_id TEXT NOT NULL, model_revision BIGINT NOT NULL, model_digest TEXT NOT NULL,
 config_revision BIGINT NOT NULL, config_digest TEXT NOT NULL,
 tenant_digest TEXT NOT NULL, tenant_revision BIGINT NOT NULL,
 actor_selector TEXT NOT NULL, parents TEXT NOT NULL, source_lineage TEXT NOT NULL,
 input_digest TEXT NOT NULL
);
CREATE TABLE knowledge_enrichment_template_results (
 id TEXT PRIMARY KEY, input_id TEXT NOT NULL REFERENCES knowledge_enrichment_template_inputs(id),
 output_digest TEXT NOT NULL
);
CREATE TRIGGER knowledge_enrichment_template_inputs_immutable BEFORE UPDATE OR DELETE ON knowledge_enrichment_template_inputs FOR EACH ROW EXECUTE FUNCTION knowledge_producer_input_immutable();
CREATE TRIGGER knowledge_enrichment_template_results_immutable BEFORE UPDATE OR DELETE ON knowledge_enrichment_template_results FOR EACH ROW EXECUTE FUNCTION knowledge_producer_input_immutable();
