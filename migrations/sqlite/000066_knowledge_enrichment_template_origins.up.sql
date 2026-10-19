-- Explicit new templates participate in the existing immutable original
-- configuration origin and monotonic actual-control counter.
ALTER TABLE knowledge_bases ADD COLUMN enrichment_templates TEXT;
CREATE TRIGGER knowledge_enrichment_template_control_update AFTER UPDATE ON knowledge_bases WHEN OLD.enrichment_templates IS NOT NEW.enrichment_templates BEGIN INSERT INTO knowledge_configuration_control_versions(knowledge_base_id,revision) VALUES(NEW.id,1) ON CONFLICT(knowledge_base_id) DO UPDATE SET revision=revision+1; END;
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
CREATE TRIGGER knowledge_enrichment_template_inputs_update BEFORE UPDATE ON knowledge_enrichment_template_inputs BEGIN SELECT RAISE(ABORT,'immutable template origin'); END;
CREATE TRIGGER knowledge_enrichment_template_inputs_delete BEFORE DELETE ON knowledge_enrichment_template_inputs BEGIN SELECT RAISE(ABORT,'immutable template origin'); END;
CREATE TRIGGER knowledge_enrichment_template_results_update BEFORE UPDATE ON knowledge_enrichment_template_results BEGIN SELECT RAISE(ABORT,'immutable template origin'); END;
CREATE TRIGGER knowledge_enrichment_template_results_delete BEFORE DELETE ON knowledge_enrichment_template_results BEGIN SELECT RAISE(ABORT,'immutable template origin'); END;
