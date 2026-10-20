-- Private collection checkpoints. Versions track absence/membership and operational ABA;
-- they never grant an original input origin to a legacy row.
CREATE TABLE knowledge_collection_scope_versions (
    tenant_id BIGINT NOT NULL CHECK(tenant_id > 0),
    knowledge_base_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK(revision > 0),
    PRIMARY KEY (tenant_id,knowledge_base_id)
);
INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) SELECT tenant_id,id,1 FROM knowledge_bases;
CREATE TRIGGER knowledge_collection_knowledges_insert AFTER INSERT ON knowledges BEGIN INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(NEW.tenant_id,NEW.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_collection_knowledges_update AFTER UPDATE ON knowledges BEGIN INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(OLD.tenant_id,OLD.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(NEW.tenant_id,NEW.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_collection_knowledges_delete AFTER DELETE ON knowledges BEGIN INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(OLD.tenant_id,OLD.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_collection_knowledge_bases_insert AFTER INSERT ON knowledge_bases BEGIN INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(NEW.tenant_id,NEW.id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_collection_knowledge_bases_update AFTER UPDATE ON knowledge_bases BEGIN INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(OLD.tenant_id,OLD.id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(NEW.tenant_id,NEW.id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_collection_knowledge_bases_delete AFTER DELETE ON knowledge_bases BEGIN INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(OLD.tenant_id,OLD.id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=revision+1; END;
