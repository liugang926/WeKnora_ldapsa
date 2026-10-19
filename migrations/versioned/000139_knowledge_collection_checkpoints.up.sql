-- Private collection checkpoints. Versions track absence/membership and operational ABA;
-- they never grant an original input origin to a legacy row.
CREATE TABLE knowledge_collection_scope_versions (
    tenant_id BIGINT NOT NULL CHECK(tenant_id > 0),
    knowledge_base_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK(revision > 0),
    PRIMARY KEY (tenant_id,knowledge_base_id)
);
INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) SELECT tenant_id,id,1 FROM knowledge_bases;
CREATE FUNCTION bump_knowledge_collection_knowledges() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(OLD.tenant_id,OLD.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=knowledge_collection_scope_versions.revision+1;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(NEW.tenant_id,NEW.knowledge_base_id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=knowledge_collection_scope_versions.revision+1;
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER knowledge_collection_knowledges AFTER INSERT OR UPDATE OR DELETE ON knowledges FOR EACH ROW EXECUTE FUNCTION bump_knowledge_collection_knowledges();
CREATE FUNCTION bump_knowledge_collection_knowledge_bases() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(OLD.tenant_id,OLD.id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=knowledge_collection_scope_versions.revision+1;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        INSERT INTO knowledge_collection_scope_versions(tenant_id,knowledge_base_id,revision) VALUES(NEW.tenant_id,NEW.id,1) ON CONFLICT(tenant_id,knowledge_base_id) DO UPDATE SET revision=knowledge_collection_scope_versions.revision+1;
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER knowledge_collection_knowledge_bases AFTER INSERT OR UPDATE OR DELETE ON knowledge_bases FOR EACH ROW EXECUTE FUNCTION bump_knowledge_collection_knowledge_bases();
