-- Original display values are admitted only by actual typed writers.
-- Legacy rows receive monotonic counters, never producer origins.
CREATE TABLE knowledge_display_versions (
 kind TEXT NOT NULL, row_id TEXT NOT NULL, revision BIGINT NOT NULL CHECK(revision > 0), PRIMARY KEY(kind,row_id)
);
CREATE TABLE knowledge_display_input_origins (
 origin_id TEXT PRIMARY KEY, kind TEXT NOT NULL, row_id TEXT NOT NULL,
 tenant_id BIGINT NOT NULL CHECK(tenant_id > 0), knowledge_base_id TEXT NOT NULL,
 revision BIGINT NOT NULL CHECK(revision > 0), origin_kind TEXT NOT NULL CHECK(origin_kind IN ('human','derived')),
 actual_input TEXT NOT NULL, input_digest TEXT NOT NULL CHECK(length(input_digest)=64),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=64), source_lineage TEXT NOT NULL CHECK(length(source_lineage)<=262144), actor TEXT NOT NULL,
 UNIQUE(kind,row_id,revision)
);
INSERT INTO knowledge_display_versions(kind,row_id,revision) SELECT 'kb-name',id,1 FROM knowledge_bases;
INSERT INTO knowledge_display_versions(kind,row_id,revision) SELECT 'relations',id,1 FROM knowledges;
INSERT INTO knowledge_display_versions(kind,row_id,revision) SELECT 'tag-name',id,1 FROM knowledge_tags;
INSERT INTO knowledge_display_versions(kind,row_id,revision) SELECT 'tag-color',id,1 FROM knowledge_tags;
INSERT INTO knowledge_display_versions(kind,row_id,revision) SELECT 'tag-sort_order',id,1 FROM knowledge_tags;
INSERT INTO knowledge_display_versions(kind,row_id,revision) SELECT 'tag-envelope',id,1 FROM knowledge_tags;
CREATE TRIGGER knowledge_display_origins_immutable_update BEFORE UPDATE ON knowledge_display_input_origins BEGIN SELECT RAISE(ABORT,'immutable actual display input'); END;
CREATE TRIGGER knowledge_display_origins_immutable_delete BEFORE DELETE ON knowledge_display_input_origins BEGIN SELECT RAISE(ABORT,'immutable actual display input'); END;
CREATE TRIGGER knowledge_display_tag_name_insert AFTER INSERT ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-name',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_name_update AFTER UPDATE ON knowledge_tags WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id OR OLD.name IS NOT NEW.name BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-name',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_name_delete AFTER DELETE ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-name',OLD.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_color_insert AFTER INSERT ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-color',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_color_update AFTER UPDATE ON knowledge_tags WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id OR OLD.color IS NOT NEW.color BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-color',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_color_delete AFTER DELETE ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-color',OLD.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_sort_order_insert AFTER INSERT ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-sort_order',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_sort_order_update AFTER UPDATE ON knowledge_tags WHEN OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id OR OLD.sort_order IS NOT NEW.sort_order BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-sort_order',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_sort_order_delete AFTER DELETE ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-sort_order',OLD.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_envelope_insert AFTER INSERT ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-envelope',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_envelope_update AFTER UPDATE ON knowledge_tags WHEN OLD.id IS NOT NEW.id OR OLD.tenant_id IS NOT NEW.tenant_id OR OLD.knowledge_base_id IS NOT NEW.knowledge_base_id OR OLD.seq_id IS NOT NEW.seq_id OR OLD.name IS NOT NEW.name OR OLD.color IS NOT NEW.color OR OLD.sort_order IS NOT NEW.sort_order OR OLD.created_at IS NOT NEW.created_at OR OLD.updated_at IS NOT NEW.updated_at BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-envelope',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_envelope_delete AFTER DELETE ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-envelope',OLD.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_kb_insert AFTER INSERT ON knowledge_bases BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('kb-name',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_kb_update AFTER UPDATE ON knowledge_bases WHEN OLD.name IS NOT NEW.name OR OLD.tenant_id IS NOT NEW.tenant_id OR OLD.deleted_at IS NOT NEW.deleted_at BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('kb-name',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_kb_delete AFTER DELETE ON knowledge_bases BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('kb-name',OLD.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_relation_seed AFTER INSERT ON knowledges BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('relations',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_relation_insert AFTER INSERT ON knowledge_tag_relations BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('relations',NEW.knowledge_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_relation_update AFTER UPDATE ON knowledge_tag_relations BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('relations',OLD.knowledge_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('relations',NEW.knowledge_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_relation_delete AFTER DELETE ON knowledge_tag_relations BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('relations',OLD.knowledge_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;

CREATE TABLE knowledge_auto_tag_input_manifests (
 manifest_id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL CHECK(tenant_id > 0),
 knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL, admission_id TEXT NOT NULL,
 input_digest TEXT NOT NULL CHECK(length(input_digest)=64),actual_inputs TEXT NOT NULL,source_lineage TEXT NOT NULL CHECK(length(source_lineage)<=262144)
 );
 INSERT INTO knowledge_display_versions(kind,row_id,revision) SELECT 'tag-list',id,1 FROM knowledge_bases;
CREATE TRIGGER knowledge_auto_tag_manifest_immutable_update BEFORE UPDATE ON knowledge_auto_tag_input_manifests BEGIN SELECT RAISE(ABORT,'immutable actual classifier input'); END;
CREATE TRIGGER knowledge_auto_tag_manifest_immutable_delete BEFORE DELETE ON knowledge_auto_tag_input_manifests BEGIN SELECT RAISE(ABORT,'immutable actual classifier input'); END;
CREATE TRIGGER knowledge_display_tag_list_seed AFTER INSERT ON knowledge_bases BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-list',NEW.id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_list_insert AFTER INSERT ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-list',NEW.knowledge_base_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_list_update AFTER UPDATE ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-list',OLD.knowledge_base_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-list',NEW.knowledge_base_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
CREATE TRIGGER knowledge_display_tag_list_delete AFTER DELETE ON knowledge_tags BEGIN INSERT INTO knowledge_display_versions(kind,row_id,revision) VALUES('tag-list',OLD.knowledge_base_id,1) ON CONFLICT(kind,row_id) DO UPDATE SET revision=revision+1; END;
