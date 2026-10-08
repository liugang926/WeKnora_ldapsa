-- Only an original trusted admission may carry a worker actor selector.
-- Legacy/no-selector workers remain closed; role/capability grants are not saved.
CREATE TABLE knowledge_input_actor_selectors (
    admission_id TEXT PRIMARY KEY REFERENCES knowledge_input_admissions(admission_id),
    caller_tenant_id BIGINT NOT NULL CHECK(caller_tenant_id > 0),
    user_id TEXT NOT NULL DEFAULT '',
    principal_type TEXT NOT NULL CHECK(principal_type IN ('web_user','api_tenant','api_external_user')),
    principal_id TEXT NOT NULL CHECK(length(trim(principal_id)) > 0),
    api_key_id BIGINT,
    CHECK((principal_type='web_user' AND length(trim(user_id)) > 0 AND principal_id=user_id AND api_key_id IS NULL) OR (principal_type IN ('api_tenant','api_external_user') AND user_id='' AND api_key_id > 0))
);
CREATE TRIGGER knowledge_input_actor_immutable_update BEFORE UPDATE ON knowledge_input_actor_selectors BEGIN SELECT RAISE(ABORT,'immutable original worker actor'); END;
CREATE TRIGGER knowledge_input_actor_immutable_delete BEFORE DELETE ON knowledge_input_actor_selectors BEGIN SELECT RAISE(ABORT,'immutable original worker actor'); END;
