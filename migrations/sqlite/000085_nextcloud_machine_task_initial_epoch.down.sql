-- Rolling back must not make an existing epoch-zero machine receipt invalid.
CREATE TEMP TABLE nextcloud_machine_initial_epoch_rollback_guard(value INTEGER CHECK(value=0));
INSERT INTO nextcloud_machine_initial_epoch_rollback_guard
SELECT COUNT(*) FROM nextcloud_machine_tasks WHERE pair_publication_epoch=0;

CREATE TEMP TABLE nextcloud_machine_task_claims_085_down AS
SELECT receipt_id, state, claim_id, build_lease_id, version
FROM nextcloud_machine_task_claims;
DROP TABLE nextcloud_machine_task_claims;

CREATE TABLE nextcloud_machine_tasks_085_down (
 receipt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL UNIQUE, task_type TEXT NOT NULL,
 tenant_id BIGINT NOT NULL CHECK(tenant_id>0), knowledge_base_id TEXT NOT NULL, knowledge_id TEXT NOT NULL,
 attempt INTEGER NOT NULL CHECK(attempt>=0), payload_digest TEXT NOT NULL CHECK(length(payload_digest)=64),
 original_proof TEXT NOT NULL, original_kind TEXT NOT NULL CHECK(original_kind IN ('upload','build')), build_generation BIGINT NOT NULL CHECK(build_generation>0),
 datasource_id TEXT NOT NULL, external_id TEXT NOT NULL, pair_operation_id TEXT NOT NULL,
 pair_publication_epoch BIGINT NOT NULL CHECK(pair_publication_epoch>0),
 manager_purpose_id TEXT NOT NULL REFERENCES nextcloud_onboarding_manager_purposes(purpose_id),
 manager_version BIGINT NOT NULL CHECK(manager_version>0), manager_digest TEXT NOT NULL CHECK(length(manager_digest)=64),
 created_at_ms BIGINT NOT NULL CHECK(created_at_ms>0)
);
INSERT INTO nextcloud_machine_tasks_085_down
SELECT * FROM nextcloud_machine_tasks;
DROP TABLE nextcloud_machine_tasks;
ALTER TABLE nextcloud_machine_tasks_085_down RENAME TO nextcloud_machine_tasks;

CREATE TABLE nextcloud_machine_task_claims_085_down_new (
 receipt_id TEXT PRIMARY KEY REFERENCES nextcloud_machine_tasks(receipt_id),
 state TEXT NOT NULL CHECK(state IN ('ready','running','done','failed')),
 claim_id TEXT NOT NULL DEFAULT '', build_lease_id TEXT NOT NULL DEFAULT '', version BIGINT NOT NULL CHECK(version>0)
);
INSERT INTO nextcloud_machine_task_claims_085_down_new
SELECT receipt_id, state, claim_id, build_lease_id, version
FROM nextcloud_machine_task_claims_085_down;
DROP TABLE nextcloud_machine_task_claims_085_down;
ALTER TABLE nextcloud_machine_task_claims_085_down_new RENAME TO nextcloud_machine_task_claims;

CREATE TRIGGER nextcloud_machine_task_update BEFORE UPDATE ON nextcloud_machine_tasks
BEGIN SELECT RAISE(ABORT,'immutable_machine_task'); END;
CREATE TRIGGER nextcloud_machine_task_delete BEFORE DELETE ON nextcloud_machine_tasks
BEGIN SELECT RAISE(ABORT,'immutable_machine_task'); END;

DROP TABLE nextcloud_machine_initial_epoch_rollback_guard;
