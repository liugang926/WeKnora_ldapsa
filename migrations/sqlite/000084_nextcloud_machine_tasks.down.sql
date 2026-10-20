-- A rollback must not discard receipts which fence previously emitted queue work.
CREATE TEMP TABLE nextcloud_machine_rollback_guard(value INTEGER CHECK(value=0));
INSERT INTO nextcloud_machine_rollback_guard SELECT COUNT(*) FROM nextcloud_machine_tasks;
DROP TABLE nextcloud_machine_rollback_guard;
DROP TABLE nextcloud_machine_task_claims;
DROP TABLE nextcloud_machine_tasks;
