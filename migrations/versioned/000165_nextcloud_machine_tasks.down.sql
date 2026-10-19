-- A rollback must not discard receipts which fence previously emitted queue work.
DO $$ BEGIN IF EXISTS(SELECT 1 FROM nextcloud_machine_tasks) THEN RAISE EXCEPTION 'machine_task_history_exists'; END IF; END $$;
DROP TABLE nextcloud_machine_task_claims;
DROP TABLE nextcloud_machine_tasks;
DROP FUNCTION nextcloud_machine_task_immutable();
