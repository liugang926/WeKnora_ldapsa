-- Rolling back must not make an existing epoch-zero machine receipt invalid.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_machine_tasks WHERE pair_publication_epoch = 0) THEN
        RAISE EXCEPTION 'machine_task_initial_epoch_exists';
    END IF;
END $$;

ALTER TABLE nextcloud_machine_tasks
    DROP CONSTRAINT nextcloud_machine_tasks_pair_publication_epoch_check,
    ADD CONSTRAINT nextcloud_machine_tasks_pair_publication_epoch_check
    CHECK (pair_publication_epoch > 0);
