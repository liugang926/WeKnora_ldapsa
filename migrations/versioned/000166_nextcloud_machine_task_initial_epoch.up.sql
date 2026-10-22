-- A fresh Nextcloud binding starts at publication epoch zero. Machine work
-- carries the exact non-negative source generation observed for its item.
ALTER TABLE nextcloud_machine_tasks
    DROP CONSTRAINT nextcloud_machine_tasks_pair_publication_epoch_check,
    ADD CONSTRAINT nextcloud_machine_tasks_pair_publication_epoch_check
    CHECK (pair_publication_epoch >= 0);
