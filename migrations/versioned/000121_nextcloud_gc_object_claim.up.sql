-- Keep 000119 immutable: installations may already have applied it.
ALTER TABLE nextcloud_gc_items
    ADD COLUMN lease_token TEXT NOT NULL DEFAULT '',
    ADD COLUMN lease_until TIMESTAMPTZ;

ALTER TABLE nextcloud_gc_items
    DROP CONSTRAINT nextcloud_gc_items_state_check;
ALTER TABLE nextcloud_gc_items
    ADD CONSTRAINT nextcloud_gc_items_state_check
    CHECK (state IN ('pending', 'blocked', 'deleting', 'collected'));
