-- An inventory with exact derived identities cannot be represented by 000121.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM nextcloud_gc_items
               WHERE kind IN ('derived_chunk', 'postgres_embedding', 'sqlite_embedding')) THEN
        RAISE EXCEPTION 'Nextcloud GC has durable derived-index inventory';
    END IF;
END $$;

ALTER TABLE nextcloud_gc_items
    DROP CONSTRAINT nextcloud_gc_items_kind_check;
ALTER TABLE nextcloud_gc_items
    ADD CONSTRAINT nextcloud_gc_items_kind_check
    CHECK (kind IN ('knowledge', 'source_file', 'extracted_image', 'derived_index'));
