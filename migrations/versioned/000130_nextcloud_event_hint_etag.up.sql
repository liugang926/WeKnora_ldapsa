-- Keep the signed source version with each durable hint. A later full-manifest
-- proof can reject a stale snapshot even when its changes cursor is newer.
ALTER TABLE nextcloud_event_inbox
    ADD COLUMN etag TEXT CHECK (etag IS NULL OR LENGTH(etag) <= 255);
ALTER TABLE nextcloud_event_inbox
    ADD COLUMN path TEXT CHECK (path IS NULL OR LENGTH(path) <= 4096);
ALTER TABLE nextcloud_event_inbox
    ADD COLUMN relative_path TEXT CHECK (relative_path IS NULL OR LENGTH(relative_path) <= 4096);
