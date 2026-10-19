-- The source's durable write revision complements opaque provider ETags.
ALTER TABLE nextcloud_source_versions ADD COLUMN desired_content_revision TEXT NOT NULL DEFAULT '0';
