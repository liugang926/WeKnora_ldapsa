ALTER TABLE sessions ADD COLUMN title_source_lineage TEXT;
ALTER TABLE sessions ADD COLUMN description_source_lineage TEXT;
ALTER TABLE sessions ADD COLUMN title_revision INTEGER NOT NULL DEFAULT 0 CHECK (title_revision >= 0);
CREATE TRIGGER nextcloud_session_title_revision_guard BEFORE UPDATE OF title_revision ON sessions
WHEN NEW.title_revision != OLD.title_revision + 1 OR OLD.title_revision >= 9223372036854775807
BEGIN SELECT RAISE(ABORT, 'session_title_revision_not_monotonic'); END;
CREATE TRIGGER nextcloud_session_title_revision AFTER UPDATE OF title, title_source_lineage ON sessions
BEGIN UPDATE sessions SET title_revision = OLD.title_revision + 1 WHERE id = NEW.id; END;
