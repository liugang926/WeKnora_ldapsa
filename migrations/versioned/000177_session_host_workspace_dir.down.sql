DO $$ BEGIN RAISE NOTICE '[Migration 000177 down] Dropping sessions.host_workspace_dir'; END $$;

ALTER TABLE sessions DROP COLUMN IF EXISTS host_workspace_dir;
