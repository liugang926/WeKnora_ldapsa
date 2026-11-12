package database

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
)

// TestPostgresNextcloudUnbindIgnoresNonGuardFunctions exercises the real catalog,
// including aggregate and window functions which pg_get_functiondef rejects.
func TestPostgresNextcloudUnbindIgnoresNonGuardFunctions(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("disposable PostgreSQL migration fixture required")
	}
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	isolated := postgresLineageDatabaseDSN(t, base, dsn)
	db, err := sql.Open("postgres", isolated)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	root := sqliteRepoRoot(t)
	m, err := migrate.New("file://"+filepath.Join(root, "migrations/versioned"), isolated)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	require.NoError(t, m.Migrate(174))
	_, err = db.Exec(`
 ALTER FUNCTION preserve_kb_nextcloud_provenance() RENAME TO renamed_provenance_guard;
 CREATE TRIGGER renamed_guard BEFORE UPDATE ON knowledge_bases
 FOR EACH ROW EXECUTE FUNCTION renamed_provenance_guard();
 CREATE FUNCTION unrelated_message() RETURNS text LANGUAGE sql AS
 $$ SELECT 'provenance cannot be cleared'::text $$;
 CREATE SCHEMA unrelated_schema;
 CREATE TABLE unrelated_schema.knowledge_bases(id integer);
 CREATE FUNCTION unrelated_schema.retained_guard() RETURNS trigger LANGUAGE plpgsql AS
 $$ BEGIN RAISE EXCEPTION 'provenance cannot be cleared'; END $$;
 CREATE TRIGGER retained_guard BEFORE UPDATE ON unrelated_schema.knowledge_bases
 FOR EACH ROW EXECUTE FUNCTION unrelated_schema.retained_guard();
 `)
	require.NoError(t, err)
	var aggregates, windows int
	require.NoError(t, db.QueryRow(`SELECT count(*) FILTER (WHERE prokind='a'),
 count(*) FILTER (WHERE prokind='w') FROM pg_proc`).Scan(&aggregates, &windows))
	require.Positive(t, aggregates)
	require.Positive(t, windows)
	require.NoError(t, m.Migrate(175))
	requirePostgresMigrationVersion(t, db, 175)
	var removed bool
	require.NoError(t,
		db.QueryRow(`SELECT to_regprocedure('public.renamed_provenance_guard()') IS NULL`).Scan(&removed))
	require.True(t, removed)
	var preserved string
	require.NoError(t, db.QueryRow(`SELECT unrelated_message()`).Scan(&preserved))
	require.Equal(t, "provenance cannot be cleared", preserved)
	var guards int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pg_trigger tg
 JOIN pg_class c ON c.oid=tg.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE tg.tgname='retained_guard' AND n.nspname='unrelated_schema'`).Scan(&guards))
	require.Equal(t, 1, guards)
	var present bool
	require.NoError(t,
		db.QueryRow(`SELECT to_regprocedure('unrelated_schema.retained_guard()') IS NOT NULL`).Scan(&present))
	require.True(t, present)
}
