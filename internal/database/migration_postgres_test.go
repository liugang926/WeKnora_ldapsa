package database

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
)

// TestPostgresMigrationsServeAgentHistory runs the versioned migrations against
// a real PostgreSQL. Skipped unless WEKNORA_MIGRATION_TEST_POSTGRES_DSN points
// at a disposable database, for example:
//
//	docker run -d --rm -e POSTGRES_PASSWORD=pg -e POSTGRES_DB=weknora -p 55432:5432 \
//	  paradedb/paradedb:v0.22.6-pg17
//	WEKNORA_MIGRATION_TEST_POSTGRES_DSN=postgres://postgres:pg@localhost:55432/weknora?sslmode=disable
//
// It checks what SQLite cannot: that the full migration chain serves agent
// history without sorting, and that 000106 builds its index CONCURRENTLY
// through golang-migrate (which fails inside a transaction block). The 106
// down/up check uses a fresh schema so it never crosses later rollback guards.
func TestPostgresMigrationsServeAgentHistory(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_MIGRATION_TEST_POSTGRES_DSN to run PostgreSQL migration tests")
	}
	root := sqliteRepoRoot(t)
	chdirAndRestore(t, root)

	require.NoError(t, RunMigrationsWithOptions(dsn, MigrationOptions{}))

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var version int
	var dirty bool
	require.NoError(t, db.QueryRow("SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty))
	latest := latestVersionedMigration(t, root)
	require.Equal(t, latest, version)
	require.False(t, dirty)
	requirePostgresIndexValid(t, db)
	requirePostgresDirectoryConfigVersionSchema(t, db)

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	// An empty table is always cheapest to scan; rule the scan out to see
	// which index the planner can use and whether it still has to sort.
	_, err = conn.ExecContext(ctx, "SET enable_seqscan = off")
	require.NoError(t, err)
	for name, query := range map[string]string{
		"backwards page": `SELECT * FROM messages WHERE session_id = 's'
			AND (created_at < now() OR (created_at = now() AND id < 'x'))
			AND deleted_at IS NULL ORDER BY created_at DESC, id DESC LIMIT 200`,
		"newest checkpoint": `SELECT id FROM messages WHERE session_id = 's' AND role = 'assistant'
			AND context_checkpoint IS NOT NULL AND deleted_at IS NULL
			ORDER BY created_at DESC, id DESC LIMIT 1`,
	} {
		rows, err := conn.QueryContext(ctx, "EXPLAIN "+query)
		require.NoError(t, err, name)
		var plan strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line), name)
			plan.WriteString(line + "\n")
		}
		require.NoError(t, rows.Close(), name)
		require.Contains(t, plan.String(), "idx_messages_session_created_id", "%s plan:\n%s", name, plan.String())
		require.NotContains(t, plan.String(), "Sort", "%s must not sort:\n%s", name, plan.String())
	}

	t.Run("ConcurrentIndexRoundtripAt106", func(t *testing.T) {
		// Migrations 126 and 129 intentionally guard provenance on rollback.
		// Start another migration chain at zero instead of downgrading latest.
		var random [8]byte
		_, err := rand.Read(random[:])
		require.NoError(t, err)
		schema := "weknora_migration_106_" + hex.EncodeToString(random[:])
		_, err = db.Exec("CREATE SCHEMA " + schema)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, err := db.Exec("DROP SCHEMA " + schema + " CASCADE")
			require.NoError(t, err)
		})

		isolatedDSN := postgresSchemaDSN(t, dsn, schema)
		isolatedDB, err := sql.Open("postgres", isolatedDSN)
		require.NoError(t, err)
		t.Cleanup(func() { _ = isolatedDB.Close() })
		m, err := migrate.New("file://migrations/versioned", isolatedDSN)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = m.Close() })

		require.NoError(t, m.Migrate(106))
		requirePostgresMigrationVersion(t, isolatedDB, 106)
		requirePostgresIndexValid(t, isolatedDB)
		require.NoError(t, m.Steps(-1))
		requirePostgresMigrationVersion(t, isolatedDB, 105)
		var indexes int
		require.NoError(t, isolatedDB.QueryRow(`SELECT count(*) FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = current_schema() AND c.relname = 'idx_messages_session_created_id'`).Scan(&indexes))
		require.Zero(t, indexes, "the down migration drops the index")
		require.NoError(t, m.Steps(1))
		requirePostgresMigrationVersion(t, isolatedDB, 106)
		requirePostgresIndexValid(t, isolatedDB)

		// The terminal schema was never rolled back.
		requirePostgresMigrationVersion(t, db, latest)
	})
}

func postgresSchemaDSN(t *testing.T, dsn, schema string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	query := u.Query()
	query.Set("search_path", schema+",public")
	u.RawQuery = query.Encode()
	return u.String()
}

func requirePostgresMigrationVersion(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var version int
	var dirty bool
	require.NoError(t, db.QueryRow("SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty))
	require.Equal(t, want, version)
	require.False(t, dirty)
}

func requirePostgresDirectoryConfigVersionSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	var dataType, nullable, defaultValue string
	require.NoError(t, db.QueryRow(`SELECT data_type, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'directories' AND column_name = 'config_version'`).
		Scan(&dataType, &nullable, &defaultValue))
	require.Equal(t, "bigint", dataType)
	require.Equal(t, "NO", nullable)
	require.Contains(t, defaultValue, "1")

	var fingerprintLength int
	var fingerprintNullable, fingerprintDefault string
	require.NoError(t, db.QueryRow(`SELECT character_maximum_length, is_nullable, column_default
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'directories' AND column_name = 'security_config_fingerprint'`).
		Scan(&fingerprintLength, &fingerprintNullable, &fingerprintDefault))
	require.Equal(t, 64, fingerprintLength)
	require.Equal(t, "NO", fingerprintNullable)
	require.Contains(t, fingerprintDefault, "''")
}

func requirePostgresIndexValid(t *testing.T, db *sql.DB) {
	t.Helper()
	var valid bool
	require.NoError(t, db.QueryRow(`SELECT i.indisvalid FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname = 'idx_messages_session_created_id'`).Scan(&valid))
	require.True(t, valid, "a CONCURRENTLY build that failed leaves an INVALID index")
}

func latestVersionedMigration(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "migrations", "versioned"))
	require.NoError(t, err)
	latest := 0
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			continue
		}
		if n, err := strconv.Atoi(prefix); err == nil && n > latest {
			latest = n
		}
	}
	return latest
}
