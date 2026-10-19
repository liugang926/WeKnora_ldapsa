package database

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
)

var steerOriginalTables = []string{
	"steer_authored_origins", "steer_control_admissions", "steer_control_heads",
	"steer_consumption_origins", "steer_control_versions", "message_completion_input_manifests",
}

func TestSQLiteSteerOriginalUpgradeFrom72PreservesUnknownHistory(t *testing.T) {
	root := sqliteRepoRoot(t)
	prior := copySQLiteMigrationsThrough(t, root, 72)
	chdirAndRestore(t, prior)
	path := filepath.Join(t.TempDir(), "steer-upgrade.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: path}))
	db := openSQLiteDB(t, path)
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, 72, version)
	require.False(t, dirty)
	seedPreSteerUnknownHistory(t, db)

	current := copySQLiteMigrationsThrough(t, root, 73)
	chdirAndRestore(t, current)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: path}))
	version, dirty = sqliteMigrationState(t, db)
	require.Equal(t, 73, version)
	require.False(t, dirty)
	requireSteerOriginalSchema(t, db, "sqlite")
	requirePreSteerHistoryStillUnknown(t, db)

	m, err := migrate.New("file://"+filepath.Join(root, "migrations/sqlite"), "sqlite3://"+path)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	require.ErrorContains(t, m.Steps(-1), "steer_original_schema_rollback_forbidden")
	requireSteerOriginalSchema(t, db, "sqlite")
	requirePreSteerHistoryStillUnknown(t, db)
	require.ErrorContains(
		t,
		RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: path}),
		"dirty",
	)
}

func TestPostgresSteerOriginalUpgradeFrom153PreservesUnknownHistory(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set disposable migration PostgreSQL DSN")
	}
	root := sqliteRepoRoot(t)
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	isolated := postgresLineageDatabaseDSN(t, base, dsn)
	db, err := sql.Open("postgres", isolated)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	m, err := migrate.New("file://"+filepath.Join(root, "migrations/versioned"), isolated)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	require.NoError(t, m.Migrate(153))
	requirePostgresMigrationVersion(t, db, 153)
	seedPreSteerUnknownHistory(t, db)
	require.NoError(t, m.Migrate(154))
	requirePostgresMigrationVersion(t, db, 154)
	requireSteerOriginalSchema(t, db, "postgres")
	requirePreSteerHistoryStillUnknown(t, db)
	require.ErrorContains(t, m.Steps(-1), "original steer schema rollback forbidden")
	requireSteerOriginalSchema(t, db, "postgres")
	requirePreSteerHistoryStillUnknown(t, db)
}

// Legacy saved input is deliberately not admitted by a producer. The migration
// must preserve its bytes without treating them as original Steer or message
// material, even when their payload claims a cached original ID.
func seedPreSteerUnknownHistory(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, statement := range []string{
		`INSERT INTO sessions(id,tenant_id,user_id) VALUES('legacy-steer-session',7,'legacy-owner')`,
		`INSERT INTO messages(id,request_id,session_id,role,content,is_completed)
		 VALUES('legacy-steer-message','legacy-request','legacy-steer-session','user',
		 '{"query":"legacy cached steer","original_admission_id":"redis-only-original"}',TRUE)`,
		`INSERT INTO nextcloud_source_tombstones(tenant_id,scope_type,scope_id)
		 VALUES(7,'datasource','retired-before-steer-upgrade')`,
	} {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}
}

func requirePreSteerHistoryStillUnknown(t *testing.T, db *sql.DB) {
	t.Helper()
	var content string
	var lineage sql.NullString
	require.NoError(t, db.QueryRow(`SELECT content,source_lineage FROM messages
		WHERE id='legacy-steer-message'`).Scan(&content, &lineage))
	require.Equal(t, `{"query":"legacy cached steer","original_admission_id":"redis-only-original"}`, content)
	require.False(t, lineage.Valid, "migration must not reconstruct missing original source lineage")
	for _, table := range append(append([]string{}, steerOriginalTables...),
		"message_generation_origins", "message_material_admissions", "message_material_receipts") {
		var count int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count, "%s cannot be backfilled from legacy input or a copied Redis original ID", table)
	}
	var tombstones int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM nextcloud_source_tombstones
		WHERE tenant_id=7 AND scope_type='datasource' AND scope_id='retired-before-steer-upgrade'`).Scan(&tombstones))
	require.Equal(t, 1, tombstones, "upgrade and refused rollback preserve source retirement facts")
}

func requireSteerOriginalSchema(t *testing.T, db *sql.DB, dialect string) {
	t.Helper()
	for _, table := range steerOriginalTables {
		var count int
		if dialect == "sqlite" {
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master
				WHERE type='table' AND name=?`, table).Scan(&count))
		} else {
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM information_schema.tables
				WHERE table_schema=current_schema() AND table_name=$1`, table).Scan(&count))
		}
		require.Equal(t, 1, count, "missing Steer proof table %s", table)
	}
	for _, table := range []string{"steer_authored_origins", "steer_control_admissions", "steer_consumption_origins", "message_completion_input_manifests"} {
		if dialect == "sqlite" {
			for _, operation := range []string{"update", "delete"} {
				var count int
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master
					WHERE type='trigger' AND tbl_name=? AND name=?`, table, table+"_immutable_"+operation).Scan(&count))
				require.Equal(t, 1, count, "%s %s must retain immutable original proofs", table, operation)
			}
		} else {
			var count int
			require.NoError(
				t,
				db.QueryRow(("SELECT count(*) FROM pg_trigger t\n\t\t\t\tJOIN pg_class c ON c.oid=t.tgre"+
					"lid JOIN pg_namespace n ON n.oid=c.relnamespace\n\t\t\t\tWHERE n.nspname=c"+
					"urrent_schema() AND c.relname=$1 AND t.tgname=$2\n\t\t\t\tAND NOT t.tgisin"+
					"ternal AND t.tgenabled='O' AND (t.tgtype & 26)=26"), table, table+"_immutable").Scan(&count),
			)
			require.Equal(t, 1, count, "%s must reject update and delete of original proofs", table)
		}
	}
}
