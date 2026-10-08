package database

import (
	"database/sql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSessionTextLineageSQLiteUpgradeFrom51LeavesLegacyUnknown(t *testing.T) {
	root := sqliteRepoRoot(t)
	prior := copySQLiteMigrationsThrough(t, root, 51)
	chdirAndRestore(t, prior)
	path := filepath.Join(t.TempDir(), "title.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: path}))
	db := openSQLiteDB(t, path)
	_, err := db.Exec(`INSERT INTO sessions(id,tenant_id,title,description) VALUES('legacy',7,'legacy secret title','legacy secret description')`)
	require.NoError(t, err)
	chdirAndRestore(t, root)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: path}))
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, 52, version)
	require.False(t, dirty)
	var title, description string
	var titleProof, descriptionProof sql.NullString
	require.NoError(t, db.QueryRow(`SELECT title,description,title_source_lineage,description_source_lineage FROM sessions WHERE id='legacy'`).Scan(&title, &description, &titleProof, &descriptionProof))
	require.Equal(t, "legacy secret title", title)
	require.Equal(t, "legacy secret description", description)
	require.False(t, titleProof.Valid)
	require.False(t, descriptionProof.Valid)
	down, err := os.ReadFile(filepath.Join(root, "migrations/sqlite/000052_session_text_source_lineage.down.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.ErrorContains(t, err, "session_text_lineage_rollback_requires_audit_export")
}

func TestSessionTextLineagePostgresUpgradeFrom132LeavesLegacyUnknown(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set disposable migration PostgreSQL DSN")
	}
	root := sqliteRepoRoot(t)
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	schema := "session_title_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	_, err = base.Exec("CREATE SCHEMA " + schema)
	require.NoError(t, err)
	t.Cleanup(func() { _, err := base.Exec("DROP SCHEMA " + schema + " CASCADE"); require.NoError(t, err) })
	isolate := postgresSchemaDSN(t, dsn, schema)
	parsed, err := url.Parse(isolate)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("options", strings.TrimSpace(query.Get("options")+" -c app.skip_embedding=true"))
	parsed.RawQuery = query.Encode()
	isolate = parsed.String()
	db, err := sql.Open("postgres", isolate)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	m, err := migrate.New("file://"+filepath.Join(root, "migrations/versioned"), isolate)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	require.NoError(t, m.Migrate(132))
	_, err = db.Exec(`INSERT INTO sessions(id,tenant_id,title,description) VALUES('legacy',7,'legacy secret title','legacy secret description')`)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(133))
	requirePostgresMigrationVersion(t, db, 133)
	var titleProof, descriptionProof sql.NullString
	require.NoError(t, db.QueryRow(`SELECT title_source_lineage,description_source_lineage FROM sessions WHERE id='legacy'`).Scan(&titleProof, &descriptionProof))
	require.False(t, titleProof.Valid)
	require.False(t, descriptionProof.Valid)
	require.ErrorContains(t, m.Steps(-1), "session_text_lineage_rollback_requires_audit_export")
}
