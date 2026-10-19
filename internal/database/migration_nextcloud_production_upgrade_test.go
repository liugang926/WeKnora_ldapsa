package database

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/require"
)

func TestPostgresNextcloudProductionUpgradeFrom132PreservesLegacySession(t *testing.T) {
	dsn := os.Getenv("WEKNORA_MIGRATION_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("disposable PostgreSQL migration fixture required")
	}
	root := sqliteRepoRoot(t)
	base, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	isolated := postgresLineageDatabaseDSN(t, base, dsn)
	db, err := sql.Open("postgres", isolated)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	m, err := migrate.New("file://"+filepath.Join(root, "migrations/versioned"), isolated)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	require.NoError(t, m.Migrate(132))
	requirePostgresMigrationVersion(t, db, 132)
	_, err = db.Exec(
		("INSERT INTO sessions(id,tenant_id,title,description) VALUES('legacy-upgrad" +
			"e',7,'existing title','existing description')"),
	)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(uint(latestVersionedMigration(t, root))))
	requirePostgresMigrationVersion(t, db, latestVersionedMigration(t, root))
	var title, description string
	var titleLineage, descriptionLineage sql.NullString
	var scoped bool
	require.NoError(
		t,
		db.QueryRow(("SELECT title,description,title_source_lineage,description_source_lineage,n"+
			"extcloud_scoped FROM sessions WHERE id='legacy-upgrade'")).
			Scan(&title, &description, &titleLineage, &descriptionLineage, &scoped),
	)
	require.Equal(t, "existing title", title)
	require.Equal(t, "existing description", description)
	require.False(t, titleLineage.Valid)
	require.False(t, descriptionLineage.Valid)
	require.False(t, scoped)
	for _, table := range []string{
		"knowledge_configuration_control_versions", "nextcloud_publication_recovery_scopes",
		"original_body_journal_heads", "nextcloud_onboarding_operations", "nextcloud_source_grants",
		"nextcloud_file_states", "nextcloud_status_outbox", "nextcloud_source_status_epochs",
		"nextcloud_recovery_build_leases", "nextcloud_onboarding_target_closures",
		"knowledge_configuration_original_body_manifests", "nextcloud_onboarding_manager_heads", "nextcloud_machine_tasks", "nextcloud_machine_task_claims",
	} {
		var exists bool
		require.NoError(
			t,
			db.QueryRow(("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=c"+
				"urrent_schema() AND table_name=$1)"), table).
				Scan(&exists),
		)
		require.True(t, exists, "missing upgraded table %s", table)
	}
}
