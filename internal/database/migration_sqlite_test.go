package database

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteMigrationsIncludeAutoTagConfig(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	dbPath := filepath.Join(t.TempDir(), "migration.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))

	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rows, err := db.Query("PRAGMA table_info(knowledge_bases)")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	found := false
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		if name == "auto_tag_config" {
			found = true
		}
	}
	require.NoError(t, rows.Err())
	require.True(t, found, "SQLite migrations must create knowledge_bases.auto_tag_config")
}

func TestSQLiteMigrationsAllowMultipleNextcloudSourcesPerKnowledgeBase(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	dbPath := filepath.Join(t.TempDir(), "migration-multisource.db")
	require.NoError(t, RunMigrationsWithOptions(
		"sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath},
	))
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	for table, forbidden := range map[string]string{
		"nextcloud_source_pairings":            "knowledge_base_id TEXT NOT NULL UNIQUE",
		"nextcloud_onboarding_operations":      "UNIQUE(tenant_id,knowledge_base_id)",
		"nextcloud_onboarding_target_closures": "UNIQUE(tenant_id,knowledge_base_id)",
		"nextcloud_source_decommissions":       "knowledge_base_id TEXT NOT NULL UNIQUE",
		"nextcloud_indexed_withdrawals":        "knowledge_base_id TEXT NOT NULL UNIQUE",
	} {
		var definition string
		require.NoError(t, db.QueryRow(
			"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", table,
		).Scan(&definition))
		require.NotContains(t, definition, forbidden)
	}
	for _, trigger := range []string{
		"nextcloud_paired_kb_hard_delete_guard",
		"nextcloud_target_nextcloud_source_pairings_insert",
		"nextcloud_target_nextcloud_source_pairings_update",
		"nextcloud_target_nextcloud_source_pairings_delete",
		"nextcloud_source_pairing_row_guard_update",
		"nextcloud_source_pairing_row_guard_delete",
		"nextcloud_target_closure_insert_guard",
		"nextcloud_onboarding_guard_data_sources_insert",
		"nextcloud_onboarding_guard_data_sources_update",
		"nextcloud_onboarding_operation_identity_guard",
		"nextcloud_onboarding_operation_generation_guard",
		"nextcloud_target_operation_insert",
		"nextcloud_target_operation_update",
		"nextcloud_target_operation_delete",
		"nextcloud_target_closure_immutable_update",
		"nextcloud_target_closure_immutable_delete",
		"nextcloud_target_knowledges_insert",
		"nextcloud_decommission_resume_guard",
		"nextcloud_decommission_row_guard_update",
		"nextcloud_decommission_row_guard_delete",
		"nextcloud_indexed_withdrawal_immutable_update",
		"nextcloud_indexed_withdrawal_immutable_delete",
		"nextcloud_indexed_withdrawal_resume_guard",
	} {
		var count int
		require.NoError(t, db.QueryRow(
			"SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name=?", trigger,
		).Scan(&count))
		require.Equal(t, 1, count, trigger)
	}

	rows, err := db.Query("PRAGMA foreign_key_check")
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	require.False(t, rows.Next(), "migration must not leave foreign-key violations")
	require.NoError(t, rows.Err())

	var integrity string
	require.NoError(t, db.QueryRow("PRAGMA integrity_check").Scan(&integrity))
	require.Equal(t, "ok", integrity)
}

func TestSQLiteMultisourceUpgradePreservesExistingControlRows(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	legacyRoot := copySQLiteMigrationsThrough(t, repoRoot, 85)
	chdirAndRestore(t, legacyRoot)
	dbPath := filepath.Join(t.TempDir(), "multisource-upgrade.db")
	require.NoError(t, RunMigrationsWithOptions(
		"sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath},
	))
	db := openSQLiteDB(t, dbPath)

	statements := []string{
		`INSERT INTO tenants(id,name,business) VALUES(7,'multisource','migration')`,
		`INSERT INTO knowledge_bases(id,name,tenant_id,embedding_model_id,summary_model_id)
		 VALUES('kb-upgrade-a','A',7,'embed','summary'),
		       ('kb-upgrade-b','B',7,'embed','summary')`,
		`INSERT INTO data_sources(id,tenant_id,knowledge_base_id,name,type,config,status)
		 VALUES('ds-upgrade-a',7,'kb-upgrade-a','A','nextcloud','{}','paused'),
		       ('ds-upgrade-b',7,'kb-upgrade-b','B','nextcloud','{}','paused')`,
		`INSERT INTO nextcloud_onboarding_credentials
		 (id,token_sha256,instance_id,base_url,tenant_id,directory_id,template_kb_id,enabled)
		 VALUES('credential','hash','instance','https://nextcloud.example',7,'directory',
		        'kb-upgrade-a',TRUE)`,
		`INSERT INTO nextcloud_onboarding_operations
		 (operation_id,credential_id,tenant_id,instance_id,binding_id,knowledge_base_id,
		  actor_directory_id,actor_object_guid,actor_user_id,mode,name,
		  processing_template_id,request_sha256,state)
		 VALUES('op-upgrade-a','credential',7,'instance','binding-upgrade-a','kb-upgrade-a',
		        'directory','guid','user','existing','A','kb-upgrade-a','request','active'),
		       ('op-upgrade-b','credential',7,'instance','binding-upgrade-b','kb-upgrade-b',
		        'directory','guid','user','existing','B','kb-upgrade-b','request','active')`,
		`INSERT INTO nextcloud_source_pairings
		 (operation_id,tenant_id,knowledge_base_id,datasource_id,nextcloud_instance_id,
		  binding_id,datasource_base_url,datasource_config_sha256,publication_epoch,key_id,state)
		 VALUES('pair-upgrade-a',7,'kb-upgrade-a','ds-upgrade-a','instance','binding-upgrade-a',
		        'https://nextcloud.example','hash-a',1,'key-a','active'),
		       ('pair-upgrade-b',7,'kb-upgrade-b','ds-upgrade-b','instance','binding-upgrade-b',
		        'https://nextcloud.example','hash-b',1,'key-b','active')`,
		`INSERT INTO nextcloud_source_decommissions
		 (operation_id,pair_operation_id,tenant_id,knowledge_base_id,datasource_id,
		  nextcloud_instance_id,binding_id,publication_epoch,key_id,state)
		 VALUES('decommission-upgrade','pair-upgrade-a',7,'kb-upgrade-a','ds-upgrade-a',
		        'instance','binding-upgrade-a',1,'key-a','prepared')`,
		`INSERT INTO nextcloud_indexed_withdrawals
		 (operation_id,pair_operation_id,tenant_id,knowledge_base_id,datasource_id,
		  nextcloud_instance_id,binding_id,publication_epoch,key_id,state)
		 VALUES('withdrawal-upgrade','pair-upgrade-b',7,'kb-upgrade-b','ds-upgrade-b',
		        'instance','binding-upgrade-b',1,'key-b','withdrawn_inventory_incomplete')`,
	}
	for _, statement := range statements {
		_, err := db.Exec(statement)
		require.NoError(t, err)
	}

	chdirAndRestore(t, repoRoot)
	require.NoError(t, RunMigrationsWithOptions(
		"sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath},
	))
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, version)
	require.False(t, dirty)

	for table, identity := range map[string][2]string{
		"nextcloud_onboarding_operations": {"operation_id", "op-upgrade-a"},
		"nextcloud_source_pairings":       {"operation_id", "pair-upgrade-a"},
		"nextcloud_source_decommissions":  {"operation_id", "decommission-upgrade"},
		"nextcloud_indexed_withdrawals":   {"operation_id", "withdrawal-upgrade"},
	} {
		var count int
		require.NoError(t, db.QueryRow(
			"SELECT COUNT(*) FROM "+table+" WHERE "+identity[0]+"=?", identity[1],
		).Scan(&count))
		require.Equal(t, 1, count, table)
	}
}
