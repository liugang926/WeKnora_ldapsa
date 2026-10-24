package repository

import (
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Migration 000167 referred to NEW.knowledge_base_id in the outer fallback
// condition. PostgreSQL resolves that field even when a sync_logs row enters
// the previous ELSIF, so every Nextcloud sync insert failed with SQLSTATE
// 42703. Replay the real 000167 function first, then prove 000168 repairs it
// without weakening source-scoped virgin or decommission admission.
func TestPostgresNextcloudVirginSyncLogTriggerRepair(t *testing.T) {
	dsn := os.Getenv("WEKNORA_INDEXED_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_INDEXED_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	adminSQL, err := admin.DB()
	require.NoError(t, err)
	defer func() { require.NoError(t, adminSQL.Close()) }()

	schema := "virgin_trigger_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	require.NoError(t, admin.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	defer admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)

	scopedDSN := dsn + " search_path=" + schema
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		query := u.Query()
		query.Set("search_path", schema)
		u.RawQuery = query.Encode()
		scopedDSN = u.String()
	}
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: scopedDSN, PreferSimpleProtocol: true,
	}), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer func() { require.NoError(t, sqlDB.Close()) }()

	for _, statement := range []string{
		`CREATE TABLE nextcloud_source_pairings (
			operation_id TEXT PRIMARY KEY,
			tenant_id BIGINT NOT NULL,
			knowledge_base_id TEXT NOT NULL,
			datasource_id TEXT NOT NULL UNIQUE
		)`,
		`CREATE TABLE nextcloud_source_decommissions (
			pair_operation_id TEXT PRIMARY KEY
		)`,
		`CREATE TABLE nextcloud_source_virgin (
			pair_operation_id TEXT PRIMARY KEY,
			ever_touched BOOLEAN NOT NULL DEFAULT FALSE
		)`,
		`CREATE TABLE sync_logs (
			id TEXT PRIMARY KEY,
			data_source_id TEXT NOT NULL,
			tenant_id BIGINT NOT NULL
		)`,
	} {
		require.NoError(t, db.Exec(statement).Error)
	}

	multisource, err := os.ReadFile(
		"../../../migrations/versioned/000167_nextcloud_multisource.up.sql",
	)
	require.NoError(t, err)
	start := strings.Index(string(multisource), "CREATE OR REPLACE FUNCTION nextcloud_virgin_touch()")
	require.NotEqual(t, -1, start)
	end := strings.Index(string(multisource)[start:], "-- Withdrawal fences")
	require.NotEqual(t, -1, end)
	brokenFunction := strings.TrimSpace(string(multisource)[start : start+end])
	require.NoError(t, db.Exec(brokenFunction).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER nextcloud_virgin_sync_log
		BEFORE INSERT ON sync_logs FOR EACH ROW
		EXECUTE FUNCTION nextcloud_virgin_touch()`).Error)

	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_pairings
		(operation_id, tenant_id, knowledge_base_id, datasource_id) VALUES
		('pair-a', 7, 'shared-kb', 'source-a'),
		('pair-b', 7, 'shared-kb', 'source-b')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_virgin
		(pair_operation_id, ever_touched) VALUES ('pair-a', FALSE), ('pair-b', FALSE)`).Error)

	err = db.Exec(`INSERT INTO sync_logs (id, data_source_id, tenant_id)
		VALUES ('before-fix', 'source-a', 7)`).Error
	require.Error(t, err)
	require.Contains(t, err.Error(), `no field "knowledge_base_id"`)

	repair, err := os.ReadFile(
		"../../../migrations/versioned/000168_nextcloud_fix_sync_virgin_trigger.up.sql",
	)
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(repair)).Error)

	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, tenant_id)
		VALUES ('source-a-log', 'source-a', 7)`).Error)
	var touched []struct {
		PairOperationID string
		EverTouched     bool
	}
	require.NoError(t, db.Table("nextcloud_source_virgin").
		Select("pair_operation_id, ever_touched").
		Order("pair_operation_id").Scan(&touched).Error)
	require.Equal(t, []struct {
		PairOperationID string
		EverTouched     bool
	}{
		{PairOperationID: "pair-a", EverTouched: true},
		{PairOperationID: "pair-b", EverTouched: false},
	}, touched)

	require.NoError(t, db.Exec(
		`INSERT INTO nextcloud_source_decommissions (pair_operation_id) VALUES ('pair-a')`,
	).Error)
	err = db.Exec(`INSERT INTO sync_logs (id, data_source_id, tenant_id)
		VALUES ('decommissioned-log', 'source-a', 7)`).Error
	require.Error(t, err)
	require.Contains(t, err.Error(), "nextcloud_source_decommissioned")
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, tenant_id)
		VALUES ('source-b-log', 'source-b', 7)`).Error,
		"one binding's decommission must not block another source in the shared KB")
	var everTouched bool
	require.NoError(t, db.Table("nextcloud_source_virgin").Select("ever_touched").
		Where("pair_operation_id = ?", "pair-b").Scan(&everTouched).Error)
	require.True(t, everTouched)
}
