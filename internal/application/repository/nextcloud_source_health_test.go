package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func nextcloudHealthFixture(t *testing.T, db *gorm.DB) (NextcloudSourcePairing, time.Time) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.DataSource{},
		&types.SyncLog{}, &nextcloudSourceVersion{}, &NextcloudSourcePairing{}))
	for _, statement := range []string{
		`CREATE TABLE nextcloud_event_connections (connection_id TEXT PRIMARY KEY,
			tenant_id BIGINT, knowledge_base_id TEXT, datasource_id TEXT,
			nextcloud_instance_id TEXT, binding_id TEXT, datasource_base_url TEXT,
			datasource_config_sha256 TEXT, status TEXT, current_key_id TEXT,
			current_secret_ciphertext TEXT, created_at TIMESTAMP)`,
		`CREATE TABLE nextcloud_event_checkpoint (connection_id TEXT PRIMARY KEY, received_id BIGINT)`,
		`CREATE TABLE nextcloud_event_dispatch (connection_id TEXT PRIMARY KEY,
			dispatched_id BIGINT, applied_id BIGINT, state TEXT, last_error_code TEXT,
			last_sync_log_id TEXT)`,
		`CREATE TABLE nextcloud_event_inbox (connection_id TEXT, event_id BIGINT,
			received_at TIMESTAMP, PRIMARY KEY (connection_id, event_id))`,
	} {
		require.NoError(t, db.Exec(statement).Error)
	}

	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	config, err := (&types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{"binding"},
		Settings:    map[string]interface{}{"base_url": "https://private.example.test"},
		Credentials: map[string]interface{}{"token": "secret-machine-token", "key_id": "pair_fixture"},
	}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "health-ds", TenantID: 7, KnowledgeBaseID: "health-kb",
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: config,
	}
	require.NoError(t, db.Create(ds).Error)
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	pair := NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 7,
		KnowledgeBaseID: ds.KnowledgeBaseID, DataSourceID: ds.ID,
		InstanceID: "instance", BindingID: bindingID, BaseURL: baseURL,
		ConfigSHA: configSHA, State: "active",
	}
	require.NoError(t, db.Create(&pair).Error)

	addKnowledge := func(id, status, enabled, knowledgeBase string) {
		require.NoError(t, db.Create(&types.Knowledge{
			ID: id, TenantID: 7,
			KnowledgeBaseID: knowledgeBase, Channel: types.ConnectorTypeNextcloud,
			Type: "file", ParseStatus: status, EnableStatus: enabled,
			Title: "private title", FilePath: "/private/source/path",
		}).Error)
	}
	addVersion := func(externalID, candidateID, state string, updatedAt time.Time) {
		require.NoError(t, db.Create(&nextcloudSourceVersion{
			TenantID:        7,
			KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
			ExternalID: externalID, CandidateKnowledgeID: candidateID,
			State: state, UpdatedAt: updatedAt,
		}).Error)
	}
	addKnowledge("candidate-pending", types.ParseStatusPending, "disabled", pair.KnowledgeBaseID)
	addKnowledge("candidate-failed", types.ParseStatusFailed, "disabled", pair.KnowledgeBaseID)
	addKnowledge("candidate-completed", types.ParseStatusCompleted, "enabled", pair.KnowledgeBaseID)
	addKnowledge("foreign-candidate", types.ParseStatusFailed, "disabled", "another-kb")
	addVersion("nextcloud:instance:1", "candidate-pending", "staging", now.Add(-2*time.Hour))
	addVersion("nextcloud:instance:2", "candidate-failed", "staging", now.Add(-3*time.Hour))
	addVersion("nextcloud:instance:3", "candidate-completed", "published", now.Add(-time.Hour))
	addVersion("nextcloud:instance:4", "", "tombstone", now.Add(-time.Hour))
	addVersion("nextcloud:instance:5", "foreign-candidate", "staging", now.Add(-time.Hour))
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID:        8,
		KnowledgeBaseID: "foreign-kb", DataSourceID: "foreign-ds", ExternalID: "foreign",
		State: "staging", UpdatedAt: now.Add(-24 * time.Hour),
	}).Error)

	for _, log := range []types.SyncLog{
		{
			ID: uuid.NewString(), DataSourceID: ds.ID, TenantID: 7, Status: "running",
			StartedAt: now.Add(-time.Hour), ErrorMessage: "hidden running detail",
		},
		{
			ID: uuid.NewString(), DataSourceID: ds.ID, TenantID: 7, Status: "failed",
			StartedAt: now.Add(-2 * time.Hour), ErrorMessage: "/private/failure secret",
		},
		{
			ID: uuid.NewString(), DataSourceID: ds.ID, TenantID: 7, Status: "partial",
			StartedAt: now.Add(-3 * time.Hour), ErrorMessage: "private partial detail",
		},
		{
			ID: uuid.NewString(), DataSourceID: ds.ID, TenantID: 7, Status: "failed",
			StartedAt: now.Add(-25 * time.Hour),
		},
		{
			ID: uuid.NewString(), DataSourceID: "foreign-ds", TenantID: 8, Status: "failed",
			StartedAt: now.Add(-time.Minute),
		},
	} {
		require.NoError(t, db.Create(&log).Error)
	}
	return pair, now
}

func testNextcloudSourceHealthObservations(t *testing.T, db *gorm.DB) {
	t.Helper()
	pair, now := nextcloudHealthFixture(t, db)
	repo := NewNextcloudSourcePairingRepository(db)
	ctx := context.Background()
	withoutConnection, err := repo.SourceHealth(ctx, pair, now)
	require.NoError(t, err)
	require.Nil(t, withoutConnection.EventInbox)
	require.Equal(t, "active", withoutConnection.SourceStatus)
	require.Equal(t, int64(1), withoutConnection.SyncLogs.RunningCount)
	require.Equal(t, int64(3600), *withoutConnection.SyncLogs.OldestRunningAgeSeconds)
	require.Equal(t, int64(1), withoutConnection.SyncLogs.FailedLast24Hours)
	require.Equal(t, int64(1), withoutConnection.SyncLogs.PartialLast24Hours)
	require.Equal(t, "running", *withoutConnection.SyncLogs.LatestStatus)
	require.Equal(t, int64(3), withoutConnection.CurrentVersions.StagingCount)
	require.Equal(t, int64(1), withoutConnection.CurrentVersions.PublishedCount)
	require.Equal(t, int64(1), withoutConnection.CurrentVersions.TombstoneCount)
	require.Equal(t, int64(1), withoutConnection.CurrentVersions.MissingCandidateCount)
	require.Equal(t, int64(1), withoutConnection.CurrentVersions.ParsePendingCount)
	require.Equal(t, int64(1), withoutConnection.CurrentVersions.ParseFailedCount)
	require.Equal(t, int64(1), withoutConnection.CurrentVersions.ParseCompletedEnabledCount)
	require.Equal(t, int64(10800), *withoutConnection.CurrentVersions.OldestStagingAgeSeconds)

	conn := "health-conn"
	require.NoError(t, db.Table("nextcloud_event_connections").Create(map[string]interface{}{
		"connection_id": conn, "tenant_id": 7, "knowledge_base_id": pair.KnowledgeBaseID,
		"datasource_id": pair.DataSourceID, "nextcloud_instance_id": pair.InstanceID,
		"binding_id": pair.BindingID, "datasource_base_url": pair.BaseURL,
		"datasource_config_sha256": pair.ConfigSHA, "status": "active",
		"current_key_id": "event-key-secret", "current_secret_ciphertext": "enc:v1:private",
		"created_at": now.Add(-time.Hour),
	}).Error)
	require.NoError(t, db.Table("nextcloud_event_checkpoint").Create(map[string]interface{}{
		"connection_id": conn, "received_id": 4,
	}).Error)
	require.NoError(t, db.Table("nextcloud_event_dispatch").Create(map[string]interface{}{
		"connection_id": conn, "dispatched_id": 3, "applied_id": 2,
		"state": "retry", "last_error_code": "publication_unproven",
	}).Error)
	for _, event := range []struct {
		id int64
		at time.Time
	}{{3, now.Add(-40 * time.Minute)}, {4, now.Add(-20 * time.Minute)}} {
		require.NoError(t, db.Table("nextcloud_event_inbox").Create(map[string]interface{}{
			"connection_id": conn, "event_id": event.id, "received_at": event.at,
		}).Error)
	}
	health, err := repo.SourceHealth(ctx, pair, now)
	require.NoError(t, err)
	require.NotNil(t, health.EventInbox)
	require.Equal(t, "4", health.EventInbox.ReceivedThroughEventID)
	require.Equal(t, "3", health.EventInbox.DispatchedThroughEventID)
	require.Equal(t, "2", health.EventInbox.AppliedThroughEventID)
	require.Equal(t, int64(2), health.EventInbox.UnappliedCount)
	require.Equal(t, int64(1), health.EventInbox.UndispatchedCount)
	require.Equal(t, int64(2400), *health.EventInbox.OldestUnappliedAgeSeconds)
	require.Equal(t, "retry", health.EventInbox.DispatchState)
	require.Equal(t, "publication_unproven", health.EventInbox.LastErrorCode)
	require.NoError(t, db.Table("nextcloud_event_dispatch").Where("connection_id = ?", conn).
		Update("last_error_code", "/private/path secret-token").Error)
	health, err = repo.SourceHealth(ctx, pair, now)
	require.NoError(t, err)
	require.Equal(t, "unknown", health.EventInbox.LastErrorCode)
}

func TestSQLiteNextcloudSourceHealthObservations(t *testing.T) {
	// The fixture uses a temporary, isolated in-memory database.
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	testNextcloudSourceHealthObservations(t, db)
}

func TestPostgresNextcloudSourceHealthObservations(t *testing.T) {
	dsn := os.Getenv("WEKNORA_SOURCE_HEALTH_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_SOURCE_HEALTH_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	schema := "source_health_" + uuid.NewString()[:8]
	require.NoError(t, db.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	defer db.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
	require.NoError(t, db.Exec(`SET search_path TO "`+schema+`"`).Error)
	testNextcloudSourceHealthObservations(t, db)
}
