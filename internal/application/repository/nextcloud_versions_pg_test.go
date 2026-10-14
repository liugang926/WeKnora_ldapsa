package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func nextcloudVersionPGTestRepo(t *testing.T) (*knowledgeRepository, *gorm.DB) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := leaseTestPostgres(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.DataSource{},
		&NextcloudSourcePairing{}))
	script, err := os.ReadFile("../../../migrations/versioned/000112_nextcloud_source_versions.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	config, err := (&types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{"binding"},
		Settings:    map[string]interface{}{"base_url": "https://nextcloud.example"},
		Credentials: map[string]interface{}{"token": "test-machine-token", "key_id": "pair_test"},
	}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds", TenantID: 7, KnowledgeBaseID: "kb",
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: config,
	}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Where("id = ?", ds.ID).Take(ds).Error)
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	require.NoError(t, db.Create(&NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds",
		InstanceID: "instance", BindingID: bindingID, BaseURL: baseURL,
		ConfigSHA: configSHA, State: "active",
	}).Error)
	return &knowledgeRepository{db: db, nextcloudPublicationCheck: func(context.Context,
		*types.DataSourceConfig, string, string, int64, string, string,
	) error {
		return nil
	}}, db
}

func nextcloudVersionPGFenceState(t *testing.T, db *gorm.DB, id string) string {
	t.Helper()
	var fence nextcloudContentFence
	require.NoError(t, db.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
		7, "kb", id).Take(&fence).Error)
	return fence.State
}

func TestPostgresNextcloudEarlyWorkerWaitsForStage(t *testing.T) {
	repo, db := nextcloudVersionPGTestRepo(t)
	ctx := context.Background()
	candidate := insertNextcloudVersionTestKnowledge(t, db, "new", "",
		types.ParseStatusPending, "disabled")
	scope := NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: candidate.ID, DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
	}
	store := NewNextcloudContentLeaseStore(db)
	lease, err := store.AcquireKnowledge(ctx, scope, NextcloudContentBuildLease,
		"early-parser", time.Minute)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.ReleaseLease(ctx, lease.ID)) }()
	require.ErrorIs(t, store.WaitForCurrentSourceVersion(ctx, scope, lease,
		"new-etag", 150*time.Millisecond), ErrNextcloudContentLeaseDenied,
		"an unstaged candidate must not enter the parser when Stage never commits")
	admitted := make(chan error, 1)
	go func() {
		admitted <- store.WaitForCurrentSourceVersion(ctx, scope, lease, "new-etag", 5*time.Second)
	}()
	select {
	case err := <-admitted:
		t.Fatalf("parser passed admission before Stage committed: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "new-etag", candidate.ID))
	select {
	case err := <-admitted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("parser did not resume after Stage")
	}
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return store.ValidateBuildLeaseInTx(ctx, tx, lease, scope)
	}))
}

func TestPostgresNextcloudEarlyWorkerStopsOnTombstone(t *testing.T) {
	repo, db := nextcloudVersionPGTestRepo(t)
	ctx := context.Background()
	candidate := insertNextcloudVersionTestKnowledge(t, db, "new", "",
		types.ParseStatusPending, "disabled")
	scope := NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: candidate.ID, DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
	}
	store := NewNextcloudContentLeaseStore(db)
	lease, err := store.AcquireKnowledge(ctx, scope, NextcloudContentBuildLease,
		"early-parser", time.Minute)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.ReleaseLease(ctx, lease.ID)) }()
	admitted := make(chan error, 1)
	go func() {
		admitted <- store.WaitForCurrentSourceVersion(ctx, scope, lease, "new-etag", 5*time.Second)
	}()
	select {
	case err := <-admitted:
		t.Fatalf("parser passed admission before Tombstone: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, repo.TombstoneNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77"))
	select {
	case err := <-admitted:
		require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied)
	case <-time.After(time.Second):
		t.Fatal("retired parser waited for timeout instead of rejecting immediately")
	}
}

func TestPostgresNextcloudStalePublishPreservesNewCandidateFence(t *testing.T) {
	repo, db := nextcloudVersionPGTestRepo(t)
	ctx := context.Background()
	old := insertNextcloudVersionTestKnowledge(t, db, "old", "",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "old-etag", old.ID))
	newer := insertNextcloudVersionTestKnowledge(t, db, "new", "",
		types.ParseStatusPending, "disabled")
	checks := 0
	repo.nextcloudPublicationCheck = func(context.Context, *types.DataSourceConfig,
		string, string, int64, string, string,
	) error {
		checks++
		if checks == 1 {
			// This commits through a second connection after the old candidate's
			// remote probe and before its publication transaction begins.
			return repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
				"nextcloud:instance:77", "new-etag", newer.ID)
		}
		return nil
	}
	published, err := repo.PublishNextcloudVersion(ctx, old.ID)
	require.NoError(t, err)
	require.False(t, published)
	require.Equal(t, 1, checks)
	require.Equal(t, "retired", nextcloudVersionPGFenceState(t, db, old.ID))
	require.Equal(t, "open", nextcloudVersionPGFenceState(t, db, newer.ID))
	_, err = NewNextcloudContentLeaseStore(db).AcquireKnowledge(ctx, NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: old.ID,
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
	}, NextcloudContentBuildLease, "stale-parser", time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied,
		"a displaced worker must fail immediately at its retired exact fence")
	lease, err := NewNextcloudContentLeaseStore(db).AcquireKnowledge(ctx, NextcloudContentScope{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: newer.ID,
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
	}, NextcloudContentBuildLease, "new-parser", time.Minute)
	require.NoError(t, err)
	require.NoError(t, NewNextcloudContentLeaseStore(db).ReleaseLease(ctx, lease.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", newer.ID).
		Updates(map[string]any{
			"parse_status":  types.ParseStatusCompleted,
			"enable_status": "enabled",
		}).Error)
	published, err = repo.PublishNextcloudVersion(ctx, newer.ID)
	require.NoError(t, err)
	require.True(t, published, "the new candidate must remain publishable")
	require.Equal(t, "new-etag", reloadedNextcloudETag(t, db, newer.ID))
	require.Equal(t, "open", nextcloudVersionPGFenceState(t, db, newer.ID))
}

func TestPostgresNextcloudStageThenTombstoneRetiresAllFences(t *testing.T) {
	repo, db := nextcloudVersionPGTestRepo(t)
	ctx := context.Background()
	old := insertNextcloudVersionTestKnowledge(t, db, "old", "",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "old-etag", old.ID))
	newer := insertNextcloudVersionTestKnowledge(t, db, "new", "",
		types.ParseStatusPending, "disabled")
	blocker := db.Begin()
	require.NoError(t, blocker.Error)
	defer blocker.Rollback()
	require.NoError(t, blocker.Exec(`LOCK TABLE nextcloud_source_versions IN ACCESS EXCLUSIVE MODE`).Error)
	staged := make(chan error, 1)
	go func() {
		staged <- repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
			"nextcloud:instance:77", "new-etag", newer.ID)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int64
		require.NoError(t, db.Raw(`SELECT count(*) FROM pg_locks l
			JOIN pg_class c ON c.oid = l.relation
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE NOT l.granted AND n.nspname = current_schema()
			AND c.relname = 'nextcloud_source_versions'`).Scan(&waiting).Error)
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stage did not reach the held source-version table")
		}
		time.Sleep(20 * time.Millisecond)
	}
	tombstoned := make(chan error, 1)
	go func() {
		tombstoned <- repo.TombstoneNextcloudVersion(ctx, 7, "kb", "ds",
			"nextcloud:instance:77")
	}()
	select {
	case err := <-tombstoned:
		t.Fatalf("tombstone crossed the uncommitted stage: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, blocker.Commit().Error)
	select {
	case err := <-staged:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("stage did not finish")
	}
	select {
	case err := <-tombstoned:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("tombstone did not finish")
	}
	require.Equal(t, "retired", nextcloudVersionPGFenceState(t, db, old.ID))
	require.Equal(t, "retired", nextcloudVersionPGFenceState(t, db, newer.ID))
	var version nextcloudSourceVersion
	require.NoError(t, db.First(&version).Error)
	require.Equal(t, "tombstone", version.State)
}
