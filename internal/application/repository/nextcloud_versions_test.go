package repository

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	nextcloudconnector "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newNextcloudVersionTestRepo(t *testing.T) (*knowledgeRepository, *gorm.DB) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}, &nextcloudSourceVersion{},
		&types.DataSource{}, &NextcloudSourcePairing{}, &nextcloudCandidateRetryJob{}))
	leaseSchema, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(leaseSchema)).Error)
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
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	require.NoError(t, db.Create(&NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds",
		InstanceID: "instance", BindingID: bindingID, BaseURL: baseURL,
		ConfigSHA: configSHA, State: "active",
	}).Error)
	return &knowledgeRepository{
		db: db,
		nextcloudPublicationCheck: func(context.Context, *types.DataSourceConfig,
			string, string, int64, string, string,
		) error {
			return nil
		},
	}, db
}

func insertNextcloudVersionTestKnowledge(t *testing.T, db *gorm.DB, id, etag, status, enabled string) *types.Knowledge {
	t.Helper()
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "ds", "external_id": "nextcloud:instance:77",
		"nextcloud_instance_id": "instance", "nextcloud_binding_id": "binding",
		"nextcloud_file_id": "77", "nextcloud_etag": etag,
		"source_resource_id": "binding", "nextcloud_path": "file.md",
	})
	require.NoError(t, err)
	k := &types.Knowledge{
		ID: id, TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud,
		Type: "file", ParseStatus: status, EnableStatus: enabled, Metadata: types.JSON(metadata),
	}
	require.NoError(t, db.Create(k).Error)
	return k
}

func reloadedNextcloudETag(t *testing.T, db *gorm.DB, id string) string {
	t.Helper()
	var k types.Knowledge
	require.NoError(t, db.Unscoped().Where("id = ?", id).Take(&k).Error)
	return k.GetMetadata()["nextcloud_etag"]
}

func TestNextcloudVersionPublishesOnlyCompletedDesiredCandidate(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	old := insertNextcloudVersionTestKnowledge(t, db, "old", "old-etag", types.ParseStatusCompleted, "enabled")
	newer := insertNextcloudVersionTestKnowledge(t, db, "new", "", types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "new-etag", newer.ID))
	require.Empty(t, reloadedNextcloudETag(t, db, old.ID), "old row is retained but no longer published")
	require.Empty(t, reloadedNextcloudETag(t, db, newer.ID))
	published, err := repo.PublishNextcloudVersion(ctx, newer.ID)
	require.NoError(t, err)
	require.False(t, published, "pending candidate must remain hidden")

	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", newer.ID).
		Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "enable_status": "enabled"}).Error)
	published, err = repo.PublishNextcloudVersion(ctx, newer.ID)
	require.NoError(t, err)
	require.True(t, published)
	require.Equal(t, "new-etag", reloadedNextcloudETag(t, db, newer.ID))
	require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
}

func TestNextcloudPlainTextVersionRequiresRetrievableChunkBeforePublication(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	candidate := insertNextcloudVersionTestKnowledge(t, db, "candidate", "", types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(candidate).Update("file_type", "md").Error)
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-77", candidate.ID))
	publish := func() bool {
		published, err := repo.PublishNextcloudVersion(ctx, candidate.ID)
		require.NoError(t, err)
		return published
	}
	require.False(t, publish(), "completed parsing with zero chunks cannot publish text")

	chunk := &types.Chunk{
		ID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: candidate.ID, ChunkType: types.ChunkTypeParentText,
		Content: "parent only", IsEnabled: true, IndexStatus: "ready",
	}
	require.NoError(t, db.Create(chunk).Error)
	require.False(t, publish(), "parent chunks are context, not retrievable text")
	require.NoError(t, db.Model(chunk).Updates(map[string]any{
		"chunk_type": types.ChunkTypeText, "index_status": "failed",
	}).Error)
	require.False(t, publish(), "failed chunk indexing does not make text ready")
	require.NoError(t, db.Model(chunk).Updates(map[string]any{
		"index_status": "ready", "content": "   ",
	}).Error)
	require.False(t, publish(), "whitespace-only content is not retrievable")
	require.NoError(t, db.Model(chunk).Update("content", "current text").Error)
	require.True(t, publish())
	require.Equal(t, "etag-77", reloadedNextcloudETag(t, db, candidate.ID))
}

func TestNextcloudFailedCandidateSameETagGetsNewGeneration(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	old := insertNextcloudVersionTestKnowledge(t, db, "old", "old-etag", types.ParseStatusCompleted, "enabled")
	first := insertNextcloudVersionTestKnowledge(t, db, "first", "", types.ParseStatusPending, "disabled")
	for _, id := range []string{old.ID, first.ID} {
		require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", id).
			Updates(map[string]any{"file_hash": "same-body", "file_type": "md"}).Error)
	}
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", first.ID))
	var firstIntent nextcloudSourceVersion
	require.NoError(t, db.First(&firstIntent).Error)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", first.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	claims, err := NewNextcloudEventInboxRepository(db).CandidateRetryClaims(ctx, time.Now().UTC().Add(2*time.Minute))
	require.NoError(t, err)
	require.Len(t, claims, 1)
	retryCtx := WithNextcloudCandidateRetryClaim(ctx, claims[0])
	exists, duplicate, err := repo.CheckKnowledgeExists(ctx, 7, "kb", &types.KnowledgeCheckParams{
		Type: "file", FileHash: "same-body", FileType: "md", DataSourceID: "ds",
		ExternalID: "nextcloud:instance:77", NextcloudTargetETag: "retry-etag",
	})
	require.NoError(t, err)
	require.False(t, exists, "failed current and completed retired rows cannot be reused")
	require.Nil(t, duplicate)
	second := insertNextcloudVersionTestKnowledge(t, db, "second", "", types.ParseStatusPending, "disabled")
	require.NotEqual(t, first.ID, second.ID)
	require.ErrorIs(t, repo.AdmitNextcloudCandidateRetry(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag"), ErrNextcloudCandidateRetryNotDue)
	require.ErrorIs(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", second.ID), ErrNextcloudCandidateRetryNotDue)
	var held nextcloudSourceVersion
	require.NoError(t, db.First(&held).Error)
	require.Equal(t, first.ID, held.CandidateKnowledgeID)
	var heldJob nextcloudCandidateRetryJob
	require.NoError(t, db.First(&heldJob).Error)
	require.Equal(t, 1, heldJob.AttemptCount)
	require.NoError(t, repo.AdmitNextcloudCandidateRetry(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag"))
	require.NoError(t, repo.StageNextcloudVersion(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", second.ID))
	var current nextcloudSourceVersion
	require.NoError(t, db.First(&current).Error)
	require.Equal(t, firstIntent.UpdatedAt, current.UpdatedAt,
		"retry deadline remains anchored to first staging time")
	require.Equal(t, second.ID, current.CandidateKnowledgeID)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", first.ID).
		Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "enable_status": "enabled"}).Error)
	published, err := repo.PublishNextcloudVersion(ctx, first.ID)
	require.NoError(t, err)
	require.False(t, published, "late completion of failed generation cannot publish")
	require.Empty(t, reloadedNextcloudETag(t, db, first.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", second.ID).
		Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "enable_status": "enabled"}).Error)
	published, err = repo.PublishNextcloudVersion(ctx, second.ID)
	require.NoError(t, err)
	require.True(t, published)
	require.Equal(t, "retry-etag", reloadedNextcloudETag(t, db, second.ID))
}

func TestNextcloudRetryClaimRevokedBySameBindingKeyRotation(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&nextcloudCandidateRetryJob{}, &types.SyncLog{},
		&NextcloudSourceRotation{}))
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, tenant_id INTEGER, ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 1,
		deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id, ever_had_nextcloud_source)
		VALUES ('kb', 7, 1)`).Error)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).
		Where("datasource_id = ?", "ds").Update("key_id", "pair_test").Error)
	failed := insertNextcloudVersionTestKnowledge(t, db, "before-rotation", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", failed.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", failed.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	scheduler := NewNextcloudEventInboxRepository(db)
	claims, err := scheduler.CandidateRetryClaims(ctx, time.Now().UTC().Add(2*time.Minute))
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.NoError(t, scheduler.ValidateCandidateRetryTask(ctx, claims[0], true))
	var pair NextcloudSourcePairing
	require.NoError(t, db.Where("datasource_id = ?", "ds").Take(&pair).Error)
	rotations := NewNextcloudSourcePairingRepository(db)
	rotation, created, err := rotations.PrepareSourceRotation(ctx, NextcloudSourceRotation{
		OperationID: uuid.NewString(), PairOperationID: pair.OperationID,
		TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds",
		InstanceID: "instance", BindingID: "binding", NewKeyID: "pair_rotated",
	}, "rotated-machine-token")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, rotations.SwitchSourceRotation(ctx, rotation))
	require.ErrorIs(t, scheduler.ValidateCandidateRetryTask(ctx, claims[0], true), ErrNextcloudEventScope)
	oldClaimCtx := WithNextcloudCandidateRetryClaim(ctx, claims[0])
	require.ErrorIs(t, repo.AdmitNextcloudCandidateRetry(oldClaimCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag"), ErrNextcloudCandidateRetryNotDue)
	newCandidate := insertNextcloudVersionTestKnowledge(t, db, "after-rotation", "",
		types.ParseStatusPending, "disabled")
	require.ErrorIs(t, repo.StageNextcloudVersion(oldClaimCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", newCandidate.ID), ErrNextcloudCandidateRetryNotDue)
	require.ErrorIs(t, repo.AdmitNextcloudCandidateRetry(oldClaimCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "changed-etag"), ErrNextcloudCandidateRetryNotDue)
	require.ErrorIs(t, repo.StageNextcloudVersion(oldClaimCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "changed-etag", newCandidate.ID), ErrNextcloudCandidateRetryNotDue)
	var current nextcloudSourceVersion
	require.NoError(t, db.First(&current).Error)
	require.Equal(t, failed.ID, current.CandidateKnowledgeID)
	require.Equal(t, "retry-etag", current.DesiredETag)
}

func TestNextcloudRetryClaimAllowsChangedETagOnlyWhileClaimedFailureCurrent(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	failed := insertNextcloudVersionTestKnowledge(t, db, "failed-a", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-a", failed.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", failed.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	claims, err := NewNextcloudEventInboxRepository(db).CandidateRetryClaims(ctx, time.Now().UTC().Add(2*time.Minute))
	require.NoError(t, err)
	require.Len(t, claims, 1)
	retryCtx := WithNextcloudCandidateRetryClaim(ctx, claims[0])
	require.NoError(t, repo.AdmitNextcloudCandidateRetry(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-b"), "a changed live ETag may replace claimed A")
	newer := insertNextcloudVersionTestKnowledge(t, db, "changed-b", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-b", newer.ID))
	var current nextcloudSourceVersion
	require.NoError(t, db.First(&current).Error)
	require.Equal(t, newer.ID, current.CandidateKnowledgeID)
	require.Equal(t, "etag-b", current.DesiredETag)

	// The old queue task still carries A's lease. A delayed second Emit cannot
	// overwrite the newer candidate even if it fetched yet another ETag.
	stale := insertNextcloudVersionTestKnowledge(t, db, "stale-c", "",
		types.ParseStatusPending, "disabled")
	require.ErrorIs(t, repo.AdmitNextcloudCandidateRetry(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-c"), ErrNextcloudCandidateRetryNotDue)
	require.ErrorIs(t, repo.StageNextcloudVersion(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-c", stale.ID), ErrNextcloudCandidateRetryNotDue)
	require.NoError(t, db.First(&current).Error)
	require.Equal(t, newer.ID, current.CandidateKnowledgeID)
	require.Equal(t, "etag-b", current.DesiredETag)
}

func TestNextcloudRetryClaimCannotReplaceNewVersionFromOrdinarySync(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	failed := insertNextcloudVersionTestKnowledge(t, db, "failed-a", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-a", failed.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", failed.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	claims, err := NewNextcloudEventInboxRepository(db).CandidateRetryClaims(ctx, time.Now().UTC().Add(2*time.Minute))
	require.NoError(t, err)
	require.Len(t, claims, 1)
	retryCtx := WithNextcloudCandidateRetryClaim(ctx, claims[0])
	newer := insertNextcloudVersionTestKnowledge(t, db, "ordinary-b", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-b", newer.ID))
	stale := insertNextcloudVersionTestKnowledge(t, db, "stale-c", "",
		types.ParseStatusPending, "disabled")
	require.ErrorIs(t, repo.AdmitNextcloudCandidateRetry(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-c"), ErrNextcloudCandidateRetryNotDue)
	require.ErrorIs(t, repo.StageNextcloudVersion(retryCtx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-c", stale.ID), ErrNextcloudCandidateRetryNotDue)
	var current nextcloudSourceVersion
	require.NoError(t, db.First(&current).Error)
	require.Equal(t, newer.ID, current.CandidateKnowledgeID)
	require.Equal(t, "etag-b", current.DesiredETag)
}

func TestNextcloudEventAppliedAfterFreshManifestWithSettledChanges(t *testing.T) {
	ctx := context.Background()
	_, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases
		(id TEXT PRIMARY KEY, tenant_id INTEGER, deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb', 7)`).Error)
	for _, name := range []string{
		"000032_nextcloud_event_inbox.up.sql",
		"000033_nextcloud_event_dispatch.up.sql",
		"000049_nextcloud_event_hint_etag.up.sql",
	} {
		schema, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(schema)).Error)
	}
	ready := insertNextcloudVersionTestKnowledge(t, db, "event-ready", "etag-v2",
		types.ParseStatusCompleted, "enabled")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", ready.ID).
		Update("file_name", "file.md").Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77", DesiredETag: "etag-v2",
		CandidateKnowledgeID: ready.ID, State: "published", UpdatedAt: time.Now().UTC(),
	}).Error)
	var source types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&source).Error)
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(source.Config)
	require.NoError(t, err)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_connections
		(connection_id, tenant_id, knowledge_base_id, datasource_id, nextcloud_instance_id,
		 binding_id, datasource_base_url, datasource_config_sha256, status,
		 current_key_id, current_secret_ciphertext)
		 VALUES (?, 7, 'kb', 'ds', 'instance', ?, ?, ?, 'active', 'test-key', 'enc:v1:test')`,
		"event-connection", bindingID, baseURL, configSHA).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_checkpoint (connection_id, received_id)
		VALUES ('event-connection', 1)`).Error)
	require.NoError(t, db.Exec(("INSERT INTO nextcloud_event_inbox\n"+
		"\t\t(connection_id, event_id, payload_sha256, event_type,"+
		" file_id, etag, path, relative_path, state)\n"+
		"\t\tVALUES ('event-connection', 1, ?, 'upsert', 77, 'etag"+
		"-v2', '/Published/file.md', 'file.md', 'dispatched')"), strings.Repeat("0", 64)).Error)
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	log := &types.SyncLog{
		ID: "event-log", DataSourceID: "ds", TenantID: 7,
		Status: types.SyncLogStatusSuccess, StartedAt: startedAt, FinishedAt: &now,
	}
	require.NoError(t, db.Create(log).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_dispatch
		(connection_id, dispatched_id, target_event_id, state, last_sync_log_id, next_attempt_at)
		VALUES ('event-connection', 1, 1, 'queued', ?, ?)`,
		log.ID, now.Add(-time.Second)).Error)
	changeBytes, err := json.Marshal([]any{1, bindingID, "1", strings.Repeat("0", 64)})
	require.NoError(t, err)
	changeCursor := base64.RawURLEncoding.EncodeToString(changeBytes)
	setCursor := func(reconciledAt int64) {
		t.Helper()
		cursor := &types.SyncCursor{LastSyncTime: now, ConnectorCursor: map[string]interface{}{
			"instance_id": "instance", "last_reconcile_at": reconciledAt,
			"changes": map[string]interface{}{bindingID: changeCursor},
			"files": map[string]interface{}{bindingID: map[string]interface{}{
				"77": map[string]string{"etag": "etag-v2", "name": "file.md", "path": "file.md"},
			}},
			"missing": map[string]interface{}{}, "tombstones": map[string]interface{}{},
		}}
		raw, err := cursor.ToJSON()
		require.NoError(t, err)
		require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
			Update("last_sync_cursor", raw).Error)
	}
	setCursor(startedAt.Add(-2 * time.Minute).Unix())
	inbox := NewNextcloudEventInboxRepository(db)
	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now)
	require.NoError(t, err)
	require.Nil(t, claim)
	var dispatch nextcloudEventDispatchRow
	require.NoError(t, db.Table(
		"nextcloud_event_dispatch",
	).Where("connection_id = ?", "event-connection").Take(&dispatch).Error)
	require.Equal(t, "deletion_or_cursor_pending", dispatch.LastErrorCode)
	require.Zero(t, dispatch.AppliedID)

	// The same event and already-consumed changes cursor become applicable
	// once this attempt records a complete, fresh metadata manifest.
	setCursor(now.Unix())
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch
		SET state = 'queued', next_attempt_at = ? WHERE connection_id = 'event-connection'`,
		now.Add(-time.Second)).Error)
	claim, err = inbox.ClaimDispatch(ctx, "event-connection", now)
	require.NoError(t, err)
	require.Nil(t, claim)
	require.NoError(t, db.Table(
		"nextcloud_event_dispatch",
	).Where("connection_id = ?", "event-connection").Take(&dispatch).Error)
	require.Equal(t, "idle", dispatch.State)
	require.Equal(t, int64(1), dispatch.AppliedID)
	var receipt nextcloudEventInboxRow
	require.NoError(t, db.Table(
		"nextcloud_event_inbox",
	).Where("connection_id = ? AND event_id = 1", "event-connection").Take(&receipt).Error)
	require.Equal(t, "applied", receipt.State)
}

func TestNextcloudFailedFetchReclaimsWithoutPausingSource(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	failed := insertNextcloudVersionTestKnowledge(t, db, "failed-fetch", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", failed.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", failed.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	scheduler := NewNextcloudEventInboxRepository(db)
	now := time.Now().UTC().Add(2 * time.Minute)
	first, err := scheduler.CandidateRetryClaims(ctx, now)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.NoError(t, db.Create(&types.SyncLog{
		ID:           first[0].SyncLogID,
		DataSourceID: "ds", TenantID: 7, Status: types.SyncLogStatusFailed,
		StartedAt: now, ErrorMessage: "temporary upstream 503",
	}).Error)
	var ds types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&ds).Error)
	require.Equal(t, types.DataSourceStatusActive, ds.Status)
	second, err := scheduler.CandidateRetryClaims(ctx, now.Add(6*time.Minute))
	require.NoError(t, err)
	require.Len(t, second, 1, "a transient fetch failure must not strand the failed candidate")
	require.NotEqual(t, first[0].LeaseToken, second[0].LeaseToken)
	require.Equal(t, failed.ID, second[0].FailedCandidateID)
	var job nextcloudCandidateRetryJob
	require.NoError(t, db.First(&job).Error)
	require.Equal(t, 2, job.AttemptCount)
}

func TestNextcloudRetryScanPassesPausedAndMalformedRows(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	var active types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&active).Error)
	require.NoError(t, db.Create(&types.DataSource{
		ID: "a-paused", TenantID: 7,
		KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusPaused, Config: active.Config,
	}).Error)
	for i := 1; i <= 64; i++ {
		fileID := strconv.Itoa(i)
		paused := insertNextcloudVersionTestKnowledge(t, db, "paused-"+fileID, "",
			types.ParseStatusFailed, "disabled")
		require.NoError(t, db.Create(&nextcloudSourceVersion{
			TenantID: 7, KnowledgeBaseID: "kb",
			DataSourceID: "a-paused", ExternalID: "nextcloud:instance:" + fileID,
			DesiredETag: "etag", CandidateKnowledgeID: paused.ID, State: "staging",
			UpdatedAt: time.Now().UTC().Add(-time.Hour),
		}).Error)
		malformed := insertNextcloudVersionTestKnowledge(t, db, "malformed-"+fileID, "",
			types.ParseStatusFailed, "disabled")
		// Its candidate metadata still names file 77, so this exact file is
		// malformed but must not abort the whole retry scan.
		require.NoError(t, db.Create(&nextcloudSourceVersion{
			TenantID: 7, KnowledgeBaseID: "kb",
			DataSourceID: "ds", ExternalID: "nextcloud:instance:" + fileID,
			DesiredETag: "etag", CandidateKnowledgeID: malformed.ID, State: "staging",
			UpdatedAt: time.Now().UTC().Add(-time.Hour),
		}).Error)
	}
	valid := insertNextcloudVersionTestKnowledge(t, db, "valid-77", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "valid-etag", valid.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", valid.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	claims, err := NewNextcloudEventInboxRepository(db).CandidateRetryClaims(ctx,
		time.Now().UTC().Add(2*time.Minute))
	require.NoError(t, err)
	require.Len(t, claims, 1, "eligible file beyond a full page of malformed rows must be reached")
	require.Equal(t, "nextcloud:instance:77", claims[0].ExternalID)
}

func TestNextcloudRetryTaskLeaseCannotCrossAutomaticDeadline(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	failed := insertNextcloudVersionTestKnowledge(t, db, "deadline-failed", "", types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", failed.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", failed.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	scheduler := NewNextcloudEventInboxRepository(db)
	claims, err := scheduler.CandidateRetryClaims(ctx, time.Now().UTC().Add(2*time.Minute))
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.NoError(t, scheduler.ValidateCandidateRetryTask(ctx, claims[0], true))
	require.NoError(t, db.Model(&nextcloudCandidateRetryJob{}).
		Where("external_id = ?", "nextcloud:instance:77").
		Update("first_staged_at", time.Now().UTC().Add(-24*time.Hour-time.Second)).Error)
	require.ErrorIs(t, scheduler.ValidateCandidateRetryTask(ctx, claims[0], true), ErrNextcloudEventScope,
		"an unexpired lease cannot extend the same-ETag automatic window")
}

func TestNextcloudRetryJobExpiresBeforeItsFutureBackoff(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	failed := insertNextcloudVersionTestKnowledge(t, db, "future-backoff", "", types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", failed.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", failed.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	now := time.Now().UTC()
	require.NoError(t, db.Create(&nextcloudCandidateRetryJob{
		TenantID:        7,
		KnowledgeBaseID: "kb", DataSourceID: "ds", ExternalID: "nextcloud:instance:77",
		DesiredETag: "retry-etag", FailedCandidateID: failed.ID,
		FirstStagedAt: now.Add(-25 * time.Hour), NextAttemptAt: now.Add(time.Hour),
		State: "retry", UpdatedAt: now,
	}).Error)
	claims, err := NewNextcloudEventInboxRepository(db).CandidateRetryClaims(ctx, now)
	require.NoError(t, err)
	require.Empty(t, claims)
	var job nextcloudCandidateRetryJob
	require.NoError(t, db.First(&job).Error)
	require.Equal(t, "manual", job.State)
	require.Equal(t, "candidate_retry_exhausted", job.LastErrorCode)
}

func TestNextcloudCandidateRetryDelayHasStableBoundedJitter(t *testing.T) {
	for attempt := 1; attempt <= 12; attempt++ {
		base := candidateRetryBackoff(attempt)
		delay := candidateRetryDelay("nextcloud:instance:77", "failed-candidate", attempt)
		require.GreaterOrEqual(t, delay, base)
		require.LessOrEqual(t, delay, base+base/5)
		require.Equal(t, delay, candidateRetryDelay("nextcloud:instance:77", "failed-candidate", attempt),
			"retry timing must survive dispatcher restarts")
	}
}

func TestNextcloudFailedCandidateRetryScheduledWithoutEvent(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&nextcloudCandidateRetryJob{}, &types.SyncLog{}))
	first := insertNextcloudVersionTestKnowledge(t, db, "failed-one", "", types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", first.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", first.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	now := time.Now().UTC().Add(2 * time.Minute)
	scheduler := NewNextcloudEventInboxRepository(db)
	claims, err := scheduler.CandidateRetryClaims(ctx, now)
	require.NoError(t, err)
	require.Len(t, claims, 1, "failed staging is discovered with no pending event")
	require.Equal(t, first.ID, claims[0].FailedCandidateID)
	require.NoError(t, scheduler.ValidateCandidateRetryTask(ctx, claims[0], true))
	ordinary, err := repo.FailedNextcloudCandidateETags(ctx, 7, "kb", "ds",
		"instance", "binding", now, "")
	require.NoError(t, err)
	require.Empty(t, ordinary, "ordinary sync cannot consume an uncounted retry")
	due, err := repo.FailedNextcloudCandidateETags(ctx, 7, "kb", "ds",
		"instance", "binding", now, claims[0].LeaseToken)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"77": "retry-etag"}, due)
	second := insertNextcloudVersionTestKnowledge(t, db, "failed-two", "", types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(WithNextcloudCandidateRetryClaim(ctx, claims[0]),
		7, "kb", "ds", "nextcloud:instance:77", "retry-etag", second.ID))
	require.ErrorIs(t, scheduler.ValidateCandidateRetryTask(ctx, claims[0], true), ErrNextcloudEventScope)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", second.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	claims, err = scheduler.CandidateRetryClaims(ctx, now.Add(3*time.Minute))
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.Equal(t, second.ID, claims[0].FailedCandidateID)
	var job nextcloudCandidateRetryJob
	require.NoError(t, db.First(&job).Error)
	require.Equal(t, 2, job.AttemptCount)
	require.Equal(t, "retry-etag", job.DesiredETag)
	// A restart or another failed generation cannot extend the 24-hour budget.
	_, err = scheduler.CandidateRetryClaims(ctx, now.Add(25*time.Hour))
	require.NoError(t, err)
	require.NoError(t, db.First(&job).Error)
	require.Equal(t, "manual", job.State)
	require.Equal(t, "candidate_retry_exhausted", job.LastErrorCode)
	unexpected := insertNextcloudVersionTestKnowledge(t, db, "unexpected", "", types.ParseStatusPending, "disabled")
	require.ErrorIs(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "retry-etag", unexpected.ID), ErrNextcloudCandidateRetryManual)
	var stillCurrent nextcloudSourceVersion
	require.NoError(t, db.First(&stillCurrent).Error)
	require.Equal(t, second.ID, stillCurrent.CandidateKnowledgeID)
	var pair NextcloudSourcePairing
	require.NoError(t, db.Where("datasource_id = ?", "ds").Take(&pair).Error)
	adminAt := now.Add(25*time.Hour + time.Minute)
	status, err := NewNextcloudSourcePairingRepository(db).FailedCandidateRetryStatus(ctx, pair, 77)
	require.NoError(t, err)
	require.Equal(t, "manual", status.State)
	require.Equal(t, "candidate_retry_exhausted", status.LastErrorCode)
	require.ErrorIs(t, NewNextcloudSourcePairingRepository(db).RetryFailedCandidate(ctx,
		pair, 77, "stale-etag", second.ID, adminAt), ErrNextcloudSourcePairingConflict)
	require.NoError(t, NewNextcloudSourcePairingRepository(db).RetryFailedCandidate(ctx,
		pair, 77, "retry-etag", second.ID, adminAt))
	status, err = NewNextcloudSourcePairingRepository(db).FailedCandidateRetryStatus(ctx, pair, 77)
	require.NoError(t, err)
	require.Equal(t, "retry", status.State)
	require.Equal(t, 0, status.AttemptCount)
	claims, err = scheduler.CandidateRetryClaims(ctx, adminAt.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, claims, 1, "administrator restart queues a fresh task")
}

func TestNextcloudRetryLeaseSelectsOnlyItsFailedFile(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	for _, file := range []struct{ id, fileID string }{{"failed-77", "77"}, {"failed-88", "88"}} {
		k := insertNextcloudVersionTestKnowledge(t, db, file.id, "", types.ParseStatusPending, "disabled")
		if file.fileID != "77" {
			metadata := k.GetMetadata()
			metadata["external_id"] = "nextcloud:instance:" + file.fileID
			metadata["nextcloud_file_id"] = file.fileID
			encoded, err := json.Marshal(metadata)
			require.NoError(t, err)
			require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).
				Update("metadata", types.JSON(encoded)).Error)
		}
		require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
			"nextcloud:instance:"+file.fileID, "retry-etag", k.ID))
		require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).
			Update("parse_status", types.ParseStatusFailed).Error)
	}
	now := time.Now().UTC().Add(2 * time.Minute)
	claims, err := NewNextcloudEventInboxRepository(db).CandidateRetryClaims(ctx, now)
	require.NoError(t, err)
	require.Len(t, claims, 1, "only one retry sync may lease a data source at a time")
	require.Equal(t, "nextcloud:instance:77", claims[0].ExternalID)
	selected, err := repo.FailedNextcloudCandidateETags(ctx, 7, "kb", "ds", "instance", "binding",
		now, claims[0].LeaseToken)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"77": "retry-etag"}, selected,
		"a leased retry cannot fetch another due failed file")
}

func TestNextcloudExpiredFailedETagNeedsAdministratorButNewETagStages(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	first := insertNextcloudVersionTestKnowledge(t, db, "failed-old", "", types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "old-etag", first.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", first.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	require.NoError(t, db.Model(&nextcloudSourceVersion{}).
		Where("external_id = ?", "nextcloud:instance:77").
		Update("updated_at", time.Now().UTC().Add(-25*time.Hour)).Error)
	second := insertNextcloudVersionTestKnowledge(t, db, "new-generation", "", types.ParseStatusPending, "disabled")
	require.ErrorIs(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "old-etag", second.ID), ErrNextcloudCandidateRetryManual)
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "new-etag", second.ID))
	var current nextcloudSourceVersion
	require.NoError(t, db.First(&current).Error)
	require.Equal(t, "new-etag", current.DesiredETag)
	require.Equal(t, second.ID, current.CandidateKnowledgeID)
}

func TestNextcloudPublicationRechecksSourceBeforeAndAfterCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo, db := newNextcloudVersionTestRepo(t)
	candidate := insertNextcloudVersionTestKnowledge(t, db, "candidate", "", types.ParseStatusCompleted, "enabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-1", candidate.ID))
	checks := 0
	repo.nextcloudPublicationCheck = func(_ context.Context, source *types.DataSourceConfig,
		instance, binding string, fileID int64, etag, path string,
	) error {
		checks++
		require.Equal(t, []string{"binding"}, source.ResourceIDs)
		require.Equal(t, "instance", instance)
		require.Equal(t, "binding", binding)
		require.Equal(t, int64(77), fileID)
		require.Equal(t, "etag-1", etag)
		require.Equal(t, "file.md", path)
		if checks == 2 {
			cancel() // Cleanup must still hide the marker after task cancellation.
			return errors.New("source was withdrawn during publication")
		}
		return nil
	}
	published, err := repo.PublishNextcloudVersion(ctx, candidate.ID)
	require.Error(t, err)
	require.False(t, published)
	require.Equal(t, 2, checks)
	require.Empty(t, reloadedNextcloudETag(t, db, candidate.ID))
	var version nextcloudSourceVersion
	require.NoError(t, db.First(&version).Error)
	require.Equal(t, "staging", version.State)
}

// The source can change after the first remote check has succeeded but before
// the local publication commit and its final remote check. Exercise the real
// signed HTTP connector against a mutable source, then assert the repository
// has not left either retained version visible or reported a successful publish.
func TestNextcloudPublicationRejectsRemoteChangeBetweenChecks(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withdrawn bool
		newETag   string
	}{
		{name: "publication withdrawn", withdrawn: true},
		{name: "source etag advanced", newETag: "etag-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			repo, db := newNextcloudVersionTestRepo(t)
			var mu sync.Mutex
			checks := 0
			markerAtFinalProbe := false
			publishedAtSource := true
			currentETag := "etag-1"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path !=
					"/index.php/apps/integration_weknora/api/v1/bindings/binding/files/77/publication-check" ||
					r.Header.Get("Authorization") != "Bearer test-machine-token" ||
					r.Header.Get("X-WeKnora-Signature") == "" {
					t.Errorf("unexpected publication request: %s %s", r.Method, r.URL.String())
					w.WriteHeader(http.StatusForbidden)
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read publication request: %v", err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var requested struct {
					InstanceID string `json:"instance_id"`
					ETag       string `json:"etag"`
					Path       string `json:"path"`
				}
				if err := json.Unmarshal(body, &requested); err != nil || requested.InstanceID != "instance" ||
					requested.ETag != "etag-1" || requested.Path != "file.md" {
					t.Errorf("unexpected publication identity: %+v, %v", requested, err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				checks++
				if checks == 2 {
					// The local commit precedes this final source probe. Record the
					// short-lived SQL marker explicitly; read paths must still use
					// the independent live source publication guard.
					var row types.Knowledge
					if err := db.Where("id = ?", "candidate").Take(&row).Error; err != nil {
						t.Errorf("load candidate during final source probe: %v", err)
					} else {
						markerAtFinalProbe = row.GetMetadata()["nextcloud_etag"] == "etag-1"
					}
				}
				if !publishedAtSource || currentETag != requested.ETag {
					w.WriteHeader(http.StatusConflict)
					return
				}
				w.WriteHeader(http.StatusNoContent)
				if checks == 1 {
					// The first check has accepted the version; the remote source
					// changes before the repository reaches its final check.
					if tc.withdrawn {
						publishedAtSource = false
					} else {
						currentETag = tc.newETag
					}
				}
			}))
			defer server.Close()
			t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
			t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
			config, err := (&types.DataSourceConfig{
				Type:        types.ConnectorTypeNextcloud,
				ResourceIDs: []string{"binding"},
				Settings:    map[string]interface{}{"base_url": server.URL},
				Credentials: map[string]interface{}{"token": "test-machine-token", "key_id": "pair_test"},
			}).ToJSON()
			require.NoError(t, err)
			require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
				Update("config", config).Error)
			baseURL, configSHA, _, err := NextcloudEventDataSourceIdentity(config)
			require.NoError(t, err)
			require.NoError(t, db.Model(&NextcloudSourcePairing{}).Where("datasource_id = ?", "ds").
				Updates(map[string]any{"datasource_base_url": baseURL, "datasource_config_sha256": configSHA}).Error)
			repo.nextcloudPublicationCheck = nextcloudconnector.CheckCurrentPublication

			old := insertNextcloudVersionTestKnowledge(t, db, "old", "old-etag",
				types.ParseStatusCompleted, "enabled")
			candidate := insertNextcloudVersionTestKnowledge(t, db, "candidate", "",
				types.ParseStatusCompleted, "enabled")
			require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
				"nextcloud:instance:77", "etag-1", candidate.ID))
			require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
			published, err := repo.PublishNextcloudVersion(ctx, candidate.ID)
			require.ErrorContains(t, err, "source publication changed after commit")
			require.False(t, published)
			mu.Lock()
			observedChecks, observedMarker := checks, markerAtFinalProbe
			mu.Unlock()
			require.Equal(t, 2, observedChecks, "both exact source checks must run")
			t.Logf("transient SQL publication marker during final source probe: %t", observedMarker)
			require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
			require.Empty(t, reloadedNextcloudETag(t, db, candidate.ID))
			var version nextcloudSourceVersion
			require.NoError(t, db.First(&version).Error)
			require.Equal(t, "staging", version.State)
			var visible int64
			require.NoError(t, db.Model(&types.Knowledge{}).
				Where("id IN ?", []string{old.ID, candidate.ID}).
				Where("metadata->>'nextcloud_etag' != ''").Count(&visible).Error)
			require.Zero(t, visible, "no retained source candidate may remain visible")
		})
	}
}

func TestNextcloudPublicationFailsClosedBeforeCommit(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	candidate := insertNextcloudVersionTestKnowledge(t, db, "candidate", "", types.ParseStatusCompleted, "enabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-1", candidate.ID))
	repo.nextcloudPublicationCheck = func(context.Context, *types.DataSourceConfig,
		string, string, int64, string, string,
	) error {
		return errors.New("source unavailable")
	}
	published, err := repo.PublishNextcloudVersion(ctx, candidate.ID)
	require.Error(t, err)
	require.False(t, published)
	require.Empty(t, reloadedNextcloudETag(t, db, candidate.ID))
}

func TestNextcloudPublicationClosesAfterConcurrentPairRetirement(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	candidate := insertNextcloudVersionTestKnowledge(t, db, "candidate", "", types.ParseStatusCompleted, "enabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-1", candidate.ID))
	checks := 0
	repo.nextcloudPublicationCheck = func(context.Context, *types.DataSourceConfig,
		string, string, int64, string, string,
	) error {
		checks++
		if checks == 2 {
			require.NoError(t, db.Model(&NextcloudSourcePairing{}).
				Where("datasource_id = ?", "ds").Update("state", "retired").Error)
		}
		return nil
	}
	published, err := repo.PublishNextcloudVersion(ctx, candidate.ID)
	require.Error(t, err)
	require.False(t, published)
	require.Equal(t, 2, checks)
	require.Empty(t, reloadedNextcloudETag(t, db, candidate.ID))
}

func TestNextcloudPublicationRejectsConcurrentStageAndPairClosure(t *testing.T) {
	for _, concurrentChange := range []string{"new candidate", "pair closed"} {
		t.Run(concurrentChange, func(t *testing.T) {
			ctx := context.Background()
			repo, db := newNextcloudVersionTestRepo(t)
			old := insertNextcloudVersionTestKnowledge(t, db, "old", "", types.ParseStatusCompleted, "enabled")
			require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
				"nextcloud:instance:77", "old-etag", old.ID))
			checks := 0
			repo.nextcloudPublicationCheck = func(context.Context, *types.DataSourceConfig,
				string, string, int64, string, string,
			) error {
				checks++
				if concurrentChange == "new candidate" {
					newer := insertNextcloudVersionTestKnowledge(t, db, "new", "", types.ParseStatusPending, "disabled")
					require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds",
						"nextcloud:instance:77", "new-etag", newer.ID))
				} else {
					require.NoError(t, db.Model(&NextcloudSourcePairing{}).
						Where("datasource_id = ?", "ds").Update("state", "retired").Error)
				}
				return nil
			}
			published, err := repo.PublishNextcloudVersion(ctx, old.ID)
			if concurrentChange == "pair closed" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.False(t, published)
			require.Equal(t, 1, checks)
			require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
		})
	}
}

func TestNextcloudFailedParseLeavesOldBytesRetainedAndBothVersionsHidden(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	old := insertNextcloudVersionTestKnowledge(t, db, "old", "old-etag", types.ParseStatusCompleted, "enabled")
	newer := insertNextcloudVersionTestKnowledge(t, db, "new", "", types.ParseStatusPending, "disabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "new-etag", newer.ID))
	require.NoError(t, repo.UpdateKnowledgeColumns(ctx, newer.ID,
		map[string]interface{}{"parse_status": types.ParseStatusFailed, "error_message": "parse failed"}))
	published, err := repo.PublishNextcloudVersion(ctx, newer.ID)
	require.NoError(t, err)
	require.False(t, published)
	require.Empty(t, reloadedNextcloudETag(t, db, newer.ID))
	require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
	var count int64
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id IN ?", []string{old.ID, newer.ID}).Count(&count).Error)
	require.Equal(t, int64(2), count, "failed parse must not destroy either recoverable version")
}

func TestNextcloudLateOldSaveCannotRestorePublication(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	old := insertNextcloudVersionTestKnowledge(t, db, "old", "old-etag", types.ParseStatusCompleted, "enabled")
	newer := insertNextcloudVersionTestKnowledge(t, db, "new", "", types.ParseStatusPending, "disabled")
	stale := *old // worker snapshot taken before the new version was staged
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "new-etag", newer.ID))
	require.NoError(t, repo.UpdateKnowledge(ctx, &stale))
	require.Empty(t, reloadedNextcloudETag(t, db, old.ID))
	published, err := repo.PublishNextcloudVersion(ctx, old.ID)
	require.NoError(t, err)
	require.False(t, published)
	require.NoError(t, repo.UpdateKnowledgeColumns(ctx, newer.ID,
		map[string]interface{}{"parse_status": types.ParseStatusCompleted, "enable_status": "enabled"}))
	require.Equal(t, "new-etag", reloadedNextcloudETag(t, db, newer.ID))
}

func TestNextcloudRenameReusesCompletedCandidateAndSurvivesLateParserSave(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	k := insertNextcloudVersionTestKnowledge(t, db, "renamed", "", types.ParseStatusCompleted, "enabled")
	oldMetadata := k.GetMetadata()
	oldMetadata["source_resource_id"] = "binding"
	oldMetadata["nextcloud_path"] = "old.md"
	oldMetadata["nextcloud_target_etag"] = "etag-1"
	encoded, err := json.Marshal(oldMetadata)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).
		Updates(map[string]any{"file_name": "old.md", "title": "old.md", "metadata": types.JSON(encoded)}).Error)
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "etag-1", k.ID))
	_, err = repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	var stale types.Knowledge
	require.NoError(t, db.Where("id = ?", k.ID).Take(&stale).Error)

	source := map[string]string{
		"datasource_id": "ds", "external_id": "nextcloud:instance:77", "source_resource_id": "binding",
		"nextcloud_instance_id": "instance", "nextcloud_binding_id": "binding", "nextcloud_file_id": "77",
		"nextcloud_etag": "", "nextcloud_target_etag": "etag-2", "nextcloud_path": "new.md",
		"source_updated_at": "2026-09-23T00:00:00Z",
	}
	require.NoError(t, repo.StageNextcloudVersionWithSource(ctx, 7, "kb", "ds", "nextcloud:instance:77",
		"etag-2", k.ID, "new.md", source))
	require.Empty(t, reloadedNextcloudETag(t, db, k.ID), "candidate stays hidden until publication")
	published, err := repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published, "completed bytes are reused without parsing")
	stale.Description = "late parser write"
	require.NoError(t, repo.UpdateKnowledge(ctx, &stale))
	var current types.Knowledge
	require.NoError(t, db.Where("id = ?", k.ID).Take(&current).Error)
	require.Equal(t, "new.md", current.FileName)
	require.Equal(t, "new.md", current.Title)
	require.Equal(t, "new.md", current.GetMetadata()["nextcloud_path"])
	require.Equal(t, "etag-2", current.GetMetadata()["nextcloud_etag"])
	require.Equal(t, "2026-09-23T00:00:00Z", current.GetMetadata()["source_updated_at"])
	require.Equal(t, "late parser write", current.Description)
}

func TestNextcloudTombstonePersistsAndBlocksLateCompletion(t *testing.T) {
	ctx := context.Background()
	repo, db := newNextcloudVersionTestRepo(t)
	k := insertNextcloudVersionTestKnowledge(t, db, "old", "old-etag", types.ParseStatusCompleted, "enabled")
	require.NoError(t, repo.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77", "old-etag", k.ID))
	_, err := repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	stale := *k
	require.NoError(t, repo.TombstoneNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:77"))
	require.Empty(t, reloadedNextcloudETag(t, db, k.ID))
	require.NoError(t, repo.UpdateKnowledge(ctx, &stale))
	require.Empty(t, reloadedNextcloudETag(t, db, k.ID))
	published, err := repo.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.False(t, published)
	var version nextcloudSourceVersion
	require.NoError(t, db.First(&version).Error)
	require.Equal(t, "tombstone", version.State)
	var count int64
	require.NoError(t, db.Unscoped().Model(&types.Knowledge{}).Where("id = ?", k.ID).Count(&count).Error)
	require.Equal(t, int64(1), count, "tombstone must not destroy the recoverable row")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", k.ID).Count(&count).Error)
	require.Zero(t, count, "tombstoned row must disappear from ordinary listings")
}

func TestSQLiteNextcloudVersionMigrationCreatesDurableUniqueCandidate(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	script, err := os.ReadFile("../../../migrations/sqlite/000031_nextcloud_source_versions.up.sql")
	require.NoError(t, err)
	for _, statement := range strings.Split(string(script), ";\n\n") {
		statement = strings.TrimSpace(statement)
		if statement != "" {
			require.NoError(t, db.Exec(statement).Error)
		}
	}
	first := nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds",
		ExternalID: "one", DesiredETag: "v1", CandidateKnowledgeID: "candidate", State: "staging",
	}
	require.NoError(t, db.Create(&first).Error)
	second := first
	second.ExternalID = "two"
	require.Error(t, db.Create(&second).Error, "one knowledge row cannot be desired by two source files")
	second.CandidateKnowledgeID = ""
	second.State = "tombstone"
	require.NoError(t, db.Create(&second).Error)
}
