package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ragSnapshotPostgresFixture(t *testing.T) (*knowledgeRepository, *gorm.DB, []RAGChunkReadRequest) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := leaseTestPostgres(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.KnowledgeBase{}, &types.Chunk{}, &nextcloudSourceVersion{}, &types.DataSource{}, &NextcloudSourcePairing{}, &nextcloudCandidateRetryJob{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "source", EmbeddingModelID: "embed", SummaryModelID: "summary"}).Error)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	migrations, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
	require.NoError(t, err)
	script, err := os.ReadFile("../../../migrations/versioned/000126_nextcloud_source_revisions.up.sql")
	require.NoError(t, err)
	require.NoError(t, migrations.Exec(string(script)).Error)
	config, err := (&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding"}, Settings: map[string]interface{}{"base_url": "https://nextcloud.example"}, Credentials: map[string]interface{}{"token": "test-token", "key_id": "test_key"}}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: config}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Where("id='ds'").Take(ds).Error)
	base, sha, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	require.NoError(t, db.Create(&NextcloudSourcePairing{OperationID: "pair", TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds", InstanceID: "instance", BindingID: binding, BaseURL: base, ConfigSHA: sha, State: "active"}).Error)
	repo := &knowledgeRepository{db: db, nextcloudPublicationCheck: func(context.Context, *types.DataSourceConfig, string, string, int64, string, string) error {
		return nil
	}}
	var requests []RAGChunkReadRequest
	for i, file := range []string{"77", "78"} {
		id := []string{"doc-a", "doc-b"}[i]
		k := insertNextcloudVersionTestKnowledge(t, db, id, "", types.ParseStatusCompleted, "enabled")
		metadata := k.GetMetadata()
		metadata["nextcloud_file_id"] = file
		metadata["external_id"] = "nextcloud:instance:" + file
		encoded, err := json.Marshal(metadata)
		require.NoError(t, err)
		k.Metadata = types.JSON(encoded)
		require.NoError(t, db.Model(k).Update("metadata", k.Metadata).Error)
		chunk := &types.Chunk{ID: "chunk-" + id, TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: id, Content: "actual " + id, ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready"}
		require.NoError(t, db.Create(chunk).Error)
		require.NoError(t, repo.StageNextcloudVersion(context.Background(), 7, "kb", "ds", "nextcloud:instance:"+file, "etag-"+file, id))
		published, err := repo.PublishNextcloudVersion(context.Background(), id)
		require.NoError(t, err)
		require.True(t, published)
		lease, err := NewNextcloudContentLeaseStore(db).AcquireKnowledge(context.Background(), NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: id, DataSourceID: "ds", ExternalID: "nextcloud:instance:" + file}, NextcloudContentReadLease, "reader-"+id, time.Minute)
		require.NoError(t, err)
		requests = append(requests, RAGChunkReadRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: id, ChunkIDs: []string{chunk.ID}, Lease: ragTestReadToken(lease)})
	}
	return repo, db, requests
}

type ragSnapshotPauseKey struct{}

func TestPostgresRAGChunkSnapshotCoherentCaptureAndWithdrawalBeforeRelease(t *testing.T) {
	repo, db, requests := ragSnapshotPostgresFixture(t)
	reader := NewRAGChunkSnapshotReader(db, nil)
	baseline, err := reader.HydrateRAGChunkSnapshots(context.Background(), requests)
	require.NoError(t, err)
	require.Len(t, baseline, 2)
	store := NewNextcloudContentLeaseStore(db)
	fired := false
	callbackName := "rag_snapshot_withdraw_between_documents"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if fired || tx.Statement.Context.Value(ragSnapshotPauseKey{}) != true || tx.Statement.Table != "knowledges" {
			return
		}
		fired = true
		// Another connection commits withdrawal after the first document read.
		require.NoError(t, store.RetireKnowledge(context.Background(), NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc-b", DataSourceID: "ds", ExternalID: "nextcloud:instance:78"}))
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(callbackName) })
	rows, err := reader.HydrateRAGChunkSnapshots(context.WithValue(context.Background(), ragSnapshotPauseKey{}, true), requests)
	require.NoError(t, err)
	require.True(t, fired)
	require.Len(t, rows, 2, "one RR snapshot, not a mixed pair of generations")
	union, err := types.UnionSourceLineage(rows[0].SourceLineage, rows[1].SourceLineage)
	require.NoError(t, err)
	current, err := repo.RecheckNextcloudSourceLineage(context.Background(), union)
	require.NoError(t, err)
	require.False(t, current, "release/model authorization must reject the committed withdrawal")
	_, err = store.RenewLease(context.Background(), requests[1].Lease.ID, time.Minute)
	require.ErrorIs(t, err, ErrNextcloudContentLeaseDenied, "the mandatory lease checkpoint suppresses captured old material")
	rows, err = reader.HydrateRAGChunkSnapshots(context.Background(), requests)
	require.Error(t, err)
	require.Nil(t, rows, "a denied second source cannot return a partial first result")
}

func TestPostgresRAGChunkSnapshotRejectsWeakerNestedTransaction(t *testing.T) {
	_, db, requests := ragSnapshotPostgresFixture(t)
	tx := db.Begin()
	require.NoError(t, tx.Error)
	defer tx.Rollback()
	rows, err := NewRAGChunkSnapshotReader(tx, nil).HydrateRAGChunkSnapshots(context.Background(), requests)
	require.ErrorIs(t, err, ErrNextcloudLineageSnapshotStore)
	require.Nil(t, rows)
}
