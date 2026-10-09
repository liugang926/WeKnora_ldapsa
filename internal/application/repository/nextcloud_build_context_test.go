package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudBuildChunkWriteUsesSameTransactionFence(t *testing.T) {
	db := leaseTestSQLite(t)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}))
	store := NewNextcloudContentLeaseStore(db)
	chunkRepo := &chunkRepository{db: db}
	scope := leaseTestScope("build-chunks")
	lease, err := store.AcquireKnowledge(context.Background(), scope,
		NextcloudContentBuildLease, "worker", time.Minute)
	require.NoError(t, err)
	ctx := WithNextcloudBuildLease(context.Background(), store, lease, scope)
	chunk := &types.Chunk{
		ID: "chunk-1", TenantID: scope.TenantID,
		KnowledgeBaseID: scope.KnowledgeBaseID, KnowledgeID: scope.KnowledgeID,
		Content: "first", ChunkType: types.ChunkTypeText,
	}
	require.NoError(t, chunkRepo.CreateChunks(ctx, []*types.Chunk{chunk}))

	wrong := *chunk
	wrong.ID = "chunk-cross-source"
	wrong.KnowledgeID = "another-doc"
	require.ErrorIs(t, chunkRepo.CreateChunks(ctx, []*types.Chunk{&wrong}),
		ErrNextcloudContentLeaseInvalid)

	require.NoError(t, store.RetireKnowledge(context.Background(), scope))
	second := *chunk
	second.ID = "chunk-after-retire"
	require.ErrorIs(t, chunkRepo.CreateChunks(ctx, []*types.Chunk{&second}),
		ErrNextcloudContentLeaseDenied)
	chunk.Content = "changed"
	require.ErrorIs(t, chunkRepo.UpdateChunk(ctx, chunk), ErrNextcloudContentLeaseDenied)
	require.ErrorIs(t, chunkRepo.DeleteChunksByKnowledgeID(ctx, scope.TenantID, scope.KnowledgeID),
		ErrNextcloudContentLeaseDenied)
	var saved []types.Chunk
	require.NoError(t, db.Find(&saved).Error)
	require.Len(t, saved, 1)
	require.Equal(t, "first", saved[0].Content)
}

func TestNextcloudBuildSaveChunkRevisionRequiresExactLiveLease(t *testing.T) {
	db := leaseTestSQLite(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}, &types.ChunkRevision{}))
	store := NewNextcloudContentLeaseStore(db)
	repo := &chunkRepository{db: db}
	scope := leaseTestScope("source-revision")
	other := leaseTestScope("other-revision")
	for _, item := range []NextcloudContentScope{scope, other} {
		require.NoError(t, db.Create(&types.Knowledge{
			ID: item.KnowledgeID, TenantID: item.TenantID,
			KnowledgeBaseID: item.KnowledgeBaseID, Channel: types.ConnectorTypeNextcloud,
		}).Error)
	}
	lease, err := store.AcquireKnowledge(context.Background(), scope,
		NextcloudContentBuildLease, "worker", time.Minute)
	require.NoError(t, err)
	ctx := WithNextcloudBuildLease(context.Background(), store, lease, scope)
	chunk := &types.Chunk{
		ID: "revision-source-chunk", TenantID: scope.TenantID,
		KnowledgeBaseID: scope.KnowledgeBaseID, KnowledgeID: scope.KnowledgeID,
		Content: "before", ChunkType: types.ChunkTypeText,
	}
	require.NoError(t, repo.CreateChunks(ctx, []*types.Chunk{chunk}))
	readLease, err := store.AcquireKnowledge(context.Background(), scope,
		NextcloudContentReadLease, "reader", time.Minute)
	require.NoError(t, err)

	edit := *chunk
	edit.Content, edit.ContentRevision = "after", 1
	revision := &types.ChunkRevision{
		ID: "revision-before", TenantID: scope.TenantID,
		KnowledgeBaseID: scope.KnowledgeBaseID, KnowledgeID: scope.KnowledgeID,
		ChunkID: chunk.ID, Revision: 0, Content: "before",
	}
	require.ErrorIs(t, repo.SaveChunkRevision(context.Background(), &edit, revision, 0),
		ErrNextcloudContentLeaseInvalid)

	otherLease, err := store.AcquireKnowledge(context.Background(), other,
		NextcloudContentBuildLease, "other-worker", time.Minute)
	require.NoError(t, err)
	otherCtx := WithNextcloudBuildLease(context.Background(), store, otherLease, other)
	forged := edit
	forged.KnowledgeID = other.KnowledgeID
	forgedRevision := *revision
	forgedRevision.ID, forgedRevision.KnowledgeID = "revision-forged", other.KnowledgeID
	require.ErrorIs(t, repo.SaveChunkRevision(otherCtx, &forged, &forgedRevision, 0),
		ErrChunkRevisionConflict)
	require.ErrorIs(t, repo.SaveChunkRevision(ctx, &forged, &forgedRevision, 0),
		ErrNextcloudContentLeaseInvalid)
	badRevision := *revision
	badRevision.ID, badRevision.ChunkID = "revision-wrong-chunk", "other-chunk"
	require.ErrorIs(t, repo.SaveChunkRevision(ctx, &edit, &badRevision, 0),
		ErrChunkRevisionConflict)

	require.NoError(t, repo.SaveChunkRevision(ctx, &edit, revision, 0))
	stored, err := repo.GetChunkByID(context.Background(), scope.TenantID, chunk.ID)
	require.NoError(t, err)
	require.Equal(t, "after", stored.Content)
	require.Equal(t, 1, stored.ContentRevision)
	require.NoError(t, store.RetireKnowledge(context.Background(), scope))
	late := edit
	late.Content, late.ContentRevision = "late", 2
	lateRevision := *revision
	lateRevision.ID, lateRevision.Revision, lateRevision.Content = "revision-late", 1, "after"
	require.ErrorIs(t, repo.SaveChunkRevision(ctx, &late, &lateRevision, 1),
		ErrNextcloudContentLeaseDenied)
	stored, err = repo.GetChunkByID(context.Background(), scope.TenantID, chunk.ID)
	require.NoError(t, err)
	require.Equal(t, "after", stored.Content)
	require.Equal(t, 1, stored.ContentRevision)
	var count int64
	require.NoError(t, db.Model(&types.ChunkRevision{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
	require.NoError(t, store.ReleaseLease(context.Background(), readLease.ID))
}

func TestNextcloudMarkedChunkRevisionRejectsNoLease(t *testing.T) {
	db := leaseTestSQLite(t)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}, &types.ChunkRevision{}))
	repo := &chunkRepository{db: db}
	chunk := &types.Chunk{
		ID: "marked-chunk", TenantID: 7,
		KnowledgeBaseID: "kb-1", KnowledgeID: "marked-source", Content: "before",
		ChunkType: types.ChunkTypeText,
	}
	require.NoError(t, db.Create(&types.Knowledge{
		ID: chunk.KnowledgeID, TenantID: chunk.TenantID,
		KnowledgeBaseID: chunk.KnowledgeBaseID,
		Metadata:        types.JSON(`{"nextcloud_file_id":"41"}`),
	}).Error)
	require.NoError(t, repo.CreateChunks(context.Background(), []*types.Chunk{chunk}))
	chunk.Content, chunk.ContentRevision = "after", 1
	revision := &types.ChunkRevision{
		ID: "marked-revision", TenantID: chunk.TenantID,
		KnowledgeBaseID: chunk.KnowledgeBaseID, KnowledgeID: chunk.KnowledgeID,
		ChunkID: chunk.ID, Revision: 0, Content: "before",
	}
	require.ErrorIs(t, repo.SaveChunkRevision(context.Background(), chunk, revision, 0),
		ErrNextcloudContentLeaseInvalid)
	var count int64
	require.NoError(t, db.Model(&types.ChunkRevision{}).Count(&count).Error)
	require.Zero(t, count)
}

func TestNextcloudBuildDetachedFinalizationCannotReviveCanceledWorker(t *testing.T) {
	db := leaseTestSQLite(t)
	require.NoError(t, db.AutoMigrate(&types.Chunk{}))
	store := NewNextcloudContentLeaseStore(db)
	scope := leaseTestScope("detached-worker")
	lease, err := store.AcquireKnowledge(context.Background(), scope,
		NextcloudContentBuildLease, "worker", time.Minute)
	require.NoError(t, err)
	workerCtx, cancel := context.WithCancel(context.Background())
	leased := WithNextcloudBuildLease(workerCtx, store, lease, scope)
	detached := context.WithoutCancel(leased)
	cancel()
	require.NoError(t, detached.Err(), "the detached context itself stays live")
	chunk := &types.Chunk{
		ID: "late-chunk", TenantID: scope.TenantID,
		KnowledgeBaseID: scope.KnowledgeBaseID, KnowledgeID: scope.KnowledgeID,
		Content: "late", ChunkType: types.ChunkTypeText,
	}
	require.ErrorIs(t, (&chunkRepository{db: db}).CreateChunks(detached, []*types.Chunk{chunk}),
		context.Canceled)
	var count int64
	require.NoError(t, db.Model(&types.Chunk{}).Count(&count).Error)
	require.Zero(t, count)
}
