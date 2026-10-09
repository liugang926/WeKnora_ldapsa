package router

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type buildLeaseKnowledgeReader struct{ row *types.Knowledge }

func (r buildLeaseKnowledgeReader) GetKnowledgeByID(_ context.Context, _ uint64, _ string) (*types.Knowledge, error) {
	if r.row == nil {
		return nil, repository.ErrKnowledgeNotFound
	}
	return r.row, nil
}

type buildLeaseChunkReader struct{ row *types.Chunk }

func (r buildLeaseChunkReader) GetChunkByID(context.Context, uint64, string) (*types.Chunk, error) {
	return r.row, nil
}

type buildLeaseStoreFake struct {
	acquired    []repository.NextcloudContentScope
	released    []string
	waited      []string
	waitStarted chan struct{}
	waitUntil   chan struct{}
	waitErr     error
	deny        bool
}

func (s *buildLeaseStoreFake) AcquireKnowledge(_ context.Context, scope repository.NextcloudContentScope,
	kind repository.NextcloudContentLeaseKind, owner string, _ time.Duration,
) (repository.NextcloudContentLease, error) {
	if s.deny {
		return repository.NextcloudContentLease{}, repository.ErrNextcloudContentLeaseDenied
	}
	s.acquired = append(s.acquired, scope)
	return repository.NextcloudContentLease{
		ID: "lease-1", TenantID: scope.TenantID,
		KnowledgeBaseID: scope.KnowledgeBaseID, KnowledgeID: scope.KnowledgeID,
		Kind: kind, Epoch: 1, OwnerID: owner,
	}, nil
}

func (s *buildLeaseStoreFake) RenewLease(_ context.Context, id string, _ time.Duration) (
	repository.NextcloudContentLease, error,
) {
	if s.deny {
		return repository.NextcloudContentLease{}, repository.ErrNextcloudContentLeaseDenied
	}
	return repository.NextcloudContentLease{ID: id}, nil
}

func (s *buildLeaseStoreFake) WaitForCurrentSourceVersion(ctx context.Context,
	_ repository.NextcloudContentScope, _ repository.NextcloudContentLease,
	etag string, _ time.Duration,
) error {
	s.waited = append(s.waited, etag)
	if s.waitStarted != nil {
		close(s.waitStarted)
	}
	if s.waitUntil != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.waitUntil:
		}
	}
	if s.deny {
		return repository.ErrNextcloudContentLeaseDenied
	}
	return s.waitErr
}

func (s *buildLeaseStoreFake) ReleaseLease(_ context.Context, id string) error {
	s.released = append(s.released, id)
	return nil
}

func (*buildLeaseStoreFake) ValidateBuildLeaseInTx(context.Context, *gorm.DB,
	repository.NextcloudContentLease, repository.NextcloudContentScope,
) error {
	return nil
}

func buildTask(t *testing.T, identity nextcloudBuildTaskIdentity) *asynq.Task {
	t.Helper()
	b, err := json.Marshal(identity)
	require.NoError(t, err)
	return asynq.NewTask(types.TypeDocumentProcess, b)
}

func TestNextcloudBuildWorkerLeaseAdmissionAndRelease(t *testing.T) {
	row := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud,
		Metadata: []byte(`{"datasource_id":"source","external_id":"nextcloud:instance:file","nextc` +
			`loud_target_etag":"etag-1"}`),
	}
	store := &buildLeaseStoreFake{}
	called := 0
	wrapped := wrapNextcloudBuild(store, buildLeaseKnowledgeReader{row}, nil,
		func(ctx context.Context, _ *asynq.Task) error {
			called++
			require.True(t, repository.NextcloudBuildLeasePresent(ctx, 7, "kb", "doc"))
			return nil
		})
	task := buildTask(t, nextcloudBuildTaskIdentity{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc"})
	require.NoError(t, wrapped(context.Background(), task))
	require.Equal(t, 1, called)
	require.Len(t, store.acquired, 1)
	require.Equal(t, "source", store.acquired[0].DataSourceID)
	require.Equal(t, []string{"etag-1"}, store.waited)
	require.Equal(t, []string{"lease-1"}, store.released)

	store.deny = true
	require.ErrorIs(t, wrapped(context.Background(), task), repository.ErrNextcloudContentLeaseDenied)
	require.Equal(t, 1, called, "retired source must never enter the handler")
}

func TestNextcloudBuildWorkerWaitsForStageBeforeHandler(t *testing.T) {
	row := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud,
		Metadata: []byte(`{"datasource_id":"source","external_id":"nextcloud:instance:file","nextc` +
			`loud_target_etag":"etag-1"}`),
	}
	store := &buildLeaseStoreFake{waitStarted: make(chan struct{}), waitUntil: make(chan struct{})}
	called := make(chan struct{}, 1)
	wrapper := wrapNextcloudBuild(store, buildLeaseKnowledgeReader{row}, nil,
		func(context.Context, *asynq.Task) error {
			called <- struct{}{}
			return nil
		})
	done := make(chan error, 1)
	executor := NewSyncTaskExecutor()
	executor.RegisterHandler(types.TypeDocumentProcess, func(ctx context.Context, task *asynq.Task) error {
		err := wrapper(ctx, task)
		done <- err
		return err
	})
	info, err := executor.Enqueue(buildTask(t, nextcloudBuildTaskIdentity{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc",
	}), asynq.MaxRetry(0))
	require.NoError(t, err)
	require.NotNil(t, info, "Lite enqueue must return while the worker waits for Stage")
	select {
	case <-store.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach source-version admission")
	}
	select {
	case <-called:
		t.Fatal("parser ran before the source version was staged")
	case <-time.After(100 * time.Millisecond):
	}
	close(store.waitUntil)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("worker did not resume after staging")
	}
	require.Len(t, called, 1)
	require.Equal(t, []string{"etag-1"}, store.waited)
	require.Equal(t, []string{"lease-1"}, store.released)
}

func TestNextcloudBuildWorkerNeverRunsWhenStageAdmissionExpires(t *testing.T) {
	row := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud,
		Metadata: []byte(`{"datasource_id":"source","external_id":"nextcloud:instance:file","nextc` +
			`loud_target_etag":"etag-1"}`),
	}
	store := &buildLeaseStoreFake{waitErr: repository.ErrNextcloudContentLeaseDenied}
	called := 0
	wrapper := wrapNextcloudBuild(store, buildLeaseKnowledgeReader{row}, nil,
		func(context.Context, *asynq.Task) error {
			called++
			return nil
		})
	err := wrapper(context.Background(), buildTask(t, nextcloudBuildTaskIdentity{
		TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc",
	}))
	require.ErrorIs(t, err, repository.ErrNextcloudContentLeaseDenied)
	require.Zero(t, called)
	require.Equal(t, []string{"lease-1"}, store.released)
}

func TestNextcloudBuildWorkerRejectsMalformedAndCrossKBTasks(t *testing.T) {
	row := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud, Metadata: []byte(`{}`),
	}
	store := &buildLeaseStoreFake{}
	wrapped := wrapNextcloudBuild(store, buildLeaseKnowledgeReader{row}, nil,
		func(context.Context, *asynq.Task) error { t.Fatal("handler reached"); return nil })
	identity := nextcloudBuildTaskIdentity{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc"}
	require.ErrorIs(t, wrapped(context.Background(), buildTask(t, identity)),
		repository.ErrNextcloudContentLeaseInvalid)
	row.Metadata = []byte(`{"datasource_id":"source","external_id":"file","nextcloud_target_etag":"` +
		`etag-1"}`)
	identity.KnowledgeBaseID = "other"
	require.ErrorIs(t, wrapped(context.Background(), buildTask(t, identity)),
		repository.ErrNextcloudContentLeaseInvalid)
	require.Empty(t, store.acquired)
}

func TestNextcloudBuildWorkerResolvesLegacyChunkPayload(t *testing.T) {
	row := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud,
		Metadata: []byte(`{"datasource_id":"source","external_id":"file","nextcloud_target_etag":"` +
			`etag-1"}`),
	}
	store := &buildLeaseStoreFake{}
	wrapped := wrapNextcloudBuild(store, buildLeaseKnowledgeReader{row},
		buildLeaseChunkReader{&types.Chunk{
			ID: "chunk", TenantID: 7,
			KnowledgeBaseID: "kb", KnowledgeID: "doc",
		}},
		func(ctx context.Context, _ *asynq.Task) error {
			require.True(t, repository.NextcloudBuildLeasePresent(ctx, 7, "kb", "doc"))
			return nil
		})
	require.NoError(t, wrapped(context.Background(), buildTask(t,
		nextcloudBuildTaskIdentity{TenantID: 7, ChunkID: "chunk"})))
	require.Len(t, store.acquired, 1)
}

func TestNextcloudBuildWorkerRejectsSpoofedChunkParentAndMissingKnowledge(t *testing.T) {
	row := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud,
		Metadata: []byte(`{"datasource_id":"source","external_id":"file","nextcloud_target_etag":"` +
			`etag-1"}`),
	}
	chunk := buildLeaseChunkReader{&types.Chunk{
		ID: "chunk", TenantID: 7,
		KnowledgeBaseID: "kb", KnowledgeID: "doc",
	}}
	store := &buildLeaseStoreFake{}
	called := 0
	handler := func(context.Context, *asynq.Task) error { called++; return nil }
	wrapper := wrapNextcloudBuild(store, buildLeaseKnowledgeReader{row}, chunk, handler)
	require.ErrorIs(t, wrapper(context.Background(), buildTask(t,
		nextcloudBuildTaskIdentity{
			TenantID: 7, KnowledgeBaseID: "kb",
			KnowledgeID: "other", ChunkID: "chunk",
		})), repository.ErrNextcloudContentLeaseInvalid)
	require.Zero(t, called)
	require.Empty(t, store.acquired)

	wrapper = wrapNextcloudBuild(store, buildLeaseKnowledgeReader{}, chunk, handler)
	require.ErrorIs(t, wrapper(context.Background(), buildTask(t,
		nextcloudBuildTaskIdentity{
			TenantID: 7, KnowledgeBaseID: "kb",
			KnowledgeID: "doc", ChunkID: "chunk",
		})), repository.ErrKnowledgeNotFound)
	require.Zero(t, called, "an orphan chunk task must not run without a lease")
	require.Empty(t, store.acquired)
}

func TestNextcloudBuildWorkerCancelsOnLostHeartbeat(t *testing.T) {
	store := &buildLeaseStoreFake{}
	g := newNextcloudBuildGuard(context.Background(), store,
		repository.NextcloudContentLease{ID: "lease-1"})
	defer g.Close()
	store.deny = true
	require.True(t, errors.Is(g.Check(), repository.ErrNextcloudContentLeaseDenied))
	require.ErrorIs(t, g.ctx.Err(), context.Canceled)
}
