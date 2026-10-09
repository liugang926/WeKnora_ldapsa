package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

const (
	nextcloudBuildLeaseTTL       = 2 * time.Minute
	nextcloudBuildLeaseHeartbeat = 30 * time.Second
	nextcloudBuildStageWait      = 15 * time.Second
)

type nextcloudBuildLeaseStore interface {
	repository.NextcloudBuildLeaseValidator
	AcquireKnowledge(context.Context, repository.NextcloudContentScope,
		repository.NextcloudContentLeaseKind, string, time.Duration) (repository.NextcloudContentLease, error)
	WaitForCurrentSourceVersion(context.Context, repository.NextcloudContentScope,
		repository.NextcloudContentLease, string, time.Duration) error
	RenewLease(context.Context, string, time.Duration) (repository.NextcloudContentLease, error)
	ReleaseLease(context.Context, string) error
}

type nextcloudBuildKnowledgeReader interface {
	GetKnowledgeByID(context.Context, uint64, string) (*types.Knowledge, error)
}

type nextcloudBuildChunkReader interface {
	GetChunkByID(context.Context, uint64, string) (*types.Chunk, error)
}

type nextcloudBuildTaskIdentity struct {
	TenantID        uint64 `json:"tenant_id"`
	KnowledgeBaseID string `json:"knowledge_base_id"`
	KnowledgeID     string `json:"knowledge_id"`
	ChunkID         string `json:"chunk_id"`
}

// The tuple is loaded from the persisted knowledge row, never from task JSON.
func nextcloudBuildScope(k *types.Knowledge) (repository.NextcloudContentScope, error) {
	var m struct {
		DataSourceID string `json:"datasource_id"`
		ExternalID   string `json:"external_id"`
	}
	if k == nil || json.Unmarshal(k.Metadata, &m) != nil || k.TenantID == 0 ||
		k.ID == "" || k.KnowledgeBaseID == "" || m.DataSourceID == "" || m.ExternalID == "" {
		return repository.NextcloudContentScope{}, repository.ErrNextcloudContentLeaseInvalid
	}
	return repository.NextcloudContentScope{
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID,
		DataSourceID: m.DataSourceID, ExternalID: m.ExternalID,
	}, nil
}

// wrapNextcloudBuild admits each dequeued worker under an exact, renewable
// build lease. The same wrapper is used for Redis/Asynq and Lite task routes.
// It does not cover KB-global builders or direct non-task writes.
func wrapNextcloudBuild(store nextcloudBuildLeaseStore, knowledges nextcloudBuildKnowledgeReader,
	chunks nextcloudBuildChunkReader, handler func(context.Context, *asynq.Task) error,
) func(context.Context, *asynq.Task) error {
	return func(ctx context.Context, task *asynq.Task) (retErr error) {
		var identity nextcloudBuildTaskIdentity
		if err := json.Unmarshal(task.Payload(), &identity); err != nil {
			return handler(ctx, task)
		}
		if identity.ChunkID != "" {
			if chunks == nil || identity.TenantID == 0 {
				return repository.ErrNextcloudContentLeaseInvalid
			}
			chunk, err := chunks.GetChunkByID(ctx, identity.TenantID, identity.ChunkID)
			if err != nil {
				return err
			}
			if chunk == nil || chunk.TenantID != identity.TenantID || chunk.ID != identity.ChunkID ||
				chunk.KnowledgeID == "" || chunk.KnowledgeBaseID == "" ||
				(identity.KnowledgeID != "" && identity.KnowledgeID != chunk.KnowledgeID) ||
				(identity.KnowledgeBaseID != "" && identity.KnowledgeBaseID != chunk.KnowledgeBaseID) {
				return repository.ErrNextcloudContentLeaseInvalid
			}
			identity.KnowledgeID = chunk.KnowledgeID
			identity.KnowledgeBaseID = chunk.KnowledgeBaseID
		}
		if identity.KnowledgeID == "" {
			return handler(ctx, task)
		}
		if identity.TenantID == 0 || knowledges == nil {
			return repository.ErrNextcloudContentLeaseInvalid
		}
		k, err := knowledges.GetKnowledgeByID(ctx, identity.TenantID, identity.KnowledgeID)
		if errors.Is(err, repository.ErrKnowledgeNotFound) {
			// A stale chunk task may still read its chunk after the parent row
			// disappears. Never run it outside a source-generation lease.
			return err
		}
		if err != nil {
			return err
		}
		if k == nil || k.ID != identity.KnowledgeID || k.TenantID != identity.TenantID ||
			(identity.KnowledgeBaseID != "" && k.KnowledgeBaseID != identity.KnowledgeBaseID) {
			return repository.ErrNextcloudContentLeaseInvalid
		}
		if k.Channel != types.ConnectorTypeNextcloud {
			return handler(ctx, task)
		}
		scope, err := nextcloudBuildScope(k)
		if err != nil {
			return err
		}
		if store == nil {
			return apperrors.NewProtocolError(fmt.Errorf("nextcloud build lease store unavailable: %w",
				repository.ErrNextcloudContentLeaseInvalid), fmt.Sprintf(
				"Nextcloud build lease store unavailable: %s", apperrors.PublicMessage(
					repository.ErrNextcloudContentLeaseInvalid)))
		}
		lease, err := store.AcquireKnowledge(ctx, scope, repository.NextcloudContentBuildLease,
			uuid.NewString(), nextcloudBuildLeaseTTL)
		if err != nil {
			return err
		}
		guard := newNextcloudBuildGuard(ctx, store, lease)
		defer guard.Close()
		if err := store.WaitForCurrentSourceVersion(guard.ctx, scope, lease,
			k.GetMetadata()["nextcloud_target_etag"], nextcloudBuildStageWait); err != nil {
			return err
		}
		if err := guard.Check(); err != nil {
			return err
		}
		ctx = repository.WithNextcloudBuildLease(guard.ctx, store, lease, scope)
		retErr = handler(ctx, task)
		if err := guard.Check(); err != nil {
			if retErr != nil {
				return fmt.Errorf("worker error: %v; Nextcloud build lease: %w", retErr, err)
			}
			return err
		}
		return retErr
	}
}

type nextcloudBuildGuard struct {
	store     nextcloudBuildLeaseStore
	lease     repository.NextcloudContentLease
	ctx       context.Context
	cancel    context.CancelFunc
	stop      chan struct{}
	done      chan struct{}
	mu        sync.Mutex
	err       error
	closeOnce sync.Once
}

func newNextcloudBuildGuard(parent context.Context, store nextcloudBuildLeaseStore,
	lease repository.NextcloudContentLease,
) *nextcloudBuildGuard {
	ctx, cancel := context.WithCancel(parent)
	g := &nextcloudBuildGuard{
		store: store, lease: lease, ctx: ctx, cancel: cancel,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go g.heartbeat()
	return g
}

func (g *nextcloudBuildGuard) Check() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.err != nil {
		return g.err
	}
	if err := g.ctx.Err(); err != nil {
		return err
	}
	if _, err := g.store.RenewLease(g.ctx, g.lease.ID, nextcloudBuildLeaseTTL); err != nil {
		g.err = err
		g.cancel()
		return err
	}
	return nil
}

func (g *nextcloudBuildGuard) heartbeat() {
	defer close(g.done)
	ticker := time.NewTicker(nextcloudBuildLeaseHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			if g.Check() != nil {
				return
			}
		}
	}
}

func (g *nextcloudBuildGuard) Close() {
	g.closeOnce.Do(func() {
		close(g.stop)
		g.cancel()
		<-g.done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.store.ReleaseLease(ctx, g.lease.ID)
	})
}
