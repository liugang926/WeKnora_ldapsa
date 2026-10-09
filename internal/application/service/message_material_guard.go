package service

import (
	"context"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

type historyMaterialGuardContextKey struct{}
type MessageMaterialGuard struct {
	mu       sync.Mutex
	store    *repository.KnowledgeInputProvenanceStore
	verifier *messageService
	snapshot *types.MessageMaterialSnapshot
	plan     *types.MessageMaterialReadPlan
	ctx      context.Context
	cancel   context.CancelFunc
	leases   []repository.NextcloudContentLease
	stop     sync.WaitGroup
	once     sync.Once
}

func (s *messageService) beginMessageMaterialRead(ctx context.Context, request types.MessageMaterialReadRequest) (*MessageMaterialGuard, error) {
	if s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	plan, err := s.materialStore.PeekMessageMaterialReadPlan(ctx, request)
	if err != nil {
		return nil, err
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	guard := &MessageMaterialGuard{store: s.materialStore, verifier: s, plan: plan, ctx: leaseCtx, cancel: cancel, leases: []repository.NextcloudContentLease{}}
	store := s.materialStore.ContentLeaseStore()
	owner := uuid.NewString()
	broad := map[types.KnowledgeSearchScope]repository.NextcloudContentLease{}
	for _, scope := range plan.Scopes {
		lease, err := store.AcquireKBRead(leaseCtx, scope.TenantID, scope.KBID, owner, 2*time.Minute)
		if err != nil {
			guard.Close()
			return nil, err
		}
		broad[scope] = lease
		guard.leases = append(guard.leases, lease)
	}
	for _, source := range plan.SourceLineage.Sources {
		kb, ok := broad[types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID}]
		if !ok {
			guard.Close()
			return nil, repository.ErrNextcloudContentGCUncovered
		}
		lease, err := store.UpgradeKBRead(leaseCtx, kb.ID, repository.NextcloudContentScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}, owner, 2*time.Minute)
		if err != nil {
			guard.Close()
			return nil, err
		}
		guard.leases = append(guard.leases, lease)
		if uint64(lease.Epoch) != source.Revision {
			guard.Close()
			return nil, repository.ErrKnowledgeInputChanged
		}
	}
	if len(guard.leases) > 0 {
		leaseCtx, err = repository.WithFAQReadLeaseCoverage(leaseCtx, guard.leases)
		if err != nil {
			guard.Close()
			return nil, err
		}
	}
	leaseCtx, err = repository.WithMessageMaterialReadPlan(leaseCtx, plan)
	if err != nil {
		guard.Close()
		return nil, err
	}
	guard.ctx = leaseCtx
	guard.snapshot, err = s.materialStore.CaptureMessageMaterialInputs(leaseCtx, request)
	if err != nil {
		guard.Close()
		return nil, err
	}
	if err = guard.Recheck(); err != nil {
		guard.Close()
		return nil, err
	}
	guard.stop.Add(1)
	runCtx := guard.ctx
	go func() {
		defer guard.stop.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if guard.Recheck() != nil {
					guard.cancel()
					return
				}
			}
		}
	}()
	return guard, nil
}
func (g *MessageMaterialGuard) Context() context.Context { return g.ctx }
func (g *MessageMaterialGuard) Recheck() error {
	if g == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.ctx.Err(); err != nil {
		return err
	}
	for _, lease := range g.leases {
		if _, err := g.store.ContentLeaseStore().RenewLease(g.ctx, lease.ID, 2*time.Minute); err != nil {
			return err
		}
	}
	// Remote/current actor checks happen outside the body SQL transaction.
	if len(g.snapshot.Messages) > 0 {
		if err := g.verifier.CheckMessagePublications(g.ctx, g.snapshot.Messages); err != nil {
			return err
		}
	}
	if len(g.snapshot.Messages) > 0 {
		if err := g.store.CheckKnowledgeHistoryOriginalSources(g.ctx, g.snapshot.Messages); err != nil {
			return err
		}
	}
	if err := g.store.RecheckMessageMaterialInputs(g.ctx, g.snapshot); err != nil {
		return err
	}
	if len(g.snapshot.Messages) == 0 {
		return nil
	}
	return g.verifier.CheckMessagePublications(g.ctx, g.snapshot.Messages)
}
func (g *MessageMaterialGuard) Close() {
	if g == nil {
		return
	}
	g.once.Do(func() {
		g.cancel()
		g.stop.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := len(g.leases) - 1; i >= 0; i-- {
			_ = g.store.ContentLeaseStore().ReleaseLease(ctx, g.leases[i].ID)
		}
	})
}

func closeHistoryMaterialGuard(ctx context.Context) {
	if guard, ok := ctx.Value(historyMaterialGuardContextKey{}).(*MessageMaterialGuard); ok {
		guard.Close()
	}
}

func (g *MessageMaterialGuard) advanceOwnGeneratedField(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	updated, err := g.store.AdvanceMessageMaterialAfterOwnFieldWrite(ctx, g.snapshot)
	if err != nil {
		return err
	}
	plan, err := g.store.PeekMessageMaterialReadPlan(ctx, updated.Request)
	if err != nil {
		return err
	}
	marked, err := repository.WithMessageMaterialReadPlan(g.ctx, plan)
	if err != nil {
		return err
	}
	// New immutable own admission has already passed the exact transfer check.
	// Use only the new matching plan; old unrelated plans are not re-certified.
	g.ctx = marked
	g.plan = plan
	g.snapshot = updated
	return nil
}

func (g *MessageMaterialGuard) OriginalSnapshot() (*types.MessageMaterialSnapshot, error) {
	if err := g.Recheck(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	result, err := cloneCollection(g.snapshot)
	if err != nil {
		return nil, err
	}
	for i, message := range g.snapshot.Messages {
		if message == nil {
			continue
		}
		result.Messages[i].SourceLineage, _ = types.UnionSourceLineage(message.SourceLineage)
		result.Messages[i].ExecutionContext = message.ExecutionContext
		result.Messages[i].AgentTenantID = message.AgentTenantID
		result.Messages[i].RenderedContent = message.RenderedContent
		result.Messages[i].ContextCheckpoint, err = cloneCollection(message.ContextCheckpoint)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (g *MessageMaterialGuard) advanceOwnCompletion(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.advanceOwnCompletionLocked(ctx)
}
func (g *MessageMaterialGuard) advanceOwnCompletionLocked(ctx context.Context) error {
	updated, err := g.store.AdvanceMessageMaterialAfterOwnCompletion(ctx, g.snapshot)
	if err != nil {
		return err
	}
	plan, err := g.store.PeekMessageMaterialReadPlan(ctx, updated.Request)
	if err != nil {
		return err
	}
	marked, err := repository.WithMessageMaterialReadPlan(g.ctx, plan)
	if err != nil {
		return err
	}
	g.ctx = marked
	g.plan = plan
	g.snapshot = updated
	return nil
}
