package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

type (
	historyMaterialGuardContextKey struct{}
	// MessageMaterialGuard retains original message inputs and rechecks current read controls.
	MessageMaterialGuard struct {
		mu            sync.Mutex
		store         *repository.KnowledgeInputProvenanceStore
		verifier      *messageService
		snapshot      *types.MessageMaterialSnapshot
		plan          *types.MessageMaterialReadPlan
		ctx           context.Context
		cancel        context.CancelFunc
		cancelCause   context.CancelCauseFunc
		leases        []repository.NextcloudContentLease
		bodyLeases    []string
		stop          sync.WaitGroup
		heartbeatStop chan struct{}
		heartbeatOnce sync.Once
		once          sync.Once
	}
)

func (s *messageService) beginMessageMaterialRead(
	ctx context.Context,
	request types.MessageMaterialReadRequest,
) (*MessageMaterialGuard, error) {
	if s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	plan, err := s.materialStore.PeekMessageMaterialReadPlan(ctx, request)
	if err != nil {
		return nil, err
	}
	leaseCtx, cancelCause := context.WithCancelCause(ctx)
	cancel := func() { cancelCause(context.Canceled) }
	guard := &MessageMaterialGuard{
		store:         s.materialStore,
		verifier:      s,
		plan:          plan,
		ctx:           leaseCtx,
		cancel:        cancel,
		cancelCause:   cancelCause,
		leases:        []repository.NextcloudContentLease{},
		heartbeatStop: make(chan struct{}),
	}
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
		lease, err := store.UpgradeKBRead(
			leaseCtx,
			kb.ID,
			repository.NextcloudContentScope{
				TenantID:        source.TenantID,
				KnowledgeBaseID: source.KnowledgeBaseID,
				KnowledgeID:     source.KnowledgeID,
				DataSourceID:    source.DataSourceID,
				ExternalID:      source.ExternalID,
			},
			owner,
			2*time.Minute,
		)
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
	guard.bodyLeases, err = s.materialStore.AcquireMessageBodyReadLeases(leaseCtx, plan, owner, 2*time.Minute)
	if err != nil {
		guard.Close()
		return nil, err
	}
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
			case <-guard.heartbeatStop:
				return
			case <-ticker.C:
				if guard.heartbeatRecheck() != nil {
					return
				}
			}
		}
	}()
	return guard, nil
}

// The actual ticker and deterministic SQL regression share this checkpoint.
// Cancel the consumer immediately, retaining the original failure cause; a
// known current-source denial must not become an unrelated unknown outage.
func (g *MessageMaterialGuard) heartbeatRecheck() error {
	if g == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	err := g.Recheck()
	if err != nil {
		if g.cancelCause != nil {
			g.cancelCause(err)
		} else {
			g.cancel()
		}
	}
	return err
}

// Context returns the context carrying the retained read controls.
func (g *MessageMaterialGuard) Context() context.Context { return g.ctx }

// Recheck rechecks the retained inputs and their current read controls.
func (g *MessageMaterialGuard) Recheck() error {
	if g == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.ctx.Err(); err != nil {
		if errors.Is(context.Cause(g.ctx), access.ErrNextcloudPublicationDenied) {
			return context.Cause(g.ctx)
		}
		return err
	}
	for _, lease := range g.leases {
		if _, err := g.store.ContentLeaseStore().RenewLease(g.ctx, lease.ID, 2*time.Minute); err != nil {
			return err
		}
	}
	for _, id := range g.bodyLeases {
		if err := g.store.RenewMessageBodyReadLease(g.ctx, id, 2*time.Minute); err != nil {
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
	// A raw-user plan may remain valid after an actual unconsumed generated
	// field write. Hold every new physical head without recertifying that
	// field as consumed original material.
	added, err := g.store.AcquireMessageBodyReadLeases(g.ctx, g.plan, uuid.NewString(), 2*time.Minute, g.bodyLeases)
	if err != nil {
		return err
	}
	g.bodyLeases = append(g.bodyLeases, added...)
	if len(g.snapshot.Messages) == 0 {
		return nil
	}
	return g.verifier.CheckMessagePublications(g.ctx, g.snapshot.Messages)
}

// Close releases the retained original input reads.
func (g *MessageMaterialGuard) Close() {
	if g == nil {
		return
	}
	g.once.Do(func() {
		g.cancel()
		g.stop.Wait()
		g.mu.Lock()
		bodies := append([]string{}, g.bodyLeases...)
		leases := append([]repository.NextcloudContentLease{}, g.leases...)
		g.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := len(bodies) - 1; i >= 0; i-- {
			_ = g.store.ReleaseMessageBodyReadLease(ctx, bodies[i])
		}
		for i := len(leases) - 1; i >= 0; i-- {
			_ = g.store.ContentLeaseStore().ReleaseLease(ctx, leases[i].ID)
		}
	})
}

func closeHistoryMaterialGuard(ctx context.Context) {
	if guard, ok := ctx.Value(historyMaterialGuardContextKey{}).(*MessageMaterialGuard); ok {
		guard.Close()
	}
}

// prepareOwnPlanGrowthLocked runs only after the immutable own-write transfer
// has passed. New scopes belong to this guard, not a borrowed sibling/slot
// context. Preserve its original caller/cancel values and its previous leases.
func (g *MessageMaterialGuard) checkOwnPlanGrowthAuthorization(
	ctx, marked context.Context,
	plan *types.MessageMaterialReadPlan,
	updated *types.MessageMaterialSnapshot,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := marked.Err(); err != nil {
		return err
	}
	if g.verifier == nil || g.verifier.sourceLineageGuard == nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	targets := []*types.SearchTarget{}
	for _, scope := range plan.Scopes {
		targets = append(
			targets,
			&types.SearchTarget{
				Type:            types.SearchTargetTypeKnowledgeBase,
				TenantID:        scope.TenantID,
				KnowledgeBaseID: scope.KBID,
			},
		)
	}
	if err := g.verifier.sourceLineageGuard.Check(
		marked,
		plan.SourceLineage,
		access.LineageReadScope{Targets: targets},
	); err != nil {
		return err
	}
	if len(updated.Messages) > 0 {
		if err := g.verifier.CheckMessagePublications(marked, updated.Messages); err != nil {
			return err
		}
		if err := g.store.CheckKnowledgeHistoryOriginalSources(marked, updated.Messages); err != nil {
			return err
		}
	}
	return nil
}

func (g *MessageMaterialGuard) releaseAddedOwnBodyLeases(ids []string) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := len(ids) - 1; i >= 0; i-- {
		_ = g.store.ReleaseMessageBodyReadLease(cleanup, ids[i])
	}
}

func (g *MessageMaterialGuard) prepareOwnPlanGrowthLocked(
	ctx context.Context,
	plan *types.MessageMaterialReadPlan,
	updated *types.MessageMaterialSnapshot,
) (context.Context, []repository.NextcloudContentLease, func(), error) {
	added := []repository.NextcloudContentLease{}
	rollback := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := len(added) - 1; i >= 0; i-- {
			_ = g.store.ContentLeaseStore().ReleaseLease(cleanup, added[i].ID)
		}
	}
	fail := func(err error) (context.Context, []repository.NextcloudContentLease, func(), error) {
		rollback()
		return g.ctx, nil, func() {}, err
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := g.ctx.Err(); err != nil {
		return fail(err)
	}
	leases := append([]repository.NextcloudContentLease{}, g.leases...)
	broad := map[types.KnowledgeSearchScope]repository.NextcloudContentLease{}
	for _, lease := range leases {
		if lease.Kind == repository.NextcloudContentReadLease && lease.KnowledgeID == "" {
			broad[types.KnowledgeSearchScope{TenantID: lease.TenantID, KBID: lease.KnowledgeBaseID}] = lease
		}
	}
	for _, scope := range plan.Scopes {
		if _, exists := broad[scope]; exists {
			continue
		}
		lease, err := g.store.ContentLeaseStore().
			AcquireKBRead(g.ctx, scope.TenantID, scope.KBID, uuid.NewString(), 2*time.Minute)
		if err != nil {
			return fail(err)
		}
		added = append(added, lease)
		leases = append(leases, lease)
		broad[scope] = lease
	}
	for _, source := range plan.SourceLineage.Sources {
		found := false
		originalScope := false
		for _, captured := range g.plan.SourceLineage.Sources {
			if captured == source {
				originalScope = true
			}
		}
		for _, lease := range leases {
			// The lease struct does not expose datasource/external ID. Its
			// original owning plan supplies that full immutable source tuple;
			// SQL coverage then verifies the fence tuple and epoch again.
			if originalScope && lease.Kind == repository.NextcloudContentReadLease &&
				lease.TenantID == source.TenantID &&
				lease.KnowledgeBaseID == source.KnowledgeBaseID &&
				lease.KnowledgeID == source.KnowledgeID &&
				uint64(lease.Epoch) == source.Revision {
				found = true
			}
		}
		if found {
			continue
		}
		kb, exists := broad[types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID}]
		if !exists {
			return fail(repository.ErrNextcloudContentGCUncovered)
		}
		lease, err := g.store.ContentLeaseStore().
			UpgradeKBRead(g.ctx, kb.ID, repository.NextcloudContentScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}, kb.OwnerID, 2*time.Minute)
		if err != nil {
			return fail(err)
		}
		added = append(added, lease)
		leases = append(leases, lease)
		if uint64(lease.Epoch) != source.Revision {
			return fail(repository.ErrKnowledgeInputChanged)
		}
	}
	marked := g.ctx
	var err error
	if len(leases) > 0 {
		marked, err = repository.WithFAQReadLeaseCoverage(marked, leases)
		if err != nil {
			return fail(err)
		}
	}
	marked, err = repository.WithMessageMaterialReadPlan(marked, plan)
	if err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := marked.Err(); err != nil {
		return fail(err)
	}
	// Raw-user snapshots may omit unconsumed generated fields; still check
	// the full original union, source and actor after physical acquisition.
	if err := g.checkOwnPlanGrowthAuthorization(ctx, marked, plan, updated); err != nil {
		return fail(err)
	}
	return marked, leases, rollback, nil
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
	marked, leases, rollback, err := g.prepareOwnPlanGrowthLocked(ctx, plan, updated)
	if err != nil {
		return err
	}
	// Hold the new inline fields before exposing the updated snapshot. Keep the
	// earlier body leases until the same model/output guard finally closes.
	bodyLeases, err := g.store.AcquireMessageBodyReadLeases(marked, plan, uuid.NewString(), 2*time.Minute, g.bodyLeases)
	if err != nil {
		rollback()
		return err
	}
	// Cancellation/withdrawal can commit during body acquisition. Install no
	// new snapshot and release only this transition's newly acquired pins.
	if err := g.checkOwnPlanGrowthAuthorization(ctx, marked, plan, updated); err != nil {
		g.releaseAddedOwnBodyLeases(bodyLeases)
		rollback()
		return err
	}
	g.leases = leases
	g.bodyLeases = append(g.bodyLeases, bodyLeases...)
	// New immutable own admission has already passed the exact transfer check.
	// Use only the new matching plan; old unrelated plans are not re-certified.
	g.ctx = marked
	g.plan = plan
	g.snapshot = updated
	return nil
}

// OriginalSnapshot returns the retained original message input snapshot.
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

func (g *MessageMaterialGuard) advanceOwnCompletionLocked(ctx context.Context) error {
	updated, err := g.store.AdvanceMessageMaterialAfterOwnCompletion(ctx, g.snapshot)
	if err != nil {
		return err
	}
	plan, err := g.store.PeekMessageMaterialReadPlan(ctx, updated.Request)
	if err != nil {
		return err
	}
	marked, leases, rollback, err := g.prepareOwnPlanGrowthLocked(ctx, plan, updated)
	if err != nil {
		return err
	}
	bodyLeases, err := g.store.AcquireMessageBodyReadLeases(marked, plan, uuid.NewString(), 2*time.Minute, g.bodyLeases)
	if err != nil {
		rollback()
		return err
	}
	// Cancellation/withdrawal can commit during body acquisition. Install no
	// new snapshot and release only this transition's newly acquired pins.
	if err := g.checkOwnPlanGrowthAuthorization(ctx, marked, plan, updated); err != nil {
		g.releaseAddedOwnBodyLeases(bodyLeases)
		rollback()
		return err
	}
	g.leases = leases
	g.bodyLeases = append(g.bodyLeases, bodyLeases...)
	g.ctx = marked
	g.plan = plan
	g.snapshot = updated
	return nil
}

func (g *MessageMaterialGuard) stopOriginalHeartbeat() {
	if g == nil {
		return
	}
	g.heartbeatOnce.Do(func() { close(g.heartbeatStop) })
	g.stop.Wait()
}
