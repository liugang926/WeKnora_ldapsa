package handler

import (
	"context"
	"sort"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Acquire broad scopes in a stable order before any material capture, then
// upgrade every immutable controlled parent generation while the broad leases
// remain held. Publication/current actor checks remain independent.
func acquireFAQReadPlanLeases(ctx context.Context, store nextcloudHTTPReadLeaseStore, scopes []types.KnowledgeSearchScope, lineage *types.SourceLineage, heartbeat time.Duration) (*nextcloudHTTPReadLeases, error) {
	if store == nil {
		return nil, errors.NewServiceUnavailableError("FAQ content lease store unavailable")
	}
	if lineage.RequireComplete() != nil || len(scopes) == 0 {
		return nil, knowledgeOutputHTTPError(types.ErrSourceLineageUnknown)
	}
	scopes = append([]types.KnowledgeSearchScope{}, scopes...)
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].TenantID != scopes[j].TenantID {
			return scopes[i].TenantID < scopes[j].TenantID
		}
		return scopes[i].KBID < scopes[j].KBID
	})
	if heartbeat <= 0 {
		heartbeat = nextcloudHTTPReadLeaseHeartbeat
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	group := &nextcloudHTTPReadLeases{store: store, ctx: leaseCtx, cancel: cancel, heartbeat: heartbeat, rows: []repository.NextcloudContentLease{}}
	owner := uuid.NewString()
	broad := map[types.KnowledgeSearchScope]repository.NextcloudContentLease{}
	for _, scope := range scopes {
		if _, exists := broad[scope]; exists {
			continue
		}
		row, err := store.AcquireKBRead(leaseCtx, scope.TenantID, scope.KBID, owner, nextcloudHTTPReadLeaseTTL)
		if err != nil {
			group.Close()
			return nil, nextcloudHTTPLeaseError(err)
		}
		broad[scope] = row
		group.rows = append(group.rows, row)
	}
	sources := append([]types.SourceIdentity{}, lineage.Sources...)
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].TenantID != sources[j].TenantID {
			return sources[i].TenantID < sources[j].TenantID
		}
		if sources[i].KnowledgeBaseID != sources[j].KnowledgeBaseID {
			return sources[i].KnowledgeBaseID < sources[j].KnowledgeBaseID
		}
		return sources[i].KnowledgeID < sources[j].KnowledgeID
	})
	for _, source := range sources {
		parent, ok := broad[types.KnowledgeSearchScope{TenantID: source.TenantID, KBID: source.KnowledgeBaseID}]
		if !ok {
			group.Close()
			return nil, knowledgeOutputHTTPError(repository.ErrNextcloudContentGCUncovered)
		}
		upgrader, ok := store.(interface {
			UpgradeKBRead(context.Context, string, repository.NextcloudContentScope, string, time.Duration) (repository.NextcloudContentLease, error)
		})
		if !ok {
			group.Close()
			return nil, errors.NewServiceUnavailableError("FAQ exact content lease upgrade unavailable")
		}
		row, err := upgrader.UpgradeKBRead(leaseCtx, parent.ID, repository.NextcloudContentScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}, owner, nextcloudHTTPReadLeaseTTL)
		if err != nil {
			group.Close()
			return nil, nextcloudHTTPLeaseError(err)
		}
		group.rows = append(group.rows, row)
		if uint64(row.Epoch) != source.Revision {
			group.Close()
			return nil, knowledgeOutputHTTPError(repository.ErrKnowledgeInputChanged)
		}
	}
	covered, err := repository.WithFAQReadLeaseCoverage(leaseCtx, group.rows)
	if err != nil {
		group.Close()
		return nil, knowledgeOutputHTTPError(err)
	}
	group.ctx = covered
	group.stop.Add(1)
	go group.heartbeatLoop()
	return group, nil
}

func bindFAQHTTPLeaseWriter(c *gin.Context, group *nextcloudHTTPReadLeases) func() {
	prior := c.Writer
	c.Writer = &knowledgeCollectionAuthorizationWriter{ResponseWriter: prior, check: func() error { return nextcloudHTTPLeaseErrorOrNil(group.Verify()) }}
	return func() { c.Writer = prior; group.Close() }
}
func nextcloudHTTPLeaseErrorOrNil(err error) error {
	if err == nil {
		return nil
	}
	return nextcloudHTTPLeaseError(err)
}

func beginFAQHTTPRead(c *gin.Context, ctx context.Context, a *service.FAQOutputAuthorizer, kb string) (context.Context, func(), error) {
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 {
		return ctx, nil, knowledgeOutputHTTPError(types.ErrSourceLineageUnknown)
	}
	plan, err := a.ReadPlan(ctx, types.FAQInputReadRequest{TenantID: tenant, KnowledgeBaseID: kb, AllEntries: true})
	if err != nil {
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	group, err := acquireFAQReadPlanLeases(ctx, a.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	if err != nil {
		return ctx, nil, err
	}
	marked, err := repository.WithFAQInputReadPlan(group.Context(ctx), plan)
	if err != nil {
		group.Close()
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	return marked, bindFAQHTTPLeaseWriter(c, group), nil
}

func beginFAQProgressHTTPRead(c *gin.Context, ctx context.Context, a *service.FAQOutputAuthorizer, task string) (context.Context, func(), error) {
	plan, err := a.ProgressReadPlan(ctx, task)
	if err != nil {
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	group, err := acquireFAQReadPlanLeases(ctx, a.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	if err != nil {
		return ctx, nil, err
	}
	marked, err := repository.WithFAQImportOutputReadPlan(group.Context(ctx), plan)
	if err != nil {
		group.Close()
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	return marked, bindFAQHTTPLeaseWriter(c, group), nil
}

type faqKnowledgePreReadKey struct{}

// Resolve FAQ result identities and pin their parents before the ordinary
// knowledge service loads the result JSON. beginNextcloudRead reuses this group.
func (h *KnowledgeHandler) beginFAQKnowledgePreRead(c *gin.Context, ctx context.Context, ids []string) (context.Context, func(), error) {
	if h.outputAuthorizer == nil {
		return ctx, func() {}, nil
	}
	plan, err := h.outputAuthorizer.FAQResultReadPlan(ctx, ids)
	if err != nil {
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	if len(plan.Plans) == 0 && len(plan.MessagePlans) == 0 {
		return ctx, func() {}, nil
	}
	group, err := acquireFAQReadPlanLeases(ctx, h.contentLeases, plan.Scopes, plan.SourceLineage, h.leaseHeartbeat)
	if err != nil {
		return ctx, nil, err
	}
	ctx, err = repository.WithFAQImportOutputReadSetPlan(group.Context(ctx), plan)
	if err != nil {
		group.Close()
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	ctx = context.WithValue(ctx, faqKnowledgePreReadKey{}, group)
	group.ctx = ctx
	c.Request = c.Request.WithContext(ctx)
	return ctx, bindFAQHTTPLeaseWriter(c, group), nil
}

func beginFAQTagHTTPRead(c *gin.Context, ctx context.Context, a *service.TagOutputAuthorizer, kb string) (context.Context, func(), error) {
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 {
		return ctx, nil, knowledgeOutputHTTPError(types.ErrSourceLineageUnknown)
	}
	plan, err := a.FAQReadPlan(ctx, tenant, kb)
	if err != nil {
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	if plan == nil {
		return beginCollectionHTTPRead(c, ctx, a.ContentLeaseStore(), kb)
	}
	group, err := acquireFAQReadPlanLeases(ctx, a.ContentLeaseStore(), plan.Scopes, plan.SourceLineage, 0)
	if err != nil {
		return ctx, nil, err
	}
	marked, err := repository.WithFAQInputReadPlan(group.Context(ctx), plan)
	if err != nil {
		group.Close()
		return ctx, nil, knowledgeOutputHTTPError(err)
	}
	return marked, bindFAQHTTPLeaseWriter(c, group), nil
}
