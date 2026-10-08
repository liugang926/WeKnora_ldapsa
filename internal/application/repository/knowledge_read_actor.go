package repository

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// ConfigureKnowledgeCurrentReadActor binds the SQL lineage resolver to the
// current account/key authority. A missing production capability is closed.
func ConfigureKnowledgeCurrentReadActor(repo interfaces.KnowledgeRepository, current interfaces.CurrentReadActorRefresher) {
	if r, ok := repo.(*knowledgeRepository); ok {
		r.currentReadActor = current
	}
}

func (r *knowledgeRepository) RefreshCurrentReadActor(ctx context.Context) (context.Context, error) {
	if r.currentReadActor == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	return r.currentReadActor.RefreshCurrentReadActor(ctx)
}

func (s *KnowledgeInputProvenanceStore) RefreshCurrentReadActor(ctx context.Context) (context.Context, error) {
	actor, err := captureKnowledgeActorSelector(ctx, "")
	if err != nil {
		return ctx, err
	}
	if s.actorGroups == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	executionTenant, ok := types.TenantIDFromContext(ctx)
	if !ok || executionTenant == 0 {
		executionTenant = actor.CallerTenantID
	}
	fresh, err := s.restoreCurrentKnowledgeActor(ctx, actor, executionTenant)
	if err != nil {
		return ctx, err
	}
	if scope, machine := types.TenantAPIKeyScopeFromContext(fresh); machine {
		requirement, bound := types.APIKeyOperationRequirementFromContext(ctx)
		if !bound {
			// Internal read producers have no HTTP route policy. They must
			// still hold a real read/conversation capability, never ingest.
			requirement = types.APIKeyOperationRequirement{RequireFullAccess: true, Capabilities: []types.APIKeyCapability{types.APIKeyCapabilityRetrieve, types.APIKeyCapabilityChat, types.APIKeyCapabilityMessageHistory, types.APIKeyCapabilityReadAgents, types.APIKeyCapabilityManageKnowledgeBases, types.APIKeyCapabilityManageAgents}}
		}
		if !requirement.Allows(scope) {
			return ctx, ErrKnowledgeInputChanged
		}
	}
	return fresh, nil
}

var _ interfaces.CurrentReadActorRefresher = (*KnowledgeInputProvenanceStore)(nil)
