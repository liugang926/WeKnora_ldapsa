package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// ConfigureKnowledgePostProcessHistoryMaterial attaches the actual knowledge
// service after DI construction. No constructor dependency cycle is introduced.
func ConfigureKnowledgePostProcessHistoryMaterial(
	handler interfaces.TaskHandler,
	knowledge interfaces.KnowledgeService,
) {
	if post, ok := handler.(*KnowledgePostProcessService); ok {
		post.historyMaterials, _ = knowledge.(interfaces.KnowledgeHistoryTaskMaterialRestorer)
	}
}

func beginKnowledgeHistoryTaskMaterial(
	ctx context.Context,
	reader interfaces.KnowledgeBuildInputReader,
	restorer interfaces.KnowledgeHistoryTaskMaterialRestorer,
	proof *types.KnowledgeBuildInputProof,
) (context.Context, func(), error) {
	noop := func() {}
	if ctx == nil || reader == nil || proof == nil {
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	selector, ok := reader.(interfaces.KnowledgeHistoryMaterialRequestReader)
	if !ok {
		return ctx, noop, access.ErrNextcloudPublicationUnavailable
	}
	request, err := selector.KnowledgeHistoryMaterialRequest(ctx, proof)
	if err != nil {
		return ctx, noop, err
	}
	// A metadata-proven ordinary input keeps its existing NC build scope,
	// actor and producer context. Absence of a selector is never this case.
	if request == nil {
		return ctx, noop, nil
	}
	if restorer == nil {
		return ctx, noop, access.ErrNextcloudPublicationUnavailable
	}
	current, closeRead, err := restorer.BeginKnowledgeHistoryTaskMaterial(ctx, proof)
	if err != nil {
		if closeRead != nil {
			closeRead()
		}
		return ctx, noop, err
	}
	if current == nil || closeRead == nil {
		if closeRead != nil {
			closeRead()
		}
		return ctx, noop, access.ErrNextcloudPublicationUnavailable
	}
	return current, closeRead, nil
}

// BeginKnowledgeHistoryTaskMaterial opens a separate actual guard even when a
// caller already has a sibling/earlier guard. That preserves its context and
// ownership while bounding this worker's own leases to this exact admission.
func (s *knowledgeService) BeginKnowledgeHistoryTaskMaterial(
	ctx context.Context,
	proof *types.KnowledgeBuildInputProof,
) (context.Context, func(), error) {
	noop := func() {}
	if ctx == nil || proof == nil || s.knowledgeInputs == nil {
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	selector, ok := s.knowledgeInputs.(interfaces.KnowledgeHistoryMaterialRequestReader)
	if !ok {
		return ctx, noop, access.ErrNextcloudPublicationUnavailable
	}
	request, err := selector.KnowledgeHistoryMaterialRequest(ctx, proof)
	if err != nil {
		return ctx, noop, err
	}
	if request == nil {
		return ctx, noop, nil
	}
	if s.historyMaterialMessages == nil || s.historyMaterialMessages.materialStore == nil {
		return ctx, noop, access.ErrNextcloudPublicationUnavailable
	}
	current := ctx
	// Fresh queue contexts restore the actual immutable actor selector and
	// current account/key/grants. An authenticated caller is not replaced by
	// another historical owner; the real parent guard validates that caller.
	if _, captured := ctx.Value(types.CallerContextKey).(types.Caller); !captured {
		// A partially authenticated context is not a fresh worker. Do not
		// replace its existing principal/key/user with the admission owner.
		if ctx.Value(types.CallerContextKey) != nil || ctx.Value(types.PrincipalContextKey) != nil ||
			ctx.Value(types.TenantAPIKeyScopeContextKey) != nil ||
			ctx.Value(types.UserIDContextKey) != nil {
			return ctx, noop, types.ErrSourceLineageUnknown
		}
		current, err = repository.RestoreKnowledgeBuildActorIfRequired(ctx, s.knowledgeInputs, proof)
		if err != nil {
			return ctx, noop, err
		}
	}
	guard, err := s.historyMaterialMessages.beginMessageMaterialRead(current, *request)
	if err != nil {
		return ctx, noop, err
	}
	current = context.WithValue(guard.Context(), historyMaterialGuardContextKey{}, guard)
	return current, guard.Close, nil
}

var _ interfaces.KnowledgeHistoryTaskMaterialRestorer = (*knowledgeService)(nil)
