package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// resolveKnowledgeQARerankModelID uses the session owner's workspace choice,
// then its visible default model. Listing failures preserve the historical
// non-fatal behaviour of ordinary chat rather than blocking an answer.
func (s *sessionService) resolveKnowledgeQARerankModelID(ctx context.Context, tenantID uint64) string {
	tenant, err := s.tenantService.GetTenantByID(ctx, tenantID)
	if err != nil {
		logger.Warnf(ctx, "Failed to load retrieval config for knowledge QA: %v", err)
	} else if tenant != nil && tenant.RetrievalConfig != nil && tenant.RetrievalConfig.RerankModelID != "" {
		return tenant.RetrievalConfig.RerankModelID
	}
	models, err := s.modelService.ListModels(ctx)
	if err != nil {
		logger.Warnf(ctx, "Failed to select default rerank model for knowledge QA: %v", err)
		return ""
	}
	return selectDefaultRerankModelID(models)
}

// selectDefaultRerankModelID chooses a visible, active reranker without
// relying on database row order. A tenant's default overrides the platform
// default; an explicit RetrievalConfig model ID is handled by the caller.
func selectDefaultRerankModelID(models []*types.Model) string {
	var selected *types.Model
	for _, model := range models {
		if model == nil || model.ID == "" || model.Type != types.ModelTypeRerank {
			continue
		}
		if model.Status != "" && model.Status != types.ModelStatusActive {
			continue
		}
		if selected == nil || rerankModelPriority(model) < rerankModelPriority(selected) ||
			(rerankModelPriority(model) == rerankModelPriority(selected) && model.ID < selected.ID) {
			selected = model
		}
	}
	if selected == nil {
		return ""
	}
	return selected.ID
}

func rerankModelPriority(model *types.Model) int {
	if model.IsDefault {
		if !model.IsBuiltin {
			return 0
		}
		return 1
	}
	if !model.IsBuiltin {
		return 2
	}
	return 3
}
