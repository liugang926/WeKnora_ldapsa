package service

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// ResolveIMChannelKnowledgeBases uses the same agent selection and capability
// rules as QA. It grants neither writes nor new cross-workspace shares.
func (s *sessionService) ResolveIMChannelKnowledgeBases(
	ctx context.Context,
	agent *types.CustomAgent) (map[string]uint64,
	error,
) {
	if agent == nil {
		return nil, fmt.Errorf("IM agent is unavailable")
	}
	ids := s.resolveKnowledgeBasesFromAgent(ctx, agent, types.CallerFromContext(ctx).TenantID)
	result := make(map[string]uint64, len(ids))
	for _, id := range ids {
		kb, err := s.knowledgeBaseService.GetKnowledgeBaseByIDOnly(ctx, id)
		if err != nil || kb == nil || kb.DeletedAt.Valid {
			return nil, fmt.Errorf("IM knowledge base is unavailable")
		}
		if err := s.CheckIMChannelKnowledgeBase(ctx, kb.TenantID, id); err != nil {
			return nil, err
		}
		result[id] = kb.TenantID
	}
	return result, nil
}

// CheckIMChannelKnowledgeBase rechecks resource existence and organization
// shares independently of the channel grant, avoiding a circular authorization.
func (s *sessionService) CheckIMChannelKnowledgeBase(ctx context.Context, tenant uint64, id string) error {
	base := context.WithValue(ctx, types.IMChannelScopeContextKey, nil)
	reader, ok := s.knowledgeBaseService.(interfaces.KnowledgeBaseAuthorizationReader)
	if !ok || reader == nil {
		return fmt.Errorf("IM knowledge base authorization metadata unavailable")
	}
	kb, err := reader.GetKnowledgeBaseAuthorizationMetadata(base, id)
	if err != nil || kb == nil || kb.DeletedAt.Valid || kb.TenantID != tenant {
		return fmt.Errorf("IM knowledge base is unavailable")
	}
	allowed, err := kbReadPermissions(base, s.kbShareService).Check(id, tenant, types.OrgRoleViewer)
	if err != nil || !allowed {
		return fmt.Errorf("IM knowledge base share is unavailable")
	}
	return nil
}
