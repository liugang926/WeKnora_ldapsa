package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
)

// ResolveKnowledgeCollectionScopes resolves the default own/shared scope or
// narrows a caller-selected API/Agent scope before unfiltered candidate capture.
func (s *knowledgeService) ResolveKnowledgeCollectionScopes(
	ctx context.Context,
	selected []types.KnowledgeSearchScope,
) ([]types.KnowledgeSearchScope, error) {
	scopes := append([]types.KnowledgeSearchScope{}, selected...)
	if selected == nil {
		caller := types.CallerFromContext(ctx)
		if caller.TenantID == 0 {
			return nil, errors.NewUnauthorizedError("Workspace ID not found in context")
		}
		own, err := s.kbService.ListKnowledgeBases(types.WithExecutionTenant(ctx, caller.TenantID))
		if err != nil {
			return nil, err
		}
		for _, kb := range own {
			if kb != nil && kb.Type == types.KnowledgeBaseTypeDocument {
				scopes = append(scopes, types.KnowledgeSearchScope{TenantID: kb.TenantID, KBID: kb.ID})
			}
		}
		if s.kbShareService != nil && caller.UserID != "" {
			shared, err := s.kbShareService.ListSharedKnowledgeBases(ctx, caller.TenantID, caller.Role)
			if err != nil {
				return nil, err
			}
			for _, info := range shared {
				if info != nil && info.KnowledgeBase != nil &&
					info.KnowledgeBase.Type == types.KnowledgeBaseTypeDocument {
					scopes = append(
						scopes,
						types.KnowledgeSearchScope{TenantID: info.SourceTenantID, KBID: info.KnowledgeBase.ID},
					)
				}
			}
		}
	}
	filtered, err := s.filterReadableKnowledgeScopes(ctx, scopes)
	if err != nil {
		return nil, err
	}
	seen := map[types.KnowledgeSearchScope]bool{}
	out := []types.KnowledgeSearchScope{}
	for _, scope := range filtered {
		if !seen[scope] {
			seen[scope] = true
			out = append(out, scope)
		}
	}
	return out, nil
}
