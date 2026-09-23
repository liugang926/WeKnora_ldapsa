package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// backgroundTaskAuthorizationContext reconstructs only the verified human
// identity captured when a user-triggered task was admitted. API keys and
// legacy payloads deliberately have no web-user principal, so restricted
// resources fail closed while inherit mode keeps its historical behaviour.
func backgroundTaskAuthorizationContext(
	ctx context.Context,
	tenantID uint64,
	initiator types.TaskInitiator,
) context.Context {
	ctx = initiator.Apply(ctx)
	ctx = types.WithCaller(ctx, types.Caller{
		TenantID: tenantID,
		UserID:   strings.TrimSpace(initiator.UserID),
		Role:     initiator.Role,
	})
	if userID := strings.TrimSpace(initiator.UserID); userID != "" && !types.IsSyntheticUserID(userID) {
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: userID})
	}
	return types.WithExecutionTenant(ctx, tenantID)
}

func (s *knowledgeService) revalidateBackgroundKBAccess(
	ctx context.Context,
	tenantID uint64,
	kbID string,
	action types.ResourceAction,
) error {
	if s.groupAccess == nil {
		return nil
	}
	permission, err := s.groupAccess.EffectivePermission(
		ctx,
		tenantID,
		types.GroupResourceTypeKnowledgeBase,
		kbID,
		action,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("revalidate knowledge base %s group access: %w", kbID, err)
	}
	if !permission.Allowed {
		return fmt.Errorf("%w: knowledge base %s (%s)", ErrResourceAccessDenied, kbID, permission.Reason)
	}
	return nil
}
