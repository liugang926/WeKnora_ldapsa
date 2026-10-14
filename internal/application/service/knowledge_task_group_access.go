package service

import (
	"context"
	"fmt"
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
	return types.WithTaskAuthorization(ctx, tenantID, initiator)
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

// Record terminal authorization failures separately from content writes. A
// worker rejected before registering its normal finalizers must leave a clear
// failure state instead of an indefinitely pending parse.
func (s *knowledgeService) failKnowledgeAuthorization(ctx context.Context, knowledge *types.Knowledge) error {
	if knowledge == nil {
		return nil
	}
	if s.tracker().LatestAttempt(ctx, knowledge.ID) > attemptFromCtx(ctx) {
		return nil
	}
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := s.repo.FailKnowledgeAuthorization(dctx, knowledge, false); err != nil {
		return err
	}
	if attempt := attemptFromCtx(ctx); attempt > 0 {
		s.tracker().FinalizeAttempt(dctx, knowledge.ID, attempt, types.SpanStatusFailed,
			nil, "AUTHORIZATION_REVOKED", "processing permission revoked")
	}
	return nil
}
