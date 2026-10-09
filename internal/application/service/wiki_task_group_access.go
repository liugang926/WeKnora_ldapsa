package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type wikiAuthorizationContextKey struct{}

type wikiTaskAuthorization struct {
	tenantID uint64
	kbID     string
	actors   []types.TaskInitiator
}

// A trigger only wakes a durable lane. Each pending source retains its own
// actor; a later administrator's trigger cannot authorize another user's op.
func wikiActorContext(ctx context.Context, payload WikiIngestPayload, actors []types.TaskInitiator) context.Context {
	if len(actors) == 0 {
		actors = []types.TaskInitiator{{}}
	}
	unique := make([]types.TaskInitiator, 0, len(actors))
	seen := make(map[types.TaskInitiator]struct{}, len(actors))
	for _, actor := range actors {
		if _, exists := seen[actor]; exists {
			continue
		}
		seen[actor] = struct{}{}
		unique = append(unique, actor)
	}
	ctx = types.WithTaskAuthorization(ctx, payload.TenantID, actors[0])
	return context.WithValue(ctx, wikiAuthorizationContextKey{}, wikiTaskAuthorization{
		tenantID: payload.TenantID, kbID: payload.KnowledgeBaseID,
		actors: unique,
	})
}

func wikiActorsFromContext(ctx context.Context) []types.TaskInitiator {
	if scope, ok := ctx.Value(wikiAuthorizationContextKey{}).(wikiTaskAuthorization); ok {
		return append([]types.TaskInitiator(nil), scope.actors...)
	}
	return []types.TaskInitiator{types.TaskInitiatorFromContext(ctx)}
}

func (s *wikiIngestService) checkWikiTaskAuthorization(ctx context.Context) error {
	if s.groupAccess == nil {
		return nil
	}
	scope, ok := ctx.Value(wikiAuthorizationContextKey{}).(wikiTaskAuthorization)
	if !ok || scope.tenantID == 0 || scope.kbID == "" || len(scope.actors) == 0 {
		return fmt.Errorf("%w: wiki source authorization missing", ErrResourceAccessDenied)
	}
	kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, scope.kbID)
	if err != nil {
		return err
	}
	if kb == nil || kb.ID != scope.kbID || kb.TenantID != scope.tenantID || !kb.IsWikiEnabled() {
		return fmt.Errorf("%w: wiki KB binding changed", ErrResourceAccessDenied)
	}
	for _, actor := range scope.actors {
		actorCtx := types.WithTaskAuthorization(ctx, scope.tenantID, actor)
		permission, err := s.groupAccess.EffectivePermission(actorCtx, scope.tenantID,
			types.GroupResourceTypeKnowledgeBase, scope.kbID, types.ResourceActionEdit, time.Now().UTC())
		if err != nil {
			return err
		}
		if !permission.Allowed {
			return fmt.Errorf("%w: wiki permission revoked (%s)", ErrResourceAccessDenied, permission.Reason)
		}
	}
	return nil
}

func (s *wikiIngestService) filterAuthorizedWikiOps(
	ctx context.Context, payload WikiIngestPayload, ops []WikiPendingOp,
) ([]WikiPendingOp, error) {
	if s.groupAccess == nil {
		return ops, nil
	}
	allowed := make([]WikiPendingOp, 0, len(ops))
	for _, op := range ops {
		if op.Op == WikiOpIngest && op.Attempt > 0 &&
			s.tracker().LatestAttempt(ctx, op.KnowledgeID) > op.Attempt {
			continue
		}
		actorCtx := wikiActorContext(ctx, payload, []types.TaskInitiator{op.Initiator})
		if err := s.checkWikiTaskAuthorization(actorCtx); err != nil {
			if !errors.Is(err, ErrResourceAccessDenied) {
				return nil, err
			}
			if err := s.rejectWikiOp(actorCtx, payload, op); err != nil {
				return nil, err
			}
			continue
		}
		allowed = append(allowed, op)
	}
	return allowed, nil
}

func (s *wikiIngestService) rejectWikiOp(ctx context.Context, payload WikiIngestPayload, op WikiPendingOp) error {
	dctx, cancel := wikiIngestCleanupContext(ctx)
	defer cancel()
	if s.deadLetterRepo != nil {
		encoded, err := json.Marshal(op)
		if err != nil {
			return err
		}
		if err := s.deadLetterRepo.Insert(dctx, &types.TaskDeadLetter{
			TenantID: payload.TenantID, TaskType: wikiTaskType, Scope: wikiTaskScope,
			ScopeID: payload.KnowledgeBaseID, RelatedID: op.KnowledgeID,
			Payload: encoded, LastError: "wiki processing permission revoked",
		}); err != nil {
			return err
		}
	}
	// Legacy ops have no attempt identity. Never decrement or fail a newer
	// parse merely because its old, anonymous Wiki op was rejected.
	if op.Op != WikiOpIngest || op.Attempt <= 0 || s.knowledgeRepo == nil ||
		s.tracker().LatestAttempt(dctx, op.KnowledgeID) != op.Attempt {
		return nil
	}
	knowledge, err := s.knowledgeRepo.GetKnowledgeByID(dctx, payload.TenantID, op.KnowledgeID)
	if err != nil {
		return err
	}
	if knowledge == nil || knowledge.TenantID != payload.TenantID ||
		knowledge.KnowledgeBaseID != payload.KnowledgeBaseID {
		return nil
	}
	if err := s.knowledgeRepo.FailKnowledgeAuthorization(dctx, knowledge, false); err != nil {
		return err
	}
	s.tracker().FinalizeAttempt(dctx, knowledge.ID, op.Attempt, types.SpanStatusFailed,
		nil, "AUTHORIZATION_REVOKED", "wiki processing permission revoked")
	return nil
}

// This decorator covers every mutation used by the Wiki worker, including
// writes after a model call and GetIndex's implicit default-row creation.
type wikiTaskPageService struct {
	interfaces.WikiPageService
	check func(context.Context) error
}

func (s *wikiTaskPageService) authorize(ctx context.Context, kbID string, tenantID uint64) error {
	if scope, ok := ctx.Value(wikiAuthorizationContextKey{}).(wikiTaskAuthorization); ok &&
		(scope.kbID != kbID || (tenantID != 0 && scope.tenantID != tenantID)) {
		return fmt.Errorf("%w: wiki write scope changed", ErrResourceAccessDenied)
	}
	return s.check(ctx)
}

func (s *wikiTaskPageService) CreatePage(ctx context.Context, page *types.WikiPage) (*types.WikiPage, error) {
	if err := s.authorize(ctx, page.KnowledgeBaseID, page.TenantID); err != nil {
		return nil, err
	}
	return s.WikiPageService.CreatePage(ctx, page)
}

func (s *wikiTaskPageService) UpdatePage(ctx context.Context, page *types.WikiPage) (*types.WikiPage, error) {
	if err := s.authorize(ctx, page.KnowledgeBaseID, page.TenantID); err != nil {
		return nil, err
	}
	return s.WikiPageService.UpdatePage(ctx, page)
}

func (s *wikiTaskPageService) UpdatePageMeta(ctx context.Context, page *types.WikiPage) error {
	if err := s.authorize(ctx, page.KnowledgeBaseID, page.TenantID); err != nil {
		return err
	}
	return s.WikiPageService.UpdatePageMeta(ctx, page)
}

func (s *wikiTaskPageService) UpdateAutoLinkedContent(ctx context.Context, page *types.WikiPage) error {
	if err := s.authorize(ctx, page.KnowledgeBaseID, page.TenantID); err != nil {
		return err
	}
	return s.WikiPageService.UpdateAutoLinkedContent(ctx, page)
}

func (s *wikiTaskPageService) GetIndex(ctx context.Context, kbID string) (*types.WikiPage, error) {
	if err := s.authorize(ctx, kbID, 0); err != nil {
		return nil, err
	}
	return s.WikiPageService.GetIndex(ctx, kbID)
}

func (s *wikiTaskPageService) FindOrCreateFolderPath(
	ctx context.Context, kbID string, tenantID uint64, path []string,
) (string, []string, error) {
	if err := s.authorize(ctx, kbID, tenantID); err != nil {
		return "", nil, err
	}
	return s.WikiPageService.FindOrCreateFolderPath(ctx, kbID, tenantID, path)
}

func (s *wikiTaskPageService) PruneEmptyFolderChains(
	ctx context.Context, kbID string, folderIDs []string,
) ([]string, error) {
	if err := s.authorize(ctx, kbID, 0); err != nil {
		return nil, err
	}
	return s.WikiPageService.PruneEmptyFolderChains(ctx, kbID, folderIDs)
}
