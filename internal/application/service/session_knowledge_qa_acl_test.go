package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type knowledgeQASessionRepository struct {
	interfaces.SessionRepository
	session         *types.Session
	calls           int
	waitForDeadline bool
}

func (r *knowledgeQASessionRepository) Get(
	ctx context.Context,
	_ uint64,
	_ string,
	_ string,
) (*types.Session, error) {
	r.calls++
	if r.waitForDeadline {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if r.session == nil {
		return nil, errors.New("session not found")
	}
	copied := *r.session
	return &copied, nil
}

func TestKnowledgeQAAuthorizerUsesWeKnoraACLForLegacyNextcloudChunk(t *testing.T) {
	session := &types.Session{ID: "session", TenantID: 7, UserID: "reader"}
	repository := &knowledgeQASessionRepository{session: session}
	groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{"kb": true}}
	service := &sessionService{
		sessionRepo: repository,
		// A normal attachment-free knowledge chat must not depend on the
		// Nextcloud publication guard for read authorization.
		sourceLineageGuard: nil,
		groupAccess:        groupAccess,
	}
	scope := access.LineageReadScope{Targets: types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		TenantID:        7,
		KnowledgeBaseID: "kb",
	}}}
	manager := chatpipeline.NewEventManager()
	ctx, err := manager.BeginSourceLineage(
		ragReadContext(),
		&types.ChatManage{PipelineRequest: types.PipelineRequest{
			KnowledgeBaseIDs: []string{"kb"},
		}},
		scope,
		service.knowledgeQAAuthorizer(session),
	)
	require.NoError(t, err)

	malformed := &types.SourceLineage{
		Version: types.SourceLineageVersion,
		State:   types.SourceLineageComplete,
		Sources: []types.SourceIdentity{{
			Provider:        types.ConnectorTypeNextcloud,
			TenantID:        7,
			KnowledgeBaseID: "kb",
		}},
	}
	require.NoError(t, chatpipeline.AccountResults(ctx, []*types.SearchResult{{
		ID:              "chunk",
		KnowledgeID:     "knowledge",
		KnowledgeBaseID: "kb",
		Content:         "legacy metadata",
		SourceLineage:   malformed,
	}}))

	snapshot, err := types.RAGSourceLineageFromContext(ctx).CheckedSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, snapshot.RequireComplete())
	require.Empty(t, snapshot.Sources)
	require.GreaterOrEqual(t, repository.calls, 1)
	require.NotEmpty(t, groupAccess.calls)
	for _, resourceID := range groupAccess.calls {
		require.Equal(t, "kb", resourceID)
	}
}

func TestKnowledgeQAAuthorizerFailsClosedForStaleSessionAndRevokedKB(t *testing.T) {
	session := &types.Session{ID: "session", TenantID: 7, UserID: "reader"}
	targets := types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		TenantID:        7,
		KnowledgeBaseID: "kb",
	}}

	t.Run("foreign session owner", func(t *testing.T) {
		repository := &knowledgeQASessionRepository{session: &types.Session{
			ID: "session", TenantID: 7, UserID: "other",
		}}
		service := &sessionService{sessionRepo: repository}
		err := service.knowledgeQAAuthorizer(session)(
			ragReadContext(),
			mustCompleteLineage(t),
			access.LineageReadScope{Targets: targets},
		)
		require.ErrorIs(t, err, ErrResourceAccessDenied)
	})

	t.Run("revoked KB", func(t *testing.T) {
		repository := &knowledgeQASessionRepository{session: session}
		service := &sessionService{
			sessionRepo: repository,
			groupAccess: &tagTargetGroupAccessService{allowed: map[string]bool{"kb": false}},
		}
		err := service.knowledgeQAAuthorizer(session)(
			ragReadContext(),
			mustCompleteLineage(t),
			access.LineageReadScope{Targets: targets},
		)
		require.ErrorIs(t, err, ErrResourceAccessDenied)
	})
}

func TestKnowledgeQAAuthorizerRemainsStrictForExplicitInputMaterials(t *testing.T) {
	session := &types.Session{ID: "session", TenantID: 7, UserID: "reader"}
	repository := &knowledgeQASessionRepository{session: session}
	service := &sessionService{sessionRepo: repository}
	manager := chatpipeline.NewEventManager()

	ctx, err := manager.BeginSourceLineage(
		ragReadContext(),
		&types.ChatManage{PipelineRequest: types.PipelineRequest{
			Images: []string{"resource://image"},
		}},
		access.LineageReadScope{},
		service.knowledgeQAAuthorizer(session),
	)
	require.NoError(t, err)
	_, err = types.RAGSourceLineageFromContext(ctx).CheckedSnapshot(ctx)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	require.Zero(t, repository.calls, "ordinary ACL authorizer handled explicit material input")
}

func TestKnowledgeQAAuthorizerBoundsACLRevalidation(t *testing.T) {
	session := &types.Session{ID: "session", TenantID: 7, UserID: "reader"}
	service := &sessionService{
		sessionRepo: &knowledgeQASessionRepository{waitForDeadline: true},
	}
	ctx, cancel := context.WithTimeout(ragReadContext(), 25*time.Millisecond)
	defer cancel()

	err := service.knowledgeQAAuthorizer(session)(
		ctx,
		mustCompleteLineage(t),
		access.LineageReadScope{},
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func mustCompleteLineage(t *testing.T) *types.SourceLineage {
	t.Helper()
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	return lineage
}
