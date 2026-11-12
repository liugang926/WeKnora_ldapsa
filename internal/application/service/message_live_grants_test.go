package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestLiveAnswerRechecksCurrentKBGrantForDocumentOnlyToolReference(t *testing.T) {
	ctx := ragReadContext()
	groups := &tagTargetGroupAccessService{allowed: map[string]bool{"source-kb": true}}
	session := &types.Session{ID: "session", TenantID: 7, UserID: "reader"}
	service := &sessionService{
		sessionRepo: &knowledgeQASessionRepository{session: session},
		groupAccess: groups,
		knowledgeService: &tagTargetKnowledgeService{knowledges: []*types.Knowledge{
			{ID: "source-doc", TenantID: 7, KnowledgeBaseID: "source-kb"},
		}},
	}
	targets, err := service.buildSearchTargets(ctx, 7, nil, []string{"source-doc"}, nil)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.Equal(t, uint64(7), targets[0].TenantID)
	require.Equal(t, "source-kb", targets[0].KnowledgeBaseID)
	require.Equal(t, []string{"source-doc"}, targets[0].KnowledgeIDs)

	authorize := service.knowledgeQAAuthorizer(session)
	scope := access.LineageReadScope{Targets: targets}
	lineage := mustCompleteLineage(t)
	require.NoError(t, authorize(ctx, lineage, scope))
	groups.allowed["source-kb"] = false
	require.ErrorIs(t, authorize(ctx, lineage, scope), ErrResourceAccessDenied)
}
