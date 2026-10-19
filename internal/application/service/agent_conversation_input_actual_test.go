//go:build sqlite_fts5

package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func agentConversationActualContext(ctx context.Context, messages *messageService) context.Context {
	ctx = WithMessageMaterialOutputBoundary(ctx)
	turn := types.NewRAGSourceLineage(func(checkCtx context.Context, lineage *types.SourceLineage) error {
		if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
			return err
		}
		return messages.sourceLineageGuard.Check(checkCtx, lineage, access.LineageReadScope{})
	})
	return types.WithRAGSourceLineage(ctx, turn)
}

func TestAgentConversationActualOwnedSearchOriginalsAndNonmatchingReads(t *testing.T) {
	for _, kind := range []string{"matching", "nonmatching", "projection-mutation", "foreign-owner", "empty-session"} {
		t.Run(kind, func(t *testing.T) {
			messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
			var assistant *types.Message
			if kind != "empty-session" {
				_, assistant = produceActualMessagePair(ctx, t, messages, session)
			}
			ctx = agentConversationActualContext(ctx, messages)
			defer CloseMessageMaterialOutputBoundary(ctx)
			params := &types.MessageSearchParams{
				Query: "original", Mode: types.MessageSearchModeKeyword,
				SessionIDs: []string{session.ID}, OwnerID: session.UserID, Limit: 5,
			}
			if kind == "foreign-owner" {
				params.OwnerID = "a-different-user"
			}
			if kind == "nonmatching" {
				params.Query = "this literal does not occur in either original field"
			}
			result, lineage, err := messages.SearchConversationsWithInputs(ctx, params)
			if kind == "foreign-owner" {
				require.Error(t, err)
				require.Nil(t, result)
				require.Nil(t, lineage)
				return
			}
			require.NoError(t, err)
			require.NoError(t, lineage.RequireComplete())
			require.Empty(t, lineage.Sources)
			require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
			if kind == "empty-session" || kind == "nonmatching" {
				require.Empty(t, result.Items)
			} else {
				require.Len(t, result.Items, 1)
				require.Equal(t, assistant.Content, result.Items[0].AnswerContent)
			}
			if kind == "empty-session" {
				return
			}
			if kind == "projection-mutation" {
				result.Items[0].AnswerContent = "unadmitted replacement projection"
			} else {
				require.NoError(t, db.Model(&types.Message{}).Where("id=?", assistant.ID).
					Update("content", "changed original answer").Error)
				require.NoError(t, db.Model(&types.Message{}).Where("id=?", assistant.ID).
					Update("content", assistant.Content).Error)
			}
			require.Error(t, CheckMessageMaterialOutputBoundary(ctx),
				"even a nonmatching consumed body must remain original until final output")
			require.Error(t, types.RAGSourceLineageFromContext(ctx).Check(ctx))
		})
	}
}

func TestAgentConversationActualControlledHistoryPreservesAllSourceInputs(t *testing.T) {
	t.Setenv("WEKNORA_DOCUMENT_GRAPH_HISTORY", "1")
	f := newHistoryVectorActualFixture(t)
	ctx := agentConversationActualContext(f.base, f.messages)
	defer CloseMessageMaterialOutputBoundary(ctx)
	result, lineage, err := f.messages.SearchConversationsWithInputs(ctx, &types.MessageSearchParams{
		Query: "Bob", Mode: types.MessageSearchModeHybrid, Limit: 5,
		SessionIDs: []string{f.sessions[0].ID, f.sessions[1].ID}, OwnerID: f.sessions[0].UserID,
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Items)
	require.NoError(t, lineage.RequireComplete())
	require.NotEmpty(t, lineage.Sources, "a source-aware saved answer must not become an empty-source tool result")
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	f.m.allow.Store(false)
	require.Error(t, CheckMessageMaterialOutputBoundary(ctx))
}

func TestAgentConversationActualMissingNativeBoundaryDenied(t *testing.T) {
	messages, _, _, _, ctx, _ := messageMaterialActualFixture(t)
	result, lineage, err := messages.SearchConversationsWithInputs(ctx, &types.MessageSearchParams{Query: "original"})
	require.Error(t, err)
	require.Nil(t, result)
	require.Nil(t, lineage)
}
