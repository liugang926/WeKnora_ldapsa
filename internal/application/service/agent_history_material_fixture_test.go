package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Preserve the legacy admission seam under test while current request bytes,
// owner, generation and grants still come from actual production SQL ports.
// This wrapper cannot create original-history receipts for the fake legacy rows.
type (
	historyCurrentPublication struct{ *messageService }
	actualHistoryInputKey     struct{}
	actualHistoryInput        struct {
		query   string
		lineage *types.SourceLineage
	}
)

func actualAgentHistoryRequest(t *testing.T, sessionService *sessionService, request *types.QARequest) context.Context {
	t.Helper()
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	ctx = agentPromptActualRequestContext(ctx, t)
	require.NoError(t, db.AutoMigrate(&types.CustomAgent{}))
	actualAgent := *request.CustomAgent
	actualAgent.TenantID = session.TenantID
	actualAgent.CreatedBy = types.CallerFromContext(ctx).UserID
	require.NoError(t, db.Create(&actualAgent).Error)
	request.CustomAgent = &actualAgent
	request.Session = session
	user, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: "actual-history-current-turn", Role: "user",
		Content: request.Query, IsCompleted: true,
	})
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(ctx, &types.Message{
		SessionID: session.ID, RequestID: user.RequestID, Role: "assistant",
		AgentID: actualAgent.ID, AgentTenantID: actualAgent.TenantID,
	})
	require.NoError(t, err)
	request.UserMessageID, request.AssistantMessageID = user.ID, assistant.ID
	ctx = WithMessageMaterialOutputBoundary(ctx)
	ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, user.RequestID)
	t.Cleanup(func() { CloseMessageMaterialOutputBoundary(ctx) })
	actualUser, err := messages.GetMessage(ctx, session.ID, user.ID)
	require.NoError(t, err)
	require.NoError(t, actualUser.SourceLineage.RequireComplete())
	ctx = context.WithValue(
		ctx,
		actualHistoryInputKey{},
		actualHistoryInput{query: actualUser.Content, lineage: actualUser.SourceLineage},
	)
	require.NoError(t, messages.bindInitialMessageMaterial(ctx, assistant.ID, user.ID, user.RequestID))
	sessionService.lineageVerifier = &historyCurrentPublication{messages}
	sessionService.sourceLineageGuard = messages.sourceLineageGuard
	sessionService.sessionRepo = messages.sessionRepo
	sessionService.groupAccess = NewGroupAccessService(repository.NewGroupAccessRepository(db))
	if agents, ok := sessionService.agentService.(*agentService); ok {
		agents.db = db
		agents.messageService = messages
		agents.ragChunkSnapshotReader = repository.NewRAGChunkSnapshotReader(db, messages.materialStore)
	}
	return ctx
}

// Only the locally constructed literal rows in the checkpoint unit test use
// this adapter. Unknown, incomplete and nonempty sources remain rejected.
type literalHistoryAdmission struct{ guard *access.NextcloudHistoryGuard }

// CheckAgentHistoryReplay rechecks the exact source-state seam under test.
func (a literalHistoryAdmission) CheckAgentHistoryReplay(ctx context.Context) error {
	return a.guard.CheckAgentHistoryReplay(ctx)
}

// CheckMessageSourceLineage accepts only the explicitly authored local literals.
func (a literalHistoryAdmission) CheckMessageSourceLineage(_ context.Context, message *types.Message) error {
	if message == nil || message.SourceLineage.RequireComplete() != nil || len(message.SourceLineage.Sources) != 0 {
		return types.ErrSourceLineageUnknown
	}
	if checkpoint := message.ContextCheckpoint; checkpoint != nil {
		if checkpoint.SourceLineage.RequireComplete() != nil || len(checkpoint.SourceLineage.Sources) != 0 {
			return types.ErrSourceLineageUnknown
		}
	}
	return nil
}

func literalAgentHistoryTurns(t *testing.T, count int) []*types.Message {
	t.Helper()
	var rows []*types.Message
	for index := 0; index < count; index++ {
		userLineage, err := types.NewCompleteSourceLineage()
		require.NoError(t, err)
		answerLineage, err := types.NewCompleteSourceLineage()
		require.NoError(t, err)
		rows = append(
			rows,
			&types.Message{
				ID:            fmt.Sprintf("literal-user-%d", index),
				SessionID:     "s1",
				RequestID:     fmt.Sprintf("literal-turn-%d", index),
				Role:          "user",
				Content:       "Locally constructed human question",
				SourceLineage: userLineage,
				IsCompleted:   true,
				CreatedAt:     time.Unix(int64(index*2+100), 0),
			},
			&types.Message{
				ID:            fmt.Sprintf("literal-answer-%d", index),
				SessionID:     "s1",
				RequestID:     fmt.Sprintf("literal-turn-%d", index),
				Role:          "assistant",
				Content:       "Locally constructed independent answer",
				SourceLineage: answerLineage,
				IsCompleted:   true,
				CreatedAt:     time.Unix(int64(index*2+101), 0),
			},
		)
	}
	return rows
}
