package service

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type savedAgentSessions struct {
	interfaces.SessionRepository
	gotOwner string
	calls    int
	reject   string
}

func (r *savedAgentSessions) Get(_ context.Context, tenant uint64, owner, id string) (*types.Session, error) {
	r.calls++
	r.gotOwner = owner
	if owner == r.reject {
		return nil, access.ErrNextcloudPublicationDenied
	}
	return &types.Session{ID: id, TenantID: tenant, UserID: owner}, nil
}

func (r *savedAgentSessions) GetIMPlatform(context.Context, uint64, string) (string, error) {
	return "", nil
}

type savedAgentRows struct {
	interfaces.MessageRepository
	rows []*types.Message
}

func (r *savedAgentRows) GetMessage(_ context.Context, _, id string) (*types.Message, error) {
	for _, row := range r.rows {
		if row.ID == id {
			return row, nil
		}
	}
	return nil, access.ErrNextcloudPublicationDenied
}

func (r *savedAgentRows) GetMessagesBySession(context.Context, string, int, int) ([]*types.Message, error) {
	return r.rows, nil
}

func (r *savedAgentRows) GetRecentMessagesBySession(context.Context, string, int) ([]*types.Message, error) {
	return r.rows, nil
}

func (r *savedAgentRows) GetMessagesBySessionBeforeTime(
	context.Context,
	string,
	time.Time,
	int,
) ([]*types.Message, error) {
	return r.rows, nil
}

func (r *savedAgentRows) SearchMessagesByKeyword(
	context.Context,
	uint64,
	string,
	string,
	[]string,
	int,
) ([]*types.MessageWithSession, error) {
	var out []*types.MessageWithSession
	for _, row := range r.rows {
		out = append(out, &types.MessageWithSession{Message: *row})
	}
	return out, nil
}

func (r *savedAgentRows) OwnedSessionIDs(context.Context, uint64, string, []string) (map[string]bool, error) {
	return map[string]bool{"session": true}, nil
}

func (r *savedAgentRows) GetMessagesByRequestIDs(
	context.Context,
	string,
	[]string,
) ([]*types.MessageWithSession, error) {
	return nil, nil
}

func savedAgentFixture(t *testing.T) (*messageService, *savedAgentSessions, context.Context) {
	t.Helper()
	current, _, _, _, ctx, _ := messageMaterialActualFixture(t)
	independent, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	sessions := &savedAgentSessions{}
	rows := &savedAgentRows{rows: []*types.Message{
		{ID: "q", SessionID: "session", RequestID: "turn", Role: "user", Content: "my text"},
		{
			ID:               "a",
			SessionID:        "session",
			RequestID:        "turn",
			Role:             "assistant",
			AgentID:          "agent",
			Content:          "old secret paraphrase",
			ExecutionContext: types.MessageExecutionContext{AgentKBSelectionMode: "none"},
			AgentSteps: types.AgentSteps{
				{
					ToolCalls: []types.ToolCall{
						{ID: "real-call", Name: "opaque_tool", Result: &types.ToolResult{Output: "old tool secret"}},
					},
				},
			},
		},
		{
			ID:            "ordinary",
			SessionID:     "session",
			RequestID:     "ordinary",
			Role:          "assistant",
			Content:       "ordinary RAG text",
			SourceLineage: independent,
			AgentSteps: types.AgentSteps{
				{
					ToolCalls: []types.ToolCall{
						{
							ID:     types.PipelineToolCallIDPrefix + "search",
							Name:   "search_knowledge",
							Result: &types.ToolResult{Output: "normal timeline"},
						},
					},
				},
			},
		},
	}}
	return &messageService{
		messageRepo:        rows,
		sessionRepo:        sessions,
		historyGuard:       access.NewNextcloudHistoryGuard(nil, &instanceHistorySources{ever: true}, nil),
		sourceLineageGuard: current.sourceLineageGuard,
	}, sessions, ctx
}

func TestSavedAgentHistoryGetAndListUseWeKnoraSessionACL(t *testing.T) {
	s, _, ctx := savedAgentFixture(t)
	got, err := s.GetMessage(ctx, "session", "a")
	require.NoError(t, err)
	require.Equal(t, "old secret paraphrase", got.Content)
	for _, load := range []func() ([]*types.Message, error){
		func() ([]*types.Message, error) { return s.GetMessagesBySession(ctx, "session", 1, 10) },
		func() ([]*types.Message, error) { return s.GetRecentMessagesBySession(ctx, "session", 10) },
		func() ([]*types.Message, error) {
			return s.GetMessagesBySessionBeforeTime(ctx, "session", time.Now(), 10)
		},
	} {
		rows, err := load()
		require.NoError(t, err)
		require.Len(t, rows, 3)
		require.Equal(t, "my text", rows[0].Content)
		require.Equal(t, "old secret paraphrase", rows[1].Content)
		require.Equal(t, "ordinary RAG text", rows[2].Content)
	}
	result, err := s.SearchMessages(
		ctx,
		&types.MessageSearchParams{Query: "text", Mode: types.MessageSearchModeKeyword},
	)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationUnavailable,
		"this fixture has no keyword repository port; the failure must come from the missing capability")
	require.Nil(t, result)
}

func TestSavedAgentHistoryRejectsDifferentWeKnoraSessionOwner(t *testing.T) {
	s, sessions, ctx := savedAgentFixture(t)
	sessions.reject = "other-user"
	ctx = context.WithValue(ctx, types.SessionTenantIDContextKey, nil)
	other := types.WithPrincipal(
		types.WithCaller(ctx, types.Caller{
			TenantID: types.CallerFromContext(ctx).TenantID,
			UserID:   "other-user",
			Role:     types.TenantRoleViewer,
		}),
		types.Principal{Type: types.PrincipalWebUser, ID: "other-user"},
	)
	other = context.WithValue(other, types.UserIDContextKey, "other-user")
	got, err := s.GetMessage(other, "session", "a")
	require.Error(t, err)
	require.Nil(t, got)
	require.Equal(t, "other-user", sessions.gotOwner)
}

func TestSavedAgentOrdinarySearchUsesCurrentSQLMaterialInputs(t *testing.T) {
	messages, _, _, _, ctx, session := messageMaterialActualFixture(t)
	_, answer := produceActualMessagePair(ctx, t, messages, session)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	result, err := messages.SearchMessages(ctx, &types.MessageSearchParams{
		Query: answer.Content, Mode: types.MessageSearchModeKeyword,
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.Total)
	require.Equal(t, answer.RequestID, result.Items[0].RequestID)
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
}

func TestSavedAgentControlLookupKeepsOwnerScopeAndReturnsNoContent(t *testing.T) {
	s, sessions, ctx := savedAgentFixture(t)
	ctx = context.WithValue(ctx, types.SessionTenantIDContextKey, uint64(99))
	row, err := s.GetMessageForControl(ctx, "session", "a")
	require.NoError(t, err)
	require.Equal(t, types.CallerFromContext(ctx).UserID, sessions.gotOwner)
	require.Empty(t, row.Content)
	require.Empty(t, row.AgentSteps)
	require.Empty(t, row.KnowledgeReferences)
	row, err = s.GetMessageForStream(ctx, "session", "a")
	require.NoError(t, err)
	require.Equal(t, "old secret paraphrase", row.Content)
	missing := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
	_, err = s.GetMessageForControl(missing, "session", "a")
	require.Error(t, err)
	_, err = s.GetMessageForStream(types.WithCaller(ctx, types.Caller{TenantID: 2, UserID: "person"}), "session", "a")
	require.Error(t, err)
}

func TestSavedAgentHistoryAllowsLegacyRowsOnlyWithinOwningSession(t *testing.T) {
	s, _, ctx := savedAgentFixture(t)
	currentGuard := s.sourceLineageGuard
	s.sourceLineageGuard = nil
	s.historyGuard = access.NewNextcloudHistoryGuard(nil, nil, nil)
	got, err := s.GetMessage(ctx, "session", "a")
	require.NoError(t, err)
	require.Equal(t, "old secret paraphrase", got.Content)
	row := s.messageRepo.(*savedAgentRows).rows[1]
	row.AgentID = ""
	got, err = s.GetMessage(ctx, "session", "a")
	require.NoError(t, err)
	require.Equal(t, "old secret paraphrase", got.Content)
	row.SourceLineage, err = types.NewCompleteSourceLineage()
	require.NoError(t, err)
	got, err = s.GetMessage(ctx, "session", "a")
	require.NoError(t, err)
	require.Equal(t, "old secret paraphrase", got.Content)
	ordinary := s.messageRepo.(*savedAgentRows).rows[2]
	ordinary.SourceLineage, err = types.NewCompleteSourceLineage()
	require.NoError(t, err)
	got, err = s.GetMessage(ctx, "session", "ordinary")
	require.NoError(t, err)
	require.Equal(t, "ordinary RAG text", got.Content)
	s.sourceLineageGuard = currentGuard
	ordinary, err = s.GetMessage(ctx, "session", "ordinary")
	require.NoError(t, err)
	require.Equal(t, "ordinary RAG text", ordinary.Content)
}
