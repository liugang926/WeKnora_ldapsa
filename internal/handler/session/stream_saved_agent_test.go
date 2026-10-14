package session

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type guardedReplayMessages struct {
	interfaces.MessageService
	completed                  bool
	historicalCalls, liveCalls int
}

func (s *guardedReplayMessages) GetMessage(context.Context, string, string) (*types.Message, error) {
	return nil, access.ErrNextcloudPublicationDenied
}

func (s *guardedReplayMessages) GetMessageForStream(_ context.Context, session, id string) (*types.Message, error) {
	return &types.Message{
		ID: id, SessionID: session, RequestID: "turn", Role: "assistant", AgentID: "agent",
		IsCompleted:      s.completed,
		ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"nc-kb"}},
	}, nil
}

func (s *guardedReplayMessages) GetMessageForControl(_ context.Context, session, id string) (*types.Message, error) {
	return &types.Message{ID: id, SessionID: session, Role: "assistant"}, nil
}

func (s *guardedReplayMessages) CheckMessagePublication(context.Context, *types.Message) error {
	s.historicalCalls++
	return access.ErrNextcloudPublicationDenied
}

func (s *guardedReplayMessages) CheckLiveMessagePublication(_ context.Context, m *types.Message) error {
	s.liveCalls++
	if len(m.KnowledgeReferences) == 0 {
		return access.ErrNextcloudPublicationDenied
	}
	for _, ref := range m.KnowledgeReferences {
		if ref.KnowledgeID == "revoked" {
			return access.ErrNextcloudPublicationDenied
		}
	}
	return nil
}

type guardedReplayStream struct {
	stubStreamManager
	stopCalls   int
	preparatory bool
}

func (s *guardedReplayStream) GetEvents(ctx context.Context, session, id string,
	offset int,
) ([]interfaces.StreamEvent, int, error) {
	if s.preparatory && offset == 0 {
		return []interfaces.StreamEvent{{
			Type: types.ResponseTypeToolCall, Content: "secret argument",
			Data: map[string]interface{}{"tool_name": "search_knowledge", "arguments": "secret argument"},
		}}, 1, nil
	}
	if s.preparatory && offset == 1 {
		return []interfaces.StreamEvent{
			{
				Type: types.ResponseTypeToolResult, Content: "authorized result",
				Data: map[string]interface{}{
					"tool_name": "search_knowledge",
					"results":   []map[string]interface{}{{"knowledge_id": "current"}},
				},
			},
			{Type: types.ResponseTypeAnswer, Content: "authorized answer"},
			{Type: types.ResponseTypeComplete, Done: true},
		}, 4, nil
	}
	return s.stubStreamManager.GetEvents(ctx, session, id, offset)
}

func (s *guardedReplayStream) AppendEvent(context.Context, string, string, interfaces.StreamEvent) error {
	s.stopCalls++
	return nil
}

func (s *guardedReplayStream) GetLiveRun(context.Context, string) (string, string, error) {
	return "a", "turn", nil
}

type guardedOwnedSessions struct{ stubSessionService }

func (s *guardedOwnedSessions) GetOwnedSession(_ context.Context, id string) (*types.Session, error) {
	return &types.Session{ID: id, TenantID: 1, UserID: "person"}, nil
}

func TestContinueStreamUnfinishedUsesLiveSourceToolResults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, scenario := range []string{"before-references", "after-tool-results", "completed"} {
		t.Run(scenario, func(t *testing.T) {
			messages := &guardedReplayMessages{completed: scenario == "completed"}
			stream := &guardedReplayStream{
				preparatory: scenario == "before-references",
				stubStreamManager: stubStreamManager{events: []interfaces.StreamEvent{
					{
						Type: types.ResponseTypeToolResult, Content: "source output",
						Data: map[string]interface{}{
							"tool_name": "query_knowledge_graph",
							"results":   []map[string]interface{}{{"knowledge_id": "current"}},
						},
					},
					{Type: types.ResponseTypeAnswer, Content: "authorized answer"},
					{Type: types.ResponseTypeComplete, Done: true},
				}},
			}
			h := &Handler{sessionService: &guardedOwnedSessions{}, messageService: messages, streamManager: stream}
			r := gin.New()
			r.Use(middleware.ErrorHandler())
			r.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/sessions/continue-stream/session?message_id=a", nil))
			if scenario == "completed" {
				require.Equal(t, http.StatusForbidden, w.Code)
				require.Equal(t, 1, messages.historicalCalls)
				require.Zero(t, messages.liveCalls)
			} else {
				require.Equal(t, http.StatusOK, w.Code)
				require.Zero(t, messages.historicalCalls)
				require.Contains(t, w.Body.String(), "authorized answer")
				require.NotContains(t, w.Body.String(), "secret argument")
			}
		})
	}
}

func TestLiveSSEGraphResultChecksNewDocumentAfterEarlierReference(t *testing.T) {
	messages := &guardedReplayMessages{}
	m := &types.Message{Role: "assistant", KnowledgeReferences: types.References{{KnowledgeID: "current"}}}
	err := checkLiveStreamPublication(context.Background(), messages, m,
		[]interfaces.StreamEvent{{
			Type: types.ResponseTypeToolResult, Content: "revoked graph content",
			Data: map[string]interface{}{
				"tool_name": "query_knowledge_graph",
				"results":   []map[string]interface{}{{"knowledge_id": "revoked"}},
			},
		}})
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.Len(t, m.KnowledgeReferences, 1)
}

func TestLiveSSEMissingVerifierFailsClosed(t *testing.T) {
	require.ErrorIs(t, checkLiveStreamPublication(context.Background(), &stubNoVerifierMessages{},
		&types.Message{Role: "assistant"}, nil), access.ErrNextcloudPublicationUnavailable)
	require.ErrorIs(t, checkReplayPublication(context.Background(), &stubNoVerifierMessages{},
		&types.Message{Role: "assistant", IsCompleted: true}, nil), access.ErrNextcloudPublicationUnavailable)
}

type stubNoVerifierMessages struct{ interfaces.MessageService }

func TestStopSessionAndSteerCanControlRevokedSavedContent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	messages := &guardedReplayMessages{}
	stream := &guardedReplayStream{}
	h := &Handler{sessionService: &guardedOwnedSessions{}, messageService: messages, streamManager: stream}
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set(types.TenantIDContextKey.String(), uint64(1)); c.Next() })
	r.POST("/sessions/:session_id/stop", h.StopSession)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/sessions/session/stop", strings.NewReader(`{"message_id":"a"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, 1, stream.stopCalls)
	require.Zero(t, messages.historicalCalls)
	id, err := h.liveAgentRun(context.Background(), "session")
	require.NoError(t, err)
	require.Equal(t, "a", id)
}

func TestLiveSSEPreparatoryToolCallDoesNotBypassUnavailableVerifier(t *testing.T) {
	messages := &liveHistoryMessageService{denyAt: 1}
	events := []interfaces.StreamEvent{{Type: types.ResponseTypeToolCall, Content: "hidden arguments"}}
	filtered, err := protectLiveStreamBatch(context.Background(), messages, &types.Message{Role: "assistant"}, events)
	require.NoError(t, err)
	require.Empty(t, filtered)
	_, err = protectLiveStreamBatch(context.Background(), &stubNoVerifierMessages{},
		&types.Message{Role: "assistant"}, events)
	require.True(t, errors.Is(err, access.ErrNextcloudPublicationUnavailable))
}

type revokeDuringReplayMessages struct {
	stubMessageServiceForStream
	checks int
}

func (s *revokeDuringReplayMessages) GetMessageForStream(_ context.Context, session, id string) (*types.Message,
	error,
) {
	return &types.Message{ID: id, SessionID: session, Role: "assistant", IsCompleted: true}, nil
}

func (s *revokeDuringReplayMessages) CheckMessagePublication(context.Context, *types.Message) error {
	s.checks++
	if s.checks >= 2 {
		return access.ErrNextcloudPublicationDenied
	}
	return nil
}

func TestContinueStreamRechecksBetweenFramesOfOneBatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	messages := &revokeDuringReplayMessages{}
	stream := &stubStreamManager{events: []interfaces.StreamEvent{
		{Type: types.ResponseTypeAnswer, Content: "first authorized frame"},
		{
			Type: types.ResponseTypeToolResult, Content: "later revoked output",
			Data: map[string]interface{}{"tool_name": "read_document", "knowledge_id": "current"},
		},
		{Type: types.ResponseTypeComplete, Done: true},
	}}
	h := &Handler{sessionService: &stubSessionService{}, messageService: messages, streamManager: stream}
	r := gin.New()
	r.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/sessions/continue-stream/session?message_id=a", nil))
	require.Contains(t, w.Body.String(), "first authorized frame")
	require.NotContains(t, w.Body.String(), "later revoked output")
	require.Equal(t, 2, messages.checks)
}

type classifySavedReplay struct{ interfaces.MessageService }

func (classifySavedReplay) CheckMessagePublication(_ context.Context, message *types.Message) error {
	if access.IsAgentDerivedHistory(message) {
		return access.ErrNextcloudPublicationDenied
	}
	return nil
}

func TestContinueStreamPreservesSyntheticKnowledgeQATimeline(t *testing.T) {
	message := &types.Message{
		Role: "assistant", IsCompleted: true,
		AgentSteps: types.AgentSteps{{
			ToolCalls: []types.ToolCall{{
				ID:   types.PipelineToolCallIDPrefix + "search-1",
				Name: "hybrid_search",
			}},
		}},
	}
	events := []interfaces.StreamEvent{{
		Type:    types.ResponseTypeToolResult,
		Data:    map[string]interface{}{"tool_call_id": "search-1", "tool_name": "hybrid_search"},
		Content: "ordinary pipeline content",
	}}
	require.NoError(t, checkReplayPublication(context.Background(), classifySavedReplay{}, message, events))
}

func TestLiveSSEEmptyGraphResultStillRechecksEarlierSource(t *testing.T) {
	evt := interfaces.StreamEvent{
		Type:    types.ResponseTypeToolResult,
		Content: "No relevant graph information found.", Data: map[string]interface{}{
			"tool_name": "query_knowledge_graph", "success": true, "results": []interface{}{},
			"query": "user query", "knowledge_base_ids": []string{"ordinary"},
			"graph_configs": map[string]interface{}{}, "graph_config": map[string]interface{}{},
			"errors": []string{},
		},
	}
	require.True(t, contentlessKnowledgeToolResult(evt))
	messages := &liveHistoryMessageService{}
	require.NoError(t, checkLiveStreamPublication(context.Background(), messages,
		&types.Message{Role: "assistant"}, []interfaces.StreamEvent{evt}))
	require.Equal(t, 1, messages.checks)
	require.ErrorIs(t, checkLiveStreamPublication(context.Background(), &guardedReplayMessages{},
		&types.Message{Role: "assistant", KnowledgeReferences: types.References{{KnowledgeID: "revoked"}}},
		[]interfaces.StreamEvent{evt}), access.ErrNextcloudPublicationDenied)
	evt.Content = "opaque graph secret"
	require.False(t, contentlessKnowledgeToolResult(evt))
	require.ErrorIs(t, checkLiveStreamPublication(context.Background(), messages,
		&types.Message{Role: "assistant"}, []interfaces.StreamEvent{evt}), access.ErrNextcloudPublicationDenied)
}
