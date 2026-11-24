package session

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"

	"github.com/stretchr/testify/require"
)

func TestAgentTurnFailureDoesNotPersistCompletedMessage(t *testing.T) {
	require.False(t, shouldCompleteAgentTurn(true, nil))
	require.True(t, shouldCompleteAgentTurn(false, nil))
	require.True(t, shouldCompleteAgentTurn(true, context.Canceled),
		"a user-requested stop preserves the existing partial-turn completion behavior")
}

func TestAgentRunPublishesSingleFailureControl(t *testing.T) {
	ctx := context.Background()
	stream := &capturingStreamManager{}
	handler := &AgentStreamHandler{ctx: ctx, sessionID: "session", requestID: "request", streamManager: stream}
	failure := event.Event{
		SessionID: "session", RequestID: "request",
		Data: event.ErrorData{Error: "unpublished detail"},
	}
	require.NoError(t, handler.handleError(ctx, failure))
	require.NoError(t, handler.handleError(ctx, failure))
	require.Len(t, stream.events, 1)
	require.Equal(t, "generation_failed", stream.events[0].Data["error_code"])
	require.NotContains(t, stream.events[0].Content, "unpublished")
}

type failedTurnReplay struct {
	interfaces.MessageService
}

func (failedTurnReplay) BeginCompletedMessageMaterialReplay(
	context.Context, string, string,
) (*types.Message, func() error, func(), error) {
	return &types.Message{
		ID: "msg", SessionID: "session", RequestID: "request", Role: "assistant",
		RunState: types.MessageRunStateFailed, Content: "unpublished detail",
	}, nil, nil, nil
}

func TestReconnectFailedAgentTurnOnlyReturnsTerminalControl(t *testing.T) {
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Request = httptest.NewRequest("GET", "/stream", nil)
	h := &Handler{messageService: failedTurnReplay{}}
	require.True(t, h.tryNativeSavedReplay(c.Request.Context(), c, "session", "msg", nil))
	body := response.Body.String()
	require.Contains(t, body, "generation_failed")
	require.Contains(t, body, `"done":true`)
	require.Equal(t, 1, strings.Count(body, `"response_type":"error"`))
	require.NotContains(t, body, "unpublished detail")
}
