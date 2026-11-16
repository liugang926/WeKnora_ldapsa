package session

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestContinueStreamWaitsForApprovedSegment(t *testing.T) {
	for _, tt := range []struct {
		name      string
		query     string
		marker    interfaces.StreamEvent
		completed bool
		status    int
	}{
		{name: "ordinary replay", status: http.StatusOK},
		{name: "old terminal segment", query: "&resume_pending_id=p", status: http.StatusConflict},
		{
			name: "different approval", query: "&resume_pending_id=p", status: http.StatusConflict,
			marker: interfaces.StreamEvent{
				Type: types.ResponseTypeAgentQuery, Data: map[string]interface{}{"resume_pending_id": "old"},
			},
		},
		{
			name: "automation resumed", query: "&resume_pending_id=p", status: http.StatusOK,
			marker: interfaces.StreamEvent{
				Type: types.ResponseTypeAgentQuery, Data: map[string]interface{}{"resume_pending_id": "p"},
			},
		},
		{
			name: "chat resumed", query: "&resume_pending_id=p", status: http.StatusOK,
			marker: interfaces.StreamEvent{
				Type: types.ResponseTypeRunResumed, Data: map[string]interface{}{"pending_id": "p"},
			},
		},
		{
			name: "terminal failure before resume marker", query: "&resume_pending_id=p",
			completed: true, status: http.StatusOK,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			r := gin.New()
			r.Use(middleware.ErrorHandler())
			h, ctx, prompt, db := newCrashRecoveryFixture(t)
			sessionID, messageID := prompt.SessionID, prompt.AssistantMessageID
			h.sessionService = &stubSessionService{}
			lineage := literalLiveSSEFixtureLineage(t)
			if tt.completed {
				message, err := h.messageService.GetMessage(ctx, sessionID, messageID)
				require.NoError(t, err)
				require.NoError(t, db.AutoMigrate(&types.CustomAgent{}))
				require.NoError(t, db.Create(&types.CustomAgent{ID: message.AgentID, TenantID: prompt.TenantID}).Error)
				message.IsCompleted = true
				message.SourceLineage = lineage
				require.NoError(t, h.messageService.UpdateMessage(ctx, message))
			}
			if tt.marker.Type != "" {
				tt.marker.SourceLineage = lineage
				require.NoError(t, h.streamManager.AppendEvent(ctx, sessionID, messageID, tt.marker))
			}
			require.NoError(t, h.streamManager.AppendEvent(ctx, sessionID, messageID,
				interfaces.StreamEvent{Type: types.ResponseTypeComplete, SourceLineage: lineage, Done: true}))
			r.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
			recorder := httptest.NewRecorder()
			url := "/sessions/continue-stream/" + sessionID + "?message_id=" + messageID + tt.query
			request := httptest.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			r.ServeHTTP(recorder, request)
			require.Equal(t, tt.status, recorder.Code, recorder.Body.String())
			if tt.status == http.StatusConflict {
				require.NotContains(t, recorder.Body.String(), `"response_type":"complete"`)
				require.NotContains(t, recorder.Header().Get("Content-Type"), "text/event-stream")
			} else {
				require.Contains(t, recorder.Body.String(), `"response_type":"complete"`)
			}
		})
	}
}
