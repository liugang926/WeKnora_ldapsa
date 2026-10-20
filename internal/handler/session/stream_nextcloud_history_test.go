package session

import (
	"context"
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

type replayHistoryMessageService struct {
	interfaces.MessageService
	checked         []string
	expectedLineage *types.SourceLineage
}

func (s *replayHistoryMessageService) GetMessage(
	_ context.Context,
	sessionID, messageID string,
) (*types.Message, error) {
	return &types.Message{ID: messageID, SessionID: sessionID, RequestID: "turn", Role: "assistant"}, nil
}

func (s *replayHistoryMessageService) CheckMessagePublication(_ context.Context, message *types.Message) error {
	if !samePublicationFixtureLineage(message.SourceLineage, s.expectedLineage) {
		return access.ErrNextcloudPublicationDenied
	}
	for _, ref := range message.KnowledgeReferences {
		if ref != nil {
			s.checked = append(s.checked, ref.KnowledgeID)
			if ref.KnowledgeChannel == types.ConnectorTypeNextcloud {
				return access.ErrNextcloudPublicationDenied
			}
		}
	}
	return nil
}

func TestContinueStreamChecksReferencesBeforeFirstSSEFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)
	lineage := controlledLiveSSEFixtureLineage(t, "nextcloud-document")
	svc := &replayHistoryMessageService{expectedLineage: lineage}
	h := &Handler{
		sessionService: &stubSessionService{},
		messageService: svc,
		streamManager: &stubStreamManager{events: []interfaces.StreamEvent{
			{SourceLineage: lineage, Type: types.ResponseTypeAnswer, Content: "secret answer", Done: true},
			{
				SourceLineage: lineage, Type: types.ResponseTypeReferences,
				Data: map[string]interface{}{"references": types.References{{
					KnowledgeID: "nextcloud-document", KnowledgeChannel: types.ConnectorTypeNextcloud,
				}}},
			},
			{SourceLineage: lineage, Type: types.ResponseTypeComplete, Done: true},
		}},
	}
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
		"/sessions/continue-stream/sess1?message_id=msg1", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked replay should return 403, got %d: %s", w.Code, w.Body.String())
	}
	if len(svc.checked) != 1 || svc.checked[0] != "nextcloud-document" {
		t.Fatalf("SSE references were not checked: %v", svc.checked)
	}
	if body := w.Body.String(); strings.Contains(body, "secret answer") ||
		strings.Contains(body, "nextcloud-document") {
		t.Fatalf("replay leaked source content: %s", body)
	}
}

func (s *replayHistoryMessageService) CheckLiveMessagePublication(ctx context.Context, message *types.Message) error {
	return s.CheckMessagePublication(ctx, message)
}

// The expected tuple is independently supplied by these protocol fixtures at
// construction. This checker never derives a receipt from result Data or DB.
func samePublicationFixtureLineage(actual, expected *types.SourceLineage) bool {
	if actual.RequireComplete() != nil || expected.RequireComplete() != nil ||
		len(actual.Sources) != len(expected.Sources) {
		return false
	}
	for i, source := range actual.Sources {
		if source != expected.Sources[i] {
			return false
		}
	}
	return true
}

func TestContinueStreamMissingProducerReceiptNeverChecksReferences(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &replayHistoryMessageService{
		expectedLineage: controlledLiveSSEFixtureLineage(t, "nextcloud-document"),
	}
	h := &Handler{
		sessionService: &stubSessionService{}, messageService: svc,
		streamManager: &stubStreamManager{events: []interfaces.StreamEvent{{
			Type: types.ResponseTypeReferences,
			Data: map[string]interface{}{"references": types.References{{
				KnowledgeID: "nextcloud-document", KnowledgeChannel: types.ConnectorTypeNextcloud,
			}}},
		}}},
	}
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
		"/sessions/continue-stream/sess1?message_id=msg1", nil))
	require.Equal(t, http.StatusForbidden, w.Code)
	require.Empty(t, svc.checked, "unknown producer cannot be authenticated from result identities")
	require.NotContains(t, w.Body.String(), "nextcloud-document")
}
