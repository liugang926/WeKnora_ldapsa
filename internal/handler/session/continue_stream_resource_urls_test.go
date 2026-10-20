package session

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// The stubs below implement only the methods ContinueStream reaches; embedding
// each interface keeps everything else nil-panicky so an un-stubbed call fails
// loudly rather than returning a zero value.

type stubSessionService struct {
	interfaces.SessionService
}

func (s *stubSessionService) GetSession(_ context.Context, id string) (*types.Session, error) {
	return &types.Session{ID: id, TenantID: 1}, nil
}

type stubMessageServiceForStream struct {
	interfaces.MessageService
}

func (s *stubMessageServiceForStream) GetMessage(
	_ context.Context, sessionID, messageID string,
) (*types.Message, error) {
	return &types.Message{ID: messageID, SessionID: sessionID, RequestID: "req-1"}, nil
}

// stubStreamManager replays a fixed event list, mimicking a completed stream.
type stubStreamManager struct {
	events []interfaces.StreamEvent
}

func (s *stubStreamManager) AppendEvent(
	context.Context, string, string, interfaces.StreamEvent,
) error {
	return nil
}

func (s *stubStreamManager) GetEvents(
	_ context.Context, _, _ string, fromOffset int,
) ([]interfaces.StreamEvent, int, error) {
	if fromOffset >= len(s.events) {
		return nil, len(s.events), nil
	}
	return s.events[fromOffset:], len(s.events), nil
}

func (s *stubStreamManager) AppendSteerEvents(
	context.Context, string, string, []interfaces.StreamEvent,
) error {
	return nil
}

func (s *stubStreamManager) GetSteerEvents(
	_ context.Context, _, _ string, fromOffset int,
) ([]interfaces.StreamEvent, int, error) {
	return nil, fromOffset, nil
}

func (s *stubStreamManager) UpdateSteerEventData(
	context.Context, string, string, string, map[string]interface{},
) (bool, error) {
	return false, nil
}

func (s *stubStreamManager) DeleteSteerEvent(
	context.Context, string, string, string,
) (bool, error) {
	return false, nil
}

func (s *stubStreamManager) SetLiveRun(context.Context, string, string, string) error {
	return nil
}

func (s *stubStreamManager) ClaimLiveRun(context.Context, string, string, string) error {
	return nil
}

func (s *stubStreamManager) GetLiveRun(context.Context, string) (string, string, error) {
	return "", "", nil
}

func (s *stubStreamManager) ClearLiveRun(context.Context, string, string) error {
	return nil
}

// completedAnswerStream is one assistant turn whose answer embeds a knowledge-base
// image, with the resource handle straddling two deltas as it does in production
// when the model-context decoder flushes mid-reference.
func completedAnswerStream() []interfaces.StreamEvent {
	return []interfaces.StreamEvent{
		{ID: "answer-1", Type: types.ResponseTypeAnswer, Content: "The diagram ![fig](resource://xifDo7"},
		{ID: "answer-1", Type: types.ResponseTypeAnswer, Content: "NTSL300Lp1goVutw) shows the flow."},
		{ID: "answer-1", Type: types.ResponseTypeAnswer, Content: "", Done: true},
		{ID: "refs-1", Type: types.ResponseTypeReferences, Data: map[string]interface{}{
			"references": types.References{{
				ID:        "chunk-1",
				Content:   "figure ![f](" + testResourceHandle + ")",
				ImageInfo: `[{"url":"` + testResourceHandle + `"}]`,
			}},
		}},
		{ID: "complete-1", Type: types.ResponseTypeComplete, Done: true},
	}
}

// renderReplayCodec exercises formatting only. These fabricated Redis-like
// frames deliberately retain unknown origins; they cannot enter ContinueStream's
// authenticated original-material path. Its actual HTTP success and revocation
// coverage is in TestNativeLiveReplayActualPausedProducerReceiverAndRedisForgery
// and TestNativeSavedReplayActualPlainRedisReconnectAndLegacyReject.
func renderReplayCodec(t *testing.T, initial, polled []interfaces.StreamEvent, query string) (int, string) {
	t.Helper()
	for _, batch := range [][]interfaces.StreamEvent{initial, polled} {
		for _, frame := range batch {
			require.Error(t, frame.SourceLineage.RequireComplete(), "the codec fixture must not mint replay authority")
		}
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	h := &Handler{fileService: &stubResourceFileService{}}
	r.GET("/codec", func(c *gin.Context) {
		rewriter, err := h.resolveStreamRewriter(c)
		if err != nil {
			_ = c.Error(err)
			return
		}
		setSSEHeaders(c)
		for _, frame := range coalesceReplayEvents(initial) {
			emitStreamEvent(c.Request.Context(), c, frame, "req-1", rewriter)
		}
		// Keep the same holdback buffer for later polled deltas.
		for _, frame := range polled {
			emitStreamEvent(c.Request.Context(), c, frame, "req-1", rewriter)
		}
	})
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/codec?"+strings.TrimPrefix(query, "&"), nil))
	return recorder.Code, recorder.Body.String()
}

func continueUnknownReplay(t *testing.T, query string) (int, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	h := &Handler{
		sessionService: &stubSessionService{}, messageService: &stubMessageServiceForStream{},
		streamManager: &stubStreamManager{events: completedAnswerStream()}, fileService: &stubResourceFileService{},
	}
	r.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(
		http.MethodGet, "/sessions/continue-stream/sess1?message_id=msg1"+query, nil,
	))
	return recorder.Code, recorder.Body.String()
}

// Default behaviour: the SSE stream keeps carrying internal handles, which the
// WeKnora frontend resolves through the authenticated /files proxy.
func TestReplayCodec_DefaultEmitsHandles(t *testing.T) {
	code, body := renderReplayCodec(t, completedAnswerStream(), nil, "")
	require.Equal(t, http.StatusOK, code, body)

	assert.Contains(t, body, testResourceHandle)
	assert.NotContains(t, body, "cdn.example.com")
}

// With resource_urls=public the whole stream — answer text and the references
// payload — carries URLs a third-party app can load directly.
func TestReplayCodec_PublicModeEmitsLoadableURLs(t *testing.T) {
	code, body := renderReplayCodec(t, completedAnswerStream(), nil, "&resource_urls=public")
	require.Equal(t, http.StatusOK, code, body)

	assert.NotContains(t, body, "resource://",
		"no internal handle may reach a client that asked for public URLs")
	assert.Contains(t, body, "https://cdn.example.com/signed.png")

	// The handle was split across two answer deltas; the reassembled Markdown
	// image must be intact rather than broken in half.
	assert.Contains(t, body, `![fig](https://cdn.example.com/signed.png) shows the flow.`)

	// The references payload carries both the chunk text and image_info.
	assert.Contains(t, body, `figure ![f](https://cdn.example.com/signed.png)`)
	assert.Contains(t, body, `[{\"url\":\"https://cdn.example.com/signed.png\"}]`)

	// Every answer delta must precede the completion marker.
	assert.Less(t, strings.LastIndex(body, `"response_type":"answer"`),
		strings.Index(body, `"response_type":"complete"`))
}

func TestContinueStream_RejectsInvalidResourceURLMode(t *testing.T) {
	code, body := continueUnknownReplay(t, "&resource_urls=signed")
	assert.Equal(t, http.StatusBadRequest, code, body)
	assert.Contains(t, body, "resource_urls")
}

func (s *stubMessageServiceForStream) CheckLiveMessagePublication(context.Context, *types.Message) error {
	return nil
}

func (s *stubMessageServiceForStream) CheckMessagePublication(context.Context, *types.Message) error {
	return nil
}

func TestContinueStreamUnknownFramesCannotBypassOriginalMaterial(t *testing.T) {
	code, body := continueUnknownReplay(t, "&resource_urls=public")
	require.Equal(t, http.StatusForbidden, code, body)
	require.NotContains(t, body, "The diagram")
	require.NotContains(t, body, testResourceHandle)
	require.NotContains(t, body, "signed.png")
}
