package session

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const continuePollHeldMarker = "fictional-held-secret"

// Reuse the existing ContinueStream service stub, replacing only lookup and
// publication verification. The known Nextcloud reference is persisted before
// the request begins; this is not an unidentified or preparatory tool frame.
type continuePollPublicationMessages struct {
	stubMessageServiceForStream
	revoked          bool
	publicationError error
	pollReached      bool
	lookups          int
	liveChecks       int
	postPollChecks   int
	historicalChecks int
	missingReference bool
}

func (s *continuePollPublicationMessages) GetMessageForStream(
	_ context.Context, sessionID, messageID string,
) (*types.Message, error) {
	s.lookups++
	return &types.Message{
		ID: messageID, SessionID: sessionID, RequestID: "fictional-poll-turn",
		Role: "assistant", IsCompleted: false,
		KnowledgeReferences: types.References{{
			KnowledgeID:      "fictional-nextcloud-document",
			KnowledgeChannel: types.ConnectorTypeNextcloud,
		}},
	}, nil
}

func (s *continuePollPublicationMessages) CheckLiveMessagePublication(
	_ context.Context, message *types.Message,
) error {
	s.liveChecks++
	if s.pollReached {
		s.postPollChecks++
	}
	if message == nil || len(message.KnowledgeReferences) != 1 ||
		message.KnowledgeReferences[0] == nil ||
		message.KnowledgeReferences[0].KnowledgeID != "fictional-nextcloud-document" ||
		message.KnowledgeReferences[0].KnowledgeChannel != types.ConnectorTypeNextcloud {
		s.missingReference = true
		return access.ErrNextcloudPublicationUnavailable
	}
	if s.revoked {
		return access.ErrNextcloudPublicationDenied
	}
	if s.publicationError != nil {
		return s.publicationError
	}
	return nil
}

func (s *continuePollPublicationMessages) CheckMessagePublication(context.Context, *types.Message) error {
	s.historicalChecks++
	return access.ErrNextcloudPublicationUnavailable
}

// Exercise ContinueStream's real polling boundary. The first answer is neither
// Done nor complete, so public-mode rewriting retains the incomplete Markdown
// image (including its fictional alt text). Only the second read changes the
// current publication decision; the client connection remains active.
type continuePollPublicationStreams struct {
	stubStreamManager
	messages           *continuePollPublicationMessages
	recorder           *httptest.ResponseRecorder
	revoke             bool
	unavailable        bool
	cancel             context.CancelFunc
	pollError          bool
	reads              int
	prefixBeforeSecond bool
	markerBeforeSecond bool
	activeAtSecond     bool
	offsetAtSecond     int
}

func (s *continuePollPublicationStreams) GetEvents(
	ctx context.Context, _, _ string, offset int,
) ([]interfaces.StreamEvent, int, error) {
	s.reads++
	if s.reads == 1 {
		return []interfaces.StreamEvent{chunk("fictional-answer", types.ResponseTypeAnswer,
			"authorized prefix !["+continuePollHeldMarker+"](resource://xifDo7")}, 1, nil
	}
	s.prefixBeforeSecond = strings.Contains(s.recorder.Body.String(), "authorized prefix")
	s.markerBeforeSecond = strings.Contains(s.recorder.Body.String(), continuePollHeldMarker)
	s.activeAtSecond = ctx.Err() == nil
	s.offsetAtSecond = offset
	s.messages.pollReached = true
	s.messages.revoked = s.revoke
	if s.unavailable {
		s.messages.publicationError = access.ErrNextcloudPublicationUnavailable
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.pollError {
		return nil, offset, errors.New("fictional event-store read failure")
	}
	return []interfaces.StreamEvent{{ID: "fictional-complete", Type: types.ResponseTypeComplete, Done: true}}, 2, nil
}

func TestContinueStreamPollFailureRechecksHeldPublication(t *testing.T) {
	for _, tc := range []struct {
		name         string
		revoke       bool
		pollError    bool
		unavailable  bool
		cancelClient bool
		expectHeld   bool
		requireFresh bool
	}{
		{name: "revoked_source_on_poll_error", revoke: true, pollError: true, requireFresh: true},
		{name: "authorized_source_on_poll_error", pollError: true, expectHeld: true},
		{name: "revoked_source_on_successful_poll", revoke: true, requireFresh: true},
		{name: "unavailable_authorization_on_poll_error", pollError: true, unavailable: true, requireFresh: true},
		{name: "cancelled_client_on_poll_error", pollError: true, cancelClient: true, requireFresh: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			messages := &continuePollPublicationMessages{}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			streams := &continuePollPublicationStreams{
				messages: messages, recorder: recorder, revoke: tc.revoke, pollError: tc.pollError,
				unavailable: tc.unavailable,
			}
			if tc.cancelClient {
				streams.cancel = cancel
			}
			h := &Handler{
				sessionService: &stubSessionService{}, messageService: messages,
				streamManager: streams, fileService: &stubResourceFileService{},
			}
			router := gin.New()
			router.Use(middleware.ErrorHandler())
			router.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
			request := httptest.NewRequest(http.MethodGet,
				"/sessions/continue-stream/sess1?message_id=msg1&resource_urls=public", nil).WithContext(ctx)
			router.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Code)
			require.Equal(t, 1, messages.lookups)
			require.Equal(t, 2, streams.reads)
			require.Equal(t, 1, streams.offsetAtSecond)
			require.True(t, streams.activeAtSecond, "cancellation must not suppress the tested flush")
			require.True(t, streams.prefixBeforeSecond, "initial authorized content must really reach SSE")
			require.False(t, streams.markerBeforeSecond, "the marker must really be held before revocation")
			require.False(t, messages.missingReference, "publication checks must see the persisted source identity")
			require.GreaterOrEqual(t, messages.liveChecks, 1)
			require.Zero(t, messages.historicalChecks, "this test must use the live-message checker")
			if tc.cancelClient {
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			} else {
				require.NoError(t, ctx.Err(), "non-cancellation controls must not pass by a closed connection")
			}
			if tc.expectHeld {
				assert.Contains(t, recorder.Body.String(), continuePollHeldMarker,
					"an authorized event-store failure must preserve the ordinary held-tail behavior")
			} else {
				assert.NotContains(t, recorder.Body.String(), continuePollHeldMarker,
					"revoked source text must not be released from the real holdback buffer")
			}
			if tc.requireFresh {
				assert.GreaterOrEqual(t, messages.postPollChecks, 1,
					"the current publication decision must be checked after the second read changes it")
			}
		})
	}
}
