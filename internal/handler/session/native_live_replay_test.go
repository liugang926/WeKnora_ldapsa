package session

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestNativeLiveMetadataPeekExpiryAndBodyIdentityRecheck(t *testing.T) {
	for _, mode := range []string{
		"expired_after_peek",
		"body_changed_after_peek",
		"lineage_changed_after_peek",
		"closed_after_peek",
	} {
		t.Run(mode, func(t *testing.T) {
			body := []byte(`{"content":"native actual producer bytes"}`)
			entry := nativeLiveFrame{
				Sequence: 1,
				Body:     body,
				Digest:   sha256.Sum256(body),
				Parents:  []byte(`[{"metadata_digest":"original"}]`),
				Lineage:  []byte(`{"version":1,"state":"complete","sources":[]}`),
				Expires:  time.Now().Add(time.Minute),
			}
			run := &nativeLiveRun{frames: []nativeLiveFrame{entry}, expires: time.Now().Add(time.Minute)}
			metadata, err := run.read(0)
			require.NoError(t, err)
			require.Len(t, metadata, 1)
			require.Nil(t, metadata[0].Body, "metadata planning must not copy raw native frame bytes before leases")
			switch mode {
			case "expired_after_peek":
				run.frames[0].Expires = time.Now().Add(-time.Second)
			case "body_changed_after_peek":
				run.frames[0].Body = []byte(`{"content":"foreign bytes"}`)
			case "lineage_changed_after_peek":
				run.frames[0].Lineage = []byte(`{"version":1,"state":"unknown","sources":[]}`)
			case "closed_after_peek":
				eraseNativeLiveRun(run)
			}
			_, err = run.readBody(metadata[0])
			require.Error(t, err, "an old metadata plan must never certify changed or expired native bytes")
			if mode == "closed_after_peek" {
				require.Empty(t, run.frames)
				require.Zero(t, run.bytes)
			}
		})
	}
}

type admittedResumeInputs struct {
	interfaces.MessageService
	nativeLiveMaterialService
	parent *types.MessageMaterialSnapshot
}

func (m *admittedResumeInputs) CaptureNativeLiveMaterialParents(
	context.Context,
) ([]*types.MessageMaterialSnapshot, error) {
	return []*types.MessageMaterialSnapshot{m.parent}, nil
}

func TestNativeReplayRequiresCurrentApprovedSegment(t *testing.T) {
	h, ctx, originalPrompt, db := newCrashRecoveryFixture(t)
	h.sessionService = &stubSessionService{}
	session := originalPrompt.SessionID
	user, err := h.messageService.CreateMessage(ctx, &types.Message{
		SessionID: session, Role: "user", RequestID: "explicit-turn", Content: "question", IsCompleted: true,
	})
	require.NoError(t, err)
	answer, err := h.messageService.CreateMessage(ctx, &types.Message{
		SessionID: session, Role: "assistant", RequestID: user.RequestID,
		ExecutionContext: types.MessageExecutionContext{ExplicitInputMaterials: true},
	})
	require.NoError(t, err)
	store := recoveryMaterialStore(db)
	request := types.MessageMaterialReadRequest{TenantID: 1, SessionID: session, MessageIDs: []string{user.ID}}
	plan, err := store.PeekMessageMaterialReadPlan(ctx, request)
	require.NoError(t, err)
	readCtx, err := repository.WithMessageMaterialReadPlan(ctx, plan)
	require.NoError(t, err)
	parent, err := store.CaptureMessageMaterialInputs(readCtx, request)
	require.NoError(t, err)
	parent.Messages = nil
	h.messageService = &admittedResumeInputs{h.messageService, h.messageService.(nativeLiveMaterialService), parent}
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.GET("/continue/:session_id", h.ContinueStream)
	replay := func(pending string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		r.ServeHTTP(rec, httptest.NewRequestWithContext(requestCtx, http.MethodGet,
			"/continue/"+session+"?message_id="+answer.ID+"&resume_pending_id="+pending, nil))
		return rec
	}
	start := func(pending, content string) *nativeLiveAppender {
		var resume *types.AgentResume
		if pending != "" {
			resume = &types.AgentResume{Prompt: &types.AgentPrompt{ID: pending}}
		}
		bus := event.NewEventBus()
		handler := h.setupStreamHandler(ctx, session, answer.ID, answer.RequestID, 1, time.Now(), answer, bus, resume)
		appender := handler.streamManager.(*nativeLiveAppender)
		require.NoError(t, appender.registrationErr)
		t.Cleanup(func() { eraseNativeLiveRun(appender.run) })
		receipt := literalLiveSSEFixtureLineage(t)
		require.NoError(t, bus.Emit(ctx, event.Event{
			Type: event.EventAgentFinalAnswer, SourceLineage: receipt,
			Data: event.AgentFinalAnswerData{Content: content, Done: true},
		}))
		require.NoError(t, bus.Emit(ctx, event.Event{
			Type: event.EventAgentComplete, SourceLineage: receipt,
			Data: event.AgentCompleteData{
				MessageID: answer.ID, Suspended: &types.PromptSuspension{PromptID: content},
			},
		}))
		h.completeStreamAssistantMessage(ctx, &sseStreamContext{
			eventBus: bus, streamHandler: handler, assistantMessage: answer,
		}, "", "")
		return appender
	}
	old := start("", "first-segment")
	require.Equal(t, http.StatusConflict, replay("first-segment").Code)
	cur := start("first-segment", "second-segment")
	require.NotSame(t, old.run, cur.run)
	require.Error(t, old.AppendEvent(ctx, session, answer.ID, interfaces.StreamEvent{
		Type: types.ResponseTypeAnswer, Content: "late", SourceLineage: literalLiveSSEFixtureLineage(t),
	}))
	resp := replay("first-segment")
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	require.Contains(t, resp.Body.String(), "second-segment")
	require.Contains(t, resp.Body.String(), `"response_type":"complete"`)
	require.NotContains(t, resp.Body.String(), `"content":"first-segment"`)
	require.Equal(t, http.StatusConflict, replay("wrong-prompt").Code)
	require.NoError(t, db.Model(&types.Session{}).Where("id=?", session).Update("user_id", "other").Error)
	require.NotContains(t, replay("first-segment").Body.String(), "second-segment")
}
