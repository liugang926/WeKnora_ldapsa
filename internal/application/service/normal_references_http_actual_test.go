package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service"
	sessionhandler "github.com/Tencent/WeKnora/internal/handler/session"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type normalReferencesActualBaseChat interface{ chat.Chat }
type normalReferencesActualModel struct {
	normalReferencesActualBaseChat
	t          *testing.T
	f          *service.NormalReferencesHTTPFixtureForTest
	calls      atomic.Int64
	inputFound atomic.Bool
	stopped    atomic.Bool
	mode       string
}

func (m *normalReferencesActualModel) GetModelID() string   { return "normal-reference-model" }
func (m *normalReferencesActualModel) GetModelName() string { return "controlled final source model" }
func (m *normalReferencesActualModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("the full ordinary fixture must use final streaming, with rewrite disabled")
}
func (m *normalReferencesActualModel) ChatStream(ctx context.Context, input []chat.Message, _ *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	require.NoError(m.t, types.CheckHistoryMaterialLifetime(ctx))
	found := false
	for _, message := range input {
		require.Equal(m.t, m.f.Lineage, message.SourceLineage, "actual final model uses the original complete collector union")
		if strings.Contains(message.Content, "Current employee policy") {
			found = true
		}
	}
	m.inputFound.Store(found)
	if !found {
		return nil, fmt.Errorf("production search/merge/prompt did not consume the actual signed document")
	}
	var held int64
	require.NoError(m.t, m.f.DB.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&held).Error)
	require.Positive(m.t, held, "original source physical pins stay held at model entry")
	if m.mode == "source-after-start" || m.mode == "source-after-start-open" {
		m.f.Revoke()
	}
	if m.mode == "lease-after-start" {
		require.NoError(m.t, m.f.DB.Exec("UPDATE nextcloud_content_leases SET expires_at_ms=1 WHERE kind='read' AND released_at_ms IS NULL").Error)
	}
	out := make(chan types.StreamResponse, 2)
	if m.mode == "source-after-start-open" {
		go func() { <-ctx.Done(); m.stopped.Store(true); close(out) }()
		return out, nil
	}
	if m.mode == "thinking-refuse" {
		out <- types.StreamResponse{ResponseType: types.ResponseTypeThinking, Content: "CONTROLLED CURRENT THINKING", Done: false}
	}
	out <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: "CURRENT ADMITTED NORMAL ANSWER", Done: true}
	if m.mode == "done-open" {
		go func() { <-ctx.Done(); close(out) }()
	} else {
		close(out)
	}
	return out, nil
}

func runNormalReferencesActualHTTP(t *testing.T, pg bool, modes ...string) {
	mode := "current"
	if len(modes) > 0 {
		mode = modes[0]
	}
	model := &normalReferencesActualModel{t: t, mode: mode}
	f := service.NewNormalReferencesHTTPFixtureForTest(t, pg, model, mode)
	model.f = f
	mini := miniredis.RunT(t)
	redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "normal-reference-actual", time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = redis.Close() })
	var sender interfaces.StreamManager = redis
	if mode == "empty-fixed-refuse" {
		sender = normalReferencesRefusingSink{StreamManager: redis}
	}
	h := sessionhandler.NewHandler(f.Sessions, f.Messages, nil, sender, f.Config, f.KBs, nil, nil, nil, nil, nil, nil, nil, f.Models, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler())
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(f.Ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.RequestIDContextKey.String(), "normal-reference-actual-request")
		c.Next()
	})
	router.POST("/knowledge-chat/:session_id", h.KnowledgeQA)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: 8 * time.Second}
	response, err := client.Post(server.URL+"/knowledge-chat/"+f.Session.ID, "application/json", strings.NewReader("{\"query\":\"current employee policy\",\"knowledge_base_ids\":[\"kb\"],\"summary_model_id\":\"normal-reference-model\",\"disable_title\":true}"))
	require.NoError(t, err)
	defer response.Body.Close()
	var body bytes.Buffer
	_, err = body.ReadFrom(response.Body)
	require.NoError(t, err, "actual normal sender must terminate within the unchanged 8-second bound")
	require.Equal(t, http.StatusOK, response.StatusCode)
	content := body.String()
	defer func() {
		require.Eventually(t, func() bool {
			var source, body int64
			return f.DB.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&source).Error == nil && f.DB.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&body).Error == nil && source == 0 && body == 0
		}, 3*time.Second, 10*time.Millisecond, "actual terminal releases producer/reader pins before fixture cleanup")
	}()
	if mode != "current" && mode != "done-open" && mode != "empty-fixed" {
		require.Contains(t, content, "\"response_type\":\"error\"")
		require.NotContains(t, content, "CURRENT ADMITTED NORMAL ANSWER")
		require.NotContains(t, content, "\"response_type\":\"complete\"")
		wantCalls := int64(0)
		if mode == "source-after-start" || mode == "source-after-start-open" || mode == "lease-after-start" || mode == "thinking-refuse" {
			wantCalls = 1
		}
		require.Equal(t, wantCalls, model.calls.Load(), "no later model starts after the actual producer boundary fails")
		var completed int64
		require.NoError(t, f.DB.Model(&types.Message{}).Where("session_id=? AND role='assistant' AND is_completed=true", f.Session.ID).Count(&completed).Error)
		require.Zero(t, completed, "a safe static error never fabricates a completed assistant")
		if mode == "source-after-start-open" {
			require.Eventually(t, func() bool { return model.stopped.Load() }, time.Second, time.Millisecond, "the started original provider actually receives cancellation and stops")
		}
		return
	}
	if mode == "empty-fixed" {
		require.Contains(t, content, "CURRENT FIXED SAFE FALLBACK")
		require.Contains(t, content, "\"response_type\":\"complete\"")
		require.NotContains(t, content, "\"response_type\":\"error\"")
		require.EqualValues(t, 0, model.calls.Load(), "the fixed code-origin fallback needs no model")
		return
	}
	refs := strings.Index(content, "\"response_type\":\"references\"")
	answer := strings.Index(content, "\"response_type\":\"answer\"")
	complete := strings.Index(content, "\"response_type\":\"complete\"")
	require.GreaterOrEqual(t, refs, 0, "actual pre-answer references producer reaches the real subscriber")
	require.Greater(t, answer, refs)
	require.Greater(t, complete, answer)
	require.Contains(t, content, "CURRENT ADMITTED NORMAL ANSWER")
	require.NotContains(t, content, "\"response_type\":\"error\"")
	require.NotContains(t, content, "source_lineage")
	require.EqualValues(t, 1, model.calls.Load())
	require.True(t, model.inputFound.Load())
	var assistant types.Message
	require.NoError(t, f.DB.Where("session_id=? AND role='assistant'", f.Session.ID).Take(&assistant).Error)
	require.True(t, assistant.IsCompleted)
	require.Equal(t, f.Lineage, assistant.SourceLineage)
	require.Len(t, assistant.KnowledgeReferences, 2)
	require.Eventually(t, func() bool {
		var source, body int64
		return f.DB.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&source).Error == nil &&
			f.DB.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&body).Error == nil && source == 0 && body == 0
	}, 3*time.Second, 10*time.Millisecond, "real producer and final sender release all owned original pins")
}

func TestNormalReferencesActualHTTPSQLite(t *testing.T)   { runNormalReferencesActualHTTP(t, false) }
func TestPostgresNormalReferencesActualHTTP(t *testing.T) { runNormalReferencesActualHTTP(t, true) }

func TestNormalReferencesActualHTTPBoundariesSQLite(t *testing.T) {
	for _, mode := range []string{"source-before-refs", "actor-before-refs", "lease-before-refs", "unknown-prefix", "references-refuse", "empty-fixed", "empty-fixed-refuse", "source-after-start", "lease-after-start", "thinking-refuse", "done-open", "source-after-start-open"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, false, mode) })
	}
}
func TestPostgresNormalReferencesActualHTTPBoundaries(t *testing.T) {
	for _, mode := range []string{"source-before-refs", "actor-before-refs", "lease-before-refs", "unknown-prefix", "references-refuse", "empty-fixed", "empty-fixed-refuse", "source-after-start", "lease-after-start", "thinking-refuse", "done-open", "source-after-start-open"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, true, mode) })
	}
}

// Inject failure at the real strict handler's output sink before any frame or
// Done listener is committed; no fake producer event authenticates the body.
type normalReferencesRefusingSink struct{ interfaces.StreamManager }

func (s normalReferencesRefusingSink) AppendEvent(ctx context.Context, session, message string, frame interfaces.StreamEvent) error {
	if frame.Type == types.ResponseTypeAnswer {
		return fmt.Errorf("controlled actual final sender refusal")
	}
	return s.StreamManager.AppendEvent(ctx, session, message, frame)
}
