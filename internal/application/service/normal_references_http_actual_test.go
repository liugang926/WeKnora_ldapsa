package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
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
	t              *testing.T
	f              *service.NormalReferencesHTTPFixtureForTest
	calls          atomic.Int64
	inputFound     atomic.Bool
	stopped        atomic.Bool
	mode           string
	started        chan struct{}
	emptyPolls     *atomic.Int64
	idleMeasured   atomic.Bool
	idleRenewals   atomic.Int64
	idleHeldBodies atomic.Int64
}

func (m *normalReferencesActualModel) GetModelID() string   { return "normal-reference-model" }
func (m *normalReferencesActualModel) GetModelName() string { return "controlled final source model" }
func (m *normalReferencesActualModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("the full ordinary fixture must use final streaming, with rewrite disabled")
}
func (m *normalReferencesActualModel) ChatStream(ctx context.Context, input []chat.Message, _ *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	defer close(m.started)
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
	if m.mode == "caller-disconnect" {
		m.f.Cancel()
	}
	out := make(chan types.StreamResponse, 2)
	if m.mode == "idle-current" || m.mode == "idle-source-revoke" {
		var heldBodies int64
		require.NoError(m.t, m.f.DB.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&heldBodies).Error)
		require.Positive(m.t, heldBodies)
		beforeRenewals, beforeEmpty := m.f.BodyLeaseRenewals(), m.emptyPolls.Load()
		require.Positive(m.t, beforeRenewals, "the actual GORM observer captured successful physical body SQL renewals before idle sampling")
		m.idleHeldBodies.Store(heldBodies)
		go func() {
			deadline := time.NewTimer(3 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for m.emptyPolls.Load()-beforeEmpty < 10 {
				select {
				case <-ctx.Done():
					m.stopped.Store(true)
					close(out)
					return
				case <-deadline.C:
					close(out)
					return
				case <-ticker.C:
				}
			}
			// Actual Redis empty reads and actual SQL lease renewals, with the
			// production pipeline/holder still active. No synthetic emitter.
			renewals := m.f.BodyLeaseRenewals() - beforeRenewals
			m.idleRenewals.Store(renewals)
			m.idleMeasured.Store(true)
			if m.mode == "idle-source-revoke" {
				m.f.Revoke()
				<-ctx.Done()
				m.stopped.Store(true)
				close(out)
				return
			}
			out <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: "CURRENT ADMITTED NORMAL ANSWER", Done: true}
			close(out)
		}()
		return out, nil
	}

	if m.mode == "model-error" {
		out <- types.StreamResponse{ResponseType: types.ResponseTypeError, Content: "untrusted controlled provider diagnostic"}
		close(out)
		return out, nil
	}
	if m.mode == "eof-no-done" {
		close(out)
		return out, nil
	}

	if m.mode == "source-after-start-open" || m.mode == "producer-stop" {
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
	model := &normalReferencesActualModel{t: t, mode: mode, started: make(chan struct{})}
	f := service.NewNormalReferencesHTTPFixtureForTest(t, pg, model, mode)
	model.f = f
	mini := miniredis.RunT(t)
	redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "normal-reference-actual", time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = redis.Close() })
	observedStream := &normalReferencesObservedStreamForTest{RedisStreamManager: redis, watcher: make(chan context.Context, 1)}
	model.emptyPolls = &observedStream.emptyPolls
	var sender interfaces.StreamManager = observedStream
	if mode == "empty-fixed-refuse" {
		sender = normalReferencesRefusingSink{StreamManager: observedStream}
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
	router.POST("/sessions/:session_id/stop", h.StopSession)
	httpReturned := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/knowledge-chat/") {
			defer close(httpReturned)
		}
		router.ServeHTTP(w, req)
	}))
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: 8 * time.Second}
	started := time.Now()
	response, err := client.Post(server.URL+"/knowledge-chat/"+f.Session.ID, "application/json", strings.NewReader("{\"query\":\"current employee policy\",\"knowledge_base_ids\":[\"kb\"],\"summary_model_id\":\"normal-reference-model\",\"disable_title\":true}"))
	require.NoError(t, err)
	defer response.Body.Close()
	if mode == "producer-stop" {
		remaining := 8*time.Second - time.Since(started)
		require.Positive(t, remaining, "the unchanged HTTP deadline still bounds startup")
		select {
		case <-model.started:
		case <-time.After(remaining):
			t.Fatal("the real original provider did not start within the unchanged HTTP bound")
		}
		var assistant types.Message
		require.NoError(t, f.DB.Select("id").Where("session_id=? AND role='assistant'", f.Session.ID).Take(&assistant).Error)
		stopResponse, stopErr := client.Post(server.URL+"/sessions/"+f.Session.ID+"/stop", "application/json", strings.NewReader("{\"message_id\":\""+assistant.ID+"\"}"))
		require.NoError(t, stopErr)
		require.Equal(t, http.StatusOK, stopResponse.StatusCode, "actual owner-authorized StopSession route publishes the Redis stop event")
		stopResponse.Body.Close()
	}
	var body bytes.Buffer
	_, err = body.ReadFrom(response.Body)
	if mode != "caller-disconnect" {
		require.NoError(t, err, "actual normal sender must terminate within the unchanged 8-second bound")
	} else {
		require.Less(t, time.Since(started), 8*time.Second, "a canceled caller must terminate before the unchanged HTTP deadline")
	}
	require.Equal(t, http.StatusOK, response.StatusCode)
	content := body.String()
	defer func() {
		select {
		case <-f.ProducerReturned:
		case <-time.After(3 * time.Second):
			t.Fatal("the actual production KnowledgeQA did not return before final database assertions")
		}
		if mode == "producer-stop" {
			select {
			case <-httpReturned:
			case <-time.After(3 * time.Second):
				t.Fatal("the real owned HTTP sender did not exit")
			}
			var watcherCtx context.Context
			select {
			case watcherCtx = <-observedStream.watcher:
			case <-time.After(3 * time.Second):
				t.Fatal("the real owned stop watcher never called the actual Redis reader")
			}
			select {
			case <-watcherCtx.Done():
			case <-time.After(3 * time.Second):
				t.Fatal("the real owned stop watcher did not exit")
			}
			select {
			case <-f.CompletionReturned:
				require.ErrorIs(t, <-f.CompletionError, access.ErrNextcloudPublicationDenied, "the real completed write was refused by the original output boundary")
				errors := f.CompletionErrors()
				require.NotEmpty(t, errors)
				for _, completionErr := range errors {
					require.ErrorIs(t, completionErr, access.ErrNextcloudPublicationDenied, "every completed attempt ended with the same original-boundary denial after both delivery loops exited")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the actual StopSession completion call did not return")
			}
		}
		require.Eventually(t, func() bool {
			var source, body int64
			return f.DB.Table("nextcloud_content_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&source).Error == nil && f.DB.Table("original_body_leases").Where("kind='read' AND released_at_ms IS NULL").Count(&body).Error == nil && source == 0 && body == 0
		}, 3*time.Second, 10*time.Millisecond, "actual terminal releases producer/reader pins before fixture cleanup")
		if mode == "idle-current" || mode == "idle-source-revoke" {
			require.True(t, model.idleMeasured.Load(), "ten real empty HTTP Redis polls were observed, not fabricated callback counts")
			require.LessOrEqual(t, model.idleRenewals.Load(), 6*model.idleHeldBodies.Load(), "real empty native polls must not perform one full SQL body renewal per100ms tick")
		}
		var completed, manifests int64
		require.NoError(t, f.DB.Model(&types.Message{}).Where("session_id=? AND role='assistant' AND is_completed=true", f.Session.ID).Count(&completed).Error)
		require.NoError(t, f.DB.Table("message_completion_input_manifests c").Joins("JOIN message_material_receipts r ON r.admission_id=c.assistant_admission_id").Joins("JOIN messages m ON m.id=r.message_id").Where("m.session_id=? AND m.role='assistant'", f.Session.ID).Count(&manifests).Error)
		if mode == "current" || mode == "done-open" || mode == "empty-fixed" || mode == "caller-disconnect" || mode == "idle-current" {
			require.EqualValues(t, 1, completed, "actual permitted background production completed before the final oracle")
			require.EqualValues(t, 1, manifests, "the current completion receipt has its original immutable input manifest")
		} else {
			require.Zero(t, completed, "a refused or truly stopped producer never fabricates a completed assistant after it returns")
			require.Zero(t, manifests, "a refused or truly stopped producer has no current completion manifest")
		}
	}()
	if mode != "current" && mode != "done-open" && mode != "empty-fixed" && mode != "caller-disconnect" && mode != "idle-current" {
		if mode != "producer-stop" {
			require.Contains(t, content, "\"response_type\":\"error\"")
		}
		require.NotContains(t, content, "CURRENT ADMITTED NORMAL ANSWER")
		require.NotContains(t, content, "untrusted controlled provider diagnostic")
		require.NotContains(t, content, "\"response_type\":\"complete\"")
		wantCalls := int64(0)
		if mode == "source-after-start" || mode == "source-after-start-open" || mode == "lease-after-start" || mode == "thinking-refuse" || mode == "caller-disconnect" || mode == "model-error" || mode == "eof-no-done" || mode == "producer-stop" || mode == "idle-source-revoke" {
			wantCalls = 1
		}
		require.Equal(t, wantCalls, model.calls.Load(), "no later model starts after the actual producer boundary fails")
		if mode == "source-after-start-open" || mode == "producer-stop" || mode == "idle-source-revoke" {
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
	if mode == "caller-disconnect" {
		require.EqualValues(t, 1, model.calls.Load(), "HTTP caller cancellation preserves the owned background producer")
		require.True(t, model.inputFound.Load())
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

func TestNormalReferencesActualHTTPRemainingAsyncSQLite(t *testing.T) {
	for _, mode := range []string{"caller-disconnect", "producer-stop", "model-error", "eof-no-done"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, false, mode) })
	}
}
func TestPostgresNormalReferencesActualHTTPRemainingAsync(t *testing.T) {
	for _, mode := range []string{"caller-disconnect", "producer-stop", "model-error", "eof-no-done"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, true, mode) })
	}
}

// Observe the actual production stop watcher's owned context. GetEvents still
// reads the real Redis queue unchanged; its context is cancelled by the real
// watcher defer only after its synchronous stop callback returns.
type normalReferencesObservedStreamForTest struct {
	*stream.RedisStreamManager
	watcher    chan context.Context
	once       sync.Once
	emptyPolls atomic.Int64
}

func (s *normalReferencesObservedStreamForTest) GetEvents(ctx context.Context, session, message string, offset int) ([]interfaces.StreamEvent, int, error) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > time.Hour {
		s.once.Do(func() { s.watcher <- ctx })
	}
	events, next, err := s.RedisStreamManager.GetEvents(ctx, session, message, offset)
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < time.Minute && err == nil && len(events) == 0 {
		s.emptyPolls.Add(1)
	}
	return events, next, err
}

func TestNormalReferencesActualHTTPIdleMaterialSQLite(t *testing.T) {
	for _, mode := range []string{"idle-current", "idle-source-revoke"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, false, mode) })
	}
}
func TestPostgresNormalReferencesActualHTTPIdleMaterial(t *testing.T) {
	for _, mode := range []string{"idle-current", "idle-source-revoke"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, true, mode) })
	}
}
