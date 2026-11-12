package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
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

const normalReferencesHTTPReturnTimeout = 15 * time.Second

type normalReferencesActualModel struct {
	t                     *testing.T
	f                     *service.NormalReferencesHTTPFixtureForTest
	calls                 atomic.Int64
	inputFound            atomic.Bool
	stopped               atomic.Bool
	mode                  string
	started               chan struct{}
	emptyPolls            *atomic.Int64
	consecutiveEmptyPolls *atomic.Int64
	idleMeasured          atomic.Bool
	idleHeldBodies        atomic.Int64
	idleRenewals          atomic.Int64
	idleEmptyPolls        atomic.Int64
	lateIdleFaultApplied  atomic.Bool
}

func (m *normalReferencesActualModel) GetModelID() string   { return "normal-reference-model" }
func (m *normalReferencesActualModel) GetModelName() string { return "controlled final source model" }

func (m *normalReferencesActualModel) Chat(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("the full ordinary fixture must use final streaming, with rewrite disabled")
}

// The provider observes the actual current stream lifecycle. Nextcloud read
// ACL callbacks are retired; generic material lifetime checks remain active.
func (m *normalReferencesActualModel) ChatStream(
	ctx context.Context,
	input []chat.Message,
	_ *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	defer close(m.started)
	if err := types.CheckHistoryMaterialLifetime(ctx); err != nil {
		return nil, err
	}
	for _, message := range input {
		if !reflect.DeepEqual(message.SourceLineage, m.f.Lineage) {
			return nil, fmt.Errorf("actual provider source union does not match captured original")
		}
		if strings.Contains(message.Content, "Current employee policy") {
			m.inputFound.Store(true)
		}
	}

	if !m.inputFound.Load() {
		return nil, fmt.Errorf("actual retrieval did not reach provider")
	}
	if strings.HasPrefix(m.mode, "source-after-start") {
		m.f.Revoke()
	}
	if m.mode == "lease-after-start" {
		if err := m.f.DB.Exec(
			"UPDATE nextcloud_content_leases SET expires_at_ms=1 WHERE kind='read' AND released_at_ms IS NULL",
		).Error; err != nil {
			return nil, err
		}
	}
	if m.mode == "caller-disconnect" {
		m.f.Cancel()
	}
	out := make(chan types.StreamResponse, 2)
	if m.mode == "producer-stop" {
		go func() { <-ctx.Done(); m.stopped.Store(true); close(out) }()
		return out, nil
	}
	go func() {
		defer close(out)
		if strings.Contains(m.mode, "idle") {
			deadline := time.NewTimer(3 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for m.consecutiveEmptyPolls.Load() < 3 {
				select {
				case <-ctx.Done():
					m.stopped.Store(true)
					return
				case <-deadline.C:
					return
				case <-ticker.C:
				}
			}
			if m.mode == "idle-current" || m.mode == "idle-source-revoke" {
				var heldBodies int64
				if err := m.f.DB.Table("original_body_leases").
					Where("kind='read' AND released_at_ms IS NULL").Count(&heldBodies).Error; err != nil {
					m.t.Error(err)
					return
				}
				if heldBodies == 0 || m.f.BodyLeaseRenewals() == 0 {
					m.t.Error("idle sampling requires actual held and renewed original bodies")
					return
				}
				beforeRenewals, beforeEmpty := m.f.BodyLeaseRenewals(), m.emptyPolls.Load()
				m.idleHeldBodies.Store(heldBodies)
				for m.emptyPolls.Load()-beforeEmpty < 10 {
					select {
					case <-ctx.Done():
						m.stopped.Store(true)
						return
					case <-deadline.C:
						return
					case <-ticker.C:
					}
				}
				m.idleRenewals.Store(m.f.BodyLeaseRenewals() - beforeRenewals)
				m.idleEmptyPolls.Store(m.emptyPolls.Load() - beforeEmpty)
			}
			m.idleMeasured.Store(true)
			if m.mode == "idle-source-revoke" {
				m.f.Revoke()
			}
			if m.mode == "late-idle-actor" {
				if err := m.f.DB.Model(&types.User{}).Where("id=?", m.f.Session.UserID).
					Update("is_active", false).Error; err != nil {
					m.t.Error(err)
					return
				}
				m.lateIdleFaultApplied.Store(true)
			}
			if m.mode == "late-idle-body-lease" {
				if err := m.f.DB.Exec(
					"UPDATE original_body_leases SET expires_at_ms=1 WHERE kind='read' AND released_at_ms IS NULL",
				).Error; err != nil {
					m.t.Error(err)
					return
				}
				m.lateIdleFaultApplied.Store(true)
			}
		}
		if m.mode == "eof-no-done" {
			return
		}
		if m.mode == "model-error" {
			out <- types.StreamResponse{
				ResponseType: types.ResponseTypeError,
				Content:      "untrusted controlled provider diagnostic",
			}
			return
		}
		if m.mode == "thinking-refuse" {
			out <- types.StreamResponse{
				ResponseType: types.ResponseTypeThinking,
				Content:      "CONTROLLED CURRENT THINKING",
			}
		}
		select {
		case out <- types.StreamResponse{
			ResponseType: types.ResponseTypeAnswer,
			Content:      "CURRENT ADMITTED NORMAL ANSWER",
			Done:         true,
		}:
		case <-ctx.Done():
			m.stopped.Store(true)
			return
		}
		if m.mode == "done-open" {
			<-ctx.Done()
		}
	}()
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
	observedStream := &normalReferencesObservedStreamForTest{
		RedisStreamManager: redis,
		watcher:            make(chan context.Context, 1),
	}
	model.emptyPolls = &observedStream.emptyPolls
	model.consecutiveEmptyPolls = &observedStream.consecutiveEmptyPolls
	var sender interfaces.StreamManager = observedStream
	if mode == "empty-fixed-refuse" {
		sender = normalReferencesRefusingSink{StreamManager: observedStream}
	}
	h := sessionhandler.NewHandler(
		f.Sessions,
		f.Messages,
		nil,
		sender,
		f.Config,
		f.KBs,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		f.Models,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

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
	server := httptest.NewServer(router)
	t.Cleanup(func() { f.Cancel(); server.CloseClientConnections(); server.Close() })
	client := &http.Client{Timeout: 8 * time.Second}
	response, err := client.Post(
		server.URL+"/knowledge-chat/"+f.Session.ID,
		"application/json",
		strings.NewReader(`{"query":"current employee policy",`+
			`"knowledge_base_ids":["kb"],"summary_model_id":"normal-reference-model","disable_title":true}`),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	if mode == "producer-stop" {
		select {
		case <-model.started:
		case <-time.After(5 * time.Second):
			t.Fatal("provider did not start")
		}
		var assistant types.Message
		require.NoError(t, f.DB.Where("session_id=? AND role='assistant'", f.Session.ID).Take(&assistant).Error)
		r, e := client.Post(
			server.URL+"/sessions/"+f.Session.ID+"/stop",
			"application/json",
			strings.NewReader(`{"message_id":"`+assistant.ID+`"}`),
		)
		require.NoError(t, e)
		require.Equal(t, http.StatusOK, r.StatusCode)
		require.NoError(t, r.Body.Close())
	}
	var body bytes.Buffer
	_, err = body.ReadFrom(response.Body)
	if mode != "caller-disconnect" {
		require.NoError(t, err)
	}
	select {
	case <-f.ProducerReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("actual producer did not return")
	}
	require.Equal(t, http.StatusOK, response.StatusCode)
	content := body.String()
	require.NotContains(t, content, `"source_lineage":`, "internal lineage is not an SSE response field")
	// These are live user/material/transport boundaries, not the retired
	// Nextcloud-specific ACL or lease callbacks.
	denied := mode == "lease-before-refs" ||
		mode == "lease-after-start" ||
		mode == "late-idle-body-lease" ||
		mode == "actor-before-refs" ||
		mode == "late-idle-actor" ||
		mode == "unknown-prefix" ||
		mode == "references-refuse" ||
		mode == "empty-fixed-refuse" ||
		mode == "thinking-refuse" ||
		mode == "model-error" ||
		mode == "eof-no-done" ||
		mode == "producer-stop"
	if denied {
		require.NotContains(t, content, "CURRENT ADMITTED NORMAL ANSWER")
		if mode != "producer-stop" {
			require.Contains(t, content, `"response_type":"error"`)
		}
		require.NotContains(t, content, `"response_type":"complete"`)
		require.NotContains(t, content, "untrusted controlled provider diagnostic")
		var saved types.Message
		require.NoError(t, f.DB.Where("session_id=? AND role='assistant'", f.Session.ID).Take(&saved).Error)
		require.NotContains(
			t, saved.Content, "CURRENT ADMITTED NORMAL ANSWER",
			"refused output is not persisted as an answer",
		)
		if mode == "lease-before-refs" ||
			mode == "actor-before-refs" ||
			mode == "unknown-prefix" ||
			mode == "references-refuse" ||
			mode == "empty-fixed-refuse" {
			require.Zero(t, model.calls.Load())
		} else {
			require.EqualValues(t, 1, model.calls.Load())
		}
	} else if mode == "empty-fixed" {
		require.Contains(t, content, "CURRENT FIXED SAFE FALLBACK")
		require.Zero(t, model.calls.Load(), "configured fixed fallback must not invoke provider")
	} else if mode != "caller-disconnect" {
		refs := strings.Index(content, `"response_type":"references"`)
		answer := strings.Index(content, `"response_type":"answer"`)
		complete := strings.Index(content, `"response_type":"complete"`)
		require.GreaterOrEqual(t, refs, 0)
		require.Greater(t, answer, refs)
		require.Greater(t, complete, answer)
		require.Contains(t, content, "CURRENT ADMITTED NORMAL ANSWER")
		require.NotContains(t, content, `"response_type":"error"`)
		require.EqualValues(t, 1, model.calls.Load())
		require.True(t, model.inputFound.Load())
		var assistant types.Message
		require.NoError(t, f.DB.Where("session_id=? AND role='assistant'", f.Session.ID).Take(&assistant).Error)
		require.Equal(t, f.Lineage, assistant.SourceLineage)
		require.Len(t, assistant.KnowledgeReferences, 2)
	}
	if !denied {
		var completed int64
		require.NoError(t, f.DB.Model(&types.Message{}).
			Where("session_id=? AND role='assistant' AND is_completed=true", f.Session.ID).
			Count(&completed).Error)
		require.EqualValues(t, 1, completed, "actual permitted background production durably completed")
	}
	if strings.Contains(mode, "idle") {
		require.True(t, model.idleMeasured.Load(), "actual empty Redis reads preceded provider output")
	}
	if mode == "idle-current" || mode == "idle-source-revoke" {
		require.Positive(t, model.idleHeldBodies.Load())
		require.GreaterOrEqual(t, model.idleEmptyPolls.Load(), int64(10), "ten genuine empty Redis polls were sampled")
		require.GreaterOrEqual(t, model.idleRenewals.Load(), int64(0))
		require.LessOrEqual(t, model.idleRenewals.Load(), 6*model.idleHeldBodies.Load(),
			"real idle polls must not renew every physical body on each tick")
		t.Logf("actual idle SQL budget: empty_polls=%d held_bodies=%d renewals=%d budget=%d",
			model.idleEmptyPolls.Load(), model.idleHeldBodies.Load(), model.idleRenewals.Load(),
			6*model.idleHeldBodies.Load())
	}
	if mode == "producer-stop" {
		require.True(t, model.stopped.Load())
	}
	if strings.HasPrefix(mode, "late-idle-") {
		require.True(t, model.lateIdleFaultApplied.Load())
	}
	// Producer return and HTTP completion can precede asynchronous holder cleanup.
	require.Eventually(t, func() bool {
		for _, table := range []string{"nextcloud_content_leases", "original_body_leases"} {
			var held int64
			if f.DB.Table(table).Where("kind='read' AND released_at_ms IS NULL").Count(&held).
				Error != nil || held != 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
}

func TestNormalReferencesActualHTTPSQLite(t *testing.T)   { runNormalReferencesActualHTTP(t, false) }
func TestPostgresNormalReferencesActualHTTP(t *testing.T) { runNormalReferencesActualHTTP(t, true) }

func TestNormalReferencesActualHTTPBoundariesSQLite(t *testing.T) {
	for _, mode := range []string{
		"source-before-refs",
		"actor-before-refs",
		"lease-before-refs",
		"unknown-prefix",
		"references-refuse",
		"empty-fixed",
		"empty-fixed-refuse",
		"source-after-start",
		"lease-after-start",
		"thinking-refuse",
		"done-open",
		"source-after-start-open",
	} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, false, mode) })
	}
}

func TestPostgresNormalReferencesActualHTTPBoundaries(t *testing.T) {
	for _, mode := range []string{
		"source-before-refs",
		"actor-before-refs",
		"lease-before-refs",
		"unknown-prefix",
		"references-refuse",
		"empty-fixed",
		"empty-fixed-refuse",
		"source-after-start",
		"lease-after-start",
		"thinking-refuse",
		"done-open",
		"source-after-start-open",
	} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, true, mode) })
	}
}

// Inject failure at the real strict handler's output sink before any frame or
// Done listener is committed; no fake producer event authenticates the body.
type normalReferencesRefusingSink struct{ interfaces.StreamManager }

func (s normalReferencesRefusingSink) AppendEvent(
	ctx context.Context,
	session, message string,
	frame interfaces.StreamEvent,
) error {
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
	watcher               chan context.Context
	once                  sync.Once
	emptyPolls            atomic.Int64
	consecutiveEmptyPolls atomic.Int64
}

func (s *normalReferencesObservedStreamForTest) GetEvents(
	ctx context.Context,
	session, message string,
	offset int,
) ([]interfaces.StreamEvent, int, error) {
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > time.Hour {
		s.once.Do(func() { s.watcher <- ctx })
	}
	events, next, err := s.RedisStreamManager.GetEvents(ctx, session, message, offset)
	// The ordinary SSE context is logger.CloneContext of the request: it
	// has no deadline. The real stop watcher owns a two-hour deadline.
	deadline, hasDeadline := ctx.Deadline()
	if (!hasDeadline || time.Until(deadline) < time.Minute) && err == nil {
		if len(events) == 0 {
			s.emptyPolls.Add(1)
			s.consecutiveEmptyPolls.Add(1)
		} else {
			s.consecutiveEmptyPolls.Store(0)
		}
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

func TestNormalReferencesActualHTTPLateIdleMaterialSQLite(t *testing.T) {
	for _, mode := range []string{"late-idle-actor", "late-idle-body-lease"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, false, mode) })
	}
}

func TestPostgresNormalReferencesActualHTTPLateIdleMaterial(t *testing.T) {
	for _, mode := range []string{"late-idle-actor", "late-idle-body-lease"} {
		t.Run(mode, func(t *testing.T) { runNormalReferencesActualHTTP(t, true, mode) })
	}
}
