package handler

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

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/config"
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

const nativeRunningOriginal = "NATIVE RUNNING ORIGINAL SECRET ![image](resource://xifDo7NTSL300Lp1goVutw)"

type nativeRunningModel struct {
	f      *actualHTTPTailFixture
	t      *testing.T
	finish chan struct{}
	once   sync.Once
}

func (m *nativeRunningModel) GetModelName() string { return "local-paused-native-live" }
func (m *nativeRunningModel) GetModelID() string   { return "actual-http-tail-model" }
func (m *nativeRunningModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("unexpected non-stream call")
}

func (m *nativeRunningModel) ChatStream(
	ctx context.Context,
	input []chat.Message,
	_ *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	if err := types.CheckHistoryMaterialLifetime(ctx); err != nil {
		return nil, err
	}
	assertActualHTTPTailLeases(m.t, m.f.db, true, m.f.source)
	found := false
	for _, message := range input {
		if strings.Contains(message.Content, m.f.parent.Content) {
			found = true
		}
	}
	require.True(m.t, found)
	output := make(chan types.StreamResponse, 2)
	output <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: nativeRunningOriginal, Done: false}
	go func() {
		defer close(output)
		select {
		case <-ctx.Done():
			return
		case <-m.finish:
		}
		output <- types.StreamResponse{ResponseType: types.ResponseTypeAnswer, Content: " NATIVE RUNNING FINISHED", Done: true}
	}()
	return output, nil
}
func (m *nativeRunningModel) release() { m.once.Do(func() { close(m.finish) }) }

type nativeRunningModels struct {
	actualHTTPTailModels
	paused *nativeRunningModel
}

func (m nativeRunningModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m.paused, nil
}

type nativeRunningReader struct {
	interfaces.StreamManager
	reads atomic.Int64
}

func (m *nativeRunningReader) GetEvents(
	ctx context.Context,
	session, message string,
	offset int,
) ([]interfaces.StreamEvent, int, error) {
	m.reads.Add(1)
	return m.StreamManager.GetEvents(ctx, session, message, offset)
}

type nativeRunningWriter struct {
	*httptest.ResponseRecorder
	once   sync.Once
	seen   chan struct{}
	cancel context.CancelFunc
}

func (w *nativeRunningWriter) Write(body []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(body)
	if bytes.Contains(body, []byte("NATIVE RUNNING ORIGINAL SECRET")) {
		w.once.Do(func() { close(w.seen); w.cancel() })
	}
	return n, err
}

func nativeRunningHandler(
	f *actualHTTPTailFixture,
	sessions interfaces.SessionService,
	streams interfaces.StreamManager,
	cfg *config.Config,
	models interfaces.ModelService,
	files interfaces.FileService,
) *sessionhandler.Handler {
	return sessionhandler.NewHandler(
		sessions,
		f.messages,
		nil,
		streams,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		files,
		nil,
		nil,
		models,
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
}

func nativeRunningRouter(ctx context.Context, h *sessionhandler.Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middleware.ErrorHandler())
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Set(types.RequestIDContextKey.String(), "actual-native-running-request")
		c.Next()
	})
	router.POST("/knowledge-chat/:session_id", h.KnowledgeQA)
	router.GET("/sessions/continue-stream/:session_id", h.ContinueStream)
	return router
}

func TestNativeLiveReplayActualPausedProducerReceiverAndRedisForgery(t *testing.T) {
	for _, mode := range []string{"positive", "parent_aba", "actor_revoke", "source_revoke"} {
		t.Run(mode, func(t *testing.T) {
			f := newActualHTTPTailFixture(t, mode == "source_revoke")
			model := &nativeRunningModel{f: f, t: t, finish: make(chan struct{})}
			t.Cleanup(model.release)
			models := nativeRunningModels{paused: model}
			cfg := &config.Config{
				Conversation: &config.ConversationConfig{
					MaxRounds: 3,
					Summary: &config.SummaryConfig{
						Prompt:          "Actual operator configured plain chat prompt",
						ContextTemplate: "{{.Query}}",
					},
				},
			}
			manager := chatpipeline.NewEventManager()
			chatpipeline.ConfigureSourceLineage(manager, f.guard)
			chatpipeline.NewPluginLoadHistory(manager, f.messages, cfg)
			chatpipeline.NewPluginMemoryRecall(manager, nil)
			chatpipeline.NewPluginChatCompletionStream(manager, models)
			sessions := service.NewSessionService(
				cfg,
				repository.NewSessionRepository(f.db),
				repository.NewMessageRepository(f.db),
				nil,
				f.knowledge,
				nil,
				models,
				nil,
				manager,
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
				service.NewSessionBusyGate(),
				nil,
				nil,
			)
			service.ConfigureMessageSourceLineage(f.messages, sessions, &service.SessionForkService{}, manager, f.guard)
			mini := miniredis.RunT(t)
			redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "native-running-actual", time.Minute)
			require.NoError(t, err)
			t.Cleanup(func() { _ = redis.Close() })
			firstCtx, firstCancel := context.WithCancel(f.ctx)
			defer firstCancel()
			first := &nativeRunningWriter{
				ResponseRecorder: httptest.NewRecorder(),
				seen:             make(chan struct{}),
				cancel:           firstCancel,
			}
			firstDone := make(chan struct{})
			firstHandler := nativeRunningHandler(f, sessions, redis, cfg, models, nil)
			go func() {
				defer close(firstDone)
				nativeRunningRouter(
					firstCtx,
					firstHandler,
				).ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/knowledge-chat/"+f.parent.SessionID, strings.NewReader(`{"query":"current native question","summary_model_id":"actual-http-tail-model","disable_title":true}`)))
			}()
			select {
			case <-first.seen:
			case <-time.After(5 * time.Second):
				t.Fatal("actual paused producer did not write its first native frame")
			}
			select {
			case <-firstDone:
			case <-time.After(5 * time.Second):
				t.Fatal("original HTTP disconnect did not release its sender")
			}
			assertActualHTTPTailLeases(t, f.db, true, f.source)
			var current struct {
				ID          string
				IsCompleted bool
			}
			require.NoError(
				t,
				f.db.Table("messages").
					Select("id,is_completed").
					Where("session_id=? AND role='assistant' AND request_id<>?", f.parent.SessionID, f.parent.RequestID).
					Take(&current).
					Error,
			)
			require.False(t, current.IsCompleted)
			// This transport-only forged body has the same source envelope. It
			// never acquires an original native producer receipt.
			require.NoError(
				t,
				redis.AppendEvent(
					f.ctx,
					f.parent.SessionID,
					current.ID,
					interfaces.StreamEvent{
						ID:            "forged-redis-frame",
						Type:          types.ResponseTypeAnswer,
						Content:       "FORGED REDIS BODY SECRET",
						SourceLineage: f.parent.SourceLineage,
					},
				),
			)
			reader := &nativeRunningReader{StreamManager: redis}
			observed := false
			files := &actualHTTPTailFiles{beforeURL: func(context.Context) {
				assertActualHTTPTailLeases(t, f.db, true, f.source)
				observed = true
				switch mode {
				case "parent_aba":
					require.NoError(
						t,
						f.db.Model(&types.Message{}).
							Where("id=?", f.parent.ID).
							Update("content", "foreign transient parent").
							Error,
					)
					require.NoError(
						t,
						f.db.Model(&types.Message{}).
							Where("id=?", f.parent.ID).
							Update("content", f.parent.Content).
							Error,
					)
				case "actor_revoke":
					require.NoError(t, f.db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				case "source_revoke":
					f.revokeSource()
				}
			}}
			secondCtx, secondCancel := context.WithCancel(f.ctx)
			defer secondCancel()
			second := &nativeRunningWriter{
				ResponseRecorder: httptest.NewRecorder(),
				seen:             make(chan struct{}),
				cancel:           secondCancel,
			}
			nativeRunningRouter(
				secondCtx,
				nativeRunningHandler(f, sessions, reader, cfg, models, files),
			).ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/sessions/continue-stream/"+f.parent.SessionID+"?message_id="+current.ID+"&resource_urls=public", nil))
			require.True(t, observed, "actual native replay must acquire scopes before its public URL rewrite")
			require.Zero(t, reader.reads.Load(), "running replay cannot load Redis frame bodies")
			require.NotContains(t, second.Body.String(), "FORGED REDIS BODY SECRET")
			if mode == "positive" {
				require.Contains(t, second.Body.String(), "NATIVE RUNNING ORIGINAL SECRET")
				require.Contains(t, second.Body.String(), "https://isolated.example.test/http-tail-image.png")
				assertActualHTTPTailLeases(t, f.db, true, "")
				model.release()
				require.Eventually(t, func() bool {
					var count int64
					err := f.db.Table("messages").Where("id=? AND is_completed=true", current.ID).Count(&count).Error
					return err == nil && count == 1
				}, 5*time.Second, 20*time.Millisecond)
				completed := httptest.NewRecorder()
				nativeRunningRouter(
					f.ctx,
					nativeRunningHandler(f, sessions, reader, cfg, models, nil),
				).ServeHTTP(completed, httptest.NewRequest(http.MethodGet, "/sessions/continue-stream/"+f.parent.SessionID+"?message_id="+current.ID, nil))
				require.Contains(t, completed.Body.String(), "NATIVE RUNNING FINISHED")
				require.Contains(t, completed.Body.String(), `"response_type":"complete"`)
			} else {
				require.NotContains(t, second.Body.String(), "NATIVE RUNNING ORIGINAL SECRET")
				model.release()
			}
			require.Eventually(t, func() bool {
				var live int64
				err := f.db.Table("nextcloud_content_leases").
					Where("kind='read' AND released_at_ms IS NULL").
					Count(&live).
					Error
				return err == nil && live == 0
			}, 5*time.Second, 20*time.Millisecond)
		})
	}
}

func TestNativeLiveReplayActualMissingRegistryDeniesSameSourceCachedBody(t *testing.T) {
	f := newActualHTTPTailFixture(t, false)
	question, err := f.messages.CreateMessage(
		f.ctx,
		&types.Message{
			SessionID:   f.parent.SessionID,
			RequestID:   "unregistered-native-run",
			Role:        "user",
			Content:     "actual new question",
			IsCompleted: true,
		},
	)
	require.NoError(t, err)
	answer, err := f.messages.CreateMessage(
		f.ctx,
		&types.Message{SessionID: question.SessionID, RequestID: question.RequestID, Role: "assistant"},
	)
	require.NoError(t, err)
	mini := miniredis.RunT(t)
	redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "unregistered-native", time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = redis.Close() })
	require.NoError(
		t,
		redis.AppendEvent(
			f.ctx,
			answer.SessionID,
			answer.ID,
			interfaces.StreamEvent{
				Type:          types.ResponseTypeAnswer,
				Content:       "UNKNOWN SAME SOURCE BODY SECRET",
				SourceLineage: f.parent.SourceLineage,
			},
		),
	)
	reader := &nativeRunningReader{StreamManager: redis}
	h := nativeRunningHandler(
		f,
		nativeSavedReplaySessions{repo: repository.NewSessionRepository(f.db)},
		reader,
		nil,
		nil,
		nil,
	)
	w := httptest.NewRecorder()
	nativeRunningRouter(
		f.ctx,
		h,
	).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/sessions/continue-stream/"+answer.SessionID+"?message_id="+answer.ID, nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "SECRET")
	require.Zero(t, reader.reads.Load())
	assertActualHTTPTailLeases(t, f.db, false, "")
}
