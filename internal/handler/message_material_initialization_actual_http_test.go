package handler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type actualHTTPInitializationModels struct {
	actualHTTPTailModels
	once   sync.Once
	before func()
}

func (m *actualHTTPInitializationModels) GetModelByID(ctx context.Context, id string) (*types.Model, error) {
	m.once.Do(func() {
		// Actual KnowledgeQA has already admitted the current user here,
		// but has not marked the HTTP material holder Ready.
		time.Sleep(350 * time.Millisecond)
		if m.before != nil {
			m.before()
		}
	})
	return m.actualHTTPTailModels.GetModelByID(ctx, id)
}

func TestMessageMaterialActualHTTPInitializationWaitsForAdmission(t *testing.T) {
	for _, mode := range []string{"ready", "current_user_aba", "actor_revoked", "source_revoked", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f := newActualHTTPTailFixture(t, mode == "source_revoked")
			model := &actualHTTPTailModel{t: t, db: f.db, parent: f.parent}
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			models := &actualHTTPInitializationModels{actualHTTPTailModels: actualHTTPTailModels{model: model}}
			models.before = func() {
				if mode == "current_user_aba" {
					require.NoError(t, f.db.Model(&types.Message{}).Where("role='user' AND request_id='actual-initialization-request'").Update("content", "unadmitted transient original").Error)
					require.NoError(t, f.db.Model(&types.Message{}).Where("role='user' AND request_id='actual-initialization-request'").Update("content", "actual new delayed user question").Error)
				} else if mode == "actor_revoked" {
					require.NoError(t, f.db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				} else if mode == "source_revoked" {
					f.revokeSource()
				} else if mode == "cancelled" {
					cancel()
				}
			}
			cfg := &config.Config{Conversation: &config.ConversationConfig{MaxRounds: 3, Summary: &config.SummaryConfig{Prompt: "Actual operator configured plain chat prompt", ContextTemplate: "{{.Query}}"}}}
			manager := chatpipeline.NewEventManager()
			chatpipeline.ConfigureSourceLineage(manager, f.guard)
			chatpipeline.NewPluginLoadHistory(manager, f.messages, cfg)
			chatpipeline.NewPluginMemoryRecall(manager, nil)
			chatpipeline.NewPluginChatCompletionStream(manager, models)
			sessions := service.NewSessionService(cfg, repository.NewSessionRepository(f.db), repository.NewMessageRepository(f.db), nil, f.knowledge, nil, models, nil, manager, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, service.NewSessionBusyGate(), nil, nil)
			service.ConfigureMessageSourceLineage(f.messages, sessions, &service.SessionForkService{}, manager, f.guard)
			mini := miniredis.RunT(t)
			redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "actual-initialization-http", time.Minute)
			require.NoError(t, err)
			t.Cleanup(func() { _ = redis.Close() })
			h := nativeRunningHandler(f, sessions, redis, cfg, models, nil)
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.Use(middleware.ErrorHandler())
			router.Use(func(c *gin.Context) {
				c.Request = c.Request.WithContext(ctx)
				c.Set(types.TenantIDContextKey.String(), uint64(7))
				c.Set(types.RequestIDContextKey.String(), "actual-initialization-request")
				c.Next()
			})
			router.POST("/knowledge-chat/:session_id", h.KnowledgeQA)
			server := httptest.NewServer(router)
			defer server.Close()
			client := &http.Client{Timeout: 3 * time.Second}
			started := time.Now()
			response, err := client.Post(server.URL+"/knowledge-chat/"+f.parent.SessionID, "application/json", strings.NewReader(`{"query":"actual new delayed user question","summary_model_id":"actual-http-tail-model","disable_title":true}`))
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err, "the actual HTTP receiver must terminate without client timeout")
			if mode == "ready" {
				require.Equal(t, http.StatusOK, response.StatusCode, string(body))
				require.GreaterOrEqual(t, time.Since(started), 350*time.Millisecond)
				require.Contains(t, string(body), `"response_type":"agent_query"`)
				require.Contains(t, string(body), "LIVE HTTP SECRET")
				require.Contains(t, string(body), `"response_type":"complete"`)
				require.EqualValues(t, 1, model.calls.Load())
			} else {
				require.NotContains(t, string(body), "LIVE HTTP SECRET")
				require.NotContains(t, string(body), f.parent.Content)
				require.NotContains(t, string(body), `"response_type":"complete"`)
				if mode != "cancelled" {
					require.Contains(t, string(body), `"response_type":"error"`)
					require.Contains(t, string(body), `"done":true`)
				}
				var completed, admissions int64
				require.NoError(t, f.db.Table("messages").Where("request_id='actual-initialization-request' AND role='assistant' AND is_completed=true").Count(&completed).Error)
				require.Zero(t, completed, "failed initialization cannot certify the placeholder")
				// Creation legitimately seals the empty placeholder's original
				// controls. Failure must not mint a second/completed admission.
				require.NoError(t, f.db.Table("message_material_admissions a").Joins("JOIN messages m ON m.id=a.message_id").Where("m.request_id='actual-initialization-request' AND m.role='assistant'").Count(&admissions).Error)
				require.EqualValues(t, 1, admissions)
			}
			assertActualHTTPTailLeases(t, f.db, false, "")
		})
	}
}
