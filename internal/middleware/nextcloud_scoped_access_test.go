package middleware

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type scopedRouteAccess struct {
	interfaces.GroupAccessService
	role types.EffectiveTenantRole
}

func (s scopedRouteAccess) EffectiveTenantRole(
	context.Context,
	string,
	uint64,
	time.Time,
) (types.EffectiveTenantRole, error) {
	return s.role, nil
}

func (s scopedRouteAccess) Authorize(
	_ context.Context,
	_ uint64,
	_ types.ResourceType,
	id string,
	_ types.ResourceAction,
) error {
	if id != "granted-kb" {
		return errors.New("denied")
	}
	return nil
}

type scopedRouteSession struct{}

func (scopedRouteSession) ScopedSession(_ context.Context, _ uint64, _ string, id string) (bool, error) {
	return id == "private-qa", nil
}

func (scopedRouteSession) ScopedDocument(_ context.Context, _ uint64, id string) (string, error) {
	if id == "current-source-document" {
		return "granted-kb", nil
	}
	if id == "other-kb-document" {
		return "other-kb", nil
	}
	return "", errors.New("source unavailable")
}

func TestNextcloudScopedRoutesAllowExplicitQAAndBlockCrossResourceInputs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	role := types.EffectiveTenantRole{TenantID: 7, Member: true, ScopedOnly: true, Role: types.TenantRoleViewer}
	r := gin.New()
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, uint64(7))
		ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "reader", Role: types.TenantRoleViewer})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "reader"})
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.Use(RequireNextcloudScopedRoutes(scopedRouteAccess{role: role}, scopedRouteSession{}))
	echo := func(c *gin.Context) { b, _ := io.ReadAll(c.Request.Body); c.String(http.StatusOK, string(b)) }
	r.POST("/api/v1/sessions", echo)
	r.POST("/api/v1/knowledge-chat/:session_id", echo)
	r.POST("/api/v1/knowledge-search", echo)
	r.GET("/api/v1/models", echo)
	r.GET("/api/v1/knowledge/move/progress/:task_id", echo)
	r.GET("/api/v1/chunks/:knowledge_id/:id/revisions", echo)
	r.GET("/api/v1/knowledge/future-private-resource", echo)
	r.GET("/api/v1/knowledge-bases", echo)
	r.GET("/api/v1/knowledge-bases/:id", echo)
	r.GET("/api/v1/knowledge-bases/:id/move-targets", echo)
	r.GET("/api/v1/knowledge-bases/copy/progress/:task_id", echo)
	r.GET("/api/v1/knowledge-bases/:id/wiki/stats", echo)
	r.GET("/api/v1/messages/:session_id/load", echo)
	r.GET("/api/v1/sessions/:id", echo)
	r.GET("/api/v1/sessions", echo)
	r.GET("/api/v1/auth/me", echo)
	r.GET("/api/v1/auth/validate", echo)
	r.PUT("/api/v1/auth/me/preferences", echo)
	r.POST("/api/v1/auth/switch-tenant", echo)
	r.POST("/api/v1/auth/logout", echo)
	r.POST("/api/v1/auth/refresh", echo)
	good := `{"query":"question","knowledge_base_ids":["granted-kb"]}`
	for _, test := range []struct {
		path, method, body string
		status             int
	}{
		{"/api/v1/sessions", "POST", `{"title":"QA"}`, 200},
		{"/api/v1/knowledge-bases", "GET", "", 200},
		{"/api/v1/knowledge/move/progress/other-task", "GET", "", 403},
		{"/api/v1/chunks/source/doc/revisions", "GET", "", 403},
		{"/api/v1/knowledge/future-private-resource", "GET", "", 403},
		{"/api/v1/knowledge-bases/granted-kb", "GET", "", 200},
		{"/api/v1/knowledge-bases/granted-kb/move-targets", "GET", "", 403},
		{"/api/v1/knowledge-bases/copy/progress/other-users-task", "GET", "", 403},
		{"/api/v1/knowledge-bases/granted-kb/wiki/stats", "GET", "", 403},
		{"/api/v1/knowledge-chat/private-qa", "POST", good, 200},
		{
			"/api/v1/knowledge-chat/private-qa",
			"POST",
			("{\"query\":\"question\",\"knowledge_base_ids\":[\"granted-kb\"],\"knowledg" +
				"e_ids\":[\"current-source-document\"]}"),
			200,
		},
		{
			"/api/v1/knowledge-chat/private-qa",
			"POST",
			("{\"query\":\"question\",\"knowledge_base_ids\":[\"granted-kb\"],\"knowledg" +
				"e_ids\":[\"other-kb-document\"]}"),
			403,
		},
		{
			"/api/v1/knowledge-chat/private-qa",
			"POST",
			("{\"query\":\"question\",\"knowledge_base_ids\":[\"granted-kb\"],\"knowledg" +
				"e_ids\":[\"unavailable-document\"]}"),
			403,
		},
		{
			"/api/v1/knowledge-search",
			"POST",
			("{\"query\":\"question\",\"knowledge_base_ids\":[\"granted-kb\"],\"knowledg" +
				"e_ids\":[\"current-source-document\"]}"),
			403,
		},
		{"/api/v1/knowledge-chat/legacy-session", "POST", good, 403},
		{"/api/v1/knowledge-search", "POST", good, 200},
		{
			"/api/v1/knowledge-chat/private-qa",
			"POST",
			`{"query":"question","knowledge_base_ids":["unrelated-kb"]}`,
			403,
		},
		{
			"/api/v1/knowledge-chat/private-qa",
			"POST",
			("{\"query\":\"question\",\"knowledge_base_ids\":[\"granted-kb\"],\"agent_id" +
				"\":\"other-agent\"}"),
			403,
		},
		{
			"/api/v1/knowledge-chat/private-qa",
			"POST",
			("{\"query\":\"question\",\"knowledge_base_ids\":[\"granted-kb\"],\"summary_" +
				"model_id\":\"other-model\"}"),
			403,
		},
		{"/api/v1/knowledge-chat/private-qa", "POST", `{"query":"question"}`, 403},
		{"/api/v1/models", "GET", "", 403},
		{"/api/v1/messages/private-qa/load", "GET", "", 200},
		{"/api/v1/messages/legacy-session/load", "GET", "", 403},
		{"/api/v1/sessions", "GET", "", 403},
		{"/api/v1/auth/me", "GET", "", 200},
		{"/api/v1/auth/validate", "GET", "", 200},
		{"/api/v1/auth/me/preferences", "PUT", `{"last_active_tenant_id":7}`, 200},
		{"/api/v1/auth/switch-tenant", "POST", `{"tenant_id":7}`, 200},
		{"/api/v1/auth/logout", "POST", `{}`, 200},
		{"/api/v1/auth/refresh", "POST", `{}`, 200},
	} {
		t.Run(test.path+test.body, func(t *testing.T) {
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
			require.Equal(t, test.status, w.Code)
			if test.status == 200 && test.method == "POST" {
				require.Equal(
					t,
					test.body,
					w.Body.String(),
					"scope checks must preserve the body for the real QA handler",
				)
			}
		})
	}
}
