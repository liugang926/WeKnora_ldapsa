package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type scopedAgentStub struct {
	interfaces.AgentShareService
	agent          *types.CustomAgent
	err            error
	caller, source uint64
	lookupCalls    int
}

func (s *scopedAgentStub) GetSharedAgentForTenant(
	_ context.Context,
	caller uint64,
	_ types.TenantRole,
	_ string,
	source ...uint64,
) (*types.CustomAgent, error) {
	s.lookupCalls++
	s.caller, s.source = caller, source[0]
	return s.agent, s.err
}

type scopedKBStub struct {
	interfaces.KnowledgeBaseService
	calls int
	kbs   []*types.KnowledgeBase
}

func (s *scopedKBStub) ListKnowledgeBasesByTenantID(context.Context, uint64) ([]*types.KnowledgeBase, error) {
	s.calls++
	return s.kbs, nil
}

type scopedKnowledgeStub struct {
	interfaces.KnowledgeService
	batchCalls, searchCalls int
	knowledges              []*types.Knowledge
	batchErr                error
}

// Delegate current scope resolution to the real SQL-backed service. The spy
// records only legacy calls; it cannot mint collection or current actor proof.
func (s *scopedKnowledgeStub) ResolveKnowledgeCollectionScopes(
	ctx context.Context, selected []types.KnowledgeSearchScope,
) ([]types.KnowledgeSearchScope, error) {
	resolver, ok := s.KnowledgeService.(knowledgeCollectionScopeResolver)
	if !ok {
		return nil, errors.New("actual collection scope resolver unavailable")
	}
	return resolver.ResolveKnowledgeCollectionScopes(ctx, selected)
}

func (s *scopedKnowledgeStub) GetKnowledgeBatch(context.Context, uint64, []string) ([]*types.Knowledge, error) {
	s.batchCalls++
	return s.knowledges, s.batchErr
}

func (s *scopedKnowledgeStub) SearchKnowledgeForScopes(
	_ context.Context,
	scopes []types.KnowledgeSearchScope,
	_ string,
	_, _ int,
	_ []string,
) ([]*types.Knowledge, bool, int64, error) {
	s.searchCalls++
	var out []*types.Knowledge
	for _, k := range s.knowledges {
		if k == nil {
			continue
		}
		for _, scope := range scopes {
			if k.TenantID == scope.TenantID && k.KnowledgeBaseID == scope.KBID {
				out = append(out, k)
				break
			}
		}
	}
	return out, false, int64(len(out)), nil
}

func TestSharedAgentScopesAgreeAcrossListSearchAndBatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		name, mode string
		ids        []string
		expected   []string
	}{
		{name: "all", mode: "all", expected: []string{"a", "b"}},
		{name: "selected", mode: "selected", ids: []string{"a"}, expected: []string{"a"}},
		{name: "selected empty", mode: "selected"},
		{name: "selected blank", mode: "selected", ids: []string{""}},
		{name: "none", mode: "none", ids: []string{"a"}},
		{name: "unknown", mode: "invalid", ids: []string{"a"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agents := &scopedAgentStub{
				agent: &types.CustomAgent{
					ID:       "agent",
					TenantID: 9,
					Config:   types.CustomAgentConfig{KBSelectionMode: tt.mode, KnowledgeBases: tt.ids},
				},
			}
			kbs := &scopedKBStub{kbs: []*types.KnowledgeBase{
				{ID: "a", TenantID: 9, Type: types.KnowledgeBaseTypeDocument},
				{ID: "b", TenantID: 9, Type: types.KnowledgeBaseTypeDocument},
				{ID: "foreign", TenantID: 3, Type: types.KnowledgeBaseTypeDocument},
				nil,
			}}
			knowledges := &scopedKnowledgeStub{knowledges: []*types.Knowledge{
				{ID: "a", TenantID: 9, KnowledgeBaseID: "a"},
				{ID: "b", TenantID: 9, KnowledgeBaseID: "b"},
				{ID: "foreign", TenantID: 3, KnowledgeBaseID: "a"},
				nil,
			}}
			kh, db, store, groups, shares, _ := collectionHTTPFixture(t)
			// Author new local fixture documents through the actual origin writer;
			// shared-agent selection must not certify a cached row or title later.
			for _, kb := range kbs.kbs[:2] {
				kb.Name = "Actual shared fixture KB " + kb.ID
				ctx := collectionHumanInputContext(9)
				require.NoError(
					t,
					store.CreateKnowledgeBaseWithInput(
						repository.WithIndependentKnowledgeBaseNameInput(
							repository.WithIndependentKnowledgeInput(ctx),
							"",
							kb.Name,
						),
						kb,
					),
				)
				row := &types.Knowledge{
					ID: kb.ID, TenantID: 9, KnowledgeBaseID: kb.ID,
					Type: types.KnowledgeTypeManual, Title: "Actual shared original test " + kb.ID,
					Channel: "web", Source: "manual", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled",
					Metadata:       types.JSON(`{"manual":{"content":"actual shared original body","version":1}}`),
					CustomMetadata: types.JSON(`{}`),
				}
				_, err := store.CreateKnowledgeWithInput(
					repository.WithIndependentKnowledgeInput(ctx),
					row,
					row.Metadata,
				)
				require.NoError(t, err)
				for index, prior := range knowledges.knowledges {
					if prior != nil && prior.ID == row.ID && prior.TenantID == row.TenantID {
						knowledges.knowledges[index] = row
					}
				}
			}

			knowledges.KnowledgeService = kh.kgService
			kh.kgService, kh.kbService, kh.agentShareService = knowledges, kbs, agents
			guard := access.NewNextcloudSourceLineageGuard(
				nil,
				collectionHTTPKB{db: db},
				shares,
				agents,
				groups,
				nil,
				store,
			)
			ConfigureKnowledgeHandlerCollectionOutput(kh, service.NewKnowledgeCollectionOutputAuthorizer(store, guard))
			ConfigureKnowledgeHandlerOutputLineage(kh, service.NewKnowledgeOutputAuthorizer(store, guard))
			bh := &KnowledgeBaseHandler{service: kbs, agentShareService: agents}
			r := gin.New()
			r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
				c.Set(types.TenantIDContextKey.String(), uint64(7))
				c.Set(types.UserIDContextKey.String(), "user")
				ctx := types.WithCaller(
					c.Request.Context(),
					types.Caller{TenantID: 7, UserID: "user", Role: types.TenantRoleAdmin},
				)
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
				ctx = types.WithExecutionTenant(ctx, 7)

				c.Request = c.Request.WithContext(ctx)
				c.Next()
			})
			r.GET("/kbs", bh.ListKnowledgeBases)
			r.GET("/search", kh.SearchKnowledge)
			r.GET("/batch", kh.GetKnowledgeBatch)
			for _, path := range []string{"/kbs", "/search", "/batch"} {
				w := httptest.NewRecorder()
				r.ServeHTTP(
					w,
					httptest.NewRequest(
						http.MethodGet,
						path+"?agent_id=agent&agent_source_tenant_id=9&keyword=test&ids=a&ids=b",
						nil,
					),
				)
				require.Equal(t, http.StatusOK, w.Code, "%s: %s", path, w.Body.String())
				var body struct {
					Data []struct {
						ID string `json:"id"`
					} `json:"data"`
				}
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
				var ids []string
				for _, row := range body.Data {
					ids = append(ids, row.ID)
				}
				require.ElementsMatch(t, tt.expected, ids, path)
				if path == "/search" && types.NewSharedAgentKBScope(agents.agent).IsEmpty() {
					require.Contains(t, w.Body.String(), `"data":[]`)
					require.Contains(t, w.Body.String(), `"total":0`)
					require.Contains(t, w.Body.String(), `"has_more":false`)
				}
			}
			require.Equal(t, uint64(7), agents.caller)
			require.Equal(t, uint64(9), agents.source)
			if types.NewSharedAgentKBScope(agents.agent).IsEmpty() {
				require.Zero(t, kbs.calls)
				require.Zero(t, knowledges.batchCalls)
				require.Zero(t, knowledges.searchCalls)
			}
		})
	}
}

func TestSharedAgentLookupFailuresDoNotReachBatchRetrieval(t *testing.T) {
	for _, tt := range []struct {
		name   string
		agent  *types.CustomAgent
		err    error
		source string
		status int
	}{
		{name: "revoked or missing", status: http.StatusForbidden},
		{name: "source mismatch", agent: &types.CustomAgent{TenantID: 3}, source: "2", status: http.StatusForbidden},
		{name: "invalid source", source: "invalid", status: http.StatusBadRequest},
		{name: "lookup unavailable", err: errors.New("offline"), status: http.StatusInternalServerError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := gin.New()
			r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
				c.Set(types.TenantIDContextKey.String(), uint64(1))
				c.Set(types.UserIDContextKey.String(), "user")
				c.Next()
			})
			h := &KnowledgeHandler{agentShareService: &scopedAgentStub{agent: tt.agent, err: tt.err}}
			r.GET("/batch", h.GetKnowledgeBatch)
			w := httptest.NewRecorder()
			r.ServeHTTP(
				w,
				httptest.NewRequest(
					http.MethodGet,
					"/batch?ids=a&agent_id=agent&agent_source_tenant_id="+tt.source,
					nil,
				),
			)
			require.Equal(t, tt.status, w.Code, w.Body.String())
		})
	}
}

func TestSharedAgentCapabilityFilterRetainsExplicitSelection(t *testing.T) {
	kb := &types.KnowledgeBase{ID: "wiki", TenantID: 2, IndexingStrategy: types.IndexingStrategy{WikiEnabled: true}}
	agent := &types.CustomAgent{
		TenantID: 2,
		Config:   types.CustomAgentConfig{AgentMode: "quick-answer", KBSelectionMode: "all"},
	}
	require.Empty(t, filterKnowledgeBasesForSharedAgent([]*types.KnowledgeBase{kb}, agent))
	agent.Config.KBSelectionMode = "selected"
	agent.Config.KnowledgeBases = []string{"wiki"}
	require.Len(t, filterKnowledgeBasesForSharedAgent([]*types.KnowledgeBase{kb}, agent), 1)
}

func TestExplicitKBBatchReadPropagatesRepositoryFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	kbs := &scopedBatchKBStub{}
	knowledge := &scopedKnowledgeStub{batchErr: errors.New("storage unavailable")}
	h := &KnowledgeHandler{kgService: knowledge, kbService: kbs}
	r := gin.New()
	r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := types.WithExecutionTenant(c.Request.Context(), 1)
		ctx = types.WithCaller(ctx, types.Caller{TenantID: 1, UserID: "user", Role: types.TenantRoleAdmin})
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.GET("/batch", h.GetKnowledgeBatch)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/batch?kb_id=kb&ids=doc", nil))
	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	require.Equal(t, 1, knowledge.batchCalls)
}

type scopedBatchKBStub struct {
	interfaces.KnowledgeBaseService
}

func (*scopedBatchKBStub) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return &types.KnowledgeBase{ID: "kb", TenantID: 1}, nil
}

// API scope intersection is a selector algebra unit, not proof that a mixed
// human-plus-machine principal may publish original material.
func TestSharedAgentAPIKeyScopeIntersectionFilters(t *testing.T) {
	for _, tc := range []struct {
		name, mode     string
		selected, want []string
	}{
		{name: "all", mode: "all", want: []string{"b"}},
		{name: "selected", mode: "selected", selected: []string{"a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := types.WithTenantAPIKeyScope(context.Background(), types.TenantAPIKeyScope{
				KeyID: 1, ScopeType: types.APIKeyScopeTenant,
				Capabilities:     types.StringArray{string(types.APIKeyCapabilityRetrieve)},
				KnowledgeBaseIDs: types.StringArray{"b"},
			})
			agent := &types.CustomAgent{
				TenantID: 9,
				Config:   types.CustomAgentConfig{KBSelectionMode: tc.mode, KnowledgeBases: tc.selected},
			}
			bases := []*types.KnowledgeBase{
				{ID: "a", TenantID: 9, Type: types.KnowledgeBaseTypeDocument},
				{ID: "b", TenantID: 9, Type: types.KnowledgeBaseTypeDocument},
			}
			selected := filterKnowledgeBasesForSharedAgent(bases, agent)
			var scopes []types.KnowledgeSearchScope
			for _, kb := range selected {
				scopes = append(scopes, types.KnowledgeSearchScope{TenantID: kb.TenantID, KBID: kb.ID})
			}
			var listed, searched []string
			for _, kb := range filterKnowledgeBasesForAPIKeyScope(ctx, selected) {
				listed = append(listed, kb.ID)
			}
			for _, scope := range filterKnowledgeSearchScopesForAPIKey(ctx, scopes) {
				searched = append(searched, scope.KBID)
			}
			require.ElementsMatch(t, tc.want, listed)
			require.ElementsMatch(t, tc.want, searched)
		})
	}
}

func TestSharedAgentEmptyCallerMachineCannotReachSharedLookupOrRetrieval(t *testing.T) {
	// This is specifically an empty-Caller.UserID machine context. Legacy API
	// middleware may retain an s.User ID; this test does not generalize to it.
	for _, path := range []string{"/kbs", "/search", "/batch"} {
		t.Run(path, func(t *testing.T) {
			h, _, _, _, _, _ := collectionHTTPFixture(t)
			agents := &scopedAgentStub{agent: &types.CustomAgent{ID: "agent", TenantID: 9}}
			knowledges := &scopedKnowledgeStub{KnowledgeService: h.kgService}
			bases := &scopedKBStub{}
			h.kgService, h.kbService, h.agentShareService = knowledges, bases, agents
			bh := &KnowledgeBaseHandler{service: bases, agentShareService: agents}
			r := gin.New()
			r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
				// A legacy Gin field cannot overwrite an already captured caller.
				c.Set(types.UserIDContextKey.String(), "legacy-user")
				ctx := types.WithCaller(c.Request.Context(), types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
				ctx = types.WithExecutionTenant(ctx, 7)
				ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{
					KeyID: 1, ScopeType: types.APIKeyScopeTenant,
					Capabilities:     types.StringArray{string(types.APIKeyCapabilityRetrieve)},
					KnowledgeBaseIDs: types.StringArray{"kb"},
				})
				c.Request = c.Request.WithContext(ctx)
				c.Next()
			})
			r.GET("/kbs", bh.ListKnowledgeBases)
			r.GET("/search", h.SearchKnowledge)
			r.GET("/batch", h.GetKnowledgeBatch)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
				path+"?agent_id=agent&agent_source_tenant_id=9&keyword=test&ids=a", nil))
			require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
			require.Zero(t, agents.lookupCalls)
			require.Zero(t, bases.calls)
			require.Zero(t, knowledges.batchCalls)
			require.Zero(t, knowledges.searchCalls)
		})
	}
}

func TestSharedAgentEmptySearchRechecksShareAndGroupBeforeHTTPWrite(t *testing.T) {
	for _, revoke := range []string{"share", "group"} {
		t.Run(revoke, func(t *testing.T) {
			h, db, store, groups, shares, _ := collectionHTTPFixture(t)
			agents := &scopedAgentStub{agent: &types.CustomAgent{
				ID: "agent", TenantID: 9,
				Config: types.CustomAgentConfig{KBSelectionMode: "none"},
			}}
			knowledge := &scopedKnowledgeStub{KnowledgeService: h.kgService}
			bases := &scopedKBStub{}
			h.kgService, h.kbService, h.agentShareService = knowledge, bases, agents
			guard := access.NewNextcloudSourceLineageGuard(
				nil, collectionHTTPKB{db: db}, shares, agents, groups, nil, store,
			)
			ConfigureKnowledgeHandlerCollectionOutput(h, service.NewKnowledgeCollectionOutputAuthorizer(store, guard))
			fired := false
			groups.hook = func(call int) {
				// Begin finishes two full current-grant rounds (four Agent-use
				// checks). The fifth starts the final HTTP writer's recheck.
				if fired || call < 5 {
					return
				}
				fired = true
				if revoke == "share" {
					agents.agent = nil
				} else {
					groups.allowed = false
				}
			}
			r := gin.New()
			r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
				c.Request = c.Request.WithContext(collectionHumanInputContext(7))
				c.Next()
			})
			r.GET("/search", h.SearchKnowledge)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
				"/search?agent_id=agent&agent_source_tenant_id=9&keyword=test", nil))
			require.True(t, fired, "revocation must occur after capture, at the HTTP writer")
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), `"success":true`)
			require.NotContains(t, w.Body.String(), `"data":[]`)
			require.Zero(t, bases.calls)
			require.Zero(t, knowledge.batchCalls)
			require.Zero(t, knowledge.searchCalls)
		})
	}
}
