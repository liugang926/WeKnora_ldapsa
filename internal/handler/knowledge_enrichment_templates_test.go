package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type templateHTTPModels struct {
	interfaces.ModelService
	calls  int
	mutate func()
}

func (m *templateHTTPModels) GetChatModel(context.Context, string) (chat.Chat, error) { return m, nil }
func (m *templateHTTPModels) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	m.calls++
	if m.mutate != nil {
		m.mutate()
	}
	return &types.ChatResponse{Content: "actual derived instruction"}, nil
}

func (m *templateHTTPModels) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	return nil, nil
}
func (m *templateHTTPModels) GetModelName() string { return "local template fixture" }
func (m *templateHTTPModels) GetModelID() string   { return "template-model" }

type templateHTTPBoundaryWriter struct {
	gin.ResponseWriter
	change func()
}

func (w *templateHTTPBoundaryWriter) Header() http.Header {
	if w.change != nil {
		change := w.change
		w.change = nil
		change()
	}
	return w.ResponseWriter.Header()
}

func templateHTTPFixture(
	t *testing.T,
) (*KnowledgeEnrichmentTemplateHandler, *gorm.DB, *templateHTTPModels, *outputHTTPGroups) {
	t.Helper()
	_, db, store, groups, _, _ := collectionHTTPFixture(t)
	require.NoError(t, db.AutoMigrate(&types.Model{}, &types.Tenant{}))
	require.NoError(t, db.Exec("ALTER TABLE knowledge_bases DROP COLUMN enrichment_templates").Error)
	for _, file := range []string{
		"000056_knowledge_original_worker_actor",
		"000062_knowledge_enrichment_inputs",
		"000066_knowledge_enrichment_template_origins",
	} {
		data, e := os.ReadFile("../../migrations/sqlite/" + file + ".up.sql")
		require.NoError(t, e)
		require.NoError(t, db.Exec(string(data)).Error)
	}
	ctx := collectionHumanInputContext(7)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
	kb.SummaryModelID = "template-model"
	require.NoError(t, store.UpdateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), &kb))
	require.NoError(t, db.Create(&types.Tenant{ID: 7, Status: "active"}).Error)
	require.NoError(
		t,
		db.Create(
			&types.Model{ID: "template-model", TenantID: 7, Name: "local model", Status: types.ModelStatusActive},
		).Error,
	)
	bases := collectionHTTPKB{db: db}
	knowledge := repository.NewKnowledgeRepository(db)
	repository.ConfigureKnowledgeCurrentReadActor(knowledge, store)
	guard := access.NewNextcloudSourceLineageGuard(
		knowledge.(access.NextcloudLineageSnapshotLookup),
		bases,
		nil,
		nil,
		groups,
		nil,
		store,
	)
	repository.ConfigureKnowledgeOriginalInputSourceVerifier(
		store,
		service.NewKnowledgeOriginalInputSourceVerifier(store, guard, nil),
	)
	parent := &types.Knowledge{
		ID:              "template-parent",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            types.KnowledgeTypeManual,
		Channel:         "web",
		Source:          "manual",
		Title:           "actual parent",
		ParseStatus:     types.ParseStatusCompleted,
		EnableStatus:    "enabled",
		Metadata:        types.JSON(`{"manual":{"content":"actual source body"}}`),
		CustomMetadata:  types.JSON(`{}`),
	}
	proof, e := store.CreateKnowledgeWithInput(
		repository.WithIndependentKnowledgeInput(ctx),
		parent,
		"actual source body",
	)
	require.NoError(t, e)
	require.NoError(
		t,
		repository.NewChunkRepository(db).
			CreateChunks(repository.WithKnowledgeBuildInputProof(ctx, store, proof), []*types.Chunk{{
				ID:              "template-parent-chunk",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     parent.ID,
				Content:         "actual source body",
				IsEnabled:       true,
				ChunkType:       types.ChunkTypeText,
				Metadata:        types.JSON(`{}`),
			}}),
	)
	models := &templateHTTPModels{}
	svc := service.NewKnowledgeEnrichmentTemplateService(
		store,
		knowledge,
		models,
		guard,
		repository.NewNextcloudContentLeaseStore(db),
	)
	return NewKnowledgeEnrichmentTemplateHandler(svc, bases), db, models, groups
}

func serveTemplateHTTP(
	h *KnowledgeEnrichmentTemplateHandler,
	method, url, body string,
	change func(),
) *httptest.ResponseRecorder {
	r := documentHandlerRouter()
	r.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(
			types.WithPrincipal(c.Request.Context(), types.Principal{Type: types.PrincipalWebUser, ID: "user"}),
		)
		if change != nil {
			c.Writer = &templateHTTPBoundaryWriter{ResponseWriter: c.Writer, change: change}
		}
		c.Next()
	})
	r.PUT("/knowledge-bases/:id/enrichment-templates/:purpose", h.Set)
	r.POST("/knowledge-bases/:id/enrichment-templates/:purpose/generate", h.Generate)
	r.GET("/knowledge-bases/:id/enrichment-templates/:purpose", h.Read)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestKnowledgeEnrichmentTemplateHTTPActualAdminAndParents(t *testing.T) {
	for _, mode := range []string{
		"actual edit",
		"actual derived parents",
		"forged cached parent",
		"current admin revoked",
		"cross scope",
	} {
		t.Run(mode, func(t *testing.T) {
			h, db, model, _ := templateHTTPFixture(t)
			method := http.MethodPut
			url := "/knowledge-bases/kb/enrichment-templates/summary"
			body := ("{\"content\":\"actual newly typed template\",\"source_lineage\":{\"status" +
				"\":\"complete\",\"sources\":[]},\"actor\":{\"role\":\"admin\"}}")
			if mode == "actual derived parents" || mode == "forged cached parent" {
				method = http.MethodPost
				url += "/generate"
				body = ("{\"knowledge_ids\":[\"template-parent\"],\"content\":\"forged cached body" +
					"\",\"original_input\":{\"known\":true}}")
			}
			if mode == "forged cached parent" {
				body = `{"knowledge_ids":["missing"],"content":"forged cached body"}`
			}
			if mode == "current admin revoked" {
				require.NoError(
					t,
					db.Model(&types.TenantMember{}).
						Where("user_id='user' AND tenant_id=7").
						Update("role", types.TenantRoleViewer).
						Error,
				)
			}
			if mode == "cross scope" {
				url = "/knowledge-bases/shared-kb/enrichment-templates/summary"
			}
			w := serveTemplateHTTP(h, method, url, body, nil)
			if mode == "forged cached parent" || mode == "current admin revoked" || mode == "cross scope" {
				require.NotEqual(t, http.StatusOK, w.Code)
				require.Zero(t, model.calls)
				return
			}
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "instruction")
			require.NotContains(t, w.Body.String(), "newly typed")
			w = serveTemplateHTTP(h, http.MethodGet, "/knowledge-bases/kb/enrichment-templates/summary", "", nil)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			if mode == "actual derived parents" {
				require.Equal(t, 1, model.calls)
				require.Contains(t, w.Body.String(), "actual derived instruction")
			} else {
				require.Contains(t, w.Body.String(), "actual newly typed template")
			}
			var kb types.KnowledgeBase
			require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
			data, e := json.Marshal(kb)
			require.NoError(t, e)
			require.NotContains(t, string(data), "enrichment_templates")
			require.NotContains(t, string(data), "instruction")
		})
	}
}

func TestKnowledgeEnrichmentTemplateHTTPFinalWriteRecheck(t *testing.T) {
	for _, mode := range []string{"grant withdrawn", "current actor revoked", "config ABA"} {
		t.Run(mode, func(t *testing.T) {
			h, db, _, groups := templateHTTPFixture(t)
			w := serveTemplateHTTP(
				h,
				http.MethodPut,
				"/knowledge-bases/kb/enrichment-templates/summary",
				`{"content":"private actual template bytes"}`,
				nil,
			)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			fired := false
			change := func() {
				fired = true
				switch mode {
				case "grant withdrawn":
					groups.allowed = false
				case "current actor revoked":
					require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				case "config ABA":
					require.NoError(
						t,
						db.Exec("UPDATE knowledge_bases SET summary_model_id='changed' WHERE id='kb'").Error,
					)
					require.NoError(
						t,
						db.Exec("UPDATE knowledge_bases SET summary_model_id='template-model' WHERE id='kb'").Error,
					)
				}
			}
			w = serveTemplateHTTP(h, http.MethodGet, "/knowledge-bases/kb/enrichment-templates/summary", "", change)
			require.True(t, fired)
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "private actual template bytes")
		})
	}
}
