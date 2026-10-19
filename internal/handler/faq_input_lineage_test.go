package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type faqHTTPEmbedder struct{}

func (faqHTTPEmbedder) Embed(context.Context, string) ([]float32, error) { return []float32{1}, nil }
func (faqHTTPEmbedder) BatchEmbed(_ context.Context, texts []string) ([][]float32, error) {
	rows := make([][]float32, len(texts))
	for i := range rows {
		rows[i] = []float32{1}
	}
	return rows, nil
}

func (e faqHTTPEmbedder) BatchEmbedWithPool(
	ctx context.Context,
	_ embedding.Embedder,
	texts []string,
) ([][]float32, error) {
	return e.BatchEmbed(ctx, texts)
}
func (faqHTTPEmbedder) GetModelName() string { return "isolated FAQ deterministic model" }
func (faqHTTPEmbedder) GetDimensions() int   { return 1 }
func (faqHTTPEmbedder) GetModelID() string   { return "isolated-faq" }

type faqHTTPModel struct{ interfaces.ModelService }

func (faqHTTPModel) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return faqHTTPEmbedder{}, nil
}

type faqHTTPRegistry struct {
	interfaces.RetrieveEngineRegistry
	engine *faqHTTPEngine
}

func (r faqHTTPRegistry) GetRetrieveEngineService(types.RetrieverEngineType) (interfaces.RetrieveEngineService, error) {
	return r.engine, nil
}

type faqHTTPEngine struct {
	interfaces.RetrieveEngineService
	calls int
	hook  func()
}

func (e *faqHTTPEngine) EngineType() types.RetrieverEngineType {
	return types.PostgresRetrieverEngineType
}

func (e *faqHTTPEngine) Support() []types.RetrieverType {
	return []types.RetrieverType{types.VectorRetrieverType}
}

func (e *faqHTTPEngine) EstimateStorageSize(
	context.Context,
	embedding.Embedder,
	[]*types.IndexInfo,
	[]types.RetrieverType,
) int64 {
	return 0
}

func (e *faqHTTPEngine) BatchIndex(
	ctx context.Context,
	model embedding.Embedder,
	rows []*types.IndexInfo,
	_ []types.RetrieverType,
) error {
	e.calls++
	if e.hook != nil {
		e.hook()
	}
	texts := make([]string, len(rows))
	for i, row := range rows {
		texts[i] = row.Content
	}
	_, err := model.BatchEmbed(ctx, texts)
	return err
}

func (e *faqHTTPEngine) BatchUpdateChunkEnabledStatus(context.Context, map[string]bool) error {
	return nil
}
func (e *faqHTTPEngine) BatchUpdateChunkTagID(context.Context, map[string]string) error { return nil }

type faqHTTPQueue struct{ tasks []*asynq.Task }

type faqHTTPFiles struct{ interfaces.FileService }

func (faqHTTPFiles) SaveBytes(_ context.Context, _ []byte, _ uint64, name string, _ bool) (string, error) {
	return "isolated/" + name, nil
}

func (faqHTTPFiles) GetFileURL(_ context.Context, path string) (string, error) {
	return "/isolated-FAQ-files/" + path, nil
}

func (q *faqHTTPQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.tasks = append(q.tasks, task)
	return &asynq.TaskInfo{ID: "isolated"}, nil
}

type faqHTTPTenants struct {
	interfaces.TenantRepository
	tenant *types.Tenant
}

func (r faqHTTPTenants) GetTenantByID(context.Context, uint64) (*types.Tenant, error) {
	return r.tenant, nil
}

type faqHTTPKB struct{ collectionHTTPKB }

func (s faqHTTPKB) HybridSearch(ctx context.Context, kb string, _ types.SearchParams) ([]*types.SearchResult, error) {
	var chunks []*types.Chunk
	err := s.db.Where("knowledge_base_id=? AND chunk_type='faq' AND is_enabled=?", kb, true).
		Order("id").
		Find(&chunks).
		Error
	rows := make([]*types.SearchResult, 0, len(chunks))
	for _, c := range chunks {
		meta, _ := c.FAQMetadata()
		rows = append(
			rows,
			&types.SearchResult{
				ID:             c.ID,
				KnowledgeID:    c.KnowledgeID,
				Content:        c.Content,
				Score:          1,
				MatchedContent: meta.StandardQuestion,
			},
		)
	}
	return rows, err
}

func faqActualHTTPFixture(
	t *testing.T,
) (*FAQHandler, *TagHandler, *gorm.DB, *repository.KnowledgeInputProvenanceStore, *faqHTTPQueue, *faqHTTPEngine, context.Context) {
	t.Helper()
	tag, db, store, _ := tagHTTPFixture(t)
	for _, file := range []string{"000056_knowledge_original_worker_actor.up.sql", "000063_faq_original_entry_inputs.up.sql"} {
		ddl, err := os.ReadFile("../../migrations/sqlite/" + file)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	ctx := collectionHumanInputContext(7)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
	kb.Type = types.KnowledgeBaseTypeFAQ
	require.NoError(
		t,
		store.ReplaceKnowledgeBaseConfigurationWithInput(repository.WithIndependentKnowledgeInput(ctx), &kb),
	)
	kbs := faqHTTPKB{collectionHTTPKB{db: db}}
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	guard := access.NewNextcloudSourceLineageGuard(nil, kbs, nil, nil, groups, nil, store)
	tenant := &types.Tenant{
		ID: 7,
		RetrieverEngines: types.RetrieverEngines{
			Engines: []types.RetrieverEngineParams{
				{RetrieverType: types.VectorRetrieverType, RetrieverEngineType: types.PostgresRetrieverEngineType},
			},
		},
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	grant, err := access.ResolveKB(
		ctx,
		access.KBRequest{Caller: types.CallerFromContext(ctx)},
		&kb,
		types.OrgRoleEditor,
		nil,
		nil,
	)
	require.NoError(t, err)
	ctx = grant.Context(ctx)
	queue, engine := &faqHTTPQueue{}, &faqHTTPEngine{}
	kg, err := service.NewKnowledgeService(
		nil,
		repository.NewKnowledgeRepository(db),
		nil,
		kbs,
		faqHTTPTenants{tenant: tenant},
		nil,
		nil,
		repository.NewChunkRepository(db),
		repository.NewKnowledgeTagRepository(db),
		nil,
		faqHTTPFiles{},
		nil,
		nil,
		faqHTTPModel{},
		queue,
		nil,
		nil,
		faqHTTPRegistry{engine: engine},
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
	require.NoError(t, err)
	service.ConfigureKnowledgeInputProvenance(kg, store, store)
	service.ConfigureFAQInputProvenance(kg, store, guard)
	h := NewFAQHandler(kg, kbs)
	ConfigureFAQHandlerOutputLineage(h, service.NewFAQOutputAuthorizer(store, guard))
	ConfigureTagHandlerOutputLineage(tag, service.NewTagOutputAuthorizer(store, guard))
	return h, tag, db, store, queue, engine, ctx
}

func serveFAQHTTP(h *FAQHandler, ctx context.Context, method, url, body string) *httptest.ResponseRecorder {
	r := documentHandlerRouter()
	r.Use(func(c *gin.Context) { c.Request = c.Request.WithContext(ctx); c.Next() })
	r.POST("/knowledge-bases/:id/faq/entry", h.CreateEntry)
	r.POST("/knowledge-bases/:id/faq/entries", h.UpsertEntries)
	r.GET("/knowledge-bases/:id/faq/entries", h.ListEntries)
	r.GET("/knowledge-bases/:id/faq/entries/export", h.ExportEntries)
	r.GET("/knowledge-bases/:id/faq/entries/:entry_id", h.GetEntry)
	r.PUT("/knowledge-bases/:id/faq/entries/:entry_id", h.UpdateEntry)
	r.POST("/knowledge-bases/:id/faq/search", h.SearchFAQ)
	r.PUT("/knowledge-bases/:id/faq/entries-fields", h.UpdateEntryFieldsBatch)
	r.GET("/faq/import/progress/:task_id", h.GetImportProgress)
	r.PUT("/knowledge-bases/:id/faq/import/last-result/display", h.UpdateLastImportResultDisplayStatus)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestFAQActualHTTPCreateImportUpdateListSearchAndTagCount(t *testing.T) {
	h, tag, db, _, queue, engine, ctx := faqActualHTTPFixture(t)
	w := serveFAQHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/faq/entry",
		`{"standard_question":"real FAQ first","answers":["actual first answer"]}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var first types.Chunk
	require.NoError(t, db.Where("chunk_type='faq'").Take(&first).Error)
	w = serveFAQHTTP(h, ctx, http.MethodGet, fmt.Sprintf("/knowledge-bases/kb/faq/entries/%d", first.SeqID), "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	w = serveFAQHTTP(
		h,
		ctx,
		http.MethodPut,
		fmt.Sprintf("/knowledge-bases/kb/faq/entries/%d", first.SeqID),
		`{"standard_question":"real FAQ first edited","answers":["edited answer"]}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	w = serveFAQHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/faq/entries",
		`{"mode":"append","entries":[{"standard_question":"real FAQ imported","answers":["imported answer"]}]}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, queue.tasks, 1)
	require.NoError(t, h.knowledgeService.ProcessFAQImport(context.Background(), queue.tasks[0]))
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/faq/entries?page_size=1", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"total":2`)
	w = serveFAQHTTP(h, ctx, http.MethodPost, "/knowledge-bases/kb/faq/search", `{"query_text":"FAQ"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "imported answer")
	w = serveTagHTTP(tag, ctx, http.MethodGet, "/knowledge-bases/kb/tags", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"chunk_count":1`)
	w = serveFAQHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/faq/entries/export?format=json", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "imported answer")
	require.Equal(t, 3, engine.calls)
}

func TestFAQActualHTTPProducerActorRevokedDuringModelClosesReceipt(t *testing.T) {
	h, _, db, store, _, engine, ctx := faqActualHTTPFixture(t)
	engine.hook = func() {
		require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
	}
	w := serveFAQHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/faq/entry",
		`{"standard_question":"revoked actor question","answers":["must not publish"]}`,
	)
	require.NotEqual(t, http.StatusOK, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "must not publish")
	snapshot, err := store.CaptureFAQInputs(
		ctx,
		types.FAQInputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", AllEntries: true},
	)
	require.NoError(t, err)
	for _, chunk := range snapshot.Chunks {
		require.Equal(t, int(types.ChunkStatusStored), chunk.Status)
	}
}
