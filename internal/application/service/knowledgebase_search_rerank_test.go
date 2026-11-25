package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
)

// scoredReranker returns one canned score per passage, or an error.
type scoredReranker struct {
	scores []float64
	err    error
}

func (r *scoredReranker) Rerank(_ context.Context, _ string, documents []string) ([]rerank.RankResult, error) {
	if r.err != nil {
		return nil, r.err
	}
	out := make([]rerank.RankResult, len(documents))
	for i := range documents {
		out[i] = rerank.RankResult{Index: i, RelevanceScore: r.scores[i]}
	}
	return out, nil
}

func (r *scoredReranker) GetModelName() string { return "scored" }
func (r *scoredReranker) GetModelID() string   { return "scored" }

// rerankModelService serves one reranker (or a load error) on top of
// stubModelService's model lookups.
type rerankModelService struct {
	stubModelService
	reranker rerank.Reranker
	loadErr  error
}

func (s *rerankModelService) GetRerankModel(context.Context, string) (rerank.Reranker, error) {
	return s.reranker, s.loadErr
}

func newRerankModelService() *rerankModelService {
	return &rerankModelService{stubModelService: stubModelService{
		modelsByID: map[string]*types.Model{
			"rr-1":   {ID: "rr-1", Type: types.ModelTypeRerank},
			"chat-1": {ID: "chat-1", Type: types.ModelTypeKnowledgeQA},
		},
		availableModels: []*types.Model{
			{ID: "chat-1", Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive},
			{ID: "rr-auto", Type: types.ModelTypeRerank, Status: types.ModelStatusActive},
		},
	}}
}

func TestResolveRerankModelID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	models := newRerankModelService()
	tenantRC := &types.RetrievalConfig{RerankModelID: "rr-tenant"}

	id, source, err := resolveRerankModelID(ctx, models, "rr-1", tenantRC)
	require.NoError(t, err)
	assert.Equal(t, "rr-1", id)
	assert.Equal(t, types.RerankModelSourceRequest, source)

	id, source, err = resolveRerankModelID(ctx, models, "", tenantRC)
	require.NoError(t, err)
	assert.Equal(t, "rr-tenant", id)
	assert.Equal(t, types.RerankModelSourceTenant, source)

	id, source, err = resolveRerankModelID(ctx, models, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "rr-auto", id)
	assert.Equal(t, types.RerankModelSourceAuto, source)

	models.availableModels = nil
	id, source, err = resolveRerankModelID(ctx, models, "", nil)
	require.NoError(t, err)
	assert.Empty(t, id)
	assert.Empty(t, source)

	// A requested model never silently falls back to another one.
	for _, requested := range []string{"missing", "chat-1"} {
		_, _, err = resolveRerankModelID(ctx, models, requested, tenantRC)
		appErr, ok := apperrors.IsAppError(err)
		require.True(t, ok, "requested %q: %v", requested, err)
		assert.Equal(t, apperrors.ErrBadRequest, appErr.Code, "requested %q", requested)
	}
}

func rerankTestCandidates() []*types.SearchResult {
	return []*types.SearchResult{
		{ID: "c1", Content: "first body", Score: 0.9},
		{ID: "c2", Content: "second body", Score: 0.8},
		{ID: "c3", Content: "third body", Score: 0.7},
	}
}

func TestRerankCandidates_reranksAndKeepsModelSource(t *testing.T) {
	t.Parallel()
	models := newRerankModelService()
	models.reranker = &scoredReranker{scores: []float64{0.1, 0.9, 0.5}}
	s := &knowledgeBaseService{modelService: models}
	diag := &types.RerankDiagnostics{ModelSource: types.RerankModelSourceRequest}

	got := s.rerankCandidates(context.Background(), "rr-1", "q", rerankTestCandidates(), 0.3, 2, 0, diag)

	require.Len(t, got, 2)
	assert.Equal(t, "c2", got[0].ID)
	assert.Equal(t, "c3", got[1].ID)
	assert.Equal(t, types.RerankOutcomeOK, diag.Outcome)
	assert.True(t, diag.Applied)
	assert.Equal(t, "rr-1", diag.ModelID)
	assert.Equal(t, types.RerankModelSourceRequest, diag.ModelSource)
	assert.Equal(t, 0.9, diag.TopScore)
}

func TestRerankCandidates_degradesToRetrievalOrder(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		modelID string
		model   rerank.Reranker
		loadErr error
		want    types.RerankOutcome
	}{
		{name: "no model", modelID: "", want: types.RerankOutcomeNoModel},
		{
			name: "model unavailable", modelID: "rr-1", loadErr: errors.New("bad credentials"),
			want: types.RerankOutcomeModelUnavailable,
		},
		{
			name: "model error", modelID: "rr-1", model: &scoredReranker{err: errors.New("upstream 500")},
			want: types.RerankOutcomeModelError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			models := newRerankModelService()
			models.reranker, models.loadErr = tt.model, tt.loadErr
			s := &knowledgeBaseService{modelService: models}
			diag := &types.RerankDiagnostics{}

			got := s.rerankCandidates(context.Background(), tt.modelID, "q", rerankTestCandidates(), 0.3, 2, 0, diag)

			require.Len(t, got, 2)
			assert.Equal(t, "c1", got[0].ID)
			assert.Equal(t, "c2", got[1].ID)
			assert.Equal(t, tt.want, diag.Outcome)
			assert.False(t, diag.Applied)
			if tt.loadErr != nil {
				assert.Equal(t, types.RerankModelUnavailableMessage, diag.Error)
			}
		})
	}
}

// A provider may echo input passages or internal details in an HTTP error.
func TestRerankProviderErrorIsPrivateInBothSearchResults(t *testing.T) {
	const providerBody = "private-provider-diagnostic-718"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, providerBody, http.StatusBadRequest)
	}))
	t.Cleanup(server.Close)
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	utils.ResetSSRFWhitelistForTest()
	t.Cleanup(utils.ResetSSRFWhitelistForTest)
	model, err := rerank.NewReranker(&rerank.RerankerConfig{
		BaseURL: server.URL, ModelName: "rerank-test", ModelID: "rr-1", APIKey: "test-only",
	})
	require.NoError(t, err)
	models := newRerankModelService()
	models.reranker = model
	ctx := logprivacy.SuppressContent(searchKnowledgeCtx())
	check := func(t *testing.T, hits []*types.SearchResult, meta types.RetrievalMeta) {
		t.Helper()
		require.Equal(t, []string{"c1", "c2", "c3"}, []string{hits[0].ID, hits[1].ID, hits[2].ID})
		require.NotNil(t, meta.Rerank)
		assert.Equal(t, types.RerankOutcomeModelError, meta.Rerank.Outcome)
		assert.False(t, meta.Rerank.Applied)
		assert.Equal(t, 3, meta.Rerank.CandidateCount)
		assert.Equal(t, 3, meta.Rerank.ResultCount)
		assert.NotEmpty(t, meta.Rerank.Error)
		wire, err := json.Marshal(meta)
		require.NoError(t, err)
		assert.NotContains(t, string(wire), providerBody)
		assert.NotContains(t, string(wire), server.URL)
	}
	t.Run("hybrid search", func(t *testing.T) {
		s := &knowledgeBaseService{modelService: models}
		diag := &types.RerankDiagnostics{}
		hits := s.rerankCandidates(ctx, "rr-1", "q", threeHits(), 0.3, 3, 0, diag)
		require.Len(t, hits, 3)
		check(t, hits, types.RetrievalMeta{Rerank: diag})
	})
	t.Run("knowledge search pipeline", func(t *testing.T) {
		s := newSearchKnowledgeService(t, &types.RetrievalConfig{RerankModelID: "rr-1"}, models,
			&fakeRetrieval{results: threeHits()})
		got, err := s.SearchKnowledge(ctx, []string{"kb-1"}, nil, nil, "q", nil)
		require.NoError(t, err)
		require.Len(t, got.Results, 3)
		check(t, got.Results, got.Meta)
	})
	require.Equal(t, int32(2), requests.Load())
}

func TestApplyKnowledgeSearchOverrides(t *testing.T) {
	t.Parallel()
	base := func() *types.ChatManage {
		return &types.ChatManage{PipelineRequest: types.PipelineRequest{
			EmbeddingTopK: 50, VectorThreshold: 0.15, KeywordThreshold: 0.3, RerankTopK: 10, RerankThreshold: 0.2,
		}}
	}

	cm := base()
	applyKnowledgeSearchOverrides(cm, &types.KnowledgeSearchOptions{})
	assert.Equal(t, base().PipelineRequest, cm.PipelineRequest, "no overrides keep the tenant config")

	vector, keyword, threshold := 0.5, 0.6, -1.5
	cm = base()
	applyKnowledgeSearchOverrides(cm, &types.KnowledgeSearchOptions{
		VectorThreshold: &vector, KeywordThreshold: &keyword, MatchCount: 80,
	})
	assert.Equal(t, 0.5, cm.VectorThreshold)
	assert.Equal(t, 0.6, cm.KeywordThreshold)
	assert.Equal(t, 80, cm.RerankTopK, "match_count is the final result count")
	assert.Equal(t, 80, cm.EmbeddingTopK, "recall must be deep enough to reach match_count")

	cm = base()
	applyKnowledgeSearchOverrides(cm, &types.KnowledgeSearchOptions{
		MatchCount: 20,
		Rerank:     &types.RerankOptions{TopK: 5, Threshold: &threshold},
	})
	assert.Equal(t, 5, cm.RerankTopK, "rerank.top_k wins over match_count")
	assert.Equal(t, -1.5, cm.RerankThreshold)
	assert.Equal(t, 50, cm.EmbeddingTopK)

	cm = base()
	applyKnowledgeSearchOverrides(cm, &types.KnowledgeSearchOptions{ParentSlotAggregation: true})
	assert.True(t, cm.ParentSlotAggregation, "the request flag reaches the top-k stage")
}
