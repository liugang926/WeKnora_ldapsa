package chatpipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/rerank"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type failingReranker struct{ err error }

func (r *failingReranker) Rerank(context.Context, string, []string) ([]rerank.RankResult, error) {
	return nil, r.err
}
func (r *failingReranker) GetModelName() string { return "failing-reranker" }
func (r *failingReranker) GetModelID() string   { return "rerank-1" }

type failingRerankModelService struct {
	interfaces.ModelService
	model rerank.Reranker
}

func (s *failingRerankModelService) GetRerankModel(context.Context, string) (rerank.Reranker, error) {
	return s.model, nil
}

func TestRerankAPIErrorFailsStrictEvaluationOnly(t *testing.T) {
	rootCause := errors.New("rerank rate limited")
	plugin := &PluginRerank{modelService: &failingRerankModelService{
		model: &failingReranker{err: rootCause},
	}}
	newChatManage := func() *types.ChatManage {
		return &types.ChatManage{
			PipelineRequest: types.PipelineRequest{RerankModelID: "rerank-1", RerankTopK: 5},
			PipelineState: types.PipelineState{
				RewriteQuery: "测试问题",
				SearchResult: []*types.SearchResult{{ID: "chunk-1", Content: "证据内容"}},
			},
		}
	}

	strictNextCalled := false
	strictErr := plugin.OnEvent(WithStrictRetrieval(context.Background()), types.CHUNK_RERANK,
		newChatManage(), func() *PluginError {
			strictNextCalled = true
			return nil
		})
	if strictErr == nil || strictErr.ErrorType != ErrRerank.ErrorType || !errors.Is(strictErr.Err, rootCause) {
		t.Fatalf("strict evaluation should fail with rerank root cause, got %#v", strictErr)
	}
	if strictNextCalled {
		t.Fatal("strict evaluation continued after rerank failure")
	}

	normalNextCalled := false
	normalErr := plugin.OnEvent(context.Background(), types.CHUNK_RERANK, newChatManage(), func() *PluginError {
		normalNextCalled = true
		return nil
	})
	if normalErr != nil || !normalNextCalled {
		t.Fatalf("normal chat should keep rerank fallback, got err=%#v next=%t", normalErr, normalNextCalled)
	}
}
