package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestEvaluationPrecisionCountsIrrelevantAndUnknownChunks(t *testing.T) {
	corpus := []string{"", "审批流程由财务部执行", "差旅标准由人事部维护"}
	const knowledgeID = "evaluation-knowledge"
	for _, tc := range []struct {
		name    string
		results []*types.SearchResult
	}{
		{name: "irrelevant corpus passage", results: []*types.SearchResult{
			{KnowledgeID: knowledgeID, ChunkIndex: 1}, {KnowledgeID: knowledgeID, ChunkIndex: 2},
		}},
		{name: "unknown chunk", results: []*types.SearchResult{
			{KnowledgeID: knowledgeID, ChunkIndex: 1}, {KnowledgeID: "foreign-knowledge", ChunkIndex: 2},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := NewHookMetric(1, corpus, knowledgeID)
			hook.recordInit(0)
			hook.recordQaPair(0, &types.QAPair{PIDs: []int{1}, Passages: []string{corpus[1]}})
			hook.recordSearchResult(0, tc.results)
			hook.recordFinish(0)
			result := hook.MetricResult()
			require.InDelta(t, 0.5, result.RetrievalMetrics.Precision, 0.001)
			require.InDelta(t, 1.0, result.RetrievalMetrics.Recall, 0.001)
		})
	}
}

func TestEvaluationPassageMappingUsesChunkIdentity(t *testing.T) {
	corpus := []string{"相同内容", "相同内容", "另一个段落"}
	results := []*types.SearchResult{
		{KnowledgeID: "evaluation-knowledge", ChunkIndex: 1, Content: "已扩展的上下文：相同内容"},
		{KnowledgeID: "evaluation-knowledge", ChunkIndex: 0, Content: "相同内容"},
		{KnowledgeID: "evaluation-knowledge", ChunkIndex: 1, Content: "重复命中"},
		{KnowledgeID: "foreign-knowledge", ChunkIndex: 2, Content: "另一个段落"},
		{KnowledgeID: "evaluation-knowledge", ChunkIndex: 99, Content: "另一个段落"},
	}
	require.Equal(t, []int{1, 0, -4, -5}, matchRetrievedPassageIDs(corpus, results, "evaluation-knowledge"))
}

func TestEvaluationMetricsExcludeUnlabeledCasesFromReferenceAverages(t *testing.T) {
	answerable := &types.MetricInput{
		RetrievalGT: [][]int{{7}}, RetrievalIDs: []int{7},
		GeneratedGT: "已批准", GeneratedTexts: "已批准",
	}
	unlabeled := &types.MetricInput{
		RetrievalGT: [][]int{nil}, RetrievalIDs: []int{9},
		GeneratedGT: "", GeneratedTexts: "不能从资料确认",
	}
	baseline := &MetricList{}
	baseline.Append(answerable)
	want := baseline.Avg()
	mixed := &MetricList{}
	mixed.Append(answerable)
	mixed.Append(unlabeled)
	got := mixed.Avg()
	require.Equal(t, 2, got.MetricVersion)
	require.Equal(t, 1, got.RetrievalEvaluated)
	require.Equal(t, 1, got.GenerationEvaluated)
	require.Equal(t, want.RetrievalMetrics, got.RetrievalMetrics)
	require.Equal(t, want.GenerationMetrics, got.GenerationMetrics)
}
