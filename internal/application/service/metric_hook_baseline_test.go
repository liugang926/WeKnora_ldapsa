package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestEvaluationPrecisionCountsIrrelevantAndUnknownChunks(t *testing.T) {
	corpus := []string{"", "审批流程由财务部执行", "差旅标准由人事部维护"}
	for _, tc := range []struct {
		name    string
		results []*types.SearchResult
	}{
		{name: "irrelevant corpus passage", results: []*types.SearchResult{
			{Content: "审批流程由财务部执行"}, {Content: "差旅标准由人事部维护"},
		}},
		{name: "unknown chunk", results: []*types.SearchResult{
			{Content: "审批流程由财务部执行"}, {Content: "索引之外的文本"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := NewHookMetric(1, corpus)
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
