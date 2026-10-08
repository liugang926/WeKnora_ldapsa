package service

import (
	"context"
	"strings"
	"sync"

	"github.com/Tencent/WeKnora/internal/application/service/metric"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// MetricList stores and aggregates metric results
type MetricList struct {
	results []metricObservation
}

type metricObservation struct {
	result              *types.MetricResult
	retrievalEvaluated  bool
	generationEvaluated bool
}

type metricScope uint8

const (
	retrievalScope metricScope = iota
	generationScope
)

// metricCalculators defines all metrics to be calculated
var metricCalculators = []struct {
	calc     interfaces.Metrics                 // Metric calculator implementation
	getField func(*types.MetricResult) *float64 // Field accessor for result
	scope    metricScope
}{
	// Retrieval Metrics
	{metric.NewPrecisionMetric(), func(r *types.MetricResult) *float64 {
		return &r.RetrievalMetrics.Precision
	}, retrievalScope},
	{metric.NewRecallMetric(), func(r *types.MetricResult) *float64 {
		return &r.RetrievalMetrics.Recall
	}, retrievalScope},
	{metric.NewNDCGMetric(3), func(r *types.MetricResult) *float64 {
		return &r.RetrievalMetrics.NDCG3
	}, retrievalScope},
	{metric.NewNDCGMetric(10), func(r *types.MetricResult) *float64 {
		return &r.RetrievalMetrics.NDCG10
	}, retrievalScope},
	{metric.NewMRRMetric(), func(r *types.MetricResult) *float64 { return &r.RetrievalMetrics.MRR }, retrievalScope},
	{metric.NewMAPMetric(), func(r *types.MetricResult) *float64 { return &r.RetrievalMetrics.MAP }, retrievalScope},

	// Generation Metrics
	{metric.NewBLEUMetric(true, metric.BLEU1Gram), func(r *types.MetricResult) *float64 {
		return &r.GenerationMetrics.BLEU1
	}, generationScope},
	{metric.NewBLEUMetric(true, metric.BLEU2Gram), func(r *types.MetricResult) *float64 {
		return &r.GenerationMetrics.BLEU2
	}, generationScope},
	{metric.NewBLEUMetric(true, metric.BLEU4Gram), func(r *types.MetricResult) *float64 {
		return &r.GenerationMetrics.BLEU4
	}, generationScope},
	{metric.NewRougeMetric(true, "rouge-1", "f"), func(r *types.MetricResult) *float64 {
		return &r.GenerationMetrics.ROUGE1
	}, generationScope},
	{metric.NewRougeMetric(true, "rouge-2", "f"), func(r *types.MetricResult) *float64 {
		return &r.GenerationMetrics.ROUGE2
	}, generationScope},
	{metric.NewRougeMetric(true, "rouge-l", "f"), func(r *types.MetricResult) *float64 {
		return &r.GenerationMetrics.ROUGEL
	}, generationScope},
}

// Append calculates and stores metrics for given input
func (m *MetricList) Append(metricInput *types.MetricInput) {
	result := &types.MetricResult{}
	retrievalEvaluated := false
	for _, groundTruth := range metricInput.RetrievalGT {
		if len(groundTruth) > 0 {
			retrievalEvaluated = true
			break
		}
	}
	generationEvaluated := strings.TrimSpace(metricInput.GeneratedGT) != ""
	// Calculate all configured metrics
	for _, c := range metricCalculators {
		if (c.scope == retrievalScope && !retrievalEvaluated) ||
			(c.scope == generationScope && !generationEvaluated) {
			continue
		}
		score := c.calc.Compute(metricInput)
		*c.getField(result) = score
	}
	logger.Infof(context.Background(), "metric: %v", result)
	m.results = append(m.results, metricObservation{result, retrievalEvaluated, generationEvaluated})
}

// Avg calculates average of all stored metric results
func (m *MetricList) Avg() *types.MetricResult {
	if len(m.results) == 0 {
		return &types.MetricResult{MetricVersion: 2}
	}

	avgResult := &types.MetricResult{MetricVersion: 2}
	for _, observation := range m.results {
		if observation.retrievalEvaluated {
			avgResult.RetrievalEvaluated++
		}
		if observation.generationEvaluated {
			avgResult.GenerationEvaluated++
		}
	}

	// Calculate average for each metric
	for _, config := range metricCalculators {
		sum := 0.0
		count := 0
		for _, observation := range m.results {
			if (config.scope == retrievalScope && !observation.retrievalEvaluated) ||
				(config.scope == generationScope && !observation.generationEvaluated) {
				continue
			}
			sum += *config.getField(observation.result)
			count++
		}
		if count > 0 {
			*config.getField(avgResult) = sum / float64(count)
		}
	}
	return avgResult
}

// HookMetric tracks evaluation metrics for QA pairs
type HookMetric struct {
	qaPairMetricList []*qaPairMetric // Per-QA pair metrics
	metricResults    *MetricList     // Aggregated results
	allPassages      []string        // Entire indexed corpus, not only this query's relevant passages
	knowledgeID      string          // The temporary evaluation knowledge that owns every corpus passage
	mu               *sync.RWMutex   // Thread safety
}

// qaPairMetric stores metrics for a single QA pair
type qaPairMetric struct {
	qaPair       *types.QAPair
	searchResult []*types.SearchResult
	rerankResult []*types.SearchResult
	chatResponse *types.ChatResponse
}

// NewHookMetric creates a new HookMetric with given capacity
func NewHookMetric(capacity int, corpus []string, knowledgeID string) *HookMetric {
	return &HookMetric{
		metricResults:    &MetricList{},
		qaPairMetricList: make([]*qaPairMetric, capacity),
		allPassages:      corpus,
		knowledgeID:      knowledgeID,
		mu:               &sync.RWMutex{},
	}
}

// recordInit initializes metric tracking for a QA pair
func (h *HookMetric) recordInit(index int) {
	h.qaPairMetricList[index] = &qaPairMetric{}
}

// recordQaPair records the QA pair data
func (h *HookMetric) recordQaPair(index int, qaPair *types.QAPair) {
	h.qaPairMetricList[index].qaPair = qaPair
}

// recordSearchResult records search results
func (h *HookMetric) recordSearchResult(index int, searchResult []*types.SearchResult) {
	h.qaPairMetricList[index].searchResult = searchResult
}

// recordRerankResult records reranked results
func (h *HookMetric) recordRerankResult(index int, rerankResult []*types.SearchResult) {
	h.qaPairMetricList[index].rerankResult = rerankResult
}

// recordChatResponse records the generated chat response
func (h *HookMetric) recordChatResponse(index int, chatResponse *types.ChatResponse) {
	h.qaPairMetricList[index].chatResponse = chatResponse
}

// recordFinish finalizes metrics for a QA pair
func (h *HookMetric) recordFinish(index int) {
	// Prepare retrieval source: prefer rerank results, fall back to search results
	retrievalSource := h.qaPairMetricList[index].rerankResult
	if len(retrievalSource) == 0 {
		retrievalSource = h.qaPairMetricList[index].searchResult
	}

	// Map chunks against the entire indexed corpus using the stable chunk index
	// assigned when the temporary knowledge was created. Search and rerank may
	// enrich or reformat Content, so text matching is not reliable. Unknown or
	// foreign chunks remain non-relevant hits instead of being omitted.
	qaPair := h.qaPairMetricList[index].qaPair
	retrievalIDs := matchRetrievedPassageIDs(h.allPassages, retrievalSource, h.knowledgeID)

	// Get generated text if available
	generatedTexts := ""
	if h.qaPairMetricList[index].chatResponse != nil {
		generatedTexts = h.qaPairMetricList[index].chatResponse.Content
	}

	// Prepare metric input data
	metricInput := &types.MetricInput{
		RetrievalGT:    [][]int{qaPair.PIDs},
		RetrievalIDs:   retrievalIDs,
		GeneratedTexts: generatedTexts,
		GeneratedGT:    qaPair.Answer,
	}

	// Thread-safe append of metrics
	h.mu.Lock()
	defer h.mu.Unlock()
	h.metricResults.Append(metricInput)
}

func matchRetrievedPassageIDs(corpus []string, results []*types.SearchResult, knowledgeID string) []int {
	retrievalIDs := make([]int, 0, len(results))
	seen := make(map[int]struct{})
	for resultIndex, r := range results {
		matchedID := -1 - resultIndex
		if r != nil && knowledgeID != "" && r.KnowledgeID == knowledgeID &&
			r.ChunkIndex >= 0 && r.ChunkIndex < len(corpus) &&
			(r.ChunkType == "" || r.ChunkType == string(types.ChunkTypeText)) {
			matchedID = r.ChunkIndex
		}
		if _, ok := seen[matchedID]; ok {
			continue
		}
		seen[matchedID] = struct{}{}
		retrievalIDs = append(retrievalIDs, matchedID)
	}
	return retrievalIDs
}

// MetricResult returns the averaged metric results
func (h *HookMetric) MetricResult() *types.MetricResult {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.metricResults.Avg()
}
