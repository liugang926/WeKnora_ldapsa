package service

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvaluationFixtureIncludesUnreferencedPassagesAndStableDenseIDs(t *testing.T) {
	d := dataset{
		queries: map[int64]string{20: "没有答案的问题", 10: "相关问题"},
		corpus:  map[int64]string{9000000: "干扰段落", 42: "相关证据"},
		answers: map[int64]string{1: "参考答案"},
		qrels:   map[int64][]int64{10: {42}},
		qas:     map[int64]int64{10: 1},
	}
	fixture, err := d.evaluationFixture()
	require.NoError(t, err)
	require.Equal(t, []string{"相关证据", "干扰段落"}, fixture.Corpus)
	require.Equal(t, []int{0}, fixture.QAPairs[0].PIDs)
	require.Equal(t, 10, fixture.QAPairs[0].QID)
	require.Equal(t, 20, fixture.QAPairs[1].QID)
	require.Empty(t, fixture.QAPairs[1].PIDs)
	second, err := d.evaluationFixture()
	require.NoError(t, err)
	require.Equal(t, fixture, second)
}

func TestDatasetFromRowsRejectsAmbiguousLabels(t *testing.T) {
	type rows struct {
		queries []TextInfo
		corpus  []TextInfo
		answers []TextInfo
		qrels   []RelsInfo
		qas     []QaInfo
	}
	base := func() rows {
		return rows{
			queries: []TextInfo{{ID: 1, Text: "问题"}},
			corpus:  []TextInfo{{ID: 2, Text: "证据"}},
			answers: []TextInfo{{ID: 3, Text: "答案"}},
			qrels:   []RelsInfo{{QID: 1, PID: 2}},
			qas:     []QaInfo{{QID: 1, AID: 3}},
		}
	}
	tests := []struct {
		name   string
		mutate func(*rows)
		want   string
	}{
		{
			"duplicate question", func(r *rows) { r.queries = append(r.queries, r.queries[0]) },
			"duplicate evaluation question",
		},
		{
			"duplicate passage", func(r *rows) { r.corpus = append(r.corpus, r.corpus[0]) },
			"duplicate evaluation passage",
		},
		{
			"duplicate answer", func(r *rows) { r.answers = append(r.answers, r.answers[0]) },
			"duplicate evaluation answer ID",
		},
		{"orphan evidence question", func(r *rows) { r.qrels[0].QID = 9 }, "evidence references missing question"},
		{"orphan evidence passage", func(r *rows) { r.qrels[0].PID = 9 }, "references missing passage"},
		{
			"duplicate evidence", func(r *rows) { r.qrels = append(r.qrels, r.qrels[0]) },
			"duplicate evaluation evidence",
		},
		{"orphan answer question", func(r *rows) { r.qas[0].QID = 9 }, "answer references missing question"},
		{"orphan answer", func(r *rows) { r.qas[0].AID = 9 }, "references missing answer"},
		{"blank linked answer", func(r *rows) { r.answers[0].Text = "  " }, "references blank answer"},
		{
			"duplicate answer link", func(r *rows) { r.qas = append(r.qas, r.qas[0]) },
			"duplicate evaluation answer link",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := base()
			tc.mutate(&r)
			_, err := datasetFromRows(r.queries, r.corpus, r.answers, r.qrels, r.qas)
			require.ErrorContains(t, err, tc.want)
		})
	}
	r := base()
	_, err := datasetFromRows(r.queries, r.corpus, r.answers, r.qrels, r.qas)
	require.NoError(t, err)
}

func TestEvaluationDatasetRejectsUnsafeID(t *testing.T) {
	service := &DatasetService{}
	for _, id := range []string{"../private", "../../etc/passwd", "a/b", "a\\b", ""} {
		_, err := service.GetDatasetByID(context.Background(), id)
		require.Error(t, err, id)
	}
}

func TestDefaultEvaluationDatasetLoadsCompleteCorpus(t *testing.T) {
	t.Setenv("EVALUATION_DEFAULT_DATASET_DIR", filepath.Join("..", "..", "..", "dataset", "samples"))
	service := &DatasetService{}
	fixture, err := service.GetDatasetByID(context.Background(), "default")
	require.NoError(t, err)
	require.NotEmpty(t, fixture.QAPairs)
	require.NotEmpty(t, fixture.Corpus)
	for _, pair := range fixture.QAPairs {
		for _, pid := range pair.PIDs {
			require.GreaterOrEqual(t, pid, 0)
			require.Less(t, pid, len(fixture.Corpus))
		}
	}
}

func TestSyntheticChineseBenchmarkLoadsNoAnswerAndCrossDocumentCases(t *testing.T) {
	t.Setenv("EVALUATION_DATASET_DIR", filepath.Join("..", "..", "..", "dataset", "benchmarks"))
	fixture, err := (&DatasetService{}).GetDatasetByID(context.Background(), "synthetic-zh-v1")
	require.NoError(t, err)
	require.Len(t, fixture.Corpus, 16)
	require.Len(t, fixture.QAPairs, 16)
	require.Len(t, fixture.QAPairs[1].PIDs, 2)
	require.Empty(t, fixture.QAPairs[11].PIDs)
	require.Empty(t, fixture.QAPairs[11].Answer)
}

func TestSyntheticEnterpriseBenchmarkLoadsAllCategories(t *testing.T) {
	t.Setenv("EVALUATION_DATASET_DIR", filepath.Join("..", "..", "..", "dataset", "benchmarks"))
	fixture, err := (&DatasetService{}).GetDatasetByID(context.Background(), "synthetic-enterprise-zh-v2")
	require.NoError(t, err)
	require.Len(t, fixture.Corpus, 42)
	require.Len(t, fixture.QAPairs, 70)
	require.Len(t, fixture.QAPairs[12].PIDs, 2) // cross-document
	require.Empty(t, fixture.QAPairs[20].PIDs)  // no-answer
	require.Empty(t, fixture.QAPairs[20].Answer)
	require.Len(t, fixture.QAPairs[31].PIDs, 2) // nested-group permission-allowed
	require.Empty(t, fixture.QAPairs[50].PIDs)  // permission-denied
	require.Empty(t, fixture.QAPairs[50].Answer)
}

func TestEvaluationQuestionLimit(t *testing.T) {
	t.Setenv("EVALUATION_MAX_QUESTIONS", "")
	limit, err := maxEvaluationQuestions()
	require.NoError(t, err)
	require.Equal(t, 100, limit)
	t.Setenv("EVALUATION_MAX_QUESTIONS", "250")
	limit, err = maxEvaluationQuestions()
	require.NoError(t, err)
	require.Equal(t, 250, limit)
	for _, invalid := range []string{"0", "10001", "many"} {
		t.Setenv("EVALUATION_MAX_QUESTIONS", invalid)
		_, err = maxEvaluationQuestions()
		require.Error(t, err)
	}
}

func TestEvaluationConcurrencyLimit(t *testing.T) {
	t.Setenv("EVALUATION_MAX_CONCURRENCY", "")
	workers, err := evaluationConcurrency()
	require.NoError(t, err)
	require.Equal(t, 2, workers)
	t.Setenv("EVALUATION_MAX_CONCURRENCY", "4")
	workers, err = evaluationConcurrency()
	require.NoError(t, err)
	require.Equal(t, 4, workers)
	for _, invalid := range []string{"0", "65", "many"} {
		t.Setenv("EVALUATION_MAX_CONCURRENCY", invalid)
		_, err = evaluationConcurrency()
		require.Error(t, err)
	}
}

func TestEvaluationExecutionMetricsPreserveUsageCoverage(t *testing.T) {
	metrics := evaluationExecutionMetrics([]int64{20, 10, 30}, 150, 25, 3, 2)
	require.Equal(t, int64(20), metrics.LatencyP50Ms)
	require.Equal(t, int64(30), metrics.LatencyP95Ms)
	require.Equal(t, int64(150), metrics.PromptTokens)
	require.Equal(t, int64(25), metrics.CompletionTokens)
	require.Equal(t, 1, metrics.UsageAccountingVersion)
	require.Equal(t, 3, metrics.ChatResponses)
	require.Equal(t, 2, metrics.UsageReportedResponses)
}
