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
