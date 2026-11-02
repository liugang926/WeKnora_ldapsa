package service

import (
 "context"
 "os"
 "testing"
 "github.com/stretchr/testify/require"
)

// The packaging smoke test must use files extracted from the built image.
func TestPackagedEvaluationBenchmark(t *testing.T) {
 dir := os.Getenv("EVALUATION_PACKAGED_BENCHMARK_DIR")
 if dir == "" { t.Skip("requires a benchmark directory extracted from the candidate image") }
 t.Setenv("EVALUATION_DATASET_DIR", dir)
 fixture, err := (&DatasetService{}).GetDatasetByID(context.Background(), "synthetic-zh-v1")
 require.NoError(t, err)
 require.Len(t, fixture.Corpus, 16)
 require.Len(t, fixture.QAPairs, 16)
 require.Len(t, fixture.QAPairs[1].PIDs, 2)
 require.Empty(t, fixture.QAPairs[11].PIDs)
 require.Empty(t, fixture.QAPairs[11].Answer)
}
