package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestEvaluationRunsPersistAndIsolateTenants(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.Exec(`CREATE TABLE evaluation_runs (
		task_id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL, dataset_id TEXT NOT NULL,
		status INTEGER NOT NULL, detail_json TEXT NOT NULL, started_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL)`).Error)
	repo := NewEvaluationRunRepository(db)
	ctx := context.Background()
	started := time.Now().UTC().Truncate(time.Second)
	detail := &types.EvaluationDetail{Task: &types.EvaluationTask{
		ID: "evaluation_7_test", TenantID: 7, DatasetID: "fixture", StartTime: started,
		Status: types.EvaluationStatueRunning, Total: 2,
	}}
	require.NoError(t, repo.Save(ctx, detail))
	detail.Task.Status = types.EvaluationStatueSuccess
	detail.Task.Finished = 2
	detail.Metric = &types.MetricResult{RetrievalMetrics: types.RetrievalMetrics{Recall: 0.75}}
	require.NoError(t, repo.Save(ctx, detail))

	// Recreate the repository to prove the result does not depend on process memory.
	restarted := NewEvaluationRunRepository(db)
	loaded, err := restarted.Get(ctx, 7, detail.Task.ID)
	require.NoError(t, err)
	require.Equal(t, types.EvaluationStatueSuccess, loaded.Task.Status)
	require.Equal(t, 2, loaded.Task.Finished)
	require.InDelta(t, 0.75, loaded.Metric.RetrievalMetrics.Recall, 0.001)
	_, err = restarted.Get(ctx, 8, detail.Task.ID)
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	listed, err := restarted.List(ctx, 7, 20)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	listed, err = restarted.List(ctx, 8, 20)
	require.NoError(t, err)
	require.Empty(t, listed)
}
