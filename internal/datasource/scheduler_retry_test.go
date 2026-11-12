package datasource

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

func TestRetryBackoffLadder(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{0, 0},
		{1, 0},
		{2, 12 * time.Hour},
		{3, 18 * time.Hour},
		{4, 24 * time.Hour},
		{50, 24 * time.Hour},
	}
	for _, tc := range cases {
		if got := retryBackoff(tc.failures); got != tc.want {
			t.Fatalf("retryBackoff(%d) = %v, want %v", tc.failures, got, tc.want)
		}
	}
}

func TestSchedulableStatus(t *testing.T) {
	for status, want := range map[string]bool{
		types.DataSourceStatusActive: true,
		types.DataSourceStatusError:  true,
		types.DataSourceStatusPaused: false,
		"deleted":                    false,
	} {
		if got := schedulableStatus(status); got != want {
			t.Fatalf("schedulableStatus(%q) = %v, want %v", status, got, want)
		}
	}
}

type failureHistoryLogs struct {
	*fakeSyncLogRepo
	count int
	last  time.Time
}

func (r *failureHistoryLogs) ConsecutiveFailures(context.Context, string) (int, time.Time, error) {
	return r.count, r.last, nil
}

type retryTestQueue struct {
	count atomic.Int64
}

func (q *retryTestQueue) Enqueue(_ *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.count.Add(1)
	return &asynq.TaskInfo{}, nil
}

// TestTriggerSyncErrorStateBackoff drives triggerSync against an error-state
// source: inside the backoff window the sync is deferred, once the window
// opens it is admitted and enqueued.
func TestTriggerSyncErrorStateBackoff(t *testing.T) {
	ds := &types.DataSource{
		ID: "ds-err", TenantID: 1, Type: types.ConnectorTypeDingTalk,
		Status: types.DataSourceStatusError, SyncSchedule: "* * * * * *",
	}
	dsRepo := newFakeDataSourceRepo()
	dsRepo.dataSources["ds-err"] = ds

	logs := newFakeSyncLogRepo()
	failedAt := time.Now().UTC().Add(-30 * time.Minute)
	logs.logs["log-1"] = &types.SyncLog{
		ID: "log-1", DataSourceID: "ds-err", Status: types.SyncLogStatusFailed,
		FinishedAt: &failedAt,
	}

	q := &retryTestQueue{}

	// Two consecutive failures 30 minutes ago -> 12h backoff -> deferred.
	s := NewScheduler(dsRepo, &failureHistoryLogs{fakeSyncLogRepo: logs, count: 2, last: failedAt}, q)
	s.triggerSync("ds-err", 1)
	if got := q.count.Load(); got != 0 {
		t.Fatalf("expected deferral inside backoff, got %d enqueues", got)
	}

	// A single failure never defers: admitted at the next window.
	s2 := NewScheduler(dsRepo, &failureHistoryLogs{fakeSyncLogRepo: logs, count: 1, last: failedAt}, q)
	s2.triggerSync("ds-err", 1)
	if got := q.count.Load(); got != 1 {
		t.Fatalf("expected admission after single failure, got %d enqueues", got)
	}
}
