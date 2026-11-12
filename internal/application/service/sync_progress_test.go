package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// progressRecorder captures UpdateProgress calls so the writer's throttling
// and field mapping can be asserted without a database.
type progressRecorder struct {
	calls []map[string]any
}

func (r *progressRecorder) Create(context.Context, *types.SyncLog) error { return nil }
func (r *progressRecorder) FindByID(context.Context, string) (*types.SyncLog, error) {
	return nil, nil
}

func (r *progressRecorder) FindByDataSource(context.Context, string, int, int) ([]*types.SyncLog, error) {
	return nil, nil
}

func (r *progressRecorder) FindLatest(context.Context, string) (*types.SyncLog, error) {
	return nil, nil
}

func (r *progressRecorder) HasRunningSync(context.Context, string) (bool, error) {
	return false, nil
}

func (r *progressRecorder) ConsecutiveFailures(context.Context, string) (int, time.Time, error) {
	return 0, time.Time{}, nil
}

func (r *progressRecorder) UpdateProgress(_ context.Context, _ string, updates map[string]any) error {
	r.calls = append(r.calls, updates)
	return nil
}

func (r *progressRecorder) ClaimNextcloudSyncStart(
	context.Context,
	string,
	string,
	uint64,
	string,
	string,
	int) (string,
	error,
) {
	return "", nil
}

func (r *progressRecorder) FinishNextcloudSyncAttempt(
	context.Context,
	string,
	string,
	uint64,
	string,
	string,
	string,
	bool,
	string) (bool,
	error,
) {
	return false, nil
}

func (r *progressRecorder) MarkNextcloudEnqueueUncertain(
	context.Context,
	string,
	string,
	uint64,
	string,
	string) (bool,
	error,
) {
	return false, nil
}
func (r *progressRecorder) Update(context.Context, *types.SyncLog) error { return nil }
func (r *progressRecorder) UpdateResult(context.Context, *types.SyncLog) error {
	return nil
}

func (r *progressRecorder) CancelPendingByDataSource(context.Context, string) error {
	return nil
}
func (r *progressRecorder) CleanupOldLogs(context.Context, int) error { return nil }

func TestSyncProgressWriterThrottlesAndMaps(t *testing.T) {
	rec := &progressRecorder{}
	w := newSyncProgressWriter(rec, "log-1")

	// First report flushes immediately.
	w.reportFetch(datasource.FetchProgress{Enumerated: 10, Processed: 4})
	if len(rec.calls) != 1 {
		t.Fatalf("first report should flush, got %d calls", len(rec.calls))
	}
	first := rec.calls[0]
	if got := first["items_total"].(int); got != 10 {
		t.Fatalf("items_total = %v, want 10", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(first["result"].(types.JSON), &payload); err != nil {
		t.Fatalf("progress payload not json: %v", err)
	}
	if payload["phase"] != "fetch" || payload["fetched"].(float64) != 4 {
		t.Fatalf("unexpected fetch payload: %v", payload)
	}

	// Immediate second report is throttled away.
	w.reportFetch(datasource.FetchProgress{Enumerated: 11, Processed: 5})
	if len(rec.calls) != 1 {
		t.Fatalf("throttled report should not flush, got %d calls", len(rec.calls))
	}

	// Import reports carry the authoritative counters.
	w.reportImport(11, 3, 2, 1, 1, 7)
	if len(rec.calls) != 1 {
		t.Fatalf("import report still inside throttle window, got %d calls", len(rec.calls))
	}
	w.last = time.Now().Add(-2 * dingTalkProgressFlushInterval)
	w.reportImport(11, 3, 2, 1, 1, 7)
	if len(rec.calls) != 2 {
		t.Fatalf("aged import report should flush, got %d calls", len(rec.calls))
	}
	second := rec.calls[1]
	if second["items_created"].(int) != 3 || second["items_failed"].(int) != 1 {
		t.Fatalf("unexpected import counters: %v", second)
	}
	if err := json.Unmarshal(second["result"].(types.JSON), &payload); err != nil || payload["phase"] != "import" {
		t.Fatalf("unexpected import payload: %v %v", payload, err)
	}
}

func TestDingTalkCancelMessage(t *testing.T) {
	if got := dingTalkCancelMessage(nil); got != "" {
		t.Fatalf("nil cause should produce empty message, got %q", got)
	}
	if got := dingTalkCancelMessage(context.Canceled); got == "" {
		t.Fatal("non-nil cause should produce a message")
	}
}
