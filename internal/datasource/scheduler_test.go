package datasource

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
)

type nextcloudAdmissionBusyLogs struct{ *fakeSyncLogRepo }

func (r nextcloudAdmissionBusyLogs) Create(context.Context, *types.SyncLog) error {
	return ErrSyncAlreadyRunning
}

type nextcloudUncertainScheduleQueue struct {
	count             atomic.Int64
	taskID            string
	acceptedTaskID    string
	acceptBeforeError bool
}

func (q *nextcloudUncertainScheduleQueue) Enqueue(_ *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	q.count.Add(1)
	for _, opt := range opts {
		if opt.Type() == asynq.TaskIDOpt {
			q.taskID, _ = opt.Value().(string)
		}
	}
	if q.acceptBeforeError {
		q.acceptedTaskID = q.taskID
	}
	return nil, errors.New("synthetic lost enqueue reply")
}

type nextcloudLateScheduleQueue struct {
	logs             *fakeSyncLogRepo
	taskID           string
	failBeforeReturn bool
}

func (q *nextcloudLateScheduleQueue) Enqueue(_ *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	for _, opt := range opts {
		if opt.Type() == asynq.TaskIDOpt {
			q.taskID, _ = opt.Value().(string)
		}
	}
	if q.failBeforeReturn {
		q.logs.mu.Lock()
		for _, log := range q.logs.logs {
			log.Status = types.SyncLogStatusFailed
			log.ErrorMessage = "sync_task_absent_before_worker_start"
			now := time.Now().UTC()
			log.FinishedAt = &now
		}
		q.logs.mu.Unlock()
	}
	return &asynq.TaskInfo{ID: q.taskID}, nil
}

func TestNextcloudEventSchedulerLateEnqueueReceipt(t *testing.T) {
	for _, tc := range []struct {
		name             string
		failBeforeReturn bool
	}{
		{name: "current admission remains accepted"},
		{name: "recovery won before late queue reply", failBeforeReturn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := &types.DataSource{
				ID: "nextcloud-scheduled", TenantID: 1,
				Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
			}
			dsRepo := newFakeDataSourceRepo()
			_ = dsRepo.Create(context.Background(), ds)
			logs := newFakeSyncLogRepo()
			logs.nextID = "late-schedule-log"
			queue := &nextcloudLateScheduleQueue{logs: logs, failBeforeReturn: tc.failBeforeReturn}
			s := NewScheduler(dsRepo, logs, queue)
			s.triggerSync(ds.ID, ds.TenantID)
			if queue.taskID != "dssync:late-schedule-log" {
				t.Fatalf("queued task ID = %q", queue.taskID)
			}
			logs.mu.Lock()
			defer logs.mu.Unlock()
			log := logs.logs["late-schedule-log"]
			if log == nil {
				t.Fatal("missing sync log")
			}
			if tc.failBeforeReturn {
				if log.Status != types.SyncLogStatusFailed || logs.markAttempts != 1 {
					t.Fatalf("late receipt must enter uncertainty branch, status=%q attempts=%d",
						log.Status, logs.markAttempts)
				}
			} else if log.Status != types.SyncLogStatusRunning || logs.markAttempts != 0 {
				t.Fatalf("current receipt rejected, status=%q attempts=%d", log.Status, logs.markAttempts)
			}
		})
	}
}

func TestNextcloudEventSchedulerSkipsAdmissionConflict(t *testing.T) {
	ds := &types.DataSource{
		ID: "nextcloud-scheduled", TenantID: 1,
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
	}
	dsRepo := newFakeDataSourceRepo()
	_ = dsRepo.Create(context.Background(), ds)
	queue := &fakeTaskEnqueuer{}
	s := NewScheduler(dsRepo, nextcloudAdmissionBusyLogs{newFakeSyncLogRepo()}, queue)
	s.triggerSync(ds.ID, ds.TenantID)
	if queue.count.Load() != 0 {
		t.Fatal("scheduler queued a Nextcloud task without admission")
	}
}

func TestNextcloudEventSchedulerUncertainEnqueueRetainsRunningLog(t *testing.T) {
	for _, tc := range []struct {
		name              string
		acceptBeforeError bool
	}{
		{"failed_before_accept", false},
		{"accepted_reply_lost", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := &types.DataSource{
				ID: "nextcloud-scheduled", TenantID: 1,
				Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
			}
			dsRepo := newFakeDataSourceRepo()
			_ = dsRepo.Create(context.Background(), ds)
			logs := newFakeSyncLogRepo()
			logs.nextID = "stable-scheduled-log"
			queue := &nextcloudUncertainScheduleQueue{acceptBeforeError: tc.acceptBeforeError}
			s := NewScheduler(dsRepo, logs, queue)
			s.triggerSync(ds.ID, ds.TenantID)
			if queue.count.Load() != 1 || queue.taskID != "dssync:stable-scheduled-log" {
				t.Fatalf("scheduler task identity = %q, attempts = %d", queue.taskID, queue.count.Load())
			}
			if tc.acceptBeforeError && queue.acceptedTaskID != queue.taskID {
				t.Fatal("accepted task lost its stable identity")
			}
			if !tc.acceptBeforeError && queue.acceptedTaskID != "" {
				t.Fatal("pre-accept failure unexpectedly recorded queue acceptance")
			}
			// A later cron tick cannot bypass an ambiguous enqueue.
			s.triggerSync(ds.ID, ds.TenantID)
			if queue.count.Load() != 1 {
				t.Fatal("scheduler retried while the uncertain log still held admission")
			}
			logs.mu.Lock()
			defer logs.mu.Unlock()
			if len(logs.logs) != 1 {
				t.Fatalf("sync log count = %d, want 1", len(logs.logs))
			}
			log := logs.logs["stable-scheduled-log"]
			if log == nil || log.Status != types.SyncLogStatusRunning ||
				log.ErrorMessage != NextcloudSyncEnqueueUncertain {
				t.Fatalf("uncertain enqueue must retain running admission: %+v", log)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────
// Fake implementations for testing
// ──────────────────────────────────────────────────────────────────────

// fakeDataSourceRepo is an in-memory DataSourceRepository.
type fakeDataSourceRepo struct {
	mu          sync.Mutex
	dataSources map[string]*types.DataSource
}

func newFakeDataSourceRepo() *fakeDataSourceRepo {
	return &fakeDataSourceRepo{dataSources: make(map[string]*types.DataSource)}
}

func (r *fakeDataSourceRepo) Create(_ context.Context, ds *types.DataSource) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dataSources[ds.ID] = ds
	return nil
}

func (r *fakeDataSourceRepo) FindByID(_ context.Context, id string) (*types.DataSource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ds, ok := r.dataSources[id]
	if !ok {
		return nil, ErrDataSourceNotFound
	}
	return ds, nil
}

func (r *fakeDataSourceRepo) FindByKnowledgeBase(_ context.Context, kbID string) ([]*types.DataSource, error) {
	return nil, nil
}

func (r *fakeDataSourceRepo) Update(_ context.Context, ds *types.DataSource) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dataSources[ds.ID] = ds
	return nil
}

func (r *fakeDataSourceRepo) UpdateSyncState(ctx context.Context, ds *types.DataSource) error {
	return r.Update(ctx, ds)
}

func (r *fakeDataSourceRepo) UpdateNextcloudSyncStateCAS(_ context.Context, ds *types.DataSource,
	expectedStatus string,
) (bool, error) {
	if ds == nil || ds.ID == "" || ds.TenantID == 0 || ds.Type != types.ConnectorTypeNextcloud ||
		(expectedStatus != types.DataSourceStatusActive && expectedStatus != types.DataSourceStatusPaused &&
			expectedStatus != types.DataSourceStatusError) {
		return false, errors.New("invalid Nextcloud sync state comparison")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.dataSources[ds.ID]
	if current == nil || current.TenantID != ds.TenantID || current.Type != ds.Type ||
		current.Status != expectedStatus || current.DeletedAt.Valid {
		return false, nil
	}
	current.Status = ds.Status
	current.LastSyncAt = ds.LastSyncAt
	current.LastSyncCursor = ds.LastSyncCursor
	current.LastSyncResult = ds.LastSyncResult
	current.ErrorMessage = ds.ErrorMessage
	current.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (r *fakeDataSourceRepo) UpdateNextcloudRetryableEventFailure(_ context.Context, ds *types.DataSource) (
	bool, error,
) {
	if ds == nil || ds.ID == "" || ds.TenantID == 0 || ds.Type != types.ConnectorTypeNextcloud {
		return false, errors.New("invalid Nextcloud event failure source")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.dataSources[ds.ID]
	if current == nil || current.TenantID != ds.TenantID || current.Type != ds.Type ||
		current.Status != types.DataSourceStatusActive || current.DeletedAt.Valid {
		return false, nil
	}
	current.LastSyncResult = ds.LastSyncResult
	current.ErrorMessage = ds.ErrorMessage
	current.UpdatedAt = time.Now().UTC()
	return true, nil
}

func (r *fakeDataSourceRepo) Delete(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.dataSources, id)
	return nil
}

func (r *fakeDataSourceRepo) FindActive(_ context.Context) ([]*types.DataSource, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []*types.DataSource
	for _, ds := range r.dataSources {
		if ds.Status == types.DataSourceStatusActive && ds.SyncSchedule != "" {
			result = append(result, ds)
		}
	}
	return result, nil
}

// fakeSyncLogRepo is an in-memory SyncLogRepository.
type fakeSyncLogRepo struct {
	mu           sync.Mutex
	logs         map[string]*types.SyncLog
	nextID       string
	markAttempts int
}

func newFakeSyncLogRepo() *fakeSyncLogRepo {
	return &fakeSyncLogRepo{logs: make(map[string]*types.SyncLog)}
}

func (r *fakeSyncLogRepo) Create(_ context.Context, log *types.SyncLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if log.ID == "" {
		log.ID = r.nextID
		if log.ID == "" {
			log.ID = "log-" + time.Now().Format("150405.000")
		}
	}
	if log.RecoveryVersion == 1 && log.QueueTaskID == "" {
		log.QueueTaskID = NextcloudSyncTaskID(log.ID)
	}
	r.logs[log.ID] = log
	return nil
}

func (r *fakeSyncLogRepo) FindByID(_ context.Context, id string) (*types.SyncLog, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	l, ok := r.logs[id]
	if !ok {
		return nil, ErrSyncLogNotFound
	}
	return l, nil
}

func (r *fakeSyncLogRepo) FindByDataSource(_ context.Context, _ string, _, _ int) (
	[]*types.SyncLog, error,
) {
	return nil, nil
}

func (r *fakeSyncLogRepo) FindLatest(_ context.Context, dsID string) (*types.SyncLog, error) {
	return nil, nil
}

func (r *fakeSyncLogRepo) ClaimNextcloudSyncStart(_ context.Context, _, _ string, _ uint64, _, _ string,
	_ int,
) (string, error) {
	return "", nil
}

func (r *fakeSyncLogRepo) FinishNextcloudSyncAttempt(context.Context, string, string, uint64, string,
	string, string, bool, string,
) (bool, error) {
	return false, nil
}

func (r *fakeSyncLogRepo) MarkNextcloudEnqueueUncertain(_ context.Context, logID, _ string, _ uint64, _,
	_ string,
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markAttempts++
	log := r.logs[logID]
	if log == nil || log.Status != types.SyncLogStatusRunning || log.WorkerStartedAt != nil {
		return false, nil
	}
	log.ErrorMessage = NextcloudSyncEnqueueUncertain
	return true, nil
}

func (r *fakeSyncLogRepo) Update(_ context.Context, log *types.SyncLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs[log.ID] = log
	return nil
}

func (r *fakeSyncLogRepo) UpdateResult(ctx context.Context, log *types.SyncLog) error {
	return r.Update(ctx, log)
}

func (r *fakeSyncLogRepo) CancelPendingByDataSource(_ context.Context, dsID string) error {
	return nil
}

func (r *fakeSyncLogRepo) CleanupOldLogs(_ context.Context, retentionDays int) error {
	return nil
}

func (r *fakeSyncLogRepo) HasRunningSync(_ context.Context, dsID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, log := range r.logs {
		if log.DataSourceID == dsID && log.Status == types.SyncLogStatusRunning {
			return true, nil
		}
	}
	return false, nil
}

// fakeTaskEnqueuer counts how many tasks are enqueued.
type fakeTaskEnqueuer struct {
	count     atomic.Int64
	lastQueue atomic.Value
}

func (e *fakeTaskEnqueuer) Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	e.count.Add(1)
	for _, opt := range opts {
		if opt.Type() == asynq.QueueOpt {
			if queue, ok := opt.Value().(string); ok {
				e.lastQueue.Store(queue)
			}
		}
	}
	return &asynq.TaskInfo{ID: "task-fake"}, nil
}

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

func TestScheduler_StartWithActiveDataSources(t *testing.T) {
	repo := newFakeDataSourceRepo()
	_ = repo.Create(context.Background(), &types.DataSource{
		ID:           "ds-1",
		TenantID:     1,
		Status:       types.DataSourceStatusActive,
		SyncSchedule: "*/2 * * * * *", // every 2 seconds (6-field cron with seconds)
	})
	_ = repo.Create(context.Background(), &types.DataSource{
		ID:           "ds-2",
		TenantID:     1,
		Status:       types.DataSourceStatusPaused, // should NOT be scheduled
		SyncSchedule: "*/2 * * * * *",
	})

	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(repo, newFakeSyncLogRepo(), enqueuer)

	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatalf("Start() error: %v", err)
	}
	defer scheduler.Stop()

	// Only ds-1 should be registered (ds-2 is paused, not returned by FindActive)
	if scheduler.EntryCount() != 1 {
		t.Errorf("EntryCount() = %d, want 1", scheduler.EntryCount())
	}
}

func TestScheduler_CronFires(t *testing.T) {
	repo := newFakeDataSourceRepo()
	_ = repo.Create(context.Background(), &types.DataSource{
		ID:           "ds-fire",
		TenantID:     1,
		Status:       types.DataSourceStatusActive,
		SyncSchedule: "* * * * * *", // every second
	})

	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(repo, newFakeSyncLogRepo(), enqueuer)

	if err := scheduler.Start(context.Background()); err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	// Wait for at least one tick
	time.Sleep(2500 * time.Millisecond)
	scheduler.Stop()

	if enqueuer.count.Load() == 0 {
		t.Error("expected at least 1 enqueue, got 0")
	}
	if queue, _ := enqueuer.lastQueue.Load().(string); queue != types.QueueSync {
		t.Errorf("scheduled sync queue = %q, want %q", queue, types.QueueSync)
	}
}

func TestScheduler_AddOrUpdate(t *testing.T) {
	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(newFakeDataSourceRepo(), newFakeSyncLogRepo(), enqueuer)
	scheduler.cron.Start()
	defer scheduler.Stop()

	ds := &types.DataSource{
		ID:           "ds-new",
		TenantID:     1,
		Status:       types.DataSourceStatusActive,
		SyncSchedule: "0 0 * * * *", // every hour
	}

	// Add
	if err := scheduler.AddOrUpdate(ds); err != nil {
		t.Fatalf("AddOrUpdate() error: %v", err)
	}
	if scheduler.EntryCount() != 1 {
		t.Errorf("EntryCount() = %d, want 1", scheduler.EntryCount())
	}

	// Update schedule
	ds.SyncSchedule = "0 30 * * * *" // every half hour
	if err := scheduler.AddOrUpdate(ds); err != nil {
		t.Fatalf("AddOrUpdate() (update) error: %v", err)
	}
	if scheduler.EntryCount() != 1 {
		t.Errorf("after update: EntryCount() = %d, want 1 (should replace, not add)", scheduler.EntryCount())
	}
}

func TestScheduler_AddOrUpdate_PausedIsNoop(t *testing.T) {
	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(newFakeDataSourceRepo(), newFakeSyncLogRepo(), enqueuer)
	scheduler.cron.Start()
	defer scheduler.Stop()

	ds := &types.DataSource{
		ID:           "ds-paused",
		TenantID:     1,
		Status:       types.DataSourceStatusPaused,
		SyncSchedule: "0 0 * * * *",
	}

	if err := scheduler.AddOrUpdate(ds); err != nil {
		t.Fatalf("AddOrUpdate() error: %v", err)
	}
	if scheduler.EntryCount() != 0 {
		t.Errorf("paused ds should not be scheduled, EntryCount() = %d", scheduler.EntryCount())
	}
}

func TestScheduler_AddOrUpdate_EmptyScheduleIsNoop(t *testing.T) {
	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(newFakeDataSourceRepo(), newFakeSyncLogRepo(), enqueuer)
	scheduler.cron.Start()
	defer scheduler.Stop()

	ds := &types.DataSource{
		ID:           "ds-no-sched",
		TenantID:     1,
		Status:       types.DataSourceStatusActive,
		SyncSchedule: "",
	}

	if err := scheduler.AddOrUpdate(ds); err != nil {
		t.Fatalf("AddOrUpdate() error: %v", err)
	}
	if scheduler.EntryCount() != 0 {
		t.Errorf("empty schedule should not be registered, EntryCount() = %d", scheduler.EntryCount())
	}
}

func TestScheduler_Remove(t *testing.T) {
	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(newFakeDataSourceRepo(), newFakeSyncLogRepo(), enqueuer)
	scheduler.cron.Start()
	defer scheduler.Stop()

	ds := &types.DataSource{
		ID:           "ds-rm",
		TenantID:     1,
		Status:       types.DataSourceStatusActive,
		SyncSchedule: "0 0 * * * *",
	}

	_ = scheduler.AddOrUpdate(ds)
	if scheduler.EntryCount() != 1 {
		t.Fatalf("pre-remove: EntryCount() = %d, want 1", scheduler.EntryCount())
	}

	scheduler.Remove("ds-rm")
	if scheduler.EntryCount() != 0 {
		t.Errorf("post-remove: EntryCount() = %d, want 0", scheduler.EntryCount())
	}

	// Remove non-existent is safe
	scheduler.Remove("does-not-exist")
}

func TestScheduler_InvalidCron(t *testing.T) {
	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(newFakeDataSourceRepo(), newFakeSyncLogRepo(), enqueuer)
	scheduler.cron.Start()
	defer scheduler.Stop()

	ds := &types.DataSource{
		ID:           "ds-bad",
		TenantID:     1,
		Status:       types.DataSourceStatusActive,
		SyncSchedule: "not a cron",
	}

	err := scheduler.AddOrUpdate(ds)
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
	if scheduler.EntryCount() != 0 {
		t.Errorf("invalid cron should not be registered, EntryCount() = %d", scheduler.EntryCount())
	}
}

func TestScheduler_TriggerSync_InactiveSkipped(t *testing.T) {
	repo := newFakeDataSourceRepo()
	// Create a data source that is paused
	_ = repo.Create(context.Background(), &types.DataSource{
		ID:       "ds-inactive",
		TenantID: 1,
		Status:   types.DataSourceStatusPaused,
	})

	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(repo, newFakeSyncLogRepo(), enqueuer)

	// Directly call triggerSync — it should skip because ds is not active
	scheduler.triggerSync("ds-inactive", 1)

	if enqueuer.count.Load() != 0 {
		t.Error("should not enqueue for inactive data source")
	}
}

func TestScheduler_TriggerSync_NotFound(t *testing.T) {
	repo := newFakeDataSourceRepo()
	enqueuer := &fakeTaskEnqueuer{}
	scheduler := NewScheduler(repo, newFakeSyncLogRepo(), enqueuer)

	// Should not panic, just skip
	scheduler.triggerSync("nonexistent", 1)

	if enqueuer.count.Load() != 0 {
		t.Error("should not enqueue for non-existent data source")
	}
}
