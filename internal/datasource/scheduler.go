package datasource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/tracing/langfuse"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/robfig/cron/v3"
)

// Scheduler manages cron-based periodic sync for data sources.
//
// robfig/cron fires at absolute wall-clock times (e.g. "0 0 * * * *" always fires
// at the top of every hour regardless of when the process started). So multiple
// instances will fire at the same moment. Dedup is handled by two layers:
//
//  1. HasRunningSync — if a previous sync is still running, skip (prevent overlap).
//  2. asynq.TaskID  — deterministic ID per admitted Nextcloud sync log, or per
//     (dataSourceID, minute) for other connectors. Redis rejects duplicate IDs.
type Scheduler struct {
	cron         *cron.Cron
	dsRepo       interfaces.DataSourceRepository
	syncLogRepo  interfaces.SyncLogRepository
	taskEnqueuer interfaces.TaskEnqueuer

	mu      sync.Mutex
	entries map[string]cron.EntryID // dataSourceID → cron entry ID
}

// NewScheduler creates a new Scheduler.
func NewScheduler(
	dsRepo interfaces.DataSourceRepository,
	syncLogRepo interfaces.SyncLogRepository,
	taskEnqueuer interfaces.TaskEnqueuer,
) *Scheduler {
	return &Scheduler{
		cron: cron.New(cron.WithSeconds(), cron.WithChain(
			cron.Recover(cron.DefaultLogger),
		)),
		dsRepo:       dsRepo,
		syncLogRepo:  syncLogRepo,
		taskEnqueuer: taskEnqueuer,
		entries:      make(map[string]cron.EntryID),
	}
}

// Start loads all schedulable data sources (active, plus error-status ones
// so they keep retrying with backoff) and registers their cron schedules.
// Then starts the cron runner in the background.
func (s *Scheduler) Start(ctx context.Context) error {
	dataSources, err := s.dsRepo.FindSchedulable(ctx)
	if err != nil {
		return fmt.Errorf("load schedulable data sources: %w", err)
	}

	for _, ds := range dataSources {
		if ds.SyncSchedule == "" {
			continue
		}
		if err := s.addEntry(ds); err != nil {
			logger.Warnf(ctx, "[Scheduler] failed to register cron for ds=%s schedule=%q: %v",
				ds.ID, ds.SyncSchedule, err)
		}
	}

	s.cron.Start()
	logger.Infof(ctx, "[Scheduler] started with %d cron entries", len(s.entries))
	return nil
}

// Stop gracefully stops the cron runner and waits for running jobs to finish.
func (s *Scheduler) Stop() {
	ctx := s.cron.Stop()
	<-ctx.Done()
}

// AddOrUpdate registers (or re-registers) a cron entry for the given data source.
func (s *Scheduler) AddOrUpdate(ds *types.DataSource) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, ok := s.entries[ds.ID]; ok {
		s.cron.Remove(entryID)
		delete(s.entries, ds.ID)
	}

	if !schedulableStatus(ds.Status) || ds.SyncSchedule == "" {
		return nil
	}

	return s.addEntryLocked(ds)
}

// Remove removes the cron entry for a data source.
func (s *Scheduler) Remove(dataSourceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, ok := s.entries[dataSourceID]; ok {
		s.cron.Remove(entryID)
		delete(s.entries, dataSourceID)
	}
}

func (s *Scheduler) addEntry(ds *types.DataSource) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addEntryLocked(ds)
}

func (s *Scheduler) addEntryLocked(ds *types.DataSource) error {
	dsID := ds.ID
	tenantID := ds.TenantID

	entryID, err := s.cron.AddFunc(ds.SyncSchedule, func() {
		s.triggerSync(dsID, tenantID)
	})
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", ds.SyncSchedule, err)
	}

	s.entries[dsID] = entryID
	return nil
}

// schedulableStatus reports whether a source status participates in cron
// scheduling. Error-state sources stay scheduled so a transient failure can
// never permanently silence a source (the pre-2026-11 behavior); admission
// applies per-window backoff instead.
func schedulableStatus(status string) bool {
	return status == types.DataSourceStatusActive || status == types.DataSourceStatusError
}

// retryBackoff returns how long an error-state source must wait after its
// last failed sync before the scheduler admits it again. The first failure
// retries at the next window; repeated failures back off linearly, capped at
// 24 hours.
func retryBackoff(consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 1 {
		return 0
	}
	backoff := time.Duration(consecutiveFailures) * 6 * time.Hour
	if backoff > 24*time.Hour {
		backoff = 24 * time.Hour
	}
	return backoff
}

// triggerSync is called by the cron runner on each tick.
//
// Layer 0 — backoff: error-state sources are admitted only after their retry
// window opens; active sources are always admitted.
//
// Layer 1 — DB: if a previous sync is still running, skip. This prevents
// overlap when a sync takes longer than the cron interval.
//
// Layer 2 — Redis: an exact, durable log ID for Nextcloud; other connectors
// retain the minute-based ID. The first Enqueue wins for a given ID.
func (s *Scheduler) triggerSync(dataSourceID string, tenantID uint64) {
	ctx := context.Background()

	ds, err := s.dsRepo.FindByID(ctx, dataSourceID)
	if err != nil || ds == nil || !schedulableStatus(ds.Status) {
		logger.Infof(ctx, "[Scheduler] skipping sync for ds=%s (not schedulable or not found)", dataSourceID)
		return
	}

	// Layer 0: error-state sources retry with linear backoff so a broken
	// source cannot hammer the connector every window, yet never gets
	// permanently excluded from scheduling.
	if ds.Status == types.DataSourceStatusError {
		failures, lastFailedAt, failErr := s.syncLogRepo.ConsecutiveFailures(ctx, dataSourceID)
		if failErr != nil {
			logger.Warnf(ctx, "[Scheduler] could not read failure history for ds=%s: %v", dataSourceID, failErr)
			return
		}
		if eligibleAt := lastFailedAt.Add(retryBackoff(failures)); time.Now().UTC().Before(eligibleAt) {
			logger.Infof(ctx, "[Scheduler] deferring sync for ds=%s (error backoff, failures=%d, eligible at %s)",
				dataSourceID, failures, eligibleAt.Format(time.RFC3339))
			return
		}
	}

	// Layer 1: prevent overlap with a still-running sync
	if running, _ := s.syncLogRepo.HasRunningSync(ctx, dataSourceID); running {
		logger.Infof(ctx, "[Scheduler] skipping sync for ds=%s (previous sync still running)", dataSourceID)
		return
	}

	syncLog := &types.SyncLog{
		DataSourceID: dataSourceID,
		TenantID:     tenantID,
		Status:       types.SyncLogStatusRunning,
		StartedAt:    time.Now().UTC(),
	}
	if ds.Type == types.ConnectorTypeNextcloud {
		syncLog.RecoveryVersion = 1
		syncLog.RecoveryTrigger = "schedule"
	}
	if err := s.syncLogRepo.Create(ctx, syncLog); err != nil {
		if errors.Is(err, ErrSyncAlreadyRunning) {
			logger.Infof(ctx, "[Scheduler] skipping sync for ds=%s (another sync was admitted)", dataSourceID)
			return
		}
		logger.Errorf(ctx, "[Scheduler] failed to create sync log for ds=%s: %v", dataSourceID, err)
		return
	}

	payload := &types.DataSourceSyncPayload{
		DataSourceID: dataSourceID,
		TenantID:     tenantID,
		SyncLogID:    syncLog.ID,
		ForceFull:    false,
		Trigger:      "schedule",
	}
	langfuse.InjectTracing(ctx, payload)
	payloadJSON, _ := json.Marshal(payload)
	task := asynq.NewTask(types.TypeDataSourceSync, payloadJSON)

	// The database admits exactly one Nextcloud running log per source. Tie
	// the queue identity to that durable log so a lost reply can be inspected.
	taskID := NextcloudSyncTaskID(syncLog.ID)
	if ds.Type != types.ConnectorTypeNextcloud {
		taskID = fmt.Sprintf("dssync:%s:%s", dataSourceID, time.Now().UTC().Truncate(time.Minute).Format(
			"200601021504"))
	}

	info, err := s.taskEnqueuer.Enqueue(task,
		asynq.Queue(types.QueueSync),
		asynq.MaxRetry(5),
		asynq.Timeout(2*time.Hour),
		asynq.TaskID(taskID),
	)
	if err != nil {
		if err == asynq.ErrTaskIDConflict && ds.Type != types.ConnectorTypeNextcloud {
			logger.Infof(ctx, "[Scheduler] sync already enqueued by another instance for ds=%s", dataSourceID)
			syncLog.Status = types.SyncLogStatusCanceled
			now := time.Now().UTC()
			syncLog.FinishedAt = &now
			syncLog.ErrorMessage = "deduplicated: another instance enqueued first"
			_ = s.syncLogRepo.Update(ctx, syncLog)
			return
		}
		logger.Errorf(ctx, "[Scheduler] failed to enqueue sync task for ds=%s: %v", dataSourceID, err)
		if ds.Type == types.ConnectorTypeNextcloud {
			// A worker may have finished before the lost enqueue reply.
			// Update only the exact still-unstarted admission.
			if _, markErr := s.syncLogRepo.MarkNextcloudEnqueueUncertain(ctx,
				syncLog.ID, dataSourceID, tenantID, "schedule", taskID); markErr != nil {
				logger.Warnf(ctx, "[Scheduler] could not mark uncertain sync enqueue: %v", markErr)
			}
			return
		}
		syncLog.Status = types.SyncLogStatusFailed
		now := time.Now().UTC()
		syncLog.FinishedAt = &now
		syncLog.ErrorMessage = fmt.Sprintf("enqueue failed: %v", err)
		_ = s.syncLogRepo.Update(ctx, syncLog)
		return
	}

	if ds.Type == types.ConnectorTypeNextcloud {
		stored, readErr := s.syncLogRepo.FindByID(ctx, syncLog.ID)
		returnedID := ""
		if info != nil {
			returnedID = info.ID
		}
		if readErr != nil || !NextcloudSyncReceiptAccepted(stored,
			dataSourceID, tenantID, "schedule", returnedID) {
			_, markErr := s.syncLogRepo.MarkNextcloudEnqueueUncertain(ctx,
				syncLog.ID, dataSourceID, tenantID, "schedule", taskID)
			logger.Warnf(ctx, "[Scheduler] Nextcloud enqueue receipt cannot be confirmed: lookup=%v mar"+
				"k=%v",
				readErr, markErr)
			return
		}
	}

	logger.Infof(ctx, "[Scheduler] sync task enqueued for ds=%s syncLog=%s", dataSourceID, syncLog.ID)
}

// EntryCount returns the number of active cron entries (for testing/monitoring).
func (s *Scheduler) EntryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}
