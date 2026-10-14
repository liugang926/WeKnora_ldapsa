package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

const (
	nextcloudEventDispatchIntervalEnv     = "WEKNORA_NEXTCLOUD_EVENT_DISPATCH_INTERVAL"
	defaultNextcloudEventDispatchInterval = 5 * time.Second
	minNextcloudEventDispatchInterval     = time.Second
	maxNextcloudEventDispatchInterval     = time.Minute
)

func nextcloudEventDispatchInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(nextcloudEventDispatchIntervalEnv))
	if raw == "" {
		return defaultNextcloudEventDispatchInterval
	}
	interval, err := time.ParseDuration(raw)
	if err != nil || interval < minNextcloudEventDispatchInterval || interval > maxNextcloudEventDispatchInterval {
		logger.Warnf(context.Background(), "[NextcloudEvents] invalid %s; using %s (allowed: %s..%s)",
			nextcloudEventDispatchIntervalEnv, defaultNextcloudEventDispatchInterval,
			minNextcloudEventDispatchInterval, maxNextcloudEventDispatchInterval)
		return defaultNextcloudEventDispatchInterval
	}
	return interval
}

// NextcloudEventDispatcher periodically turns durable hints into authoritative
// full-manifest sync jobs. Queueing is at least once; publication/applied ACK
// is deliberately separate from this worker.
type NextcloudEventDispatcher struct {
	inbox       *repository.NextcloudEventInboxRepository
	dataSources interfaces.DataSourceRepository
	syncLogs    interfaces.SyncLogRepository
	enqueuer    interfaces.TaskEnqueuer
	mu          sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
}

// NewNextcloudEventDispatcher returns the durable source-event dispatcher.
func NewNextcloudEventDispatcher(
	inbox *repository.NextcloudEventInboxRepository,
	dataSources interfaces.DataSourceRepository,
	syncLogs interfaces.SyncLogRepository,
	enqueuer interfaces.TaskEnqueuer,
) *NextcloudEventDispatcher {
	return &NextcloudEventDispatcher{inbox: inbox, dataSources: dataSources, syncLogs: syncLogs, enqueuer: enqueuer}
}

// Start starts the source-event dispatch loop.
func (d *NextcloudEventDispatcher) Start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.done = make(chan struct{})
	interval := nextcloudEventDispatchInterval()
	go func() {
		defer close(d.done)
		d.runOnce(ctx, time.Now().UTC())
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				d.runOnce(ctx, now.UTC())
			}
		}
	}()
}

// Stop stops the source-event dispatch loop.
func (d *NextcloudEventDispatcher) Stop() {
	d.mu.Lock()
	cancel, done := d.cancel, d.done
	d.cancel = nil
	d.done = nil
	d.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (d *NextcloudEventDispatcher) runOnce(ctx context.Context, now time.Time) {
	ids, err := d.inbox.DispatchCandidates(ctx, now)
	if err != nil {
		logger.Warnf(ctx, "[NextcloudEvents] find due dispatches: %v", err)
	} else {
		for _, id := range ids {
			if ctx.Err() != nil {
				return
			}
			if err := d.dispatchOne(ctx, id, now); err != nil {
				logger.Warnf(ctx, "[NextcloudEvents] dispatch connection=%s: %v", id, err)
			}
		}
	}
	claims, err := d.inbox.CandidateRetryClaims(ctx, now)
	if err != nil {
		logger.Warnf(ctx, "[NextcloudCandidateRetry] scan due candidates: %v", err)
		return
	}
	for _, claim := range claims {
		if ctx.Err() != nil {
			return
		}
		if err := d.dispatchCandidateRetry(ctx, claim); err != nil {
			logger.Warnf(ctx, "[NextcloudCandidateRetry] enqueue candidate=%s: %v", claim.FailedCandidateID, err)
		}
	}
}

func (d *NextcloudEventDispatcher) dispatchCandidateRetry(_ context.Context,
	claim repository.NextcloudCandidateRetryClaim,
) error {
	payload := types.DataSourceSyncPayload{
		DataSourceID: claim.DataSourceID, TenantID: claim.TenantID,
		SyncLogID: claim.SyncLogID, Trigger: "nextcloud_candidate_retry",
		NextcloudRetryKnowledgeBaseID: claim.KnowledgeBaseID,
		NextcloudRetryExternalID:      claim.ExternalID, NextcloudRetryETag: claim.DesiredETag,
		NextcloudRetryCandidateID: claim.FailedCandidateID, NextcloudRetryInstanceID: claim.InstanceID,
		NextcloudRetryBindingID: claim.BindingID, NextcloudRetryConfigSHA256: claim.ConfigSHA,
		NextcloudRetryPairOperationID: claim.PairOperationID,
		NextcloudRetryPairingEpoch:    claim.PairingEpoch, NextcloudRetryLeaseToken: claim.LeaseToken,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	task := asynq.NewTask(types.TypeDataSourceSync, encoded)
	_, err = d.enqueuer.Enqueue(task, asynq.Queue(types.QueueSync), asynq.MaxRetry(0),
		asynq.Timeout(2*time.Hour), asynq.TaskID("nccandidate:"+claim.SyncLogID))
	return err // An uncertain result keeps the durable claim for lease recovery.
}

func (d *NextcloudEventDispatcher) dispatchOne(ctx context.Context, connectionID string, now time.Time) error {
	claim, err := d.inbox.ClaimDispatch(ctx, connectionID, now)
	if err != nil || claim == nil {
		return err
	}
	fail := func(code string, sourceErr error) error {
		if recordErr := d.inbox.FailDispatch(ctx, *claim, code, time.Now().UTC()); recordErr != nil {
			return errors.Join(sourceErr, fmt.Errorf("record dispatch retry: %w", recordErr))
		}
		return sourceErr
	}
	ds, err := d.dataSources.FindByID(ctx, claim.DatasourceID)
	if err != nil || ds == nil || ds.TenantID != claim.TenantID || ds.Type != types.ConnectorTypeNextcloud {
		return fail("source_unavailable", errors.New("paired Nextcloud data source unavailable"))
	}
	if ds.Status == types.DataSourceStatusPaused || ds.Status == types.DataSourceStatusDeleted {
		return fail("source_paused", errors.New("paired Nextcloud data source paused"))
	}
	if ds.Status != types.DataSourceStatusActive && ds.Status != types.DataSourceStatusError {
		return fail("source_unavailable", errors.New("paired Nextcloud data source not runnable"))
	}
	running, err := d.syncLogs.HasRunningSync(ctx, ds.ID)
	if err != nil {
		return fail("source_unavailable", err)
	}
	if running {
		return fail("source_unavailable", errors.New("data source sync already running"))
	}

	// Persist the intended task ID before enqueue. The task creates its own
	// running log only when it starts, so a crash before enqueue leaves no
	// orphan running log that could block recovery.
	logID := uuid.NewString()
	if err := d.inbox.PrepareDispatch(ctx, *claim, logID, time.Now().UTC()); err != nil {
		return err
	}
	// The connector validates a complete current manifest for every hint and
	// re-downloads touched files. Broad hints, missing cursors and expired feeds
	// still force a content refresh; an ordinary event need not re-download
	// every unchanged file in the source.
	payload := types.DataSourceSyncPayload{
		DataSourceID: ds.ID, TenantID: ds.TenantID,
		SyncLogID: logID, ForceFull: false, Trigger: "nextcloud_event",
		NextcloudEventConnectionID: claim.ConnectionID,
		NextcloudEventConfigSHA256: claim.ConfigSHA256,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fail("queue_unavailable", err)
	}
	task := asynq.NewTask(types.TypeDataSourceSync, encoded)
	// Every attempt has a distinct ID. A crash after enqueue is reconciled
	// through the durable log ID; stale attempts are fenced by the task guard.
	taskID := fmt.Sprintf("ncevent:%s:%s", claim.ConnectionID, logID)
	_, err = d.enqueuer.Enqueue(task, asynq.Queue(types.QueueSync), asynq.MaxRetry(0),
		asynq.Timeout(2*time.Hour), asynq.TaskID(taskID))
	if err != nil {
		if recordErr := d.inbox.UncertainDispatch(ctx, *claim, time.Now().UTC()); recordErr != nil {
			return errors.Join(err, fmt.Errorf("record uncertain event enqueue: %w", recordErr))
		}
		return err
	}
	if err := d.inbox.FinishDispatch(ctx, *claim, logID, time.Now().UTC()); err != nil {
		// The task may already be executing. Keep the lease for expiry/retry;
		// never convert an uncertain enqueue into an applied acknowledgement.
		return err
	}
	return nil
}
