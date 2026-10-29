package container

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const nextcloudSyncRecoveryGrace = 5 * time.Minute

type nextcloudSyncTaskProbe interface {
	GetTaskInfo(queue, id string) (*asynq.TaskInfo, error)
}

type nextcloudEventSyncRecoverer interface {
	RecoverQueuedEventSync(ctx context.Context, connectionID, syncLogID string, now time.Time) (bool, error)
}

func recoverNextcloudSyncAdmissionsOnce(ctx context.Context, store *repository.NextcloudSyncRecoveryStore,
	probe nextcloudSyncTaskProbe, events nextcloudEventSyncRecoverer, distributed bool, now time.Time,
) (int, error) {
	if store == nil || (distributed && probe == nil) {
		return 0, nil
	}
	cutoff := now.Add(-nextcloudSyncRecoveryGrace)
	logs, err := store.RecoverableBefore(ctx, cutoff, 100)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, log := range logs {
		if log.QueueTaskID != "dssync:"+log.ID {
			continue
		}
		if distributed {
			// Any queue/Redis error other than a definitive absence preserves
			// the admission. An existing exact task, in any state, also wins.
			_, lookupErr := probe.GetTaskInfo(types.QueueSync, log.QueueTaskID)
			if lookupErr == nil {
				continue
			}
			if !errors.Is(lookupErr, asynq.ErrTaskNotFound) {
				return recovered, lookupErr
			}
		}
		released, err := store.FailMissing(ctx, log, cutoff, now)
		if err != nil {
			return recovered, err
		}
		if released {
			recovered++
		}
	}
	eventCandidates, err := store.QueuedEventSyncBefore(ctx, cutoff, 100)
	if err != nil {
		return recovered, err
	}
	for _, event := range eventCandidates {
		if event.ConnectionID == "" || event.SyncLogID == "" || events == nil {
			continue
		}
		if distributed {
			// Event tasks use a deterministic ID, so their exact queue entry
			// is a stronger liveness signal than the worker_active projection.
			_, lookupErr := probe.GetTaskInfo(types.QueueSync,
				fmt.Sprintf("ncevent:%s:%s", event.ConnectionID, event.SyncLogID))
			if lookupErr == nil {
				continue
			}
			if !errors.Is(lookupErr, asynq.ErrTaskNotFound) {
				return recovered, lookupErr
			}
		}
		released, err := events.RecoverQueuedEventSync(ctx, event.ConnectionID,
			event.SyncLogID, now)
		if err != nil {
			if errors.Is(err, repository.ErrNextcloudEventConflict) ||
				errors.Is(err, repository.ErrNextcloudEventScope) {
				continue
			}
			return recovered, err
		}
		if released {
			recovered++
		}
	}
	return recovered, nil
}

// Recovery is enabled by default once queue workers enforce the durable
// admission. Operators may disable it explicitly during a mixed-version
// rollout with WEKNORA_NEXTCLOUD_SYNC_RECOVERY_ENABLED=false.
func startNextcloudSyncAdmissionRecovery(db *gorm.DB, redisClient *redis.Client,
	inbox *repository.NextcloudEventInboxRepository,
	cleaner interfaces.ResourceCleaner,
) {
	distributed := redisClient != nil
	if os.Getenv("WEKNORA_NEXTCLOUD_SYNC_RECOVERY_ENABLED") == "false" {
		return
	}
	var probe nextcloudSyncTaskProbe
	if distributed {
		probe = asynq.NewInspectorFromRedisClient(redisClient)
	}
	store := repository.NewNextcloudSyncRecoveryStore(db)
	stop := make(chan struct{})
	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		count, err := recoverNextcloudSyncAdmissionsOnce(ctx, store, probe, inbox, distributed, time.Now().UTC())
		if err != nil {
			logger.Warnf(ctx, "[NextcloudSyncRecovery] scan failed: %v", err)
		}
		if count > 0 {
			logger.Infof(ctx, "[NextcloudSyncRecovery] recovered %d missing sync worker(s)", count)
		}
	}
	go func() {
		run()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-stop:
				return
			}
		}
	}()
	cleaner.RegisterWithName("NextcloudSyncAdmissionRecovery", func() error {
		close(stop)
		return nil
	})
}
