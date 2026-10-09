package container

import (
	"context"
	"errors"
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

func recoverNextcloudSyncAdmissionsOnce(ctx context.Context, store *repository.NextcloudSyncRecoveryStore,
	probe nextcloudSyncTaskProbe, distributed bool, now time.Time,
) (int, error) {
	if store == nil || (distributed && probe == nil) {
		return 0, nil
	}
	cutoff := now.Add(-nextcloudSyncRecoveryGrace)
	logs, err := store.UnstartedBefore(ctx, cutoff, 100)
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
	return recovered, nil
}

// Redis recovery is opt-in during a rolling upgrade: every worker that can
// consume the sync queue must enforce ClaimNextcloudSyncStart first. Lite has
// one process, so the prior in-memory queue is gone on restart.
func startNextcloudSyncAdmissionRecovery(db *gorm.DB, redisClient *redis.Client,
	cleaner interfaces.ResourceCleaner,
) {
	distributed := redisClient != nil
	if distributed && os.Getenv("WEKNORA_NEXTCLOUD_SYNC_RECOVERY_ENABLED") != "true" {
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
		count, err := recoverNextcloudSyncAdmissionsOnce(ctx, store, probe, distributed, time.Now().UTC())
		if err != nil {
			logger.Warnf(ctx, "[NextcloudSyncRecovery] scan failed: %v", err)
		}
		if count > 0 {
			logger.Infof(ctx, "[NextcloudSyncRecovery] released %d unstarted admission(s)", count)
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
