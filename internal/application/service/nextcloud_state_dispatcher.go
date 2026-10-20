package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	nextcloud "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

// NextcloudStateDispatcher dispatches persisted source state notifications.
type NextcloudStateDispatcher struct {
	store    *repository.NextcloudProcessingStateStore
	enqueuer interfaces.TaskEnqueuer
	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewNextcloudStateDispatcher constructs the configured NextcloudStateDispatcher.
func NewNextcloudStateDispatcher(
	store *repository.NextcloudProcessingStateStore,
	enqueuer interfaces.TaskEnqueuer,
) *NextcloudStateDispatcher {
	return &NextcloudStateDispatcher{store: store, enqueuer: enqueuer}
}

// Start starts persisted source state dispatch.
func (d *NextcloudStateDispatcher) Start() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.done = make(chan struct{})
	go func() {
		defer close(d.done)
		var wg sync.WaitGroup
		wg.Add(1)
		// Compensation has an independent clock and goroutine. A large inventory
		// walk cannot stall new result delivery or graph queue retries.
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				rctx, rcancel := context.WithTimeout(ctx, 50*time.Second)
				err := d.store.RefreshSources(rctx)
				if err == nil {
					err = d.store.Reconcile(rctx)
				}
				rcancel()
				if err != nil && ctx.Err() == nil {
					logger.Warn(ctx, "Nextcloud state compensation unavailable")
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			d.retryGraphQueues(ctx, time.Now().UTC())
			dctx, dcancel := context.WithTimeout(ctx, 4*time.Second)
			d.deliver(dctx, time.Now().UTC())
			dcancel()
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case <-ticker.C:
			}
		}
	}()
}

// Stop stops persisted source state dispatch.
func (d *NextcloudStateDispatcher) Stop() {
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

func (d *NextcloudStateDispatcher) deliver(ctx context.Context, _ time.Time) {
	for ctx.Err() == nil {
		ids, err := d.store.DueDeliveries(ctx, time.Now().UTC())
		if err != nil {
			logger.Warn(ctx, "Nextcloud state outbox unavailable")
			return
		}
		if len(ids) == 0 {
			return
		}
		deliveries, err := d.store.PrepareDeliveryBatch(ctx, ids)
		if err != nil {
			logger.Warn(ctx, "Nextcloud state batch preparation unavailable")
			return
		}
		// Deliver independent files with bounded concurrency; a failed remote must
		// not hold the entire batch behind a 10-second request per event.
		var wg sync.WaitGroup
		acks := make(chan string, len(deliveries))
		slots := make(chan struct{}, 8)
		for _, delivery := range deliveries {
			select {
			case <-ctx.Done():
				wg.Wait()
				d.flushAcks(ctx, acks)
				return
			case slots <- struct{}{}:
			}
			wg.Add(1)
			go func(delivery repository.NextcloudStatusDelivery) {
				id := delivery.EventID
				defer wg.Done()
				defer func() { <-slots }()
				current, err := d.store.DeliveryCurrent(ctx, delivery)
				if err == nil && !current {
					err = errors.New("state_head_fenced")
				}
				if err == nil {
					err = nextcloud.PushProcessingState(
						ctx,
						delivery.Config,
						delivery.ConnectionID,
						delivery.KeyID,
						delivery.EventID,
						delivery.SigningKey,
						delivery.Body,
					)
				}
				fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				var ackErr error
				if err == nil {
					acks <- id
				} else {
					ackErr = d.store.FinishDelivery(fctx, id, false, time.Now().UTC())
				}
				if ackErr != nil {
					logger.Warn(ctx, "Nextcloud state ACK persistence unavailable")
				}
			}(delivery)
		}
		wg.Wait()
		d.flushAcks(ctx, acks)
	}
}

func (d *NextcloudStateDispatcher) retryGraphQueues(ctx context.Context, now time.Time) {
	if d.enqueuer == nil {
		return
	}
	intents, err := d.store.PendingGraphQueueIntents(ctx, now)
	if err != nil {
		logger.Warn(ctx, "Nextcloud graph queue intent unavailable")
		return
	}
	for _, intent := range intents {
		task := asynq.NewTask(
			types.TypeChunkExtract,
			intent.Payload,
			asynq.TaskID("nc-graph:"+intent.OperationID),
			asynq.Queue(types.QueueGraph),
			asynq.MaxRetry(3),
			asynq.Timeout(30*time.Minute),
		)
		_, err := d.enqueuer.Enqueue(task)
		if err := d.store.FinishGraphEnqueue(
			ctx,
			intent.OperationID,
			err == nil || errors.Is(err, asynq.ErrTaskIDConflict),
			now,
		); err != nil {
			logger.Warn(ctx, "Nextcloud graph enqueue ACK unavailable")
		}
	}
}

func (d *NextcloudStateDispatcher) flushAcks(ctx context.Context, acks <-chan string) {
	events := []string{}
	for {
		select {
		case id := <-acks:
			events = append(events, id)
		default:
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if err := d.store.AcknowledgeCurrentDeliveries(fctx, events, time.Now().UTC()); err != nil {
				logger.Warn(ctx, "Nextcloud state batch ACK persistence unavailable")
			}
			return
		}
	}
}
