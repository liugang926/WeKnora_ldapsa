package container

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// SQL is authoritative for automatic classification delivery in both Redis
// and Lite mode. A failed enqueue and a crashed consumer remain recoverable.
func startKnowledgeAutoTagCompletionRecovery(
	store *repository.KnowledgeInputProvenanceStore,
	queue interfaces.TaskEnqueuer,
	cleaner interfaces.ResourceCleaner,
) {
	if store == nil || queue == nil || cleaner == nil {
		return
	}
	stop := make(chan struct{})
	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := service.RecoverKnowledgeAutoTagCompletions(ctx, store, queue, ""); err != nil {
			logger.Warnf(ctx, "[AutoTagRecovery] pending delivery failed: %v", err)
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
	cleaner.RegisterWithName("KnowledgeAutoTagCompletionRecovery", func() error { close(stop); return nil })
}
