package router

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

// WrapKnowledgeBuildTask is shared by Redis and Lite dispatch. The exact
// renewable content lease encloses both machine-receipt and ordinary ACL
// admission, so a machine task can prove its scope before the edit checkpoint.
func WrapKnowledgeBuildTask(leases *repository.NextcloudContentLeaseStore, knowledges interfaces.KnowledgeRepository,
	chunks interfaces.ChunkRepository, kbs interfaces.KnowledgeBaseService, groups interfaces.GroupAccessService,
	tracker service.SpanTracker, inputs *repository.KnowledgeInputProvenanceStore,
	handler func(context.Context, *asynq.Task) error,
) func(context.Context, *asynq.Task) error {
	guard := taskGroupAccessMiddleware(groups, kbs, knowledges, chunks, tracker, inputs)
	return wrapNextcloudBuild(leases, knowledges, chunks, guard(asynq.HandlerFunc(handler)).ProcessTask)
}
