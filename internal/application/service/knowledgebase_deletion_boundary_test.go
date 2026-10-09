package service

import (
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type kbDeletionBoundaryEnqueuer struct{ calls int }

func (q *kbDeletionBoundaryEnqueuer) Enqueue(_ *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	q.calls++
	return &asynq.TaskInfo{ID: "owned-blocked-deletion"}, nil
}

func TestDeleteKnowledgeBaseNextcloudBlockPreservesPublicCauseAndDestructiveGuards(t *testing.T) {
	const kbID = "owned-nextcloud-boundary-kb"
	dsRepo := newKBDeleteDSRepo(kbID, &types.DataSource{
		ID: "owned-nextcloud-source", KnowledgeBaseID: kbID, Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive, SyncSchedule: "0 0 * * * *",
	})
	kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
	kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "owned blocked source"}
	syncLogRepo := &kbDeleteSyncLogRepo{}
	queue := &kbDeletionBoundaryEnqueuer{}
	scheduler := datasource.NewScheduler(dsRepo, syncLogRepo, queue)
	t.Cleanup(scheduler.Stop)
	require.NoError(t, scheduler.AddOrUpdate(dsRepo.byKB[kbID][0]))
	svc := &knowledgeBaseService{
		repo: kbRepo, asynqClient: queue, dsRepo: dsRepo, syncLogRepo: syncLogRepo, dsScheduler: scheduler,
	}
	err := svc.DeleteKnowledgeBase(ctxWithTenantStorage(1, "local"), kbID)
	require.Error(t, err)
	var blocked *types.NextcloudKnowledgeBaseDeletionBlockedError
	require.ErrorAs(t, err, &blocked)
	const legacy = "Nextcloud source must be unpaired before deleting its knowledge base"
	require.Equal(t, legacy, err.Error())
	require.Equal(t, legacy, apperrors.PublicMessage(err))
	cause := errors.Unwrap(err)
	require.NotNil(t, cause)
	require.Equal(t, "nextcloud source must be unpaired before deleting its knowledge base", cause.Error())
	require.ErrorIs(t, err, cause)
	require.Same(t, cause, blocked.Unwrap())
	require.Empty(t, kbRepo.deletedID)
	require.NotNil(t, kbRepo.rows[kbID])
	require.Empty(t, dsRepo.deleteIDs)
	require.Empty(t, syncLogRepo.canceled)
	require.Zero(t, queue.calls)
	require.Equal(t, 1, scheduler.EntryCount())
}
