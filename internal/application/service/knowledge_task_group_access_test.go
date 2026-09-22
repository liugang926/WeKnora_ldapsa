package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type backgroundTaskGroupAccessStub struct {
	interfaces.GroupAccessService
	allowWithoutPrincipal bool
	seenPrincipal         types.Principal
	seenCaller            types.Caller
}

func (s *backgroundTaskGroupAccessStub) EffectivePermission(
	ctx context.Context,
	_ uint64,
	_ types.ResourceType,
	_ string,
	_ types.ResourceAction,
	_ time.Time,
) (types.EffectiveResourcePermission, error) {
	s.seenPrincipal, _ = types.PrincipalFromContext(ctx)
	s.seenCaller = types.CallerFromContext(ctx)
	allowed := s.allowWithoutPrincipal ||
		(s.seenPrincipal.Type == types.PrincipalWebUser && s.seenPrincipal.ID != "")
	return types.EffectiveResourcePermission{Allowed: allowed, Reason: "test"}, nil
}

func TestBackgroundTaskAuthorizationRestoresVerifiableHuman(t *testing.T) {
	access := &backgroundTaskGroupAccessStub{}
	svc := &knowledgeService{groupAccess: access}
	ctx := backgroundTaskAuthorizationContext(context.Background(), 7, types.TaskInitiator{
		UserID: "user-1",
		Role:   types.TenantRoleContributor,
	})

	err := svc.revalidateBackgroundKBAccess(ctx, 7, "kb-1", types.ResourceActionEdit)

	require.NoError(t, err)
	require.Equal(t, types.Principal{Type: types.PrincipalWebUser, ID: "user-1"}, access.seenPrincipal)
	require.Equal(t, types.Caller{
		TenantID: 7,
		UserID:   "user-1",
		Role:     types.TenantRoleContributor,
	}, access.seenCaller)
}

func TestBackgroundTaskAuthorizationFailsClosedWithoutHuman(t *testing.T) {
	access := &backgroundTaskGroupAccessStub{}
	svc := &knowledgeService{groupAccess: access}
	ctx := backgroundTaskAuthorizationContext(context.Background(), 7, types.TaskInitiator{})

	err := svc.revalidateBackgroundKBAccess(ctx, 7, "kb-1", types.ResourceActionEdit)

	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.False(t, access.seenPrincipal.Valid())
	require.Equal(t, uint64(7), access.seenCaller.TenantID)
}

func TestBackgroundTaskAuthorizationPreservesLegacyWhenOverlayAbsent(t *testing.T) {
	svc := &knowledgeService{}
	ctx := backgroundTaskAuthorizationContext(context.Background(), 7, types.TaskInitiator{})

	require.NoError(t, svc.revalidateBackgroundKBAccess(
		ctx, 7, "kb-1", types.ResourceActionEdit,
	))
}

func TestBackgroundTaskAuthorizationPreservesLegacyWhenDirectoryDisabled(t *testing.T) {
	access := &backgroundTaskGroupAccessStub{allowWithoutPrincipal: true}
	svc := &knowledgeService{groupAccess: access}
	ctx := backgroundTaskAuthorizationContext(context.Background(), 7, types.TaskInitiator{})

	require.NoError(t, svc.revalidateBackgroundKBAccess(
		ctx, 7, "kb-1", types.ResourceActionEdit,
	))
	require.False(t, access.seenPrincipal.Valid())
}

func TestUserTriggeredKnowledgeWorkersRevalidateGroupAccess(t *testing.T) {
	initiator := types.TaskInitiator{UserID: "user-1", Role: types.TenantRoleContributor}
	marshalTask := func(t *testing.T, taskType string, payload any) *asynq.Task {
		t.Helper()
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		return asynq.NewTask(taskType, raw)
	}

	t.Run("clone", func(t *testing.T) {
		f := transferFixture(t, access.KBTransferClone)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		err := f.svc.ProcessKBClone(context.Background(), marshalTask(t, types.TypeKBClone, types.KBClonePayload{
			TenantID: 7, TaskID: "clone-task", SourceID: "kb", TargetID: "other", Initiator: initiator,
		}))
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})

	t.Run("move", func(t *testing.T) {
		f := transferFixture(t, access.KBTransferMove)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		err := f.svc.ProcessKnowledgeMove(context.Background(), marshalTask(t, types.TypeKnowledgeMove, types.KnowledgeMovePayload{
			TenantID: 7, TaskID: "move-task", SourceKBID: "kb", TargetKBID: "other",
			KnowledgeIDs: []string{"doc"}, Mode: "reuse_vectors", Initiator: initiator,
		}))
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})

	t.Run("batch delete", func(t *testing.T) {
		f := newDocumentWriteFixture(t)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		f.repo.writes = 0
		err := f.svc.ProcessKnowledgeListDelete(context.Background(), marshalTask(t, types.TypeKnowledgeListDelete, types.KnowledgeListDeletePayload{
			TenantID: 7, KnowledgeBaseID: "kb", KnowledgeIDs: []string{"doc"}, Initiator: initiator,
		}))
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Zero(t, f.repo.writes)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})

	t.Run("batch reparse", func(t *testing.T) {
		f := newDocumentWriteFixture(t)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		f.repo.writes = 0
		err := f.svc.ProcessKnowledgeListReparse(context.Background(), marshalTask(t, types.TypeKnowledgeListReparse, types.KnowledgeListReparsePayload{
			TenantID: 7, KnowledgeBaseID: "kb", KnowledgeIDs: []string{"doc"}, Initiator: initiator,
		}))
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Zero(t, f.repo.writes)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})
}
