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
	require.Equal(
		t,
		types.Principal{Type: types.PrincipalWebUser, ID: "user-1"},
		access.seenPrincipal,
	)
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
	require.NotEqual(t, types.PrincipalWebUser, access.seenPrincipal.Type)
	require.Zero(t, access.seenCaller.TenantID)
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
	require.NotEqual(t, types.PrincipalWebUser, access.seenPrincipal.Type)
}

func TestUserTriggeredKnowledgeWorkersRevalidateGroupAccess(t *testing.T) {
	initiator := types.TaskInitiator{UserID: "user-1", Role: types.TenantRoleContributor}
	marshalTask := func(t *testing.T, taskType string, payload any) *asynq.Task {
		t.Helper()
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		return asynq.NewTask(taskType, raw)
	}
	for _, taskType := range []string{types.TypeDocumentProcess, types.TypeManualProcess} {
		t.Run(taskType, func(t *testing.T) {
			f := newDocumentWriteFixture(t)
			groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
			f.svc.groupAccess = groupAccess
			require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", "doc").
				Update("parse_status", types.ParseStatusPending).Error)
			task := marshalTask(t, taskType, types.DocumentProcessPayload{
				TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", Initiator: initiator,
			})
			var err error
			if taskType == types.TypeDocumentProcess {
				err = f.svc.ProcessDocument(context.Background(), task)
			} else {
				err = f.svc.ProcessManualUpdate(context.Background(), task)
			}
			require.ErrorIs(t, err, asynq.SkipRetry)
			require.Zero(t, f.chunkRepo.writes)
			require.Zero(t, f.graph.calls)
			row, err := f.repo.GetKnowledgeByID(context.Background(), 7, "doc")
			require.NoError(t, err)
			require.Equal(t, types.ParseStatusFailed, row.ParseStatus)
		})
	}
	t.Run("FAQ import", func(t *testing.T) {
		f := newDocumentWriteFixture(t)
		f.kbs.values["kb"].Type = types.KnowledgeBaseTypeFAQ
		f.svc.groupAccess = &tagTargetGroupAccessService{allowed: map[string]bool{}}
		require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", "doc").
			Updates(map[string]any{"type": types.KnowledgeTypeFAQ, "parse_status": types.ParseStatusPending}).Error)
		err := f.svc.ProcessFAQImport(context.Background(), marshalTask(t, types.TypeFAQImport, types.FAQImportPayload{
			TenantID: 7, KBID: "kb", KnowledgeID: "doc", Initiator: initiator,
		}))
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Zero(t, f.chunkRepo.writes)
		row, err := f.repo.GetKnowledgeByID(context.Background(), 7, "doc")
		require.NoError(t, err)
		require.Equal(t, types.ParseStatusFailed, row.ParseStatus)
	})

	t.Run("clone", func(t *testing.T) {
		f := transferFixture(t, access.KBTransferClone)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		err := f.svc.ProcessKBClone(
			context.Background(),
			marshalTask(t, types.TypeKBClone, types.KBClonePayload{
				TenantID: 7, TaskID: "clone-task", SourceID: "kb", TargetID: "other", Initiator: initiator,
			}),
		)
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})

	t.Run("move", func(t *testing.T) {
		f := transferFixture(t, access.KBTransferMove)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		err := f.svc.ProcessKnowledgeMove(
			context.Background(),
			marshalTask(t, types.TypeKnowledgeMove, types.KnowledgeMovePayload{
				TenantID: 7, TaskID: "move-task", SourceKBID: "kb", TargetKBID: "other",
				KnowledgeIDs: []string{"doc"}, Mode: "reuse_vectors", Initiator: initiator,
			}),
		)
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})

	t.Run("batch delete", func(t *testing.T) {
		f := newDocumentWriteFixture(t)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		f.repo.writes = 0
		err := f.svc.ProcessKnowledgeListDelete(
			context.Background(),
			marshalTask(t, types.TypeKnowledgeListDelete, types.KnowledgeListDeletePayload{
				TenantID: 7, KnowledgeBaseID: "kb", KnowledgeIDs: []string{"doc"}, Initiator: initiator,
			}),
		)
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Zero(t, f.repo.writes)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})

	t.Run("batch reparse", func(t *testing.T) {
		f := newDocumentWriteFixture(t)
		groupAccess := &tagTargetGroupAccessService{allowed: map[string]bool{}}
		f.svc.groupAccess = groupAccess
		f.repo.writes = 0
		err := f.svc.ProcessKnowledgeListReparse(
			context.Background(),
			marshalTask(t, types.TypeKnowledgeListReparse, types.KnowledgeListReparsePayload{
				TenantID: 7, KnowledgeBaseID: "kb", KnowledgeIDs: []string{"doc"}, Initiator: initiator,
			}),
		)
		require.ErrorIs(t, err, asynq.SkipRetry)
		require.Zero(t, f.repo.writes)
		require.Equal(t, []string{"kb"}, groupAccess.calls)
	})
}

func TestRevokedOldProcessingTaskDoesNotFailNewSource(t *testing.T) {
	for _, taskType := range []string{types.TypeDocumentProcess, types.TypeManualProcess} {
		t.Run(taskType, func(t *testing.T) {
			f := newDocumentWriteFixture(t)
			access := &tagTargetGroupAccessService{allowed: map[string]bool{}}
			f.svc.groupAccess = access
			row, err := f.repo.GetKnowledgeByID(context.Background(), 7, "doc")
			require.NoError(t, err)
			row.ParseStatus = types.ParseStatusPending
			row.FilePath = "new-file.pdf"
			require.NoError(t, row.SetManualMetadata(types.NewManualKnowledgeMetadata("new", "publish", 2)))
			require.NoError(t, f.repo.UpdateKnowledge(context.Background(), row))
			var payload any = types.DocumentProcessPayload{
				TenantID: 7, KnowledgeID: "doc", KnowledgeBaseID: "kb", FilePath: "old-file.pdf",
			}
			if taskType == types.TypeManualProcess {
				payload = types.ManualProcessPayload{
					TenantID: 7, KnowledgeID: "doc", KnowledgeBaseID: "kb", ContentVersion: 1, Content: "old",
				}
			}
			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			task := asynq.NewTask(taskType, raw)
			if taskType == types.TypeDocumentProcess {
				err = f.svc.ProcessDocument(context.Background(), task)
			} else {
				err = f.svc.ProcessManualUpdate(context.Background(), task)
			}
			require.NoError(t, err)
			require.Empty(t, access.calls)
			row, err = f.repo.GetKnowledgeByID(context.Background(), 7, "doc")
			require.NoError(t, err)
			require.Equal(t, types.ParseStatusPending, row.ParseStatus)
			if taskType == types.TypeManualProcess {
				legacyPayload, err := json.Marshal(types.ManualProcessPayload{
					TenantID: 7, KnowledgeID: "doc", KnowledgeBaseID: "kb", Content: "old",
				})
				require.NoError(t, err)
				err = f.svc.ProcessManualUpdate(context.Background(), asynq.NewTask(taskType, legacyPayload))
				require.ErrorIs(t, err, asynq.SkipRetry)
				row, err = f.repo.GetKnowledgeByID(context.Background(), 7, "doc")
				require.NoError(t, err)
				require.Equal(t, types.ParseStatusPending, row.ParseStatus)
			}
		})
	}
}
