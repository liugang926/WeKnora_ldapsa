package service

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type wikiAccessProbe struct {
	interfaces.GroupAccessService
	mu      sync.Mutex
	allowed map[string]bool
	seen    []string
}

func (s *wikiAccessProbe) EffectivePermission(
	ctx context.Context, _ uint64, _ types.ResourceType, _ string, _ types.ResourceAction, _ time.Time,
) (types.EffectiveResourcePermission, error) {
	principal, _ := types.PrincipalFromContext(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, principal.ID)
	return types.EffectiveResourcePermission{
		Allowed: principal.Type == types.PrincipalWebUser && s.allowed[principal.ID], Reason: "test",
	}, nil
}

func wikiAccessFixture() (*wikiIngestService, *wikiAccessProbe, WikiIngestPayload) {
	probe := &wikiAccessProbe{allowed: map[string]bool{"allowed-user": true, "admin-trigger": true}}
	return &wikiIngestService{groupAccess: probe, kbService: &wikiGuardKBService{kb: &types.KnowledgeBase{
		ID: "kb", TenantID: 7, IndexingStrategy: types.IndexingStrategy{WikiEnabled: true},
	}}}, probe, WikiIngestPayload{TenantID: 7, KnowledgeBaseID: "kb"}
}

func TestWikiDurableWorkRetainsFullInitiatorAndAttempt(t *testing.T) {
	actor := types.TaskInitiator{UserID: "allowed-user", Role: types.TenantRoleContributor, CallerTenantID: 7}
	ctx := types.WithTaskAuthorization(context.Background(), 7, actor)
	op, err := newWikiIngestPendingOp(ctx, 7, "kb", "doc", 4)
	require.NoError(t, err)
	var decoded WikiPendingOp
	require.NoError(t, json.Unmarshal(op.Payload, &decoded))
	require.Equal(t, actor, decoded.Initiator)
	require.Equal(t, 4, decoded.Attempt)
	queue := &wikiGuardTaskQueue{}
	require.NoError(t, enqueueWikiIngestTrigger(ctx, queue, 7, "kb"))
	var trigger WikiIngestPayload
	require.NoError(t, json.Unmarshal(queue.tasks[0].Payload(), &trigger))
	require.Equal(t, actor, trigger.Initiator)

	repo := &wikiKBGuardPendingRepo{accepted: true}
	svc := &wikiIngestService{pendingRepo: repo, task: queue}
	other := types.TaskInitiator{UserID: "other-user", CallerTenantID: 7}
	svc.enqueueFinalize(wikiActorContext(ctx, trigger, []types.TaskInitiator{actor, other}), trigger,
		[]string{"entity/item"}, nil, nil, nil)
	var row wikiFinalizeRow
	require.NoError(t, json.Unmarshal(repo.guardedOps[0].Payload, &row))
	require.Equal(t, []types.TaskInitiator{actor, other}, row.Initiators)
}

func TestWikiMixedPendingOpsCannotBorrowAdministratorTrigger(t *testing.T) {
	svc, probe, payload := wikiAccessFixture()
	ambient := types.WithTaskAuthorization(context.Background(), 7, types.TaskInitiator{UserID: "admin-trigger"})
	ops := []WikiPendingOp{
		{KnowledgeID: "revoked", Initiator: types.TaskInitiator{UserID: "revoked-user"}},
		{KnowledgeID: "allowed", Initiator: types.TaskInitiator{UserID: "allowed-user", CallerTenantID: 7}},
		{KnowledgeID: "legacy"},
		{KnowledgeID: "machine", Initiator: types.TaskInitiator{UserID: "allowed-user", APIKeyID: 3}},
	}
	allowed, err := svc.filterAuthorizedWikiOps(ambient, payload, ops)
	require.NoError(t, err)
	require.Len(t, allowed, 1)
	require.Equal(t, "allowed", allowed[0].KnowledgeID)
	for _, id := range probe.seen {
		require.NotEqual(t, "admin-trigger", id)
	}
}

func TestWikiFinalizeChecksDurableSourcesBeforeFolderMutations(t *testing.T) {
	svc, _, payload := wikiAccessFixture()
	row := func(id int64, actor types.TaskInitiator, folder string) *types.TaskPendingOp {
		encoded, err := json.Marshal(wikiFinalizeRow{
			Initiators: []types.TaskInitiator{actor}, FolderIDs: []string{folder},
		})
		require.NoError(t, err)
		return &types.TaskPendingOp{ID: id, Op: wikiFinalizeOpFolderPrune, Payload: encoded}
	}
	pending := &folderPrunePendingRepoStub{rows: []*types.TaskPendingOp{
		row(1, types.TaskInitiator{UserID: "revoked-user"}, "revoked-folder"),
		row(2, types.TaskInitiator{UserID: "allowed-user"}, "allowed-folder"),
		row(3, types.TaskInitiator{}, "legacy-folder"),
	}}
	mixed, err := json.Marshal(wikiFinalizeRow{Initiators: []types.TaskInitiator{
		{UserID: "allowed-user"}, {UserID: "revoked-user"},
	}, FolderIDs: []string{"mixed-revoked-folder"}})
	require.NoError(t, err)
	pending.rows = append(pending.rows, &types.TaskPendingOp{ID: 4, Op: wikiFinalizeOpFolderPrune, Payload: mixed})
	wiki := &folderPruneWikiServiceStub{}
	svc.pendingRepo, svc.wikiService, svc.task = pending, wiki, &folderPruneTaskStub{}
	payload.Initiator = types.TaskInitiator{UserID: "admin-trigger"}
	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, svc.ProcessWikiFinalize(context.Background(), asynq.NewTask(types.TypeWikiFinalize, encoded)))
	require.Equal(t, []string{"allowed-folder"}, wiki.folderIDs)
	require.ElementsMatch(t, []int64{1, 2, 3, 4}, pending.deletedRowIDs)
}

type wikiAuthorizationModelService struct {
	interfaces.ModelService
	model chat.Chat
}

func (s wikiAuthorizationModelService) GetChatModel(context.Context, string) (chat.Chat, error) {
	return s.model, nil
}

func TestWikiIngestRejectsAnonymousDurableOpBeforeReadingChunksOrCallingModel(t *testing.T) {
	svc, _, payload := wikiAccessFixture()
	svc.kbService.(*wikiGuardKBService).kb.SummaryModelID = "model"
	encodedOp, err := json.Marshal(WikiPendingOp{Op: WikiOpIngest, KnowledgeID: "legacy-doc"})
	require.NoError(t, err)
	pending := &folderPrunePendingRepoStub{rows: []*types.TaskPendingOp{{ID: 1, Payload: encodedOp}}}
	model := &templateCaptureChatModel{}
	svc.pendingRepo, svc.task = pending, &folderPruneTaskStub{}
	svc.modelService = wikiAuthorizationModelService{model: model}
	// No chunk/knowledge repository is configured: attempting to read the
	// denied source instead of settling it would panic and fail this test.
	payload.Initiator = types.TaskInitiator{UserID: "admin-trigger"}
	encoded, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, svc.ProcessWikiIngest(context.Background(), asynq.NewTask(types.TypeWikiIngest, encoded)))
	require.Empty(t, model.prompt)
	require.Equal(t, []int64{1}, pending.deletedRowIDs)
}

type wikiRevokingModel struct {
	*templateCaptureChatModel
	onCall func()
}

func (m wikiRevokingModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	m.onCall()
	return &types.ChatResponse{Content: "generated content"}, nil
}

type wikiMutationProbe struct {
	interfaces.WikiPageService
	writes int
}

func (s *wikiMutationProbe) UpdatePage(_ context.Context, page *types.WikiPage) (*types.WikiPage, error) {
	s.writes++
	return page, nil
}

func TestWikiRevocationAfterModelCallBlocksResultAndPageWrite(t *testing.T) {
	svc, probe, payload := wikiAccessFixture()
	ctx := wikiActorContext(context.Background(), payload, []types.TaskInitiator{{UserID: "allowed-user"}})
	model := wikiRevokingModel{templateCaptureChatModel: &templateCaptureChatModel{}, onCall: func() {
		probe.mu.Lock()
		defer probe.mu.Unlock()
		probe.allowed["allowed-user"] = false
	}}
	content, err := svc.generateWithTemplate(ctx, model, "{{.Content}}", map[string]string{"Content": "source"})
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.Empty(t, content)
	storage := &wikiMutationProbe{}
	guarded := &wikiTaskPageService{WikiPageService: storage, check: svc.checkWikiTaskAuthorization}
	_, err = guarded.UpdatePage(ctx, &types.WikiPage{KnowledgeBaseID: "kb", TenantID: 7})
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.Zero(t, storage.writes)
}

func TestWikiLegacyActorPreservesModuleDisabledBehavior(t *testing.T) {
	svc, _, payload := wikiAccessFixture()
	access := NewGroupAccessService(nil)
	ConfigureGroupAccessDirectoryRuntime(access, &disabledDirectoryRuntime{})
	svc.groupAccess = access
	ctx := wikiActorContext(context.Background(), payload, nil)
	require.NoError(t, svc.checkWikiTaskAuthorization(ctx))
	ops, err := svc.filterAuthorizedWikiOps(ctx, payload, []WikiPendingOp{{KnowledgeID: "legacy"}})
	require.NoError(t, err)
	require.Len(t, ops, 1)
}
