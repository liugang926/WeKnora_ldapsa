//go:build sqlite_fts5

package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Uses the producer's native vector/FTS backend and actual persisted chunks.
// The returned checkpoint keeps the same cached retrieval/answer dependency
// alive until the runner removes the real NC share.
func documentLiveRAG(t *testing.T, m *machinePipelineFixture, models *documentLiveModels, knowledgeID string) func() {
	t.Helper()
	svc := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(m.f.db), kgRepo: m.f.s.repo, chunkRepo: m.f.s.chunkRepo,
		contentLeases: m.f.store.ContentLeaseStore(), ragChunkSnapshots: repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store), ragChunkLineageGuard: m.f.guard,
		modelService: models, retrieveEngine: m.f.s.retrieveEngine,
		publicationGuard: access.NewNextcloudPublicationGuard(repository.NewDirectoryRepository(m.f.db), repository.NewDataSourceRepository(m.f.db))}
	m.f.s.publicationGuard = svc.publicationGuard
	var tenant types.Tenant
	require.NoError(t, m.f.db.Where("id=7").Take(&tenant).Error)
	ctx := context.WithValue(m.f.ctx, types.TenantInfoContextKey, &tenant)
	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	t.Cleanup(cancel)
	ctx, attach, checkpoint, releaseHTTP := BeginMessageMaterialHTTPBoundary(ctx)
	t.Cleanup(releaseHTTP)
	ctx, releaseModel, external := retainMessageMaterialProducer(ctx)
	require.True(t, external)
	t.Cleanup(releaseModel)
	markMessageMaterialReady(ctx)
	manager := chatpipeline.NewEventManager()
	chatpipeline.ConfigureSourceLineage(manager, m.f.guard)
	chatpipeline.NewPluginMerge(manager, m.f.s.chunkRepo, m.f.s.chunkService)
	chatpipeline.ConfigureRAGChunkMergeHydration(manager, svc)
	chatpipeline.NewPluginIntoChatMessage(manager, &ragEntryMessages{}, m.f.s)
	chatpipeline.NewPluginChatCompletionStream(manager, models)
	cm := &types.ChatManage{}
	cm.TenantID = 7
	cm.SessionID = "live-owned-rag"
	cm.Query = "Who does Alice know on Project Cedar?"
	cm.UserContent = cm.Query
	cm.ChatModelID = models.chatRow.ID
	cm.SearchTargets = types.SearchTargets{&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: m.ds.KnowledgeBaseID}}
	thinking := false
	cm.SummaryConfig = types.SummaryConfig{Thinking: &thinking, MaxCompletionTokens: 512, Prompt: "Answer using only the provided document. Give the person's name in one sentence.", ContextTemplate: "Question: {{query}}\nDocument: {{contexts}}", MaxTokens: 512, Temperature: 0.1}
	ctx, err := manager.BeginSourceLineage(ctx, cm, access.LineageReadScope{Targets: cm.SearchTargets})
	require.NoError(t, err)
	params := types.SearchParams{QueryText: cm.Query, MatchCount: 3, SkipContextEnrichment: true}
	rows, err := svc.HybridSearch(ctx, m.ds.KnowledgeBaseID, params)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	for _, row := range rows {
		require.Equal(t, knowledgeID, row.KnowledgeID)
		require.NoError(t, row.SourceLineage.RequireComplete())
		require.NotEmpty(t, row.SourceLineage.Sources)
	}
	cm.SearchResult = rows
	require.Nil(t, manager.Trigger(ctx, types.CHUNK_MERGE, cm))
	require.NoError(t, chatpipeline.CheckSourceLineageInputs(ctx, cm))
	require.NotEmpty(t, cm.MergeResult)
	var answer strings.Builder
	var answerLineage *types.SourceLineage
	done := false
	bus := event.NewEventBus()
	bus.On(event.EventAgentFinalAnswer, func(_ context.Context, evt event.Event) error {
		data := evt.Data.(event.AgentFinalAnswerData)
		answer.WriteString(data.Content)
		if data.Done {
			done = true
			answerLineage, _ = evt.SourceLineage.(*types.SourceLineage)
		}
		return nil
	})
	cm.EventBus = bus.AsEventBusInterface()
	streamCount := models.summary.streams.Load()
	session := &sessionService{eventManager: manager}
	require.NoError(t, session.KnowledgeQAByEvent(ctx, cm, []types.EventType{types.INTO_CHAT_MESSAGE, types.CHAT_COMPLETION_STREAM}))
	require.True(t, done)
	require.NotNil(t, answerLineage)
	require.NoError(t, answerLineage.RequireComplete())
	require.NotEmpty(t, answerLineage.Sources)
	require.Contains(t, strings.ToLower(cm.RenderedContexts), "bob")
	require.Contains(t, strings.ToLower(answer.String()), "bob")
	require.Equal(t, streamCount+1, models.summary.streams.Load())
	require.NoError(t, checkpoint())
	releaseModel()
	require.NoError(t, CheckMessageMaterialOutputBoundary(attach(ctx)))
	turn := types.RAGSourceLineageFromContext(ctx)
	lineage, err := turn.CheckedSnapshot(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, lineage.Sources)
	t.Log("REAL_RAG native vector/FTS retrieval, actual chunk hydration/merge and real streamed answer passed; answer depends on the source")
	return func() {
		require.Error(t, turn.Check(ctx), "cached query/answer dependency must deny after actual share removal")
		denied, err := svc.HybridSearch(ctx, m.ds.KnowledgeBaseID, params)
		require.NoError(t, err, "search filters denied source rows")
		require.Empty(t, denied, "revoked source content must not be returned")
		prior := models.summary.streams.Load()
		require.Error(t, session.KnowledgeQAByEvent(ctx, cm, []types.EventType{types.INTO_CHAT_MESSAGE, types.CHAT_COMPLETION_STREAM}))
		require.Equal(t, prior, models.summary.streams.Load(), "revoked cached input must not reach model")
		releaseHTTP()
	}
}
