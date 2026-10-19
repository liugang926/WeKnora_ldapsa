//go:build sqlite_fts5

package service

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent"
	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func documentLiveAgent(
	t *testing.T,
	m *machinePipelineFixture,
	models *documentLiveModels,
	kbs *knowledgeBaseService,
) func() {
	t.Helper()
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_AGENT") != "1" {
		return func() {}
	}
	// Use the same persisted KB and body services as retrieval; no tool response stubs.
	m.f.s.kbService = kbs
	m.f.s.contentLeases = m.f.store.ContentLeaseStore()
	m.f.s.groupAccess = NewGroupAccessService(repository.NewGroupAccessRepository(m.f.db))
	var tenant types.Tenant
	require.NoError(t, m.f.db.Where("id=7").Take(&tenant).Error)
	ctx := context.WithValue(m.f.ctx, types.TenantInfoContextKey, &tenant)
	ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	t.Cleanup(cancel)
	ctx, attach, check, releaseHTTP := BeginMessageMaterialHTTPBoundary(ctx)
	t.Cleanup(releaseHTTP)
	ctx, releaseModel, external := retainMessageMaterialProducer(ctx)
	require.True(t, external)
	t.Cleanup(releaseModel)
	markMessageMaterialReady(ctx)
	targets := types.SearchTargets{
		&types.SearchTarget{
			Type:            types.SearchTargetTypeKnowledgeBase,
			TenantID:        7,
			KnowledgeBaseID: m.ds.KnowledgeBaseID,
		},
	}
	scope := access.LineageReadScope{Targets: targets}
	verify := func(ctx context.Context, l *types.SourceLineage) error { return m.f.guard.Check(ctx, l, scope) }
	chunks := NewChunkService(
		m.f.s.chunkRepo,
		m.f.s.repo,
		repository.NewKnowledgeBaseRepository(m.f.db),
		models,
		m.f.s.retrieveEngine,
		nil,
		nil,
		nil,
	)
	registry := agenttools.NewToolRegistry()
	registry.RegisterTool(agenttools.NewSearchKnowledgeTool(kbs, m.f.s, chunks, targets, nil, nil))
	registry.RegisterTool(agenttools.NewReadDocumentTool(m.f.s, chunks, targets))
	registry.SetRAGChunkSnapshotReader(repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store))
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_AGENT_PREFLIGHT") == "1" {
		// Diagnostic only; the final acceptance run omits this preflight so
		// the model's first tool call starts with a fresh physical-read slot.
		probe := agenttools.NewSearchKnowledgeTool(kbs, m.f.s, chunks, targets, nil, nil)
		probe.SetRAGChunkSnapshotReader(
			documentAgentSnapshotDiagnostic{t, repository.NewRAGChunkSnapshotReader(m.f.db, m.f.store)},
		)
		probeCtx := types.WithRAGSourceLineage(ctx, types.NewRAGSourceLineage(verify))
		_, probeErr := probe.ExecuteSourceLineage(
			probeCtx,
			json.RawMessage(`{"query":"Who does Alice know on Project Cedar?","limit":3}`),
		)
		require.NoError(t, probeErr, "actual search tool preflight before the model loop")
	}
	independent, _ := types.NewCompleteSourceLineage()
	thinking, citations := false, false
	makeEngine := func() *agent.AgentEngine {
		engine := agent.NewAgentEngine(
			&types.AgentConfig{
				MaxIterations:       5,
				Temperature:         0.1,
				Thinking:            &thinking,
				CitationEnabled:     &citations,
				MaxCompletionTokens: 512,
				SearchTargets:       targets,
			},
			models.summary,
			registry,
			event.NewEventBus(),
			[]*agent.KnowledgeBaseInfo{
				{
					ID:            m.ds.KnowledgeBaseID,
					Name:          "Owned acceptance document collection",
					Type:          "document",
					Capabilities:  []string{"chunks"},
					SourceLineage: independent,
				},
			},
			nil,
			"live-owned-agent",
			"",
		)
		require.NotNil(t, engine)
		engine.SetSourceLineageVerifier(verify)
		return engine
	}
	result, err := makeEngine().Execute(ctx, "live-owned-agent", "live-agent-turn", ("Use search_knowledge to find the Project Cedar document, then use read_doc" +
		"ument to inspect it. Based only on that document, who does Alice know? Ans" +
		"wer with the person's name in one sentence."), nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.IsComplete)
	require.Contains(t, strings.ToLower(result.FinalAnswer), "bob")
	require.NoError(t, result.SourceLineage.RequireComplete())
	require.NotEmpty(t, result.SourceLineage.Sources)
	used := map[string]bool{}
	for _, step := range result.RoundSteps {
		for _, call := range step.ToolCalls {
			if call.ExecutionName() == agenttools.ToolSearchKnowledge ||
				call.ExecutionName() == agenttools.ToolReadDocument {
				require.NotNil(t, call.Result)
				require.True(t, call.Result.Success)
				require.NoError(t, call.Result.SourceLineage.RequireComplete())
				require.NotEmpty(t, call.Result.SourceLineage.Sources)
				used[call.ExecutionName()] = true
			}
		}
	}
	require.True(t, used[agenttools.ToolSearchKnowledge])
	require.True(t, used[agenttools.ToolReadDocument])
	require.NoError(t, check())
	releaseModel()
	require.NoError(t, CheckMessageMaterialOutputBoundary(attach(ctx)))
	t.Log(
		("REAL_AGENT actual model selected search_knowledge and read_document, real " +
			"tool outputs and final answer carry source dependencies"),
	)
	return func() {
		require.Error(t, verify(ctx, result.SourceLineage))
		calls := models.summary.streams.Load()
		syncCalls := models.summary.calls.Load()
		_, err := makeEngine().Execute(ctx, "live-owned-agent", "live-agent-after-revoke", "Explain the previous answer again.", []chat.Message{{Role: "assistant", Content: result.FinalAnswer, SourceLineage: result.SourceLineage}})
		require.Error(t, err)
		require.Equal(
			t,
			calls,
			models.summary.streams.Load(),
			"revoked Agent history must be rejected before model call",
		)
		require.Equal(
			t,
			syncCalls,
			models.summary.calls.Load(),
			"revoked Agent history must not reach non-streaming model either",
		)
		t.Log("REAL_AGENT revoked source history rejected before either model entrypoint")
		releaseHTTP()
	}
}

// Print only fixed shape flags, never document text or model credentials.
// This wrapper returns the production reader's exact result unchanged.
type documentAgentSnapshotDiagnostic struct {
	t     *testing.T
	inner interfaces.RAGChunkSnapshotReader
}

func (d documentAgentSnapshotDiagnostic) HydrateRAGChunkSnapshots(
	ctx context.Context,
	requests []types.RAGChunkReadRequest,
) ([]types.RAGChunkInputSnapshot, error) {
	rows, err := d.inner.HydrateRAGChunkSnapshots(ctx, requests)
	for _, row := range rows {
		if c := row.Chunk; c != nil {
			metadata := strings.TrimSpace(string(c.Metadata))
			images := strings.TrimSpace(c.ImageInfo)
			d.t.Logf(
				("AGENT_SNAPSHOT text=%t summary=%t metadata_empty=%t metadata_null=%t image" +
					"s_empty=%t lineage_complete=%t"),
				c.ChunkType == types.ChunkTypeText,
				c.ChunkType == types.ChunkTypeSummary,
				metadata == "" || metadata == "{}",
				metadata == "null",
				images == "" || images == "[]",
				row.SourceLineage.RequireComplete() == nil,
			)
		}
	}
	return rows, err
}
