package agent

import (
	"context"
	"testing"

	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDefaultAgentToolDefinitionsHaveSourceLineage(t *testing.T) {
	tests := map[string]types.Tool{
		agenttools.ToolSearchKnowledge:     agenttools.NewSearchKnowledgeTool(nil, nil, nil, nil, nil, nil),
		agenttools.ToolReadDocument:        agenttools.NewReadDocumentTool(nil, nil, nil),
		agenttools.ToolListDocuments:       agenttools.NewListDocumentsTool(nil, nil),
		agenttools.ToolQueryKnowledgeGraph: agenttools.NewQueryKnowledgeGraphTool(nil, nil).WithKnowledgeScope(nil),
		agenttools.ToolSearchConversations: agenttools.NewSearchConversationsTool(
			nil, "owner", "session",
		),
	}

	for name, tool := range tests {
		t.Run(name, func(t *testing.T) {
			engine := newTestEngine(t, &mockChat{})
			registry := agenttools.NewToolRegistry()
			registry.RegisterTool(tool)
			engine.toolRegistry = registry
			lineage := registry.DefinitionSourceLineageWithContext(
				context.Background(),
				engine.buildToolsForLLM(),
			)
			require.NoError(t, lineage.RequireComplete())
		})
	}
}

func TestOrdinaryKnowledgePromptReachesModelWithCompleteSourceIndependentInputs(t *testing.T) {
	model := &mockChat{responses: []mockResponse{lineageAnswer("answer")}}
	engine := newTestEngine(t, model)
	registry := agenttools.NewToolRegistry()
	registry.RegisterTool(agenttools.NewSearchKnowledgeTool(nil, nil, nil, nil, nil, nil))
	registry.RegisterTool(agenttools.NewReadDocumentTool(nil, nil, nil))
	registry.RegisterTool(agenttools.NewListDocumentsTool(nil, nil))
	registry.RegisterTool(agenttools.NewQueryKnowledgeGraphTool(nil, nil))
	engine.toolRegistry = registry
	engine.eventBus = event.NewEventBus()

	complete, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	engine.knowledgeBasesInfo = []*KnowledgeBaseInfo{{
		ID: "kb", Name: "Knowledge Base", Type: "document", SourceLineage: complete,
	}}
	engine.selectedDocs = []*SelectedDocumentInfo{{
		KnowledgeID: "doc", KnowledgeBaseID: "kb", Title: "Document", SourceLineage: complete,
	}}
	engine.SetQuestionOrigin(&QuestionOriginInfo{
		KnowledgeBaseID: "kb", KnowledgeBaseName: "Knowledge Base", SourceLineage: complete,
		Document: &SelectedDocumentInfo{
			KnowledgeID: "doc", KnowledgeBaseID: "kb", Title: "Document", SourceLineage: complete,
		},
	})

	state, err := engine.Execute(
		context.Background(),
		"session",
		"message",
		"answer from the selected knowledge",
		nil,
	)
	require.NoError(t, err)
	require.True(t, state.IsComplete)
	require.NotEmpty(t, state.FinalAnswer)
	require.Equal(t, 1, model.callCount)
}

func TestWeKnoraACLAgentLineageDoesNotBecomeNextcloudMaterialGuard(t *testing.T) {
	model := &mockChat{responses: []mockResponse{lineageAnswer("answer")}}
	engine := newTestEngine(t, model)
	checks := 0
	engine.SetWeKnoraACLSourceLineageVerifier(func(context.Context) error {
		checks++
		return nil
	})
	engine.toolRegistry = agenttools.NewToolRegistry()
	engine.toolRegistry.RegisterTool(agenttools.NewSearchKnowledgeTool(nil, nil, nil, nil, nil, nil))

	state, err := engine.Execute(
		context.Background(),
		"session",
		"message",
		"ordinary WeKnora question",
		[]chat.Message{{
			Role:          "assistant",
			Content:       "legacy WeKnora history without a private source receipt",
			SourceLineage: types.UnknownSourceLineage(),
		}},
	)
	require.NoError(t, err)
	require.True(t, state.IsComplete)
	require.NotEmpty(t, state.FinalAnswer)
	require.Positive(t, checks)
	require.NoError(t, state.SourceLineage.RequireComplete())

	strict := newTestEngine(t, &mockChat{})
	strict.SetSourceLineageVerifier(func(context.Context, *types.SourceLineage) error {
		return nil
	})
	require.NoError(t, strict.addLineage(types.UnknownSourceLineage()))
	require.ErrorIs(t, strict.checkLineage(context.Background()), ErrAgentSourceLineageDenied)
}

func TestWeKnoraACLAgentAcceptsMalformedLegacyNextcloudReceipt(t *testing.T) {
	model := &mockChat{responses: []mockResponse{lineageAnswer("answer")}}
	engine := newTestEngine(t, model)
	engine.SetWeKnoraACLSourceLineageVerifier(func(context.Context) error { return nil })

	legacy := types.UnknownSourceLineage()
	legacy.Sources = []types.SourceIdentity{{Provider: types.ConnectorTypeNextcloud}}
	state, err := engine.Execute(
		context.Background(),
		"session",
		"message",
		"ordinary question over a Nextcloud-sourced KB",
		[]chat.Message{{
			Role:          "assistant",
			Content:       "legacy answer with a malformed optional source receipt",
			SourceLineage: legacy,
		}},
	)
	require.NoError(t, err)
	require.True(t, state.IsComplete)
	require.NotEmpty(t, state.FinalAnswer)
	require.Equal(t, 1, model.callCount)
	require.NoError(t, state.SourceLineage.RequireComplete())
	require.Empty(t, state.SourceLineage.Sources)

	strict := newTestEngine(t, &mockChat{})
	strict.SetSourceLineageVerifier(func(context.Context, *types.SourceLineage) error { return nil })
	require.ErrorIs(t, strict.addLineage(legacy), ErrAgentSourceLineageDenied)
}

func TestWeKnoraACLAgentAcceptsMalformedCompleteNextcloudReceipt(t *testing.T) {
	model := &mockChat{responses: []mockResponse{lineageAnswer("answer")}}
	engine := newTestEngine(t, model)
	engine.SetWeKnoraACLSourceLineageVerifier(func(context.Context) error { return nil })

	legacy := types.UnknownSourceLineage()
	legacy.State = types.SourceLineageComplete
	legacy.Sources = []types.SourceIdentity{{
		Provider: types.ConnectorTypeNextcloud,
		TenantID: 7,
		FileID:   "1",
	}}
	state, err := engine.Execute(
		context.Background(),
		"session",
		"message",
		"ordinary question over a Nextcloud-sourced KB",
		[]chat.Message{{
			Role:          "assistant",
			Content:       "legacy answer with a malformed complete source receipt",
			SourceLineage: legacy,
		}},
	)
	require.NoError(t, err)
	require.True(t, state.IsComplete)
	require.NotEmpty(t, state.FinalAnswer)
	require.Equal(t, 1, model.callCount)
	require.NoError(t, state.SourceLineage.RequireComplete())
	require.Empty(t, state.SourceLineage.Sources)
	require.NoError(t, engine.lineageFailure())

	strict := newTestEngine(t, &mockChat{})
	strict.SetSourceLineageVerifier(func(context.Context, *types.SourceLineage) error { return nil })
	require.ErrorIs(t, strict.addLineage(legacy), ErrAgentSourceLineageDenied)
	require.Contains(t, strict.lineageFailureDiagnostic(), "invalid source lineage")
}
