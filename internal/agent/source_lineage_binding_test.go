package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/agent/skills"
	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type lineageMetadataSource struct {
	skills.SkillSource
	metadata *skills.SkillMetadata
}

func (s *lineageMetadataSource) DiscoverSkills() ([]*skills.SkillMetadata, error) {
	return []*skills.SkillMetadata{s.metadata}, nil
}

func TestAgentLineageSkillMetadataCannotBorrowSystemReceiptAtActualSend(t *testing.T) {
	source := agentLineageIdentity("1", "original", 7)
	for _, kind := range []string{"system only positive", "unknown skill with empty system receipt", "skill receipt positive", "metadata changed after receipt"} {
		t.Run(kind, func(t *testing.T) {
			model := &mockChat{responses: []mockResponse{lineageAnswer("hello")}}
			e, _ := lineageEngine(t, model, nil, grantChecker(map[types.SourceIdentity]bool{source: true}))
			e.systemPromptTemplate = "literal system prompt"
			e.SetSystemPromptSourceLineage(agentComplete(t))
			if kind != "system only positive" {
				fixtureTool := newAccountedSourceTool()
				fixtureTool.BaseTool = agenttools.NewBaseTool("read_file", "literal fixture metadata loader", json.RawMessage(`{"type":"object"}`))
				e.toolRegistry.RegisterTool(fixtureTool)
				metadata := &skills.SkillMetadata{Name: "copied-skill", Description: "source-derived confidential skill metadata"}
				manager := skills.NewManager(&skills.ManagerConfig{Enabled: true}, nil).WithTenantSource(&lineageMetadataSource{metadata: metadata})
				require.NoError(t, manager.Initialize(context.Background()))
				e.SetSkillsManager(manager)
				if kind == "skill receipt positive" || kind == "metadata changed after receipt" {
					e.SetSkillsMetadataSourceLineage(agentComplete(t, source))
				}
				if kind == "metadata changed after receipt" {
					metadata.Description = "different unaccounted source content"
				}
			}
			state, err := e.Execute(context.Background(), "s", "m", "raw greeting", nil)
			if kind == "unknown skill with empty system receipt" || kind == "metadata changed after receipt" {
				require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
				require.Zero(t, model.callCount)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, model.callCount)
				if kind == "skill receipt positive" {
					require.Equal(t, agentComplete(t, source), state.SourceLineage)
					require.Contains(t, model.calls[0][0].Content, "source-derived confidential skill metadata")
				} else {
					require.Equal(t, agentComplete(t), state.SourceLineage)
				}
			}
		})
	}
}

func TestAgentLineageActualWireDefinitionMutationClosesBeforeModel(t *testing.T) {
	for _, kind := range []string{"description", "parameters", "outer type", "registered instance", "shared parameter bytes", "literal positive"} {
		t.Run(kind, func(t *testing.T) {
			model := &mockChat{responses: []mockResponse{lineageAnswer("answer")}}
			e, _ := lineageEngine(t, model, nil, grantChecker(map[types.SourceIdentity]bool{}))
			tool := agenttools.NewSequentialThinkingTool()
			e.toolRegistry.RegisterTool(tool)
			tools := e.buildToolsForLLM()
			switch kind {
			case "description":
				tools[0].Function.Description = "unknown source body copied into a same-name schema"
			case "parameters":
				tools[0].Function.Parameters = json.RawMessage(`{"type":"object","description":"unknown source body"}`)
			case "outer type":
				tools[0].Type = "unknown source body"
			case "registered instance":
				tool.BaseTool = agenttools.NewBaseTool("thinking", "unknown source body", tool.Parameters())
				tools = e.buildToolsForLLM()
			case "shared parameter bytes":
				// The old registry/schema slice is shared by the actual wire option.
				// Mutation must not modify the trusted literal comparison basis.
				original := append(json.RawMessage(nil), tool.Parameters()...)
				t.Cleanup(func() { copy(tool.Parameters(), original) })
				tools[0].Function.Parameters[0] = ' '
			}
			_, err := e.streamLLMToEventBus(context.Background(), []chat.Message{{Role: "user", Content: "raw human", SourceLineage: agentComplete(t)}}, &chat.ChatOptions{Tools: tools}, nil)
			if kind == "literal positive" {
				require.NoError(t, err)
				require.Equal(t, 1, model.callCount)
			} else {
				require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
				require.Zero(t, model.callCount)
			}
		})
	}
}
