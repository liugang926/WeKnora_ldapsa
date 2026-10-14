package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/sandbox"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

const protectedHistoryFileMarker = "previous-turn-protected-nextcloud-workspace-marker"

type historyBoundaryFileStore struct {
	stubSessionFileStore
	reads int
}

func (s *historyBoundaryFileStore) ReadSessionFile(context.Context, string, string) ([]byte, error) {
	s.reads++
	return []byte(protectedHistoryFileMarker), nil
}

func (s *historyBoundaryFileStore) StatSessionFile(context.Context, string, string) (*sandbox.RemoteStatEntry, error) {
	return &sandbox.RemoteStatEntry{Type: sandbox.RemoteEntryFile, Size: int64(len(protectedHistoryFileMarker))}, nil
}

type historyBoundaryShell struct{ calls int }

func (
	s *historyBoundaryShell,
) ExecShellCommand(context.Context, string, string, string, time.Duration, map[string]string) (
	*sandbox.ExecuteResult,
	error,
) {
	s.calls++
	return &sandbox.ExecuteResult{Stdout: protectedHistoryFileMarker}, nil
}

type historyBoundaryModel struct {
	fakeAgentChatModel
	calls     int
	offered   []string
	sawMarker bool
}

func (
	m *historyBoundaryModel,
) ChatStream(_ context.Context, messages []chat.Message, opts *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	m.calls++
	for _, message := range messages {
		if strings.Contains(message.Content, protectedHistoryFileMarker) {
			m.sawMarker = true
		}
	}
	if opts != nil {
		for _, tool := range opts.Tools {
			m.offered = append(m.offered, tool.Function.Name)
		}
	}
	response := types.StreamResponse{
		ResponseType: types.ResponseTypeAnswer,
		Content:      "new plain answer",
		Done:         true,
		FinishReason: "stop",
	}
	if m.calls == 1 {
		response = types.StreamResponse{Done: true, FinishReason: "tool_calls", ToolCalls: []types.LLMToolCall{
			{ID: "read-old", Type: "function", Function: types.FunctionCall{
				Name:      tools.ToolReadFile,
				Arguments: `{"path":"/workspace/output/old-protected.txt"}`,
			}},
			{ID: "exec-old", Type: "function", Function: types.FunctionCall{
				Name:      tools.ToolShellExec,
				Arguments: `{"command":"cat /workspace/output/old-protected.txt"}`,
			}},
		}}
	}
	if m.sawMarker {
		response.Content = protectedHistoryFileMarker
	}
	out := make(chan types.StreamResponse, 1)
	out <- response
	close(out)
	return out, nil
}

func TestAgentHistoryAdmissionFallbackBlocksOldWorkspaceReadAndExec(t *testing.T) {
	for _, exclude := range []bool{false, true} {
		t.Run(fmt.Sprintf("exclude-history-%t", exclude), func(t *testing.T) {
			files := &historyBoundaryFileStore{}
			shell := &historyBoundaryShell{}
			model := &historyBoundaryModel{}
			svc := &agentService{sandboxResolver: stubSandboxResolver{mgr: &capableManager{
				typ:   sandbox.SandboxTypeCube,
				shell: shell,
				files: files,
			}}}
			cfg := &types.AgentConfig{
				ExcludeHistoricalContext: exclude,
				SkillsEnabled:            true,
				SandboxConfigID:          "old-sandbox",
				AllowedTools: []string{
					tools.ToolThinking,
					tools.ToolReadFile,
					tools.ToolShellExec,
					tools.ToolSearchConversations,
					tools.ToolDataAnalysis,
				}, MaxIterations: 3, MCPSelectionMode: "none",
			}
			bus := event.NewEventBus()
			var output strings.Builder
			for _, kind := range []event.EventType{event.EventAgentToolResult, event.EventAgentFinalAnswer} {
				bus.On(kind, func(_ context.Context, evt event.Event) error {
					fmt.Fprint(
						&output,
						evt.Data,
					)
					return nil
				})
			}
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			engine, err := svc.CreateAgentEngine(ctx, cfg, model, nil, bus, "old-session", "new-message")
			require.NoError(t, err)
			_, err = engine.Execute(ctx, "old-session", "new-message", "new user question", nil)
			require.NoError(t, err)
			if exclude {
				require.Zero(t, files.reads)
				require.Zero(t, shell.calls)
				require.False(t, model.sawMarker)
				require.NotContains(t, output.String(), protectedHistoryFileMarker)
				for _, tool := range []string{
					tools.ToolReadFile,
					tools.ToolShellExec,
					tools.ToolListSandboxFiles,
					tools.ToolSearchConversations,
					tools.ToolDataAnalysis,
				} {
					require.NotContains(t, model.offered, tool)
				}
				require.Contains(t, output.String(), "new plain answer")
			} else {
				require.Positive(t, files.reads)
				require.Positive(t, shell.calls)
				require.True(t, model.sawMarker)
				require.Contains(t, output.String(), protectedHistoryFileMarker)
			}
		})
	}
}

type retainedContextHistoryRepo struct {
	historyRepo
	attachmentLoads int
}

func (r *retainedContextHistoryRepo) GetSessionAttachments(context.Context, string) (types.MessageAttachments, error) {
	r.attachmentLoads++
	return nil, nil
}

func TestAgentHistoryAdmissionBothModesIsolateRetainedWorkspaceAtAgentQA(t *testing.T) {
	for _, multiTurn := range []bool{false, true} {
		for _, scenario := range []string{"ever-source", "lookup-unavailable", "missing-policy", "ordinary-no-source"} {
			t.Run(fmt.Sprintf("multi-turn-%t/%s", multiTurn, scenario), func(t *testing.T) {
				files := &historyBoundaryFileStore{}
				shell := &historyBoundaryShell{}
				model := &historyBoundaryModel{}
				sources := &instanceHistorySources{ever: scenario == "ever-source"}
				if scenario == "lookup-unavailable" {
					sources.err = errors.New("source history lookup unavailable")
				}
				guard := access.NewNextcloudHistoryGuard(nil, sources, nil)
				if scenario == "missing-policy" {
					guard = nil
				}
				repo := &retainedContextHistoryRepo{historyRepo: historyRepo{rows: storedTurns(3)}}
				agents := &agentService{
					cfg: &config.Config{},
					sandboxResolver: stubSandboxResolver{
						mgr: &capableManager{
							typ:   sandbox.SandboxTypeCube,
							shell: shell,
							files: files,
						},
					},
				}
				var agentInterface interfaces.AgentService = agents
				svc := &sessionService{
					cfg:          &config.Config{},
					historyGuard: guard,
					messageRepo:  repo,
					agentService: agentInterface,
					modelService: &stubModelService{
						chatModel: model,
						modelsByID: map[string]*types.Model{
							"chat": {
								ID:   "chat",
								Type: types.ModelTypeKnowledgeQA,
							},
						},
					},
				}
				ctx := context.WithValue(context.WithValue(context.Background(), types.TenantIDContextKey, uint64(
					1,
				)), types.TenantInfoContextKey, &types.Tenant{ID: 1})
				req := &types.QARequest{
					Session: &types.Session{
						ID:       "same-old-session",
						TenantID: 1,
					},
					AssistantMessageID: "new-message",
					Query:              "new user question",
					TurnLeaseHeld:      true,
					CustomAgent: &types.CustomAgent{
						ID:       "agent",
						TenantID: 1,
						Config: types.CustomAgentConfig{
							ModelID:             "chat",
							KBSelectionMode:     "none",
							WebSearchProviderID: "unused",
							MCPSelectionMode:    "none",
							SkillsSelectionMode: "all",
							SandboxConfigID:     "old-sandbox",
							AllowedTools: []string{
								tools.ToolThinking,
								tools.ToolReadFile,
								tools.ToolShellExec,
							}, MultiTurnEnabled: multiTurn,
						},
					},
				}
				bus := event.NewEventBus()
				var output strings.Builder
				for _, kind := range []event.EventType{event.EventAgentToolResult, event.EventAgentFinalAnswer} {
					bus.On(kind, func(_ context.Context, evt event.Event) error {
						fmt.Fprint(
							&output,
							evt.Data,
						)
						return nil
					})
				}
				require.NoError(t, svc.AgentQA(ctx, req, bus))
				require.Equal(
					t,
					multiTurn,
					req.CustomAgent.Config.MultiTurnEnabled,
					("the single-turn case must not be defaulted into multi-t" +
						"urn"))
				if scenario == "ordinary-no-source" {
					require.Positive(t, repo.attachmentLoads)
					require.Positive(t, files.reads)
					require.Positive(t, shell.calls)
					require.True(t, model.sawMarker)
					require.Contains(t, output.String(), protectedHistoryFileMarker)
				} else {
					require.Zero(t, repo.attachmentLoads)
					require.Zero(t, repo.pages)
					require.Zero(t, repo.checkpointReads)
					require.Zero(t, files.reads)
					require.Zero(t, shell.calls)
					require.False(t, model.sawMarker)
					require.NotContains(t, output.String(), protectedHistoryFileMarker)
					require.Contains(t, output.String(), "new plain answer")
					for _, tool := range []string{tools.ToolReadFile, tools.ToolShellExec, tools.ToolListSandboxFiles} {
						require.NotContains(t, model.offered, tool)
					}
				}
			})
		}
	}
}
