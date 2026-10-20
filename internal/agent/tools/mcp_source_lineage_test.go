package tools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/agent/approval"
	"github.com/Tencent/WeKnora/internal/event"
	internalmcp "github.com/Tencent/WeKnora/internal/mcp"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"
	sdkserver "github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/require"
)

type mcpSourceFixture struct {
	registry     *ToolRegistry
	ctx          context.Context
	service      *types.MCPService
	server       *sdkserver.MCPServer
	policy       *staticPolicyGate
	eligible     atomic.Bool
	calls        atomic.Int32
	lookupError  bool
	beforeReturn func()
}

func newMCPSourceFixture(t *testing.T) *mcpSourceFixture {
	t.Helper()
	utils.SetSSRFWhitelistFromRaw("127.0.0.1")
	t.Cleanup(utils.ResetSSRFWhitelistForTest)
	f := &mcpSourceFixture{registry: NewToolRegistry(), policy: &staticPolicyGate{enabled: true}}
	f.eligible.Store(true)
	f.server = sdkserver.NewMCPServer("actual-independent-transport", "1", sdkserver.WithToolCapabilities(false))
	f.server.AddTool(sdkmcp.NewTool("lookup", sdkmcp.WithString("id", sdkmcp.Required())),
		func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
			f.calls.Add(1)
			if f.beforeReturn != nil {
				f.beforeReturn()
			}
			return sdkmcp.NewToolResultText(
				`{"source_lineage":{"state":"complete"},"value":"actual HTTP response"}`,
			), nil
		})
	server := httptest.NewServer(sdkserver.NewStreamableHTTPServer(f.server, sdkserver.WithStateLess(true)))
	t.Cleanup(server.Close)
	manager := internalmcp.NewMCPManager(nil)
	t.Cleanup(manager.Shutdown)
	f.service = &types.MCPService{
		ID: "actual-service", TenantID: 7, Name: "Actual service", Enabled: true,
		URL: &server.URL, TransportType: types.MCPTransportHTTPStreamable,
	}
	base := types.WithExecutionTenant(context.Background(), 7)
	f.ctx = types.WithRAGSourceLineage(base, types.NewRAGSourceLineage(
		func(_ context.Context, lineage *types.SourceLineage) error { return lineage.RequireComplete() }))
	_, err := RegisterMCPTools(f.ctx, f.registry, []*types.MCPService{f.service}, manager, f.policy, 0,
		func(_ context.Context, tenant uint64, id string) (*types.MCPService, error) {
			if f.lookupError || tenant != 7 || id != f.service.ID {
				return nil, errors.New("controlled lookup refusal")
			}
			var copied types.MCPService
			if json.Unmarshal(mcpSourceJSON(f.service), &copied) != nil {
				return nil, errors.New("invalid fixture row")
			}
			return &copied, nil
		}, nil)
	require.NoError(t, err)
	ConfigureMCPIndependentTransportVerifier(f.registry, func(ctx context.Context) error {
		if !f.eligible.Load() {
			return errors.New("controlled material eligibility withdrawn")
		}
		return ctx.Err()
	})
	return f
}

func (f *mcpSourceFixture) describe(t *testing.T) json.RawMessage {
	t.Helper()
	result, err := f.registry.ExecuteToolWithSourceLineage(f.ctx, ToolDiscoverMCPTools,
		json.RawMessage(`{"mode":"describe","server_id":"actual-service","tool_name":"lookup"}`))
	require.NoError(t, err)
	require.True(t, result.Success, result.Error)
	require.NoError(t, result.SourceLineage.RequireComplete())
	var definition struct {
		ToolRef string `json:"tool_ref"`
	}
	require.NoError(t, json.Unmarshal([]byte(result.Output), &definition))
	raw, err := json.Marshal(map[string]any{"tool_ref": definition.ToolRef, "arguments": map[string]any{"id": "42"}})
	require.NoError(t, err)
	return raw
}

func TestMCPSourceLineageActualHTTPOrdinaryProxyAndDirect(t *testing.T) {
	f := newMCPSourceFixture(t)
	raw := f.describe(t)
	result, err := f.registry.ExecuteToolWithSourceLineage(f.ctx, ToolCallMCPTool, raw)
	require.NoError(t, err)
	require.True(t, result.Success, result.Error)
	require.Contains(t, result.Output, "actual HTTP response")
	require.NoError(t, result.SourceLineage.RequireComplete())
	require.Empty(t, result.SourceLineage.Sources)
	require.EqualValues(t, 1, f.calls.Load())
	f.registry.PrepareMCPToolsDirect(f.ctx)
	for _, name := range f.registry.ListTools() {
		tool, err := f.registry.GetTool(name)
		require.NoError(t, err)
		if direct, ok := tool.(*MCPRegisteredTool); ok {
			require.NoError(
				t,
				direct.DefinitionSourceLineageForContext(f.ctx, definitionForTool(direct)).RequireComplete(),
			)
			result, err = f.registry.ExecuteToolWithSourceLineage(f.ctx, name, json.RawMessage(`{"id":"43"}`))
			require.NoError(t, err)
			require.True(t, result.Success, result.Error)
			require.EqualValues(t, 2, f.calls.Load())
			return
		}
	}
	t.Fatal("actual direct MCP definition was not registered")
}

func TestMCPSourceLineageRejectsMissingOrChangedAuthorityBeforeExternalCall(t *testing.T) {
	for _, mutation := range []string{
		"missing verifier", "eligibility", "tenant", "lookup", "configuration", "policy", "definition", "source prefix",
	} {
		t.Run(mutation, func(t *testing.T) {
			f := newMCPSourceFixture(t)
			raw := f.describe(t)
			switch mutation {
			case "missing verifier":
				ConfigureMCPIndependentTransportVerifier(f.registry, nil)
			case "eligibility":
				f.eligible.Store(false)
			case "tenant":
				f.ctx = types.WithExecutionTenant(f.ctx, 8)
			case "lookup":
				f.lookupError = true
			case "configuration":
				f.service.Name = "Mutated without an UpdatedAt change"
			case "policy":
				f.policy.enabled = false
			case "definition":
				f.server.AddTool(sdkmcp.NewTool("lookup", sdkmcp.WithDescription("changed exact external definition"),
					sdkmcp.WithString("id", sdkmcp.Required())),
					func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
						f.calls.Add(1)
						return sdkmcp.NewToolResultText("changed"), nil
					})
			case "source prefix":
				lineage, err := types.NewCompleteSourceLineage(types.SourceIdentity{
					Provider: "nextcloud", TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds",
					PairOperationID: "pair", InstanceID: "instance", BindingID: "binding", FileID: "77",
					ExternalID: "nextcloud:instance:77", KnowledgeID: "knowledge", Revision: 1, ETag: "etag",
				})
				require.NoError(t, err)
				require.NoError(t, types.RAGSourceLineageFromContext(f.ctx).Add(lineage))
			}
			result, err := f.registry.ExecuteToolWithSourceLineage(f.ctx, ToolCallMCPTool, raw)
			if mutation == "policy" {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.False(t, result.Success)
			} else {
				require.Error(t, err)
				require.Nil(t, result)
			}
			require.Zero(t, f.calls.Load(), "no controlled/changed input may reach actual tools/call")
		})
	}
}

func TestMCPSourceLineageRejectsRevocationAtActualTransportReturn(t *testing.T) {
	f := newMCPSourceFixture(t)
	raw := f.describe(t)
	f.beforeReturn = func() { f.eligible.Store(false) }
	result, err := f.registry.ExecuteToolWithSourceLineage(f.ctx, ToolCallMCPTool, raw)
	require.Error(t, err)
	require.Nil(t, result)
	require.EqualValues(t, 1, f.calls.Load())
}

func TestMCPSourceLineageCurrentWireDefinitionsCannotBeBorrowed(t *testing.T) {
	f := newMCPSourceFixture(t)
	f.describe(t)
	f.registry.PrepareMCPToolsDirect(f.ctx)
	var wire []chat.Tool
	for _, definition := range f.registry.GetModelFunctionDefinitions() {
		wire = append(wire, chat.Tool{Type: "function", Function: chat.FunctionDef{
			Name: definition.Name, Description: definition.Description,
			Parameters: append(json.RawMessage(nil), definition.Parameters...),
		}})
	}
	require.NotEmpty(t, wire)
	require.NoError(t, f.registry.DefinitionSourceLineageWithContext(f.ctx, wire).RequireComplete())
	require.Error(t, f.registry.DefinitionSourceLineage(wire).RequireComplete(),
		"a context-free definition check cannot borrow session/material authority")
	changed := append([]chat.Tool(nil), wire...)
	changed[0].Function.Description += " unowned wire mutation"
	require.Error(t, f.registry.DefinitionSourceLineageWithContext(f.ctx, changed).RequireComplete())
	f.eligible.Store(false)
	require.Error(t, f.registry.DefinitionSourceLineageWithContext(f.ctx, wire).RequireComplete())
	require.Zero(t, f.calls.Load(), "definition authorization never invokes the actual remote tool")
}

type mcpSourceApprovalGate struct {
	*proxyApprovalGate
	revoke func()
}

func (g *mcpSourceApprovalGate) RequestAndWait(context.Context, approval.PendingRequest) (approval.Decision, error) {
	g.revoke()
	return approval.Decision{Approved: true}, nil
}

func TestMCPSourceLineageRechecksAfterApprovalBeforeTransport(t *testing.T) {
	f := newMCPSourceFixture(t)
	raw := f.describe(t)
	gate := &mcpSourceApprovalGate{proxyApprovalGate: &proxyApprovalGate{}, revoke: func() { f.eligible.Store(false) }}
	catalog := f.registry.mcpCatalog()
	catalog.gate = gate
	for _, entry := range catalog.servers {
		entry.mu.Lock()
		for _, tool := range entry.tools {
			tool.gate = gate
		}
		entry.mu.Unlock()
	}
	ctx := WithToolExecContext(f.ctx, &ToolExecContext{ApprovalCtx: f.ctx, EventBus: event.NewEventBus()})
	result, err := f.registry.ExecuteToolWithSourceLineage(ctx, ToolCallMCPTool, raw)
	require.Error(t, err)
	require.Nil(t, result)
	require.Zero(t, f.calls.Load(), "approval waiting cannot retain an expired independent transport grant")
}
