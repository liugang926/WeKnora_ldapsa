package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
)

type mcpIndependentTransportKey struct{}

type mcpIndependentTransport struct {
	catalog *MCPCatalog
	configs map[string][]byte
	target  []byte
}

// ConfigureMCPIndependentTransportVerifier accepts only a server-owned current
// session/material check. Neither an empty lineage nor provider JSON installs it.
func ConfigureMCPIndependentTransportVerifier(registry *ToolRegistry, check func(context.Context) error) {
	if registry != nil {
		if catalog := registry.mcpCatalog(); catalog != nil {
			catalog.independentTransportVerifier = check
		}
	}
}

func mcpSourceJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return raw
}

func mcpOwnedToolSnapshot(tool *MCPTool) []byte {
	if tool == nil || tool.service == nil || tool.mcpTool == nil || tool.mcpManager == nil {
		return nil
	}
	return mcpSourceJSON(struct {
		Service      *types.MCPService
		Definition   *types.MCPTool
		Instructions string
	}{tool.service, tool.mcpTool, tool.serverInstructions})
}

func mcpCopyDefinition(def types.FunctionDefinition) types.FunctionDefinition {
	def.Parameters = append(json.RawMessage(nil), def.Parameters...)
	return def
}

func bindMCPDiscoverSource(tool *MCPDiscoverTool) *MCPDiscoverTool {
	tool.sourceBase = mcpCopyDefinition(definitionForTool(&tool.BaseTool))
	return tool
}

func bindMCPCallSource(tool *MCPCallTool) *MCPCallTool {
	tool.sourceBase = mcpCopyDefinition(definitionForTool(&tool.BaseTool))
	return tool
}

func bindMCPRegisteredSource(tool *MCPRegisteredTool) *MCPRegisteredTool {
	tool.sourceDefinition = mcpCopyDefinition(definitionForTool(tool))
	return tool
}

func (c *MCPCatalog) checkIndependentTransport(ctx context.Context) error {
	if ctx == nil || c == nil || c.transport == nil || c.lookup == nil ||
		c.independentTransportVerifier == nil || c.authorize(ctx) != nil {
		return ErrToolSourceLineageUnavailable
	}
	if err := c.independentTransportVerifier(ctx); err != nil {
		return ErrToolSourceLineageUnavailable
	}
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return ErrToolSourceLineageUnavailable
	}
	lineage, err := turn.CheckedSnapshot(ctx)
	if err != nil || len(lineage.Sources) != 0 {
		return ErrToolSourceLineageUnavailable
	}
	return nil
}

func (c *MCPCatalog) currentSourceConfigs(ctx context.Context) (map[string][]byte, error) {
	if err := c.checkIndependentTransport(ctx); err != nil {
		return nil, err
	}
	configs := make(map[string][]byte, len(c.servers))
	ids := make([]string, 0, len(c.servers))
	for id := range c.servers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		entry := c.servers[id]
		entry.mu.Lock()
		service, recorded := entry.service, append([]byte(nil), entry.sourceConfig...)
		entry.mu.Unlock()
		current, err := c.lookup(ctx, c.tenantID, id)
		if err != nil || current == nil || current.ID != id || !current.Enabled || current.DeletedAt.Valid ||
			(current.TenantID != c.tenantID && !current.IsBuiltin) || len(recorded) == 0 ||
			!bytes.Equal(recorded, mcpSourceJSON(service)) || !bytes.Equal(recorded, mcpSourceJSON(current)) {
			return nil, ErrToolSourceLineageUnavailable
		}
		configs[id] = recorded
	}
	return configs, nil
}

func mcpIndependentContext(ctx context.Context, execution *mcpIndependentTransport) context.Context {
	ctx = context.WithValue(ctx, mcpIndependentTransportKey{}, execution)
	if meta, ok := ToolExecFromContext(ctx); ok && meta != nil && meta.ApprovalCtx != nil {
		copied := *meta
		copied.ApprovalCtx = context.WithValue(meta.ApprovalCtx, mcpIndependentTransportKey{}, execution)
		ctx = WithToolExecContext(ctx, &copied)
	}
	return ctx
}

func checkMCPSourceService(ctx context.Context, service *types.MCPService) error {
	execution, present := ctx.Value(mcpIndependentTransportKey{}).(*mcpIndependentTransport)
	if !present {
		return ctx.Err()
	}
	if execution == nil || service == nil || execution.catalog.checkIndependentTransport(ctx) != nil ||
		!bytes.Equal(execution.configs[service.ID], mcpSourceJSON(service)) {
		return ErrToolSourceLineageUnavailable
	}
	current, err := execution.catalog.currentSourceConfigs(ctx)
	if err != nil || !bytes.Equal(current[service.ID], execution.configs[service.ID]) {
		return ErrToolSourceLineageUnavailable
	}
	return nil
}

func checkMCPSourceTool(ctx context.Context, tool *MCPTool) error {
	if err := checkMCPSourceService(ctx, tool.service); err != nil {
		return err
	}
	if execution, present := ctx.Value(mcpIndependentTransportKey{}).(*mcpIndependentTransport); present {
		if execution == nil || tool.mcpManager != execution.catalog.transport ||
			len(execution.target) == 0 || !bytes.Equal(execution.target, mcpOwnedToolSnapshot(tool)) {
			return ErrToolSourceLineageUnavailable
		}
	}
	return nil
}

func (c *MCPCatalog) verifyOwnedTool(ctx context.Context, tool *MCPTool) error {
	if tool == nil || tool.mcpManager != c.transport || len(tool.sourceSnapshot) == 0 ||
		!bytes.Equal(tool.sourceSnapshot, mcpOwnedToolSnapshot(tool)) || c.checkEnabled(ctx, tool) != nil {
		return ErrToolSourceLineageUnavailable
	}
	// Read the current persisted directory (or actual owned tools/list), not a
	// client-supplied function or a stale description remembered by the model.
	fresh, err := c.load(ctx, tool.service, false)
	if err != nil {
		return ErrToolSourceLineageUnavailable
	}
	for _, current := range fresh {
		if bytes.Equal(current.sourceSnapshot, tool.sourceSnapshot) && current.mcpManager == c.transport {
			return c.checkEnabled(ctx, current)
		}
	}
	return ErrToolSourceLineageUnavailable
}

func mcpSourceResult(ctx context.Context, execution *mcpIndependentTransport, result *types.ToolResult,
	execErr error,
) (*types.ToolResult, error) {
	if result == nil || execution.catalog.checkIndependentTransport(ctx) != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	current, err := execution.catalog.currentSourceConfigs(ctx)
	if err != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	for id, config := range execution.configs {
		if !bytes.Equal(current[id], config) {
			return nil, ErrToolSourceLineageUnavailable
		}
	}
	// Detach all returned fields, including nested metadata/images. The receipt
	// describes this actual Go-owned transport result; remote lineage fields
	// cannot grant authority or alter the enclosing turn's accumulated sources.
	var copied2 types.ToolResult
	if json.Unmarshal(mcpSourceJSON(result), &copied2) != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	copied2.SourceLineage = independentToolInput()
	return &copied2, execErr
}

// DefinitionSourceLineageForContext accounts for the actual registered tool definition.
func (t *MCPDiscoverTool) DefinitionSourceLineageForContext(ctx context.Context,
	actual types.FunctionDefinition,
) *types.SourceLineage {
	if !sameDefinition(t.sourceBase, definitionForTool(&t.BaseTool)) ||
		!sameDefinition(actual, definitionForTool(t)) {
		return types.UnknownSourceLineage()
	}
	if _, err := t.catalog.currentSourceConfigs(ctx); err != nil {
		return types.UnknownSourceLineage()
	}
	return independentToolInput()
}

// DefinitionSourceLineageForContext accounts for the actual registered tool definition.
func (t *MCPCallTool) DefinitionSourceLineageForContext(ctx context.Context,
	actual types.FunctionDefinition,
) *types.SourceLineage {
	if !sameDefinition(t.sourceBase, definitionForTool(&t.BaseTool)) ||
		!sameDefinition(actual, definitionForTool(t)) {
		return types.UnknownSourceLineage()
	}
	if _, err := t.catalog.currentSourceConfigs(ctx); err != nil {
		return types.UnknownSourceLineage()
	}
	return independentToolInput()
}

// DefinitionSourceLineageForContext accounts for the actual registered tool definition.
func (t *MCPRegisteredTool) DefinitionSourceLineageForContext(ctx context.Context,
	actual types.FunctionDefinition,
) *types.SourceLineage {
	if !sameDefinition(t.sourceDefinition, actual) || !sameDefinition(actual, definitionForTool(t)) {
		return types.UnknownSourceLineage()
	}
	configs, err := t.catalog.currentSourceConfigs(ctx)
	if err != nil {
		return types.UnknownSourceLineage()
	}
	current := mcpIndependentContext(ctx, &mcpIndependentTransport{catalog: t.catalog, configs: configs})
	if t.catalog.verifyOwnedTool(current, t.MCPTool) != nil {
		return types.UnknownSourceLineage()
	}
	return independentToolInput()
}

// ExecuteSourceLineage executes the tool through its typed source-input boundary.
func (t *MCPDiscoverTool) ExecuteSourceLineage(ctx context.Context,
	raw json.RawMessage,
) (*types.ToolResult, error) {
	configs, err := t.catalog.currentSourceConfigs(ctx)
	if err != nil {
		return nil, err
	}
	execution := &mcpIndependentTransport{catalog: t.catalog, configs: configs}
	current := mcpIndependentContext(ctx, execution)
	result, err := t.Execute(current, raw)
	if result != nil && result.Success {
		var described struct {
			ToolRef string           `json:"tool_ref"`
			Tools   []mcpToolSummary `json:"tools"`
		}
		if json.Unmarshal([]byte(result.Output), &described) == nil && described.ToolRef != "" {
			if tool := t.catalog.cachedTool(described.ToolRef); tool != nil {
				if t.catalog.verifyOwnedTool(current, tool) != nil {
					return nil, ErrToolSourceLineageUnavailable
				}
				t.catalog.sourceDescribed.Store(described.ToolRef, append([]byte(nil), tool.sourceSnapshot...))
			}
		}
		for _, returned := range described.Tools {
			found := false
			entry := t.catalog.servers[returned.ServerID]
			if entry != nil {
				entry.mu.Lock()
				loaded := append([]*MCPTool(nil), entry.tools...)
				entry.mu.Unlock()
				for _, tool := range loaded {
					if tool.mcpTool.Name == returned.Name {
						found = t.catalog.verifyOwnedTool(current, tool) == nil
						break
					}
				}
			}
			if !found {
				return nil, ErrToolSourceLineageUnavailable
			}
		}
	}
	return mcpSourceResult(current, execution, result, err)
}

// ExecuteSourceLineage executes the tool through its typed source-input boundary.
func (t *MCPCallTool) ExecuteSourceLineage(ctx context.Context,
	raw json.RawMessage,
) (*types.ToolResult, error) {
	configs, err := t.catalog.currentSourceConfigs(ctx)
	if err != nil {
		return nil, err
	}
	execution := &mcpIndependentTransport{catalog: t.catalog, configs: configs}
	current := mcpIndependentContext(ctx, execution)
	tool, args, err := t.resolve(current, raw)
	if err != nil {
		result, failure := mcpDiscoveryFailure(err, "unavailable")
		return mcpSourceResult(current, execution, result, failure)
	}
	if t.catalog.verifyOwnedTool(current, tool) != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	described, ok := t.catalog.sourceDescribed.Load(mcpToolRef(tool))
	if !ok || !bytes.Equal(described.([]byte), tool.sourceSnapshot) {
		return nil, ErrToolSourceLineageUnavailable
	}
	execution.target = append([]byte(nil), tool.sourceSnapshot...)
	result, err := t.registry.execute(current, tool, args)
	if t.catalog.verifyOwnedTool(current, tool) != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	return mcpSourceResult(current, execution, result, err)
}

// ExecuteSourceLineage executes the tool through its typed source-input boundary.
func (t *MCPRegisteredTool) ExecuteSourceLineage(ctx context.Context,
	raw json.RawMessage,
) (*types.ToolResult, error) {
	configs, err := t.catalog.currentSourceConfigs(ctx)
	if err != nil {
		return nil, err
	}
	execution := &mcpIndependentTransport{catalog: t.catalog, configs: configs, target: t.sourceSnapshot}
	current := mcpIndependentContext(ctx, execution)
	if t.catalog.verifyOwnedTool(current, t.MCPTool) != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	result, err := t.Execute(current, raw)
	if t.catalog.verifyOwnedTool(current, t.MCPTool) != nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	return mcpSourceResult(current, execution, result, err)
}
