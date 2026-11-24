package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/Tencent/WeKnora/internal/utils"
)

var dataSchemaTool = BaseTool{
	name: ToolDataSchema,
	description: "Use this tool to get the schema information of a CSV or Excel file loaded into DuckDB. " +
		"It returns the stored table summary and column descriptions.",
	schema: utils.GenerateSchema[DataSchemaInput](),
}

type DataSchemaInput struct {
	KnowledgeID string `json:"knowledge_id" jsonschema:"doc://N document handle to query"`
}

type DataSchemaTool struct {
	BaseTool
	knowledgeService interfaces.KnowledgeService
	chunkRepo        interfaces.ChunkRepository
	snapshotReader   interfaces.RAGChunkSnapshotReader
	targetChunkTypes []types.ChunkType
	searchTargets    types.SearchTargets
	scopeEnforced    bool
}

// WithSearchTargets enables Agent request-scope authorization. An Agent turn
// with no search target must reject every document rather than fall back to
// the unrestricted service-owned lookup.
func (t *DataSchemaTool) WithSearchTargets(searchTargets types.SearchTargets) *DataSchemaTool {
	t.searchTargets = searchTargets
	t.scopeEnforced = true
	return t
}

// NewDataSchemaTool creates a tool for reading a document's table structure.
func NewDataSchemaTool(
	knowledgeService interfaces.KnowledgeService,
	chunkRepo interfaces.ChunkRepository,
	targetChunkTypes ...types.ChunkType,
) *DataSchemaTool {
	if len(targetChunkTypes) == 0 {
		targetChunkTypes = []types.ChunkType{types.ChunkTypeTableSummary, types.ChunkTypeTableColumn}
	}
	return &DataSchemaTool{
		BaseTool:         dataSchemaTool,
		knowledgeService: knowledgeService,
		chunkRepo:        chunkRepo,
		targetChunkTypes: targetChunkTypes,
	}
}

// Execute executes the tool logic
func (t *DataSchemaTool) Execute(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	var input DataSchemaInput
	if err := json.Unmarshal(args, &input); err != nil {
		return &types.ToolResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to parse input args: %v", err),
		}, err
	}

	// Get knowledge to get TenantID (use IDOnly to support cross-tenant shared KB)
	var knowledge *types.Knowledge
	var err error
	if t.scopeEnforced {
		knowledge, err = authorizeKnowledgeInSearchTargets(ctx, t.searchTargets, input.KnowledgeID, t.knowledgeService)
	} else {
		knowledge, err = t.knowledgeService.GetKnowledgeByIDOnly(ctx, input.KnowledgeID)
	}
	if err != nil || knowledge == nil {
		if err == nil {
			err = fmt.Errorf("knowledge service returned an empty result")
		}
		return &types.ToolResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to get knowledge '%s': %v", input.KnowledgeID, err),
		}, err
	}

	chunks, _, err := t.listSchemaChunks(ctx, knowledge)
	if err != nil {
		return &types.ToolResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to list chunks for knowledge ID '%s': %v", input.KnowledgeID, err),
		}, err
	}

	result := tableSchemaResult(input.KnowledgeID, chunks)
	if !result.Success {
		return result, fmt.Errorf("no schema info found")
	}
	result.Output += fmt.Sprintf("\n\nQuery this document with data_analysis as the table %q.", DataAnalysisTableName)
	return result, nil
}

func (t *DataSchemaTool) listSchemaChunks(ctx context.Context, k *types.Knowledge) ([]*types.Chunk, int64, error) {
	enabled := true
	return t.chunkRepo.ListPagedChunksByKnowledgeID(
		ctx, k.TenantID, k.ID, &types.Pagination{Page: 1, PageSize: 100}, t.targetChunkTypes,
		nil, "", "", "", "", &enabled,
	)
}

func tableSchemaResult(knowledgeID string, chunks []*types.Chunk) *types.ToolResult {
	var summaryContent, columnContent string
	for _, chunk := range chunks {
		if chunk.ChunkType == types.ChunkTypeTableSummary {
			summaryContent = chunk.Content
		} else if chunk.ChunkType == types.ChunkTypeTableColumn {
			columnContent = chunk.Content
		}
	}

	if summaryContent == "" || columnContent == "" {
		return &types.ToolResult{
			Success: false,
			Error:   fmt.Sprintf("No table schema information found for knowledge ID '%s'", knowledgeID),
		}
	}

	return &types.ToolResult{
		Success: true,
		Output:  summaryContent + "\n\n" + columnContent,
		Data: map[string]interface{}{
			"summary": summaryContent,
			"columns": columnContent,
		},
	}
}
