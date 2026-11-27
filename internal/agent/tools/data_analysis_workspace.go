package tools

import (
	"context"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

// executeWorkspaceQuery binds the raw file read to the selected workspace document.
// Strict material turns cannot use this tool without a receipt for the original bytes.
func (t *DataAnalysisTool) executeWorkspaceQuery(
	ctx context.Context, input DataAnalysisInput,
) (*types.ToolResult, error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	verified := false
	defer func() {
		// The registry also recovers panics. A partially read file must not
		// contribute even an error message until its final checks succeed.
		if !verified && turn != nil {
			turn.RefuseOutput()
		}
	}()
	if turn != nil && turn.RequiresCompleteProducerLineage() {
		return nil, ErrToolSourceLineageUnavailable
	}
	ctx, guard, err := beginAgentRead(ctx, t.knowledgeService, t.searchTargets)
	if err != nil {
		return nil, err
	}
	if guard != nil {
		defer func() { _ = guard.Close() }()
	}
	selected, err := authorizeKnowledgeInSearchTargets(ctx, t.searchTargets, input.KnowledgeID, t.knowledgeService)
	if err != nil {
		return nil, err
	}
	// Alternate services may reuse their row object. Keep the original fields
	// for the final comparison even if the backing record changes mid-read.
	doc := *selected
	if doc.ID != strings.TrimSpace(input.KnowledgeID) || doc.TenantID == 0 || doc.EnableStatus != "enabled" ||
		doc.TenantID != t.searchTargets.GetTenantIDForKB(doc.KnowledgeBaseID) {
		return nil, ErrToolSourceLineageUnavailable
	}
	if err := guard.PinKnowledge(&doc); err != nil {
		return nil, err
	}

	// Parallel tool calls share the pool. Give each call ownership of its physical
	// table and cleanup state, even when several calls select the same document.
	run := NewDataAnalysisTool(t.knowledgeBaseService, t.knowledgeService, t.tenantService,
		t.fileService, t.db, t.sessionID, t.storageResolver)
	run.localBaseDir = t.localBaseDir
	defer run.Cleanup(ctx)
	tableName := "k_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	schema, err := run.loadKnowledgeTable(ctx, &doc, tableName)
	if err != nil {
		return nil, err
	}
	result, queryErr := run.queryLoadedTable(ctx, input, schema)

	// A result or a SQL diagnostic may contain file content. Recheck both the
	// exact file reference and current grants before releasing either.
	current, err := authorizeKnowledgeInSearchTargets(ctx, t.searchTargets, input.KnowledgeID, t.knowledgeService)
	if err != nil {
		return nil, err
	}
	if current.EnableStatus != "enabled" || current.ID != doc.ID || current.TenantID != doc.TenantID ||
		current.KnowledgeBaseID != doc.KnowledgeBaseID || current.FilePath != doc.FilePath ||
		current.FileHash != doc.FileHash || current.FileType != doc.FileType {
		return nil, ErrToolSourceLineageUnavailable
	}
	err = finishAgentRead(ctx, t.knowledgeService, t.searchTargets, guard, []*types.Knowledge{current})
	if err != nil {
		return nil, err
	}
	verified = true
	return result, queryErr
}
