package tools

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func actualAgentListFixture(t *testing.T) (*ListDocumentsTool, *leasedAgentKnowledgeService) {
	t.Helper()
	read, svc, db := actualAgentDocumentFixture(t)
	old := *svc.docs["doc"]
	old.Title = "stale index title"
	old.FileName = "old source name"
	svc.pages = map[string]*types.PageResult{"kb": {Data: []*types.Knowledge{&old}, Total: 1}}
	tool := NewListDocumentsTool(svc, read.searchTargets)
	tool.SetRAGChunkSnapshotReader(repository.NewRAGChunkSnapshotReader(db, nil))
	return tool, svc
}

type enumerationOverrideKnowledgeService struct {
	*leasedAgentKnowledgeService
	total int64
}

func (s *enumerationOverrideKnowledgeService) ListPagedKnowledgeByKnowledgeBaseID(
	ctx context.Context,
	kb string,
	page *types.Pagination,
	filter types.KnowledgeListFilter,
) (*types.PageResult, error) {
	result, err := s.readDocKnowledgeService.ListPagedKnowledgeByKnowledgeBaseID(ctx, kb, page, filter)
	if result != nil {
		result.Total = s.total
	}
	return result, err
}

func TestAgentLineageListActualMetadataBeforeFilterPageCountAndHeader(t *testing.T) {
	tool, _ := actualAgentListFixture(t)
	turn, ctx := toolTurn()
	r := NewToolRegistry()
	r.RegisterTool(tool)
	result, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolListDocuments,
		json.RawMessage(`{"knowledge_base_id":"kb","keyword":"authoritative"}`),
	)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Contains(t, result.Output, "authoritative title")
	require.Contains(t, result.Output, "file.txt")
	require.NotContains(t, result.Output, "stale index title")
	require.NotContains(t, result.Output, "old source name")
	require.NotContains(t, result.Output, "unaccounted generated profile")
	require.Equal(t, 1, result.Data["total_docs"])
	require.Len(t, result.SourceLineage.Sources, 1)
	require.Equal(t, "original", result.SourceLineage.Sources[0].ETag)
	require.Equal(t, result.SourceLineage, turn.Snapshot())
	// A source inspected for filtering still influences the no-match result.
	missing, err := r.ExecuteToolWithSourceLineage(
		ctx,
		ToolListDocuments,
		json.RawMessage(`{"knowledge_base_id":"kb","keyword":"no matching title"}`),
	)
	require.NoError(t, err)
	require.Equal(t, 0, missing.Data["total_docs"])
	require.Equal(t, result.SourceLineage, missing.SourceLineage)
}

func TestAgentLineageListRevokedMetadataOrIncompleteEnumerationReturnsNothing(t *testing.T) {
	for _, kind := range []string{"revoked", "incomplete", "overflow"} {
		t.Run(kind, func(t *testing.T) {
			tool, svc := actualAgentListFixture(t)
			switch kind {
			case "revoked":
				svc.revoked.Store(true)
			case "incomplete":
				tool.knowledgeService = &enumerationOverrideKnowledgeService{leasedAgentKnowledgeService: svc, total: 2}
			case "overflow":
				tool.knowledgeService = &enumerationOverrideKnowledgeService{
					leasedAgentKnowledgeService: svc,
					total:                       listDocumentsScopeCap + 1,
				}
			}
			turn, ctx := toolTurn()
			r := NewToolRegistry()
			r.RegisterTool(tool)
			result, err := r.ExecuteToolWithSourceLineage(
				ctx,
				ToolListDocuments,
				json.RawMessage(`{"knowledge_base_id":"kb"}`),
			)
			require.Error(t, err)
			require.Nil(t, result)
			require.Error(t, turn.Check(ctx))
		})
	}
}

func TestAgentLineageListTrustedStructuredEmptyDoesNotClearEarlierSources(t *testing.T) {
	tool, svc := actualAgentListFixture(t)
	turn, ctx := toolTurn()
	r := NewToolRegistry()
	r.RegisterTool(tool)
	first, err := r.ExecuteToolWithSourceLineage(ctx, ToolListDocuments, json.RawMessage(`{"knowledge_base_id":"kb"}`))
	require.NoError(t, err)
	svc.pages["kb"] = &types.PageResult{Data: []*types.Knowledge{}, Total: 0}
	empty, err := r.ExecuteToolWithSourceLineage(ctx, ToolListDocuments, json.RawMessage(`{"knowledge_base_id":"kb"}`))
	require.NoError(t, err)
	require.Empty(t, empty.SourceLineage.Sources)
	require.Equal(t, first.SourceLineage, turn.Snapshot())
	// Current grants remain mandatory for an empty structured page.
	svc.revoked.Store(true)
	result, err := r.ExecuteToolWithSourceLineage(
		context.WithoutCancel(ctx),
		ToolListDocuments,
		json.RawMessage(`{"knowledge_base_id":"kb"}`),
	)
	require.Error(t, err)
	require.Nil(t, result)
}
