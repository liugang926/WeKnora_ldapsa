package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// SetRAGChunkSnapshotReader configures the reader used to hydrate original chunk snapshots.
func (t *ListDocumentsTool) SetRAGChunkSnapshotReader(reader interfaces.RAGChunkSnapshotReader) {
	t.snapshotReader = reader
}

// ExecuteSourceLineage executes the tool with retained original input receipts.
// Enumerate bounded candidate IDs, hydrate every actual metadata row, then
// filter/sort/page/count those snapshots. Old titles and DB totals do not
// become authenticated output by attaching a current file identity later.
func (t *ListDocumentsTool) ExecuteSourceLineage(
	ctx context.Context,
	args json.RawMessage,
) (result *types.ToolResult, err error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil || t.snapshotReader == nil || t.knowledgeService == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	defer func() {
		if err != nil {
			_ = turn.Add(types.UnknownSourceLineage())
		}
	}()
	var input ListDocumentsInput
	if err := json.Unmarshal(args, &input); err != nil {
		return nil, err
	}
	kbID := strings.TrimSpace(input.KnowledgeBaseID)
	if err := validateKnowledgeBaseIDsInSearchTargets(t.searchTargets, []string{kbID}); err != nil {
		return nil, err
	}
	page := max(1, input.Page)
	pageSize := input.PageSize
	if pageSize < 1 {
		pageSize = listDocumentsDefaultPageSize
	}
	pageSize = min(pageSize, listDocumentsMaxPageSize)
	readCtx, guard, err := beginAgentRead(ctx, t.knowledgeService, t.searchTargets)
	if err != nil {
		return nil, err
	}
	if guard != nil {
		defer func() {
			// Only read pins are released here; cleanup does not publish protected material.
			_ = guard.Close()
		}()
	}
	ctx = readCtx
	// The bound is a limit, not a permission to truncate a complete receipt.
	candidates, total, err := t.listScoped(ctx, kbID, "", 1, listDocumentsScopeCap)
	if err != nil {
		return nil, err
	}
	if total > listDocumentsScopeCap || len(candidates) > listDocumentsScopeCap {
		return nil, types.ErrSourceLineageLimit
	}
	if total < 0 || int64(len(candidates)) != total {
		return nil, ErrToolSourceLineageUnavailable
	}
	lineage := independentToolInput()
	var actual []*types.Knowledge
	seen := map[string]bool{}
	for _, candidate := range candidates {
		if candidate == nil || seen[candidate.ID] || candidate.KnowledgeBaseID != kbID ||
			candidate.TenantID != t.searchTargets.GetTenantIDForKB(kbID) {
			return nil, ErrToolSourceLineageUnavailable
		}
		seen[candidate.ID] = true
		candidate, err = authorizeLoadedKnowledge(ctx, t.searchTargets, candidate, t.knowledgeService)
		if err != nil {
			return nil, err
		}
		if err := guard.PinKnowledge(candidate); err != nil {
			return nil, err
		}
		capture := &documentLineageCapture{guard: guard, lineage: independentToolInput()}
		captureCtx := context.WithValue(ctx, documentCaptureContextKey{}, capture)
		hydrator := &ReadDocumentTool{
			knowledgeService: t.knowledgeService,
			searchTargets:    t.searchTargets,
			snapshotReader:   t.snapshotReader,
		}
		snapshot, err := hydrator.hydrateDocumentInputs(captureCtx, candidate, nil)
		if err != nil {
			return nil, err
		}
		lineage, err = types.UnionSourceLineage(lineage, capture.lineage)
		if err != nil {
			return nil, err
		}
		actual = append(actual, snapshot.Knowledge)
	}
	if err := finishAgentRead(ctx, t.knowledgeService, t.searchTargets, guard, actual); err != nil {
		return nil, err
	}
	if err := turn.Add(lineage); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	filtered := make([]*types.Knowledge, 0, len(actual))
	keyword := strings.TrimSpace(input.Keyword)
	for _, k := range actual {
		if knowledgeMatchesKeyword(k, keyword) {
			filtered = append(filtered, k)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		if !filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
		}
		return filtered[i].ID < filtered[j].ID
	})
	count := len(filtered)
	start := (page - 1) * pageSize
	if start < 0 {
		return nil, ErrToolSourceLineageUnavailable
	}
	start = min(start, count)
	end := min(start+pageSize, count)
	var output strings.Builder
	fmt.Fprintf(
		&output,
		"<documents knowledge_base_id=\"%s\" total=\"%d\" page=\"%d\" page_size=\"%d\">\n",
		xmlEscape(kbID),
		count,
		page,
		pageSize,
	)
	documents := make([]map[string]any, 0, end-start)
	for _, k := range filtered[start:end] {
		// Generated descriptions and arbitrary metadata have separate writer
		// contracts; this covered projection exposes actual file metadata only.
		fmt.Fprintf(
			&output,
			"<document knowledge_id=\"%s\" title=\"%s\" file_name=\"%s\" file_type=\"%s\" />\n",
			xmlEscape(k.ID),
			xmlEscape(k.Title),
			xmlEscape(k.FileName),
			xmlEscape(k.FileType),
		)
		documents = append(
			documents,
			map[string]any{
				"knowledge_id": k.ID,
				"title":        k.Title,
				"type":         k.Type,
				"source":       k.Source,
				"file_name":    k.FileName,
				"file_type":    k.FileType,
				"file_size":    k.FileSize,
				"parse_status": k.ParseStatus,
				"is_faq":       false,
			},
		)
	}
	output.WriteString("</documents>")
	data := map[string]any{
		"display_type":      "document_info",
		"knowledge_base_id": kbID,
		"documents":         documents,
		"total_docs":        count,
		"page":              page,
		"page_size":         pageSize,
	}
	if keyword != "" {
		data["keyword"] = keyword
	}
	if end < count {
		data["next_page"] = page + 1
	}
	return &types.ToolResult{Success: true, Output: output.String(), Data: data, SourceLineage: lineage}, nil
}
