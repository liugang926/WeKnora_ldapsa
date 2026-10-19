package tools

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type (
	searchCaptureContextKey struct{}
	searchLineageCapture    struct {
		guard     *readlease.NextcloudReadGuard
		lineage   *types.SourceLineage
		documents map[string]*types.Knowledge
		err       error
		mu        sync.Mutex
	}
)

func markSearchLineageFailure(ctx context.Context, err error) {
	if err == nil {
		return
	}
	capture := searchCaptureFromContext(ctx)
	if capture == nil {
		return
	}
	capture.mu.Lock()
	if capture.err == nil {
		capture.err = err
	}
	capture.mu.Unlock()
	if turn := types.RAGSourceLineageFromContext(ctx); turn != nil {
		_ = turn.Add(types.UnknownSourceLineage())
	}
}

func searchCaptureFromContext(ctx context.Context) *searchLineageCapture {
	capture, _ := ctx.Value(searchCaptureContextKey{}).(*searchLineageCapture)
	return capture
}

func (t *SearchKnowledgeTool) SetRAGChunkSnapshotReader(reader interfaces.RAGChunkSnapshotReader) {
	t.snapshotReader = reader
}

func (t *SearchKnowledgeTool) ExecuteSourceLineage(
	ctx context.Context,
	args json.RawMessage,
) (*types.ToolResult, error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil || t.snapshotReader == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	capture := &searchLineageCapture{lineage: independentToolInput(), documents: map[string]*types.Knowledge{}}
	ctx = context.WithValue(ctx, searchCaptureContextKey{}, capture)
	result, err := t.Execute(ctx, args)
	if capture.err != nil {
		return nil, capture.err
	}
	if result == nil || result.SourceLineage == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	if err := turn.Add(capture.lineage); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	return result, err
}

// Retrieval rows are ID hints. Rebuild all actual title/body fields before
// rerank, MMR, snippets, filtering, headers, structured output or model input.
func (t *SearchKnowledgeTool) hydrateActualSearchInputs(
	ctx context.Context,
	rows []*searchResultWithMeta,
	targets types.SearchTargets,
) (out []*searchResultWithMeta, err error) {
	capture := searchCaptureFromContext(ctx)
	defer func() {
		if err != nil {
			markSearchLineageFailure(ctx, err)
		}
	}()
	for _, row := range rows {
		if row == nil || row.SearchResult == nil || row.ID == "" || row.KnowledgeID == "" ||
			row.MatchType == types.MatchTypeGraph ||
			row.MatchType == types.MatchTypeRelationChunk {
			return nil, ErrToolSourceLineageUnavailable
		}
		k, err := t.knowledgeService.GetKnowledgeByIDOnly(ctx, row.KnowledgeID)
		if err != nil {
			return nil, err
		}
		k, err = authorizeLoadedKnowledge(ctx, targets, k, t.knowledgeService)
		if err != nil {
			return nil, err
		}
		if k == nil || k.KnowledgeBaseID != row.KnowledgeBaseID ||
			targets.GetTenantIDForKB(k.KnowledgeBaseID) != k.TenantID {
			return nil, ErrToolSourceLineageUnavailable
		}
		if err := capture.guard.PinKnowledge(k); err != nil {
			return nil, err
		}
		local := &documentLineageCapture{
			guard:           capture.guard,
			lineage:         independentToolInput(),
			metadataLineage: row.SourceLineage,
		}
		readCtx := context.WithValue(ctx, documentCaptureContextKey{}, local)
		hydrator := &ReadDocumentTool{
			knowledgeService: t.knowledgeService,
			searchTargets:    targets,
			snapshotReader:   t.snapshotReader,
		}
		snapshot, err := hydrator.hydrateDocumentInputs(
			readCtx,
			k,
			[]*types.Chunk{{ID: row.ID, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID}},
		)
		if err != nil {
			return nil, err
		}
		k = snapshot.Knowledge
		c := snapshot.Chunks[0]
		// Scores and matched/index text can belong to an older build. They are
		// not copied into the covered projection; rerank scores are computed
		// below from the actual title/body under the inherited input envelope.
		actual := &types.SearchResult{
			ID:                c.ID,
			Content:           c.Content,
			KnowledgeID:       k.ID,
			KnowledgeBaseID:   k.KnowledgeBaseID,
			KnowledgeTitle:    k.Title,
			ChunkIndex:        c.ChunkIndex,
			StartAt:           c.StartAt,
			EndAt:             c.EndAt,
			ChunkType:         string(c.ChunkType),
			KnowledgeFilename: k.FileName,
			KnowledgeSource:   k.Source,
			KnowledgeChannel:  k.Channel,
			SourceLineage:     local.lineage,
			Metadata:          map[string]string{},
		}
		out = append(
			out,
			&searchResultWithMeta{
				SearchResult:      actual,
				SourceQuery:       row.SourceQuery,
				QueryType:         row.QueryType,
				KnowledgeBaseID:   k.KnowledgeBaseID,
				KnowledgeBaseType: types.KnowledgeBaseTypeDocument,
			},
		)
		capture.documents[k.ID] = k
		capture.lineage, err = types.UnionSourceLineage(capture.lineage, local.lineage)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func checkSearchModelBoundary(ctx context.Context) error {
	if searchCaptureFromContext(ctx) == nil {
		return nil
	}
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return ErrToolSourceLineageUnavailable
	}
	err := turn.Check(ctx)
	markSearchLineageFailure(ctx, err)
	return err
}
