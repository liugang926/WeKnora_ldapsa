package tools

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// SetRAGChunkSnapshotReader binds the same authoritative reader used by other document tools.
func (t *DataSchemaTool) SetRAGChunkSnapshotReader(reader interfaces.RAGChunkSnapshotReader) {
	t.snapshotReader = reader
}

// ExecuteSourceLineage reads the table schema under the turn's selected document scope.
// The listing supplies candidate IDs only. Even a no-schema result is tied to the document's snapshot.
func (t *DataSchemaTool) ExecuteSourceLineage(
	ctx context.Context, args json.RawMessage,
) (result *types.ToolResult, err error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil || !t.scopeEnforced || t.snapshotReader == nil || t.chunkRepo == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	// Never let the registry normalize a diagnostic from a failed/unaccounted
	// read into an ordinary workspace result. An authorized no-schema response
	// below is a tool result, not a failed read.
	defer func() {
		if err != nil {
			turn.RefuseOutput()
		}
	}()
	var input DataSchemaInput
	if err := json.Unmarshal(args, &input); err != nil {
		return nil, err
	}
	ctx, guard, err := beginAgentRead(ctx, t.knowledgeService, t.searchTargets)
	if err != nil {
		return nil, err
	}
	if guard != nil {
		defer func() { _ = guard.Close() }()
	}
	k, err := authorizeKnowledgeInSearchTargets(ctx, t.searchTargets, input.KnowledgeID, t.knowledgeService)
	if err != nil {
		return nil, err
	}
	if k.ID != strings.TrimSpace(input.KnowledgeID) || k.TenantID == 0 ||
		k.TenantID != t.searchTargets.GetTenantIDForKB(k.KnowledgeBaseID) {
		return nil, ErrToolSourceLineageUnavailable
	}
	if err := guard.PinKnowledge(k); err != nil {
		return nil, err
	}
	candidates, total, err := t.listSchemaChunks(ctx, k)
	if err != nil {
		return nil, err
	}
	if total != int64(len(candidates)) {
		return nil, ErrToolSourceLineageUnavailable
	}

	capture := &documentLineageCapture{guard: guard, lineage: independentToolInput()}
	ctx = context.WithValue(ctx, documentCaptureContextKey{}, capture)
	reader := ReadDocumentTool{
		knowledgeService: t.knowledgeService, searchTargets: t.searchTargets, snapshotReader: t.snapshotReader,
	}
	snapshot, err := reader.hydrateChunkProjection(ctx, k, candidates, validateSchemaChunk)
	if err != nil {
		return nil, err
	}
	if _, err := authorizeLoadedKnowledge(ctx, t.searchTargets, snapshot.Knowledge, t.knowledgeService); err != nil {
		return nil, err
	}
	result = tableSchemaResult(k.ID, snapshot.Chunks)
	result.SourceLineage = capture.lineage
	return result, nil
}

func validateSchemaChunk(snapshot types.RAGChunkInputSnapshot, _ *types.SourceLineage, requireComplete bool) error {
	chunk := snapshot.Chunk
	if chunk == nil || (chunk.ChunkType != types.ChunkTypeTableSummary && chunk.ChunkType != types.ChunkTypeTableColumn) {
		return ErrToolSourceLineageUnavailable
	}
	// Recheck the listing's eligibility on the hydrated row as a reindex may
	// have moved it back to stored-only between those two reads.
	if !chunk.IsEnabled ||
		(chunk.Status != int(types.ChunkStatusIndexed) && chunk.Status != int(types.ChunkStatusDefault)) {
		return ErrToolSourceLineageUnavailable
	}
	// Both descriptions are generated during ingestion. Strict material turns need
	// the writer's receipt for the actual derived bytes, in addition to the document lease.
	if requireComplete && !snapshot.ProducerMaterialVerified {
		return ErrToolSourceLineageUnavailable
	}
	return nil
}
