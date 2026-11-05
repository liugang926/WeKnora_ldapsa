package tools

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/application/access"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type (
	documentCaptureContextKey struct{}
	documentLineageCapture    struct {
		guard           *readlease.NextcloudReadGuard
		lineage         *types.SourceLineage
		metadataLineage *types.SourceLineage
		err             error
	}
)

func documentCaptureFromContext(ctx context.Context) *documentLineageCapture {
	capture, _ := ctx.Value(documentCaptureContextKey{}).(*documentLineageCapture)
	return capture
}

type hydratedDocumentInputs struct {
	Knowledge *types.Knowledge
	Chunks    []*types.Chunk
}

// SetRAGChunkSnapshotReader configures the reader used to hydrate original chunk snapshots.
func (t *ReadDocumentTool) SetRAGChunkSnapshotReader(reader interfaces.RAGChunkSnapshotReader) {
	t.snapshotReader = reader
}

// SetRAGChunkSnapshotReader configures the reader used to hydrate original chunk snapshots.
func (r *ToolRegistry) SetRAGChunkSnapshotReader(reader interfaces.RAGChunkSnapshotReader) {
	for _, tool := range r.tools {
		if adapter, ok := tool.(interface {
			SetRAGChunkSnapshotReader(interfaces.RAGChunkSnapshotReader)
		}); ok {
			adapter.SetRAGChunkSnapshotReader(reader)
		}
	}
}

// ExecuteSourceLineage accounts actual metadata and every body read (including
// pages inspected for matching/counts). Candidate IDs are hints; the result
// builder receives only fresh snapshots tied to the original read lease.
func (t *ReadDocumentTool) ExecuteSourceLineage(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil || t.snapshotReader == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	capture := &documentLineageCapture{lineage: independentToolInput()}
	ctx = context.WithValue(ctx, documentCaptureContextKey{}, capture)
	result, err := t.Execute(ctx, args)
	if capture.err != nil {
		return nil, capture.err
	}
	if result == nil {
		return nil, err
	}
	// Diagnostics after any unaccounted read remain unknown. Local validation
	// diagnostics are accounted only by the registry before execution.
	if result.SourceLineage == nil {
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

func (t *ReadDocumentTool) hydrateDocumentInputs(
	ctx context.Context,
	k *types.Knowledge,
	chunks []*types.Chunk,
) (out *hydratedDocumentInputs, err error) {
	capture := documentCaptureFromContext(ctx)
	turn := types.RAGSourceLineageFromContext(ctx)
	defer func() {
		if err != nil {
			if capture != nil {
				capture.err = err
			}
			if turn != nil {
				_ = turn.Add(types.UnknownSourceLineage())
			}
		}
	}()
	if capture == nil || turn == nil || t.snapshotReader == nil || k == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	lease, err := capture.guard.ExactLease(k)
	if err != nil {
		return nil, err
	}
	request := types.RAGChunkReadRequest{
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID,
		KnowledgeID:     k.ID,
		Lease:           lease,
		MetadataOnly:    len(chunks) == 0,
		ExpectedLineage: expectedHydrationLineage(turn, capture.metadataLineage),
	}
	for _, c := range chunks {
		if c == nil || c.KnowledgeID != k.ID || c.KnowledgeBaseID != k.KnowledgeBaseID || c.TenantID != k.TenantID {
			return nil, ErrToolSourceLineageUnavailable
		}
		request.ChunkIDs = append(request.ChunkIDs, c.ID)
	}
	snapshots, err := t.snapshotReader.HydrateRAGChunkSnapshots(ctx, []types.RAGChunkReadRequest{request})
	if err != nil {
		return nil, err
	}
	if len(snapshots) != max(1, len(chunks)) {
		return nil, ErrToolSourceLineageUnavailable
	}
	out = &hydratedDocumentInputs{}
	for i, snapshot := range snapshots {
		if snapshot.Knowledge == nil || snapshot.Knowledge.ID != k.ID || snapshot.Knowledge.TenantID != k.TenantID ||
			snapshot.Knowledge.KnowledgeBaseID != k.KnowledgeBaseID {
			return nil, ErrToolSourceLineageUnavailable
		}
		if !turn.RequiresCompleteProducerLineage() && snapshot.Chunk != nil &&
			snapshot.Chunk.ChunkType == types.ChunkTypeText {
			// Generated chunk enrichment (for example question suggestions) is
			// not model-visible body text. ACL turns can omit this unaccounted
			// derivative without weakening the source-owned chunk content.
			// Explicit materials retain the strict rejection below.
			copied := *snapshot.Chunk
			copied.Metadata = nil
			copied.ImageInfo = ""
			snapshot.Chunk = &copied
		}
		sourceLineage := turn.NormalizeProducerLineage(snapshot.SourceLineage)
		if turn.RequiresCompleteProducerLineage() {
			if err := sourceLineage.RequireComplete(); err != nil {
				return nil, err
			}
		} else if err := sourceLineage.Validate(); err != nil {
			return nil, err
		}

		if len(chunks) > 0 {
			c := snapshot.Chunk
			if c == nil || c.ID != chunks[i].ID || c.TenantID != k.TenantID || c.KnowledgeBaseID != k.KnowledgeBaseID ||
				c.KnowledgeID != k.ID {
				return nil, ErrToolSourceLineageUnavailable
			}
			if err := validateDocumentChunkProjection(
				snapshot,
				sourceLineage,
				turn.RequiresCompleteProducerLineage(),
			); err != nil {
				return nil, err
			}
			out.Chunks = append(out.Chunks, c)
		} else if snapshot.Chunk != nil {
			return nil, ErrToolSourceLineageUnavailable
		}
		capture.lineage, err = types.UnionSourceLineage(capture.lineage, sourceLineage)
		if err != nil {
			return nil, err
		}
		if err := turn.Add(sourceLineage); err != nil {
			return nil, err
		}
		out.Knowledge = snapshot.Knowledge
	}
	if request.MetadataOnly {
		if turn.RequiresCompleteProducerLineage() {
			capture.metadataLineage, err = types.UnionSourceLineage(capture.lineage)
			if err != nil {
				return nil, err
			}
		} else {
			// The snapshot reader treats ExpectedLineage as a strict material
			// contract and rejects receipt-less legacy sources before the ACL
			// accumulator can normalize them. The exact read lease already
			// fences this document's build generation; ACL mode therefore
			// carries no strict expected producer lineage into chunk hydration.
			capture.metadataLineage = nil
		}
	}
	if err := finishAgentRead(
		ctx,
		t.knowledgeService,
		t.searchTargets,
		capture.guard,
		[]*types.Knowledge{out.Knowledge},
	); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// The snapshot reader treats ExpectedLineage as a strict material contract.
// ACL turns authorize the exact read lease against current WeKnora state, so
// they must not pass a malformed legacy producer receipt into that boundary.
// A valid complete receipt is retained for exact augmentation; malformed,
// receipt-less, and non-complete legacy evidence is omitted and revalidated
// from the reader's current source identity below.
func expectedHydrationLineage(
	turn *types.RAGSourceLineage,
	lineage *types.SourceLineage,
) *types.SourceLineage {
	if turn == nil || turn.RequiresCompleteProducerLineage() {
		return lineage
	}
	normalized := turn.NormalizeProducerLineage(lineage)
	if normalized == nil || len(normalized.Sources) == 0 || normalized.RequireComplete() != nil {
		return nil
	}
	return normalized
}

func validateDocumentChunkProjection(
	snapshot types.RAGChunkInputSnapshot,
	sourceLineage *types.SourceLineage,
	requireComplete bool,
) error {
	if requireComplete {
		if err := sourceLineage.RequireComplete(); err != nil {
			return err
		}
	} else if err := sourceLineage.Validate(); err != nil {
		return err
	}
	c := snapshot.Chunk
	if c == nil {
		return ErrToolSourceLineageUnavailable
	}
	// Generated summaries need an exact producer receipt. Older readers that
	// can only pin current source identity retain the original text-only scope.
	supported := c.ChunkType == types.ChunkTypeText ||
		(c.ChunkType == types.ChunkTypeSummary && snapshot.ProducerMaterialVerified)
	if !supported || (strings.TrimSpace(c.ImageInfo) != "" && strings.TrimSpace(c.ImageInfo) != "[]") ||
		(len(c.Metadata) > 0 && strings.TrimSpace(string(c.Metadata)) != "{}") {
		if access.UseWeKnoraReadAuthority() {
			// WeKnora owns read authorization (the deployment-wide opt-in):
			// parser-produced enrichment chunks (PPT slide captions, image
			// OCR, metadata-bearing text from PDF/Office parsing) are
			// first-party pipeline outputs, not external materials, and must
			// stay citable instead of failing the whole agent turn.
			return nil
		}
		return errors.New("document chunk enrichment lineage adapter is unavailable")
	}
	return nil
}

// Only fields from the covered source snapshot are rendered. Generated
// descriptions and arbitrary metadata require their own derivative receipts.
func documentProjection(ctx context.Context, k *types.Knowledge) *types.Knowledge {
	if documentCaptureFromContext(ctx) == nil {
		return k
	}
	view := *k
	view.Description = ""
	view.Metadata = nil
	return &view
}
