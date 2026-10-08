package tools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type documentCaptureContextKey struct{}
type documentLineageCapture struct {
	guard           *readlease.NextcloudReadGuard
	lineage         *types.SourceLineage
	metadataLineage *types.SourceLineage
	err             error
}

func documentCaptureFromContext(ctx context.Context) *documentLineageCapture {
	capture, _ := ctx.Value(documentCaptureContextKey{}).(*documentLineageCapture)
	return capture
}

type hydratedDocumentInputs struct {
	Knowledge *types.Knowledge
	Chunks    []*types.Chunk
}

func (t *ReadDocumentTool) SetRAGChunkSnapshotReader(reader interfaces.RAGChunkSnapshotReader) {
	t.snapshotReader = reader
}
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

func (t *ReadDocumentTool) hydrateDocumentInputs(ctx context.Context, k *types.Knowledge, chunks []*types.Chunk) (out *hydratedDocumentInputs, err error) {
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
	request := types.RAGChunkReadRequest{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, Lease: lease, MetadataOnly: len(chunks) == 0, ExpectedLineage: capture.metadataLineage}
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
		if snapshot.Knowledge == nil || snapshot.Knowledge.ID != k.ID || snapshot.Knowledge.TenantID != k.TenantID || snapshot.Knowledge.KnowledgeBaseID != k.KnowledgeBaseID {
			return nil, ErrToolSourceLineageUnavailable
		}
		if err := snapshot.SourceLineage.RequireComplete(); err != nil {
			return nil, err
		}

		if len(chunks) > 0 {
			c := snapshot.Chunk
			if c == nil || c.ID != chunks[i].ID || c.TenantID != k.TenantID || c.KnowledgeBaseID != k.KnowledgeBaseID || c.KnowledgeID != k.ID {
				return nil, ErrToolSourceLineageUnavailable
			}
			if c.ChunkType != types.ChunkTypeText || (strings.TrimSpace(c.ImageInfo) != "" && strings.TrimSpace(c.ImageInfo) != "[]") || (len(c.Metadata) > 0 && strings.TrimSpace(string(c.Metadata)) != "{}") {
				return nil, errors.New("document chunk enrichment lineage adapter is unavailable")
			}
			out.Chunks = append(out.Chunks, c)
		} else if snapshot.Chunk != nil {
			return nil, ErrToolSourceLineageUnavailable
		}
		capture.lineage, err = types.UnionSourceLineage(capture.lineage, snapshot.SourceLineage)
		if err != nil {
			return nil, err
		}
		if err := turn.Add(snapshot.SourceLineage); err != nil {
			return nil, err
		}
		out.Knowledge = snapshot.Knowledge
	}
	if request.MetadataOnly {
		capture.metadataLineage, err = types.UnionSourceLineage(capture.lineage)
		if err != nil {
			return nil, err
		}
	}
	if err := finishAgentRead(ctx, t.knowledgeService, t.searchTargets, capture.guard, []*types.Knowledge{out.Knowledge}); err != nil {
		return nil, err
	}
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
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
