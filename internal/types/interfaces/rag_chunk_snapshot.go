package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

type RAGChunkSnapshotReader interface {
	HydrateRAGChunkSnapshots(context.Context, []types.RAGChunkReadRequest) ([]types.RAGChunkInputSnapshot, error)
}

// RAGChunkAugmentationReader performs fresh authorized reads constrained by
// an already captured result. It never re-certifies that result's old body.
type RAGChunkAugmentationReader interface {
	HydrateRAGChunkAugmentations(context.Context, *types.SearchResult, []string) ([]types.RAGChunkInputSnapshot, error)
}
