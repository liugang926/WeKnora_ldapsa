package tools

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDocumentSummaryProjectionRequiresProducerMaterial(t *testing.T) {
	for _, mode := range []string{
		"text",
		"unproved-summary",
		"proved-summary",
		"unknown-lineage",
		"image",
		"metadata",
		"unsupported-type",
		"missing-chunk",
	} {
		t.Run(mode, func(t *testing.T) {
			lineage, err := types.NewCompleteSourceLineage()
			require.NoError(t, err)
			snapshot := types.RAGChunkInputSnapshot{
				Chunk:                    &types.Chunk{ChunkType: types.ChunkTypeSummary},
				SourceLineage:            lineage,
				ProducerMaterialVerified: true,
			}
			switch mode {
			case "text":
				snapshot.Chunk.ChunkType = types.ChunkTypeText
				snapshot.ProducerMaterialVerified = false
			case "unproved-summary":
				snapshot.ProducerMaterialVerified = false
			case "unknown-lineage":
				snapshot.SourceLineage = types.UnknownSourceLineage()
			case "image":
				snapshot.Chunk.ImageInfo = `[{"url":"unaccounted"}]`
			case "metadata":
				snapshot.Chunk.Metadata = types.JSON(`{"text":"unaccounted"}`)
			case "unsupported-type":
				snapshot.Chunk.ChunkType = types.ChunkTypeImageOCR
			case "missing-chunk":
				snapshot.Chunk = nil
			}
			err = validateDocumentChunkProjection(snapshot)
			if mode == "text" || mode == "proved-summary" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
