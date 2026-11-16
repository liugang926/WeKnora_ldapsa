package chatpipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// readRetrievedImage checks the original row, storage address and bytes, including
// for an image inherited from an overlapping result. Synthetic citation metadata
// is never an authority to fetch another document or external URL.
func readRetrievedImage(kb interfaces.KnowledgeBaseService) func(context.Context, *types.SearchResult) ([]byte, error) {
	return func(ctx context.Context, r *types.SearchResult) ([]byte, error) {
		reader, hasReader := kb.(interfaces.RAGChunkAugmentationReader)
		if !hasReader || r == nil || r.SourceLineage.RequireComplete() != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		rows, err := reader.HydrateRAGChunkAugmentations(ctx, r, []string{r.ID})
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, types.ErrSourceLineageUnknown
		}
		row := rows[0]
		c := row.Chunk
		if c == nil || row.Knowledge == nil || row.Knowledge.ID != r.KnowledgeID ||
			c.ID != r.ID || c.KnowledgeBaseID != r.KnowledgeBaseID || c.KnowledgeID != r.KnowledgeID ||
			!c.IsEnabled || row.SourceLineage.RequireComplete() != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		var want, actual []types.ImageInfo
		if json.Unmarshal([]byte(r.ImageInfo), &want) != nil || len(want) == 0 ||
			json.Unmarshal([]byte(c.ImageInfo), &actual) != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		urlOf := func(i types.ImageInfo) string {
			if s := strings.TrimSpace(i.URL); s != "" {
				return s
			}
			return strings.TrimSpace(i.OriginalURL)
		}
		selector := urlOf(want[0])
		if selector == "" {
			return nil, types.ErrSourceLineageUnknown
		}
		for _, info := range actual {
			if urlOf(info) != selector || len(info.SHA256) != 64 {
				continue
			}
			input := *r
			raw, _ := json.Marshal([]types.ImageInfo{info})
			input.ImageInfo = string(raw)
			input.SourceLineage, err = types.UnionSourceLineage(r.SourceLineage, row.SourceLineage)
			if err != nil {
				return nil, err
			}
			if err := AccountResults(ctx, []*types.SearchResult{&input}); err != nil {
				return nil, err
			}
			data, err := kb.ReadChunkImage(ctx, &input)
			if err != nil {
				return nil, err
			}
			digest := sha256.Sum256(data)
			if !strings.EqualFold(hex.EncodeToString(digest[:]), info.SHA256) {
				return nil, errors.New("retrieved image digest mismatch")
			}
			if err := AccountResults(ctx, []*types.SearchResult{&input}); err != nil {
				return nil, err
			}
			return data, nil
		}
		return nil, types.ErrSourceLineageUnknown
	}
}
