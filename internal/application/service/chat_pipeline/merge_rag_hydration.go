package chatpipeline

import (
	"bytes"
	"context"

	"github.com/Tencent/WeKnora/internal/searchutil"
	"github.com/Tencent/WeKnora/internal/types"
)

func hasRAGInputReceipt(results []*types.SearchResult) bool {
	for _, result := range results {
		if result != nil && result.SourceLineage != nil {
			return true
		}
	}
	return false
}

func unknownMergeInput(ctx context.Context, original *types.SearchResult) {
	if original != nil {
		original.SourceLineage = types.UnknownSourceLineage()
	}
	if turn := types.RAGSourceLineageFromContext(ctx); turn != nil {
		_ = turn.Add(types.UnknownSourceLineage())
	}
}

// Missing original history provenance or an opaque enrichment cannot be
// refreshed by document ID. Unknown poisons the prefix before later model or
// progress output; a missing capability does not read any new source body.
func (p *PluginMerge) hydrateMergeInput(
	ctx context.Context,
	original *types.SearchResult,
	ids []string,
) []*types.Chunk {
	if p.ragChunks == nil || original == nil || original.SourceLineage.RequireComplete() != nil {
		unknownMergeInput(ctx, original)
		return nil
	}
	snapshots, err := p.ragChunks.HydrateRAGChunkAugmentations(ctx, original, ids)
	if err != nil {
		unknownMergeInput(ctx, original)
		return nil
	}
	var out []*types.Chunk
	inputs := []*types.SourceLineage{original.SourceLineage}
	for _, snapshot := range snapshots {
		if snapshot.Chunk == nil || snapshot.Knowledge == nil || snapshot.Chunk.KnowledgeID != original.KnowledgeID ||
			snapshot.Chunk.KnowledgeBaseID != original.KnowledgeBaseID ||
			snapshot.SourceLineage.RequireComplete() != nil {
			unknownMergeInput(ctx, original)
			return nil
		}
		out = append(out, snapshot.Chunk)
		inputs = append(inputs, snapshot.SourceLineage)
	}
	union, err := types.UnionSourceLineage(inputs...)
	if err != nil {
		unknownMergeInput(ctx, original)
		return nil
	}
	original.SourceLineage = union
	return out
}

func (p *PluginMerge) resolveCapturedParents(ctx context.Context, results []*types.SearchResult) []*types.SearchResult {
	for _, result := range results {
		if result == nil {
			continue
		}
		if result.ChunkType == types.ChunkTypeSummary {
			// A generated summary's parent records its original text input; it
			// is not a parent_text expansion request. Revalidate the captured
			// summary itself under its existing proof and source lease, without
			// borrowing parent text or certifying a changed cached body/relation.
			rows := p.hydrateMergeInput(ctx, result, []string{result.ID})
			if len(rows) != 1 || rows[0].ChunkType != types.ChunkTypeSummary ||
				rows[0].Content != result.Content || rows[0].ParentChunkID != result.ParentChunkID ||
				rows[0].ContentRevision != result.ContentRevision || rows[0].ChunkIndex != result.ChunkIndex ||
				rows[0].StartAt != result.StartAt || rows[0].EndAt != result.EndAt ||
				rows[0].ImageInfo != result.ImageInfo || !bytes.Equal(
				rows[0].Metadata,
				result.ChunkMetadata,
			) || result.ContentRewritten {
				unknownMergeInput(ctx, result)
			}
			continue
		}
		if result.ParentChunkID == "" {
			continue
		}
		// The narrow text closure does not borrow OCR/caption/image children
		// through the generic helper without their own real capture receipts.
		if result.ChunkType != types.ChunkTypeText {
			unknownMergeInput(ctx, result)
			continue
		}
		rows := p.hydrateMergeInput(ctx, result, []string{result.ParentChunkID})
		if len(rows) != 1 || rows[0].ChunkType != types.ChunkTypeParentText {
			continue
		}
		parent := rows[0]
		result.Content = searchutil.JoinChunkContent(parent.Content, result.Content, "\n\n")
		result.ContentRewritten = true
		if parent.ImageInfo != "" {
			result.ImageInfo = searchutil.MergeImageInfoJSON(
				map[string]string{"hit": result.ImageInfo, "parent": parent.ImageInfo},
			)
		}
		if !containsID(result.SubChunkID, result.ID) {
			result.SubChunkID = append(result.SubChunkID, result.ID)
		}
	}
	return results
}

func (p *PluginMerge) expandCapturedNeighbors(
	ctx context.Context,
	results []*types.SearchResult,
) []*types.SearchResult {
	for _, result := range results {
		if result == nil || result.ChunkType != types.ChunkTypeText || result.ID == "" || result.Content == "" ||
			runeLen(result.Content) >= 350 {
			continue
		}
		rows := p.hydrateMergeInput(ctx, result, []string{result.ID})
		if len(rows) != 1 {
			continue
		}
		base := rows[0]
		previous, following := "", ""
		prevID, nextID := base.PreChunkID, base.NextChunkID
		seen := map[string]bool{base.ID: true}
		for len(seen) < 32 && runeLen(searchutil.JoinChunkContent(previous, result.Content, "\n\n")+following) < 350 {
			var ids []string
			if prevID != "" && !seen[prevID] {
				ids = append(ids, prevID)
				seen[prevID] = true
			} else {
				prevID = ""
			}
			if nextID != "" && !seen[nextID] {
				ids = append(ids, nextID)
				seen[nextID] = true
			} else {
				nextID = ""
			}
			if len(ids) == 0 {
				break
			}
			neighbors := p.hydrateMergeInput(ctx, result, ids)
			if result.SourceLineage.RequireComplete() != nil {
				break
			}
			byID := map[string]*types.Chunk{}
			for _, chunk := range neighbors {
				byID[chunk.ID] = chunk
			}
			if prevID != "" {
				if chunk := byID[prevID]; chunk != nil && chunk.ChunkType == types.ChunkTypeText {
					previous = searchutil.JoinChunkContent(chunk.Content, previous, "\n\n")
					result.SubChunkID = append(result.SubChunkID, chunk.ID)
					prevID = chunk.PreChunkID
				} else {
					prevID = ""
				}
			}
			if nextID != "" {
				if chunk := byID[nextID]; chunk != nil && chunk.ChunkType == types.ChunkTypeText {
					following = searchutil.JoinChunkContent(following, chunk.Content, "\n\n")
					result.SubChunkID = append(result.SubChunkID, chunk.ID)
					nextID = chunk.NextChunkID
				} else {
					nextID = ""
				}
			}
		}
		if previous != "" || following != "" {
			result.Content = mergeOrderedContent(previous, result.Content, following, 850)
			result.ContentRewritten = true
		}
	}
	return results
}
