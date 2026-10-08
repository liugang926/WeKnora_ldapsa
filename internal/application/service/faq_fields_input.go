package service

import (
	"context"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/types"
)

func (s *knowledgeService) updateFAQFieldsWithInput(ctx context.Context, kb *types.KnowledgeBase, req *types.FAQEntryFieldsBatchUpdate) error {
	plan, err := s.planFAQFields(ctx, kb, req)
	if err != nil {
		return err
	}
	snapshot, err := s.faqInputs.FAQSessionSnapshot(ctx)
	if err != nil {
		return err
	}
	excluded := map[int64]bool{}
	for _, id := range req.ExcludeIDs {
		excluded[id] = true
	}
	updates := map[string]*types.Chunk{}
	enabled := map[string]bool{}
	tags := map[string]string{}
	apply := func(chunk *types.Chunk, update types.FAQEntryFieldsUpdate) {
		if update.IsEnabled != nil {
			chunk.IsEnabled = *update.IsEnabled
			enabled[chunk.ID] = *update.IsEnabled
		}
		if update.IsRecommended != nil {
			if *update.IsRecommended {
				chunk.Flags = chunk.Flags.SetFlag(types.ChunkFlagRecommended)
			} else {
				chunk.Flags = chunk.Flags.ClearFlag(types.ChunkFlagRecommended)
			}
		}
		if update.TagID != nil {
			chunk.TagID = ""
			if *update.TagID > 0 {
				chunk.TagID = plan.tags[*update.TagID].ID
			}
			tags[chunk.ID] = chunk.TagID
		}
		chunk.UpdatedAt = time.Now()
		updates[chunk.ID] = chunk
	}
	for _, tagSeq := range sortedFAQIDs(req.ByTag) {
		tag := plan.tags[tagSeq]
		for _, chunk := range snapshot.Chunks {
			if chunk.TagID == tag.ID && !excluded[chunk.SeqID] {
				apply(chunk, req.ByTag[tagSeq])
			}
		}
	}
	for _, id := range sortedFAQIDs(req.ByID) {
		chunk := plan.chunks[id]
		if current := updates[chunk.ID]; current != nil {
			chunk = current
		}
		apply(chunk, req.ByID[id])
	}
	rows := make([]*types.Chunk, 0, len(updates))
	for _, chunk := range updates {
		rows = append(rows, chunk)
	}
	if err := s.saveFAQChunks(ctx, rows, false); err != nil {
		return err
	}
	if len(enabled) == 0 && len(tags) == 0 {
		return nil
	}
	engine, err := retriever.CreateRetrieveEngineForKB(ctx, s.retrieveEngine, s.ownership, kb.TenantID, kb.VectorStoreID)
	if err != nil {
		return err
	}
	if err := s.checkFAQInput(ctx); err != nil {
		return err
	}
	if len(enabled) > 0 {
		if err := engine.BatchUpdateChunkEnabledStatus(ctx, enabled); err != nil {
			return err
		}
	}
	if len(tags) > 0 {
		if err := engine.BatchUpdateChunkTagID(ctx, tags); err != nil {
			return err
		}
	}
	return s.checkFAQInput(ctx)
}
