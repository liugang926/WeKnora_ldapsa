package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// Auto classification reads final summary/question material. The completion
// transition wins once in SQL; the observer then captures the actual sealed
// material for this new producer, rather than re-signing a cached old body.
func ConfigureKnowledgeAutoTagCompletion(post interfaces.TaskHandler, repo interfaces.KnowledgeRepository, store *repository.KnowledgeInputProvenanceStore) {
	s, ok := post.(*KnowledgePostProcessService)
	if !ok {
		return
	}
	repository.ConfigureKnowledgeCompletionObserver(repo, func(ctx context.Context, id string) error {
		k, err := repo.GetKnowledgeByIDOnly(ctx, id)
		if err != nil {
			return err
		}
		kb, err := s.kbService.GetKnowledgeBaseByIDOnly(ctx, k.KnowledgeBaseID)
		if err != nil {
			return err
		}
		if kb.AutoTagConfig == nil || !kb.AutoTagConfig.Enabled {
			return nil
		}
		original := repository.KnowledgeBuildInputProofFromContext(ctx)
		if original == nil || original.KnowledgeID != id || original.TenantID != k.TenantID || original.KnowledgeBaseID != k.KnowledgeBaseID {
			return types.ErrSourceLineageUnknown
		}
		root := *original
		root.BodyDigest = ""
		root.ChunkIDs = nil
		root.Projection = ""
		ctx = repository.WithKnowledgeBuildInputProof(ctx, store, &root)
		if err := store.RecheckKnowledgeBuildInputs(ctx, &root); err != nil {
			return err
		}
		attempt := s.tracker().LatestAttempt(ctx, id)
		if !s.enqueueAutoTagTask(ctx, types.KnowledgePostProcessPayload{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: id, Language: types.LanguageFromContextOrDefault(ctx)}, attempt) {
			return types.ErrSourceLineageUnknown
		}
		return nil
	})
}
