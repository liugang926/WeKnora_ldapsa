package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/types"
)

// Called only after LoadDingTalkWork has resolved the exact retired row. It
// does not remove that row or its receipt, and never adjusts storage accounting.
// Keep chunk metadata until every external deletion succeeds so a retry still
// knows the complete image and wiki reference set.
func (s *knowledgeService) cleanupDingTalkRetiredKnowledge(ctx context.Context, store *repository.KnowledgeInputProvenanceStore, intent repository.DingTalkWorkIntent, kb *types.KnowledgeBase, k *types.Knowledge) error {
	if !k.DeletedAt.Valid || k.Channel != types.ConnectorTypeDingTalk || kb.ID != k.KnowledgeBaseID || kb.TenantID != k.TenantID {
		return errors.New("DingTalk cleanup target is not an exact retired knowledge")
	}
	if err := s.revalidateBackgroundKBAccess(ctx, k.TenantID, k.KnowledgeBaseID, types.ResourceActionEdit); err != nil {
		return err
	}
	chunks, err := s.chunkRepo.ListAllChunksByKnowledgeID(ctx, k.TenantID, k.ID)
	if err != nil {
		return err
	}
	refs := map[string]bool{}
	images := map[string]bool{}
	for _, chunk := range chunks {
		if chunk == nil {
			continue
		}
		refs[chunk.ID] = true
		if chunk.ImageInfo == "" {
			continue
		}
		var infos []*types.ImageInfo
		if err := json.Unmarshal([]byte(chunk.ImageInfo), &infos); err != nil {
			return err
		}
		for _, image := range infos {
			if image != nil && image.URL != "" {
				images[image.URL] = true
			}
		}
	}
	if kb.IsWikiEnabled() {
		if err := s.cleanupWikiReferences(ctx, k, refs); err != nil {
			return err
		}
	}
	if k.EmbeddingModelID != "" {
		engine, err := retriever.CreateRetrieveEngineForKB(ctx, s.retrieveEngine, s.ownership, k.TenantID, kb.VectorStoreID)
		if err != nil {
			return err
		}
		model, err := s.modelService.GetEmbeddingModel(ctx, k.EmbeddingModelID)
		if err != nil {
			return err
		}
		if err := engine.DeleteByKnowledgeIDList(ctx, []string{k.ID}, model.GetDimensions(), k.Type); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.graphEngine != nil {
		if err := s.graphEngine.DelGraph(ctx, []types.NameSpace{{KnowledgeBase: k.KnowledgeBaseID, Knowledge: k.ID}}); err != nil {
			return err
		}
	} else if kb.IsGraphEnabled() {
		return errors.New("DingTalk graph cleanup backend unavailable")
	}
	if k.FilePath != "" {
		images[k.FilePath] = true
	}
	for ref := range images {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, catalogued := types.ParseResourcePath(ref); !catalogued {
			// Minimal file-only deployments have no shared resource registry.
			// Only the original private knowledge directory is a safe target.
			if ref != k.FilePath || !strings.Contains(ref, "/"+k.ID+"/") {
				return errors.New("DingTalk cleanup requires a registered shared resource")
			}
			if err := s.resolveFileServiceForPath(ctx, kb, ref).DeleteFile(ctx, ref); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			continue
		}
		resource, err := store.ClaimDingTalkCleanupResource(ctx, intent, ref)
		if err != nil {
			return err
		}
		if resource == nil {
			continue
		}
		fileSvc := s.resolveFileServiceForPath(ctx, kb, resource.PhysicalPath)
		if s.storageResolver != nil {
			tenant, err := s.tenantRepo.GetTenantByID(ctx, resource.TenantID)
			if err != nil {
				return err
			}
			fileSvc, _, err = s.storageResolver.ResolveFileService(ctx, tenant, resource.StorageBackendID, resource.Provider, strings.TrimSpace(os.Getenv("LOCAL_STORAGE_BASE_DIR")))
			if err != nil {
				return err
			}
		}
		if fileSvc == nil {
			return errors.New("DingTalk retired storage backend unavailable")
		}
		if err := fileSvc.DeleteFile(ctx, resource.PhysicalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := store.FinishDingTalkCleanupResource(ctx, intent, ref, resource); err != nil {
			return err
		}
	}
	if err := s.repo.DeleteKnowledgeTagRelations(ctx, k.ID); err != nil {
		return err
	}
	return s.chunkRepo.DeleteChunksByKnowledgeID(ctx, k.TenantID, k.ID)
}
