package service

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/searchutil"
	"github.com/Tencent/WeKnora/internal/types"
)

func readOriginalUploadArtifact(file *multipart.FileHeader) ([]byte, error) {
	reader, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func (s *knowledgeService) createAdmittedKnowledge(ctx context.Context, k *types.Knowledge, input any) (context.Context, error) {
	if s.knowledgeInputWriter == nil {
		return ctx, s.repo.CreateKnowledge(ctx, k)
	}
	marked, err := isNextcloudKnowledge(k)
	if err != nil {
		return ctx, err
	}
	if marked {
		return ctx, s.repo.CreateKnowledge(ctx, k)
	} // admitted by exact staged worker, never as independent
	proof, err := s.knowledgeInputWriter.CreateKnowledgeWithInput(ctx, k, input)
	if err != nil {
		return ctx, err
	}
	if s.knowledgeInputs == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	return repository.WithKnowledgeBuildInputProof(ctx, s.knowledgeInputs, proof), nil
}

func (s *knowledgeService) resumeKnowledgeInput(ctx context.Context, proof *types.KnowledgeBuildInputProof, input any) (context.Context, error) {
	if s.knowledgeInputs == nil {
		return ctx, nil
	}
	if proof == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	if err := repository.KnowledgeInputArtifactMatches(proof, input); err != nil {
		return ctx, err
	}
	var actorErr error
	ctx, actorErr = s.restoreKnowledgeTaskActor(ctx, proof)
	if actorErr != nil {
		return ctx, actorErr
	}
	if err := s.knowledgeInputs.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return ctx, err
	}
	return repository.WithKnowledgeBuildInputProof(ctx, s.knowledgeInputs, proof), nil
}

func (s *knowledgeService) restoreKnowledgeTaskActor(ctx context.Context, proof *types.KnowledgeBuildInputProof) (context.Context, error) {
	current, err := repository.RestoreKnowledgeBuildActorIfRequired(ctx, s.knowledgeInputs, proof)
	if err != nil {
		return ctx, err
	}
	if s.historyMaterialMessages != nil && s.historyMaterialMessages.materialStore != nil && current.Value(historyMaterialGuardContextKey{}) == nil {
		request, err := s.historyMaterialMessages.materialStore.KnowledgeHistoryMaterialRequest(current, proof)
		if err != nil {
			return ctx, err
		}
		if request != nil {
			guard, err := s.historyMaterialMessages.beginMessageMaterialRead(current, *request)
			if err != nil {
				return ctx, err
			}
			current = context.WithValue(guard.Context(), historyMaterialGuardContextKey{}, guard)
		}
	}
	return current, nil
}

func requireActualKnowledgeBuildInput(ctx context.Context) error {
	if err := repository.CheckNextcloudBuildLease(ctx); err != nil {
		return err
	}
	if err := repository.CheckKnowledgeProducerInput(ctx); err != nil {
		return err
	}
	if err := repository.CheckKnowledgeBuildInputs(ctx); err != nil {
		return fmt.Errorf("original producer input: %w", err)
	}
	return nil
}

func (s *knowledgeService) resumeKnowledgeTaskInput(ctx context.Context, proof *types.KnowledgeBuildInputProof) (context.Context, error) {
	if s.knowledgeInputs == nil {
		return ctx, nil
	}
	if proof == nil || proof.BodyDigest == "" || (proof.Projection != "" && proof.Projection != types.KnowledgeBuildProjectionEnrichment) {
		return ctx, types.ErrSourceLineageUnknown
	}
	var actorErr error
	ctx, actorErr = s.restoreKnowledgeTaskActor(ctx, proof)
	if actorErr != nil {
		return ctx, actorErr
	}
	if err := s.knowledgeInputs.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return ctx, err
	}
	return repository.WithKnowledgeBuildInputProof(ctx, s.knowledgeInputs, proof), nil
}

// Actual input capture preserves the enqueued complete manifest. Callers may
// select rows from it, but cannot authenticate a cached row or change its IDs.
func (s *knowledgeService) captureKnowledgeTaskManifest(ctx context.Context) (*types.KnowledgeBuildInputSnapshot, error) {
	if s.knowledgeInputs == nil {
		return nil, nil
	}
	proof := repository.KnowledgeBuildInputProofFromContext(ctx)
	if proof == nil || proof.BodyDigest == "" || (proof.Projection != "" && proof.Projection != types.KnowledgeBuildProjectionEnrichment) {
		return nil, types.ErrSourceLineageUnknown
	}
	return s.knowledgeInputs.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID, KnowledgeID: proof.KnowledgeID, ChunkIDs: proof.ChunkIDs, Expected: proof})
}

func actualManifestImageInfo(snapshot *types.KnowledgeBuildInputSnapshot, parentIDs []string) map[string]string {
	wanted := map[string]bool{}
	for _, id := range parentIDs {
		wanted[id] = true
	}
	result := map[string]string{}
	if snapshot == nil {
		return result
	}
	for _, c := range snapshot.Chunks {
		if !c.IsEnabled || c.ImageInfo == "" {
			continue
		}
		if c.ChunkType != types.ChunkTypeImageCaption && c.ChunkType != types.ChunkTypeImageOCR {
			continue
		}
		if !wanted[c.ParentChunkID] {
			continue
		}
		// MergeImageInfoJSON parses and deduplicates the actual captured child
		// payloads. It does not query uncaptured or current image rows.
		result[c.ParentChunkID] = searchutil.MergeImageInfoJSON(map[string]string{"previous": result[c.ParentChunkID], c.ID: c.ImageInfo})
	}
	return result
}
