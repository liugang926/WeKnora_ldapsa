package service

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"

	"github.com/Tencent/WeKnora/internal/application/repository"
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
	if err := s.knowledgeInputs.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return ctx, err
	}
	return repository.WithKnowledgeBuildInputProof(ctx, s.knowledgeInputs, proof), nil
}

func requireActualKnowledgeBuildInput(ctx context.Context) error {
	if err := repository.CheckNextcloudBuildLease(ctx); err != nil {
		return err
	}
	if err := repository.CheckKnowledgeBuildInputs(ctx); err != nil {
		return fmt.Errorf("original producer input: %w", err)
	}
	return nil
}
