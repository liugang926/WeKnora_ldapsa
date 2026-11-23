package service

import (
	"context"
	"fmt"
	"io"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// WebDAV imports already carry an ordinary derived admission. Reuse it under
// the caller's write authorization without a remote fetch or a new admission.
func (s *knowledgeService) prepareWebDAVReparseInput(
	ctx context.Context, k *types.Knowledge, kb *types.KnowledgeBase, overrides *types.KnowledgeProcessOverrides,
) (*types.KnowledgeBuildInputProof, error) {
	store, ok := s.knowledgeInputs.(interface {
		LoadWebDAVReparseInput(context.Context, *types.Knowledge) (*types.KnowledgeBuildInputProof, error)
	})
	if !ok || k.FilePath == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	proof, err := store.LoadWebDAVReparseInput(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("读取 WebDAV 原文来源记录失败: %w", err)
	}
	// Check that the worker can still act on the admitted input before
	// deleting the previous result. The caller retains their own identity.
	if _, err := repository.RestoreKnowledgeBuildActorIfRequired(ctx, s.knowledgeInputs, proof); err != nil {
		return nil, fmt.Errorf("WebDAV 原文入库身份已失效，已保留现有内容: %w", err)
	}
	return s.verifyRetainedReparseInput(ctx, k, kb, overrides, proof, "WebDAV")
}

func (s *knowledgeService) verifyRetainedReparseInput(
	ctx context.Context, k *types.Knowledge, kb *types.KnowledgeBase, overrides *types.KnowledgeProcessOverrides,
	proof *types.KnowledgeBuildInputProof, source string,
) (*types.KnowledgeBuildInputProof, error) {
	if err := s.knowledgeInputs.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return nil, fmt.Errorf("%s来源记录已失效或变更，已保留现有内容: %w", source, err)
	}
	proposed := *k
	if overrides != nil {
		if err := ValidateProcessOverrides(ctx, kb, overrides, reparseFileTypes(k)); err != nil {
			return nil, err
		}
		if err := proposed.SetProcessOverrides(overrides); err != nil {
			return nil, err
		}
	}
	if err := repository.CheckKnowledgeOriginalConfiguration(proof, kb, &proposed); err != nil {
		return nil, fmt.Errorf("%s原文对应的处理配置已变更，请先同步来源后重新解析: %w", source, err)
	}
	reader, err := s.resolveFileServiceForPath(ctx, kb, k.FilePath).GetFile(ctx, k.FilePath)
	if err != nil {
		return nil, fmt.Errorf("读取%s原文失败，已保留现有内容: %w", source, err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := repository.KnowledgeInputArtifactMatches(proof, data); err != nil {
		return nil, fmt.Errorf("%s原文与来源记录不一致，已保留现有内容: %w", source, err)
	}
	return proof, nil
}
