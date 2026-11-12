package service

import (
	"context"
	"fmt"
	"io"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// prepareDingTalkReparseInput runs after the caller's normal write authorization
// and before opening an attempt or removing any existing output. It reuses the
// admitted source bytes; it never substitutes the original actor for the caller.
func (s *knowledgeService) prepareDingTalkReparseInput(
	ctx context.Context,
	k *types.Knowledge,
	kb *types.KnowledgeBase,
	overrides *types.KnowledgeProcessOverrides) (
	*types.KnowledgeBuildInputProof,
	error,
) {
	if k.Channel != types.ConnectorTypeDingTalk {
		return nil, nil
	}
	store, ok := s.knowledgeInputs.(interface {
		LoadKnowledgeOriginalInput(context.Context, uint64, string, string) (*types.KnowledgeBuildInputProof, error)
	})
	if !ok || k.FilePath == "" {
		return nil, fmt.Errorf("钉钉文档缺少可验证的原文记录，无法重新解析: %w", types.ErrSourceLineageUnknown)
	}
	proof, err := store.LoadKnowledgeOriginalInput(ctx, k.TenantID, k.KnowledgeBaseID, k.ID)
	if err != nil {
		return nil, fmt.Errorf("读取钉钉原文记录失败，已保留现有内容: %w", err)
	}
	if err := s.knowledgeInputs.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return nil, fmt.Errorf("钉钉来源记录已失效或变更，已保留现有内容: %w", err)
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
		return nil, fmt.Errorf("钉钉原文对应的处理配置已变更，请先同步来源后重新解析: %w", err)
	}
	reader, err := s.resolveFileServiceForPath(ctx, kb, k.FilePath).GetFile(ctx, k.FilePath)
	if err != nil {
		return nil, fmt.Errorf("读取钉钉原文失败，已保留现有内容: %w", err)
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
		return nil, fmt.Errorf("钉钉原文与来源记录不一致，已保留现有内容: %w", err)
	}
	return proof, nil
}

// admitDingTalkLocalReparseInput runs after the caller's normal write authorization
// and before opening an attempt or removing output. It binds the retained local
// bytes and current controls without substituting another actor.
func (s *knowledgeService) admitDingTalkLocalReparseInput(
	ctx context.Context,
	k *types.Knowledge,
	kb *types.KnowledgeBase,
	overrides *types.KnowledgeProcessOverrides) (
	*types.KnowledgeBuildInputProof,
	error,
) {
	if k.Channel != types.ConnectorTypeDingTalk {
		return nil, nil
	}
	store, ok := s.knowledgeInputs.(interface {
		AdmitDingTalkLocalReparse(context.Context,
			*types.Knowledge,
			*types.Knowledge,
			*types.KnowledgeBase,
			[]byte) (*types.KnowledgeBuildInputProof,
			error)
	})
	if !ok || k.FilePath == "" {
		return nil, fmt.Errorf("钉钉文档缺少本地原文，无法重新解析: %w", types.ErrSourceLineageUnknown)
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
	reader, err := s.resolveFileServiceForPath(ctx, kb, k.FilePath).GetFile(ctx, k.FilePath)
	if err != nil {
		return nil, fmt.Errorf("读取钉钉原文失败，已保留现有内容: %w", err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	proof, err := store.AdmitDingTalkLocalReparse(ctx, k, &proposed, kb, data)
	if err != nil {
		return nil, fmt.Errorf("本地原文或解析配置校验失败，已保留现有内容: %w", err)
	}
	*k = proposed
	return proof, nil
}
