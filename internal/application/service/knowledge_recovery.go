package service

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type dingTalkRecoveryStore interface {
	DingTalkRecoveryEligibility(context.Context, *types.Knowledge) (*types.KnowledgeRecoveryEligibility, error)
	FailDingTalkRecoveryHandoff(context.Context, *types.KnowledgeBuildInputProof) error
}

func (s *knowledgeService) checkRecoveryQueue(ctx context.Context, id string) error {
	scanner, ok := s.taskInspector.(interfaces.QueuedKnowledgeInspector)
	if !ok {
		return fmt.Errorf("无法确认任务队列状态，请稍后重试")
	}
	queued, err := scanner.QueuedKnowledgeIDs(ctx)
	if err != nil || queued == nil {
		return fmt.Errorf("无法确认任务队列状态，请稍后重试")
	}
	if _, found := queued[id]; found {
		return fmt.Errorf("文档存在执行中或等待重试的任务，请稍后重试")
	}
	return nil
}

func (s *knowledgeService) recoveryEligibility(
	ctx context.Context,
	k *types.Knowledge) (*types.KnowledgeRecoveryEligibility,
	error,
) {
	if k.Channel != types.ConnectorTypeDingTalk {
		return &types.KnowledgeRecoveryEligibility{Allowed: true, Action: "reparse"}, nil
	}
	store, ok := s.knowledgeInputs.(dingTalkRecoveryStore)
	if !ok {
		return nil, fmt.Errorf("文档恢复状态不可用")
	}
	r, err := store.DingTalkRecoveryEligibility(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("无法确认文档恢复状态，请稍后重试: %w", err)
	}
	if !r.Allowed {
		return r, nil
	}
	if err := s.checkRecoveryQueue(ctx, k.ID); err != nil {
		r.Allowed = false
		r.Action = "wait"
		r.Reason = err.Error()
	}
	return r, nil
}

// The same current editor check is used by eligibility and all reparse callers.
func (s *knowledgeService) KnowledgeRecoveryEligibility(
	ctx context.Context,
	id string) (*types.KnowledgeRecoveryEligibility,
	error,
) {
	k, _, err := loadKnowledgeWrite(ctx, s.repo, s.kbService, id)
	if err != nil {
		return nil, err
	}
	return s.recoveryEligibility(ctx, k)
}

func (s *knowledgeService) prepareRecoveryGate(ctx context.Context, k *types.Knowledge) (context.Context, error) {
	if k.Channel != types.ConnectorTypeDingTalk {
		return ctx, nil
	}
	r, err := s.recoveryEligibility(ctx, k)
	if err != nil {
		return ctx, err
	}
	if !r.Allowed {
		return ctx, fmt.Errorf("%s", r.Reason)
	}
	if expected := types.KnowledgeRecoverySnapshot(ctx); expected != "" && expected != r.Snapshot {
		return ctx, fmt.Errorf("文档状态已变化，请刷新后重试")
	}
	return repository.WithDingTalkRecoveryGate(
			ctx,
			r.Snapshot,
			func(checkCtx context.Context) error {
				return s.checkRecoveryQueue(checkCtx,
					k.ID)
			}),
		nil
}
