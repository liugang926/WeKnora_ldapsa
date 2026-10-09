package tools

import (
	"context"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type nextcloudAgentReadProvider interface {
	BeginNextcloudRead(context.Context, types.SearchTargets) (*readlease.NextcloudReadGuard, error)
	CheckNextcloudReadTargets(context.Context, types.SearchTargets) error
}

// beginAgentRead executes before a tool loads index, metadata, chunks, or
// images. Alternate test services may omit the capability, but source-backed
// rows are still refused at the final PinKnowledge checkpoint.
func beginAgentRead(ctx context.Context, service interfaces.KnowledgeService,
	targets types.SearchTargets,
) (context.Context, *readlease.NextcloudReadGuard, error) {
	provider, ok := service.(nextcloudAgentReadProvider)
	if !ok {
		return ctx, nil, nil
	}
	guard, err := provider.BeginNextcloudRead(ctx, targets)
	if err != nil {
		return ctx, nil, fmt.Errorf("protect Agent content read: %w", err)
	}
	return guard.Context(), guard, nil
}

// finishAgentRead checks the exact generation, current KB/share/group grants,
// and live Nextcloud source access just before a tool result reaches the model.
func finishAgentRead(ctx context.Context, service interfaces.KnowledgeService,
	targets types.SearchTargets, guard *readlease.NextcloudReadGuard, documents []*types.Knowledge,
) error {
	provider, ok := service.(nextcloudAgentReadProvider)
	if ok {
		if err := provider.CheckNextcloudReadTargets(ctx, targets); err != nil {
			return err
		}
	}
	for _, document := range documents {
		if document == nil {
			return fmt.Errorf("document is unavailable")
		}
		if err := guard.PinKnowledge(document); err != nil {
			return err
		}
		if err := checkKnowledgePublication(ctx, document, service); err != nil {
			return err
		}
	}
	return guard.Verify()
}
