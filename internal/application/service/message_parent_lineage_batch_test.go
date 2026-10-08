package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type parentBatchPolicy struct{ consumerBatchReviewPolicy }

func (p *parentBatchPolicy) CheckBatch(ctx context.Context, reads []access.SourceLineageRead) error {
	p.batches++
	union, _ := types.NewCompleteSourceLineage()
	for _, read := range reads {
		if err := p.Check(ctx, read.Lineage, read.Scope); err != nil {
			return err
		}
		union, _ = types.UnionSourceLineage(union, read.Lineage)
	}
	return p.Check(ctx, union, access.LineageReadScope{})
}

func TestMessageParentLineageBatchIncludesRawParentAndOriginalAgentScopes(t *testing.T) {
	policy := &parentBatchPolicy{}
	svc := &messageService{sourceLineageGuard: policy}
	var verifier interfaces.MessageSourceLineageBatchVerifier = svc
	parents := []*types.Message{
		{Role: "assistant", AgentID: "agent-a", AgentTenantID: 7, SourceLineage: consumerBatchReviewProof(t, "1")},
		{Role: "user", AgentID: "agent-b", AgentTenantID: 7, SourceLineage: consumerBatchReviewProof(t, "2")},
	}
	// The raw parent's source callback commits a change to an earlier source.
	// The API must include that proof and perform the final batch capability.
	err := verifier.CheckMessagePublications(context.Background(), parents)
	require.True(t, policy.sawB)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.Equal(t, 1, policy.batches)
	require.Contains(t, policy.scopes, "agent-a")
	require.Contains(t, policy.scopes, "agent-b")
}

func TestMessageParentLineageBatchNeverUsesRawUserLegacyExemption(t *testing.T) {
	empty, _ := types.NewCompleteSourceLineage()
	svc := &messageService{sourceLineageGuard: &consumerBatchReviewPolicy{}}
	require.ErrorIs(t, svc.CheckMessagePublications(context.Background(), []*types.Message{
		{Role: "assistant", SourceLineage: empty}, {Role: "user"},
	}), access.ErrNextcloudPublicationDenied)
	require.ErrorIs(t, svc.CheckMessagePublications(context.Background(), nil), access.ErrNextcloudPublicationDenied)
	svc.sourceLineageGuard = nil
	require.ErrorIs(t, svc.CheckMessagePublications(context.Background(), []*types.Message{{Role: "assistant", SourceLineage: empty}}), access.ErrNextcloudPublicationUnavailable)
}
