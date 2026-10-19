package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type consumerBatchReviewPolicy struct {
	revokedA bool
	sawB     bool
	batches  int
	scopes   []string
}

func (p *consumerBatchReviewPolicy) Check(
	_ context.Context,
	lineage *types.SourceLineage,
	scope access.LineageReadScope,
) error {
	p.scopes = append(p.scopes, scope.AgentID)
	if lineage.RequireComplete() != nil {
		return access.ErrNextcloudPublicationDenied
	}
	for _, source := range lineage.Sources {
		if source.FileID == "1" && p.revokedA {
			return access.ErrNextcloudPublicationDenied
		}
	}
	for _, source := range lineage.Sources {
		if source.FileID == "2" {
			p.sawB = true
			p.revokedA = true
		}
	}
	return nil
}

func (p *consumerBatchReviewPolicy) CheckBatch(ctx context.Context, reads []access.SourceLineageRead) error {
	p.batches++
	for _, read := range reads {
		if err := p.Check(ctx, read.Lineage, read.Scope); err != nil {
			return err
		}
	}
	return nil
}

func consumerBatchReviewProof(t *testing.T, file string) *types.SourceLineage {
	proof := ragEntryProof(t)
	proof.Sources[0].FileID = file
	proof.Sources[0].ExternalID = "nextcloud:instance:" + file
	proof.Sources[0].KnowledgeID = "knowledge-" + file
	require.NoError(t, proof.Validate())
	return proof
}

func TestLineageConsumerReviewSingleTitleFinalUnionRejectsEarlierRevocation(t *testing.T) {
	policy := &consumerBatchReviewPolicy{}
	svc := &sessionService{sourceLineageGuard: policy}
	row := &types.Session{
		Title:                    "secret A",
		Description:              "secret B",
		TitleSourceLineage:       consumerBatchReviewProof(t, "1"),
		DescriptionSourceLineage: consumerBatchReviewProof(t, "2"),
	}
	got, err := svc.authorizeSessionText(context.Background(), row)
	require.True(t, policy.sawB, "mutation callback did not run")
	require.True(t, policy.revokedA)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.Nil(t, got)
	require.Greater(t, policy.batches, 0)
}

func TestLineageConsumerReviewHistoryFinalBatchRetainsEachAgentScope(t *testing.T) {
	policy := &consumerBatchReviewPolicy{}
	svc := &messageService{sourceLineageGuard: policy}
	rows := []*types.Message{
		{
			Role:          "assistant",
			Content:       "secret A",
			AgentID:       "agent-a",
			AgentTenantID: 7,
			SourceLineage: consumerBatchReviewProof(t, "1"),
		},
		{
			Role:          "assistant",
			Content:       "secret B",
			AgentID:       "agent-b",
			AgentTenantID: 7,
			SourceLineage: consumerBatchReviewProof(t, "2"),
		},
	}
	got, err := svc.filterHistoryMessages(context.Background(), rows)
	require.True(t, policy.sawB)
	require.True(t, policy.revokedA)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.Nil(t, got)
	require.Greater(t, policy.batches, 0)
	require.Contains(t, policy.scopes, "agent-a")
	require.Contains(t, policy.scopes, "agent-b")
}

func TestLineageConsumerReviewSearchFinalBatchRejectsEarlierTitleRevocation(t *testing.T) {
	policy := &consumerBatchReviewPolicy{}
	svc := &messageService{sourceLineageGuard: policy}
	rows := []*types.MessageSearchResultItem{
		{
			MessageWithSession: types.MessageWithSession{
				Message:                   types.Message{Role: "user", Content: "own A"},
				SessionTitle:              "secret A",
				SessionTitleSourceLineage: consumerBatchReviewProof(t, "1"),
			},
		},
		{
			MessageWithSession: types.MessageWithSession{
				Message:                   types.Message{Role: "user", Content: "own B"},
				SessionTitle:              "secret B",
				SessionTitleSourceLineage: consumerBatchReviewProof(t, "2"),
			},
		},
	}
	got, err := svc.filterHistoricalSearchPairs(context.Background(), rows)
	require.True(t, policy.sawB)
	require.True(t, policy.revokedA)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	require.Nil(t, got)
	require.Greater(t, policy.batches, 0)
}
