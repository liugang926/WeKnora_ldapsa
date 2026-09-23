package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestRevalidateAgentExecutionChecksAgentAndKnowledgeBases(t *testing.T) {
	access := &tagTargetGroupAccessService{allowed: map[string]bool{
		"agent-1": true,
		"kb-1":    false,
	}}
	svc := &sessionService{groupAccess: access}
	agent := &types.CustomAgent{ID: "agent-1", TenantID: 7}
	targets := types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		KnowledgeBaseID: "kb-1",
		TenantID:        7,
	}}

	err := svc.revalidateAgentExecution(context.Background(), agent, targets)

	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.Equal(t, []string{"agent-1", "kb-1"}, access.calls)
}

func TestRevalidateAgentExecutionStopsWhenAgentGrantRevoked(t *testing.T) {
	access := &tagTargetGroupAccessService{allowed: map[string]bool{"kb-1": true}}
	svc := &sessionService{groupAccess: access}

	err := svc.revalidateAgentExecution(
		context.Background(),
		&types.CustomAgent{ID: "agent-1", TenantID: 7},
		types.SearchTargets{{KnowledgeBaseID: "kb-1", TenantID: 7}},
	)

	require.ErrorIs(t, err, ErrResourceAccessDenied)
	require.Equal(t, []string{"agent-1"}, access.calls)
}
