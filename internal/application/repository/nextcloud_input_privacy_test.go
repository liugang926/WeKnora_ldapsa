package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudBuildContextsSuppressContentWithoutGrantingAccess(t *testing.T) {
	scope := NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc"}
	ctx := WithNextcloudBuildLease(context.Background(), nil, NextcloudContentLease{}, scope)
	require.True(t, logprivacy.ContentSuppressed(ctx))
	require.True(t, logprivacy.ContentSuppressed(context.WithoutCancel(ctx)))
	require.ErrorIs(t, CheckNextcloudBuildIdentity(ctx, 7, "kb", "doc"), ErrNextcloudContentLeaseInvalid)

	for _, item := range []struct {
		name       string
		lineage    *types.SourceLineage
		suppressed bool
	}{
		{"ordinary", &types.SourceLineage{
			State:   types.SourceLineageComplete,
			Sources: []types.SourceIdentity{},
		}, false},
		{"protected", &types.SourceLineage{State: types.SourceLineageComplete, Sources: []types.SourceIdentity{{
			Provider: types.ConnectorTypeNextcloud,
		}}}, true},
		{"unknown", types.UnknownSourceLineage(), true},
	} {
		t.Run(item.name, func(t *testing.T) {
			proof := &types.KnowledgeBuildInputProof{SourceLineage: item.lineage}
			guarded := WithKnowledgeBuildInputProof(context.Background(), nil, proof)
			require.Equal(t, item.suppressed, logprivacy.ContentSuppressed(guarded))
			require.ErrorIs(t, CheckKnowledgeBuildInputs(guarded), types.ErrSourceLineageUnknown)
		})
	}
	marked := WithKnowledgeBuildInputProof(logprivacy.SuppressContent(context.Background()), nil, nil)
	require.True(t, logprivacy.ContentSuppressed(marked))
	canceled, cancel := context.WithCancel(context.Background())
	guarded := WithKnowledgeBuildInputProof(canceled, nil, nil)
	cancel()
	require.ErrorIs(t, CheckKnowledgeBuildInputs(guarded), context.Canceled)
}
