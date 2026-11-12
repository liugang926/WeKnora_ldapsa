package chatpipeline

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func malformedNextcloudResult() *types.SearchResult {
	return &types.SearchResult{
		ID:              "chunk",
		KnowledgeID:     "knowledge",
		KnowledgeBaseID: "kb",
		Content:         "legacy metadata",
		SourceLineage: &types.SourceLineage{
			Version: types.SourceLineageVersion,
			State:   types.SourceLineageComplete,
			Sources: []types.SourceIdentity{{
				Provider:        types.ConnectorTypeNextcloud,
				TenantID:        7,
				KnowledgeBaseID: "kb",
			}},
		},
	}
}

func validNextcloudResult() *types.SearchResult {
	return &types.SearchResult{
		ID:              "valid-chunk",
		KnowledgeID:     "valid-knowledge",
		KnowledgeBaseID: "kb",
		Content:         "validated metadata",
		SourceLineage: &types.SourceLineage{
			Version: types.SourceLineageVersion,
			State:   types.SourceLineageComplete,
			Sources: []types.SourceIdentity{{
				Provider:        types.ConnectorTypeNextcloud,
				TenantID:        7,
				KnowledgeBaseID: "kb",
				DataSourceID:    "ds",
				PairOperationID: "pair",
				InstanceID:      "instance",
				BindingID:       "binding",
				FileID:          "1",
				ExternalID:      "nextcloud:instance:1",
				KnowledgeID:     "valid-knowledge",
				Revision:        2,
				ETag:            "etag",
			}},
		},
	}
}

func TestBeginSourceLineageUsesWeKnoraACLForOrdinaryRAG(t *testing.T) {
	manager := NewEventManager()
	scope := access.LineageReadScope{Targets: types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		TenantID:        7,
		KnowledgeBaseID: "kb",
	}}}
	aclCalls := 0
	lastSources := -1
	ctx, err := manager.BeginSourceLineage(
		context.Background(),
		&types.ChatManage{},
		scope,
		func(_ context.Context, lineage *types.SourceLineage, got access.LineageReadScope) error {
			aclCalls++
			require.Equal(t, scope.Targets, got.Targets)
			require.NoError(t, lineage.RequireComplete())
			lastSources = len(lineage.Sources)
			return nil
		},
	)
	require.NoError(t, err)
	require.NoError(t, AccountResults(ctx, []*types.SearchResult{malformedNextcloudResult()}))
	require.GreaterOrEqual(t, aclCalls, 1)
	require.Zero(t, lastSources)

	require.NoError(t, AccountResults(ctx, []*types.SearchResult{validNextcloudResult()}))
	require.Equal(t, 1, lastSources)

	snapshot, err := types.RAGSourceLineageFromContext(ctx).CheckedSnapshot(ctx)
	require.NoError(t, err)
	require.NoError(t, snapshot.RequireComplete())
	require.Len(t, snapshot.Sources, 1)
	require.Equal(t, "valid-knowledge", snapshot.Sources[0].KnowledgeID)
}

func TestBeginSourceLineageKeepsExplicitInputMaterialsStrict(t *testing.T) {
	manager := NewEventManager()
	ctx, err := manager.BeginSourceLineage(
		context.Background(),
		&types.ChatManage{PipelineRequest: types.PipelineRequest{
			Images: []string{"resource://image"},
		}},
		access.LineageReadScope{},
		func(context.Context, *types.SourceLineage, access.LineageReadScope) error {
			t.Fatal("ACL-only authorization must not handle explicit materials")
			return nil
		},
	)
	require.NoError(t, err)
	// A valid retrieval must not clear the unaccounted image before model input.
	err = AccountResults(ctx, []*types.SearchResult{validNextcloudResult()})
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
}

func TestBeginSourceLineageACLFailureStaysFailClosed(t *testing.T) {
	manager := NewEventManager()
	ctx, err := manager.BeginSourceLineage(
		context.Background(),
		&types.ChatManage{},
		access.LineageReadScope{},
		func(context.Context, *types.SourceLineage, access.LineageReadScope) error {
			return access.ErrNextcloudPublicationDenied
		},
	)
	require.NoError(t, err)
	err = AccountResults(ctx, []*types.SearchResult{validNextcloudResult()})
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
}
