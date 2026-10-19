package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// Explicit component-only reader/checker fakes exercise the no-union property;
// production authorizers always use actual SQL receipt and current actor stores.
type tagOutputComponentReader struct{ snapshot *types.TagOutputInputSnapshot }

func (f tagOutputComponentReader) CaptureTagOutputInputs(
	context.Context,
	types.TagOutputReadRequest,
) (*types.TagOutputInputSnapshot, error) {
	return cloneCollection(f.snapshot)
}

func (f tagOutputComponentReader) SelectTagOutputInputs(
	context.Context,
	*types.TagOutputInputSnapshot,
	types.TagOutputSelectionRequest,
) (*types.TagOutputSelection, error) {
	return &types.TagOutputSelection{Tags: []*types.KnowledgeTagWithStats{}, Total: 10000}, nil
}

func (f tagOutputComponentReader) RecheckTagOutputInputs(
	context.Context,
	*types.TagOutputInputSnapshot,
	*types.TagOutputSelection,
) error {
	return nil
}

func TestTagOutputTenThousandOriginalProducersDoNotFormWholeKBUnion(t *testing.T) {
	snapshot := &types.TagOutputInputSnapshot{Request: types.TagOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb"}}
	for i := 0; i < 10000; i++ {
		id := fmt.Sprint(i + 1)
		source := types.SourceIdentity{
			Provider:        types.ConnectorTypeNextcloud,
			TenantID:        7,
			KnowledgeBaseID: "kb",
			DataSourceID:    "ds",
			PairOperationID: "pair",
			InstanceID:      "instance",
			BindingID:       "binding",
			FileID:          id,
			ExternalID:      "nextcloud:instance:" + id,
			KnowledgeID:     "knowledge-" + id,
			Revision:        1,
			ETag:            "original",
		}
		lineage, e := types.NewCompleteSourceLineage(source)
		require.NoError(t, e)
		snapshot.TagProofs = append(
			snapshot.TagProofs,
			&types.KnowledgeCollectionAuxiliaryProof{
				Kind:            "tag-name",
				ID:              id,
				TenantID:        7,
				KnowledgeBaseID: "kb",
				SourceLineage:   lineage,
			},
		)
	}
	count := 0
	a := &TagOutputAuthorizer{
		reader: tagOutputComponentReader{snapshot: snapshot},
		checker: outputCheckerFunc(
			func(_ context.Context, l *types.SourceLineage, scope access.LineageReadScope) error {
				require.NoError(t, l.RequireComplete())
				require.LessOrEqual(t, len(l.Sources), 1)
				count++
				return nil
			},
		),
	}
	guard, e := a.Begin(context.Background(), snapshot.Request)
	require.NoError(t, e)
	require.NotNil(t, guard)
	require.Equal(t, 20002, count)
}
