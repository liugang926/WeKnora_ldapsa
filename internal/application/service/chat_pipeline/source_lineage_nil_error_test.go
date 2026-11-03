package chatpipeline

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// TestSourceLineagePluginErrorNilStaysNil locks the r27 root-cause fix:
// callers pass turn.Check results directly, and a passing check (nil error)
// used to come back as a non-nil PluginError{Err:nil}. The stage loop treats
// that as failure yet returns the nil Err, silently ending the pipeline with
// no answer and an eternally incomplete assistant message (production
// signature: query_understand tool_result success=false with an empty error,
// then silence).
func TestSourceLineagePluginErrorNilStaysNil(t *testing.T) {
	require.Nil(t, sourceLineagePluginError(nil))

	authorize := func(context.Context, *types.SourceLineage) error { return nil }
	turn := types.NewWeKnoraACLSourceLineage(authorize)
	ctx := types.WithRAGSourceLineage(context.Background(), turn)
	require.NoError(t, turn.Check(ctx))
	require.Nil(t, sourceLineagePluginError(turn.Check(ctx)))
}
