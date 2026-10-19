package tools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

// This double tests only the tool port and unchanged formatter. Actual native
// original-body/lease/owner and result-projection coverage lives in service.
type conversationSearchFixture struct {
	interfaces.MessageService
	result      *types.MessageSearchResult
	params      *types.MessageSearchParams
	legacyCalls int
}

func (f *conversationSearchFixture) SearchMessages(_ context.Context,
	params *types.MessageSearchParams,
) (*types.MessageSearchResult, error) {
	f.legacyCalls++
	copied := *params
	f.params = &copied
	return f.result, nil
}

type conversationInputFixture struct {
	*conversationSearchFixture
	lineage    *types.SourceLineage
	inputCalls int
	err        error
	after      func()
}

func (f *conversationInputFixture) SearchConversationsWithInputs(_ context.Context,
	params *types.MessageSearchParams,
) (*types.MessageSearchResult, *types.SourceLineage, error) {
	f.inputCalls++
	copied2 := *params
	f.params = &copied2
	if f.after != nil {
		f.after()
	}
	return f.result, f.lineage, f.err
}

func conversationToolFixtureResult() *types.MessageSearchResult {
	return &types.MessageSearchResult{Items: []*types.MessageSearchGroupItem{
		{SessionID: "this-session", SessionTitle: "excluded current", QueryContent: "current"},
		{
			SessionID: "previous", SessionTitle: "owned <history>", QueryContent: "actual <query>",
			AnswerContent: "actual & answer", CreatedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		},
	}}
}

func TestSearchConversationsTypedPortPreservesOrdinaryProjectionAndLimits(t *testing.T) {
	for _, args := range []string{`{"query":"  previous config  ","limit":500}`, `{"query":"previous config"}`} {
		legacy := &conversationSearchFixture{result: conversationToolFixtureResult()}
		old, err := NewSearchConversationsTool(
			legacy,
			"owner",
			"this-session",
		).Execute(t.Context(), json.RawMessage(args))
		require.NoError(t, err)
		empty, err := types.NewCompleteSourceLineage()
		require.NoError(t, err)
		actual := &conversationInputFixture{conversationSearchFixture: &conversationSearchFixture{
			result: conversationToolFixtureResult(),
		}, lineage: empty}
		tool := NewSearchConversationsTool(actual, "owner", "this-session")
		registry := NewToolRegistry()
		registry.RegisterTool(tool)
		_, ctx := toolTurn()
		got, err := registry.ExecuteToolWithSourceLineage(ctx, ToolSearchConversations, json.RawMessage(args))
		require.NoError(t, err)
		require.True(t, got.Success)
		require.Equal(t, old.Output, got.Output)
		require.Equal(t, old.Data, got.Data)
		require.Equal(t, legacy.params, actual.params)
		require.Equal(t, "owner", actual.params.OwnerID)
		require.Equal(t, types.MessageSearchModeHybrid, actual.params.Mode)
		require.Equal(t, "previous config", actual.params.Query)
		require.Zero(t, actual.legacyCalls)
		require.Equal(t, 1, actual.inputCalls)
		require.NoError(t, got.SourceLineage.RequireComplete())
		require.NotContains(t, got.Output, "excluded current")
		require.Contains(t, got.Output, "owned &lt;history&gt;")
		require.Contains(t, got.Output, "actual &amp; answer")
	}
}

func TestSearchConversationsTypedPortCannotCertifyLegacySearchOrUnknownInputs(t *testing.T) {
	legacy := &conversationSearchFixture{result: conversationToolFixtureResult()}
	_, ctx := toolTurn()
	got, err := NewSearchConversationsTool(legacy, "owner", "this-session").ExecuteSourceLineage(ctx,
		json.RawMessage(`{"query":"previous"}`))
	require.ErrorIs(t, err, ErrToolSourceLineageUnavailable)
	require.Nil(t, got)
	require.Zero(t, legacy.legacyCalls)
	for _, lineage := range []*types.SourceLineage{nil, types.UnknownSourceLineage()} {
		actual := &conversationInputFixture{conversationSearchFixture: &conversationSearchFixture{
			result: conversationToolFixtureResult(),
		}, lineage: lineage}
		_, ctx := toolTurn()
		got, err := NewSearchConversationsTool(actual, "owner", "this-session").ExecuteSourceLineage(ctx,
			json.RawMessage(`{"query":"previous"}`))
		require.Error(t, err)
		require.Nil(t, got)
		require.Equal(t, 1, actual.inputCalls)
		require.Error(t, types.RAGSourceLineageFromContext(ctx).Snapshot().RequireComplete())
	}
}

func TestSearchConversationsTypedPortKeepsActualReadUnionAndRechecksBeforeReturn(t *testing.T) {
	source := types.SourceIdentity{
		Provider: "nextcloud", TenantID: 7, KnowledgeBaseID: "history-kb",
		DataSourceID: "history-source", PairOperationID: "pair", InstanceID: "instance", BindingID: "binding",
		FileID: "77", ExternalID: "nextcloud:instance:77", KnowledgeID: "original", Revision: 1, ETag: "original-etag",
	}
	lineage, err := types.NewCompleteSourceLineage(source)
	require.NoError(t, err)
	for _, mutation := range []string{"ordinary", "denied before read", "revoked during read", "native refusal"} {
		t.Run(mutation, func(t *testing.T) {
			denied := false
			turn := types.NewRAGSourceLineage(func(_ context.Context, current *types.SourceLineage) error {
				if denied && len(current.Sources) > 0 {
					return errors.New("source read revoked")
				}
				return current.RequireComplete()
			})
			ctx := types.WithRAGSourceLineage(t.Context(), turn)
			actual := &conversationInputFixture{conversationSearchFixture: &conversationSearchFixture{
				result: &types.MessageSearchResult{},
			}, lineage: lineage}
			if mutation == "denied before read" {
				require.NoError(t, turn.Add(lineage))
				denied = true
			}
			if mutation == "revoked during read" {
				actual.after = func() { denied = true }
			}
			if mutation == "native refusal" {
				actual.err = errors.New("original current owner guard refused")
			}
			got, err := NewSearchConversationsTool(actual, "owner", "this-session").ExecuteSourceLineage(ctx,
				json.RawMessage(`{"query":"previous"}`))
			if mutation == "ordinary" {
				require.NoError(t, err)
				require.True(t, got.Success)
				require.Equal(t, lineage, got.SourceLineage)
				require.Equal(t, lineage, turn.Snapshot(), "empty result preserves admitted source reads")
			} else {
				require.Error(t, err)
				require.Nil(t, got)
			}
			if mutation == "denied before read" {
				require.Zero(t, actual.inputCalls)
			}
		})
	}
}

func TestSearchConversationsTypedLocalDiagnosticKeepsAuthorizedPrefixWithoutReading(t *testing.T) {
	actual := &conversationInputFixture{conversationSearchFixture: &conversationSearchFixture{}}
	tool := NewSearchConversationsTool(actual, "owner", "this-session")
	turn, ctx := toolTurn()
	prefix, err := types.NewCompleteSourceLineage(types.SourceIdentity{
		Provider: "nextcloud", TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds", PairOperationID: "pair",
		InstanceID: "instance", BindingID: "binding", FileID: "77", ExternalID: "nextcloud:instance:77",
		KnowledgeID: "original", Revision: 1, ETag: "original-etag",
	})
	require.NoError(t, err)
	require.NoError(t, turn.Add(prefix))
	for _, args := range []string{`{"query":"  "}`, `{"query":`} {
		result, err := tool.ExecuteSourceLineage(ctx, json.RawMessage(args))
		require.Error(t, err)
		require.NotNil(t, result)
		require.False(t, result.Success)
		require.NoError(t, result.SourceLineage.RequireComplete())
		require.NoError(t, turn.Snapshot().RequireComplete())
		require.Equal(t, prefix, turn.Snapshot())
		require.Zero(t, actual.inputCalls)
		require.Zero(t, actual.legacyCalls)
	}
}
