package access

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

func TestNextcloudRetiredHistoryPreservesSavedReference(t *testing.T) {
	_, ctx, knowledge := publicationFixture(t)
	message := &types.Message{
		Role: "assistant", Content: "saved source answer",
		KnowledgeReferences: types.References{
			{KnowledgeID: knowledge.ID, KnowledgeChannel: types.ConnectorTypeNextcloud},
		},
	}
	history := &NextcloudHistoryGuard{knowledge: retiredPanicKnowledge{}, sources: retiredPanicSources{}}
	assertRetiredReadUnchanged(t, message, func() error { return history.CheckMessage(ctx, message) })
}

func TestNextcloudRetiredHistoryPreservesBroadAndLegacyScopes(t *testing.T) {
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 12, UserID: "person-1"})
	history := &NextcloudHistoryGuard{knowledge: retiredPanicKnowledge{}, sources: retiredPanicSources{}}
	for _, message := range []*types.Message{
		{Role: "assistant", ExecutionContext: types.MessageExecutionContext{
			KnowledgeBaseIDs: []string{"nextcloud-kb"},
		}},
		{Role: "assistant", AgentID: "legacy-agent", AgentTenantID: 12},
		{
			Role: "assistant", AgentID: "all-agent", AgentTenantID: 12,
			ExecutionContext: types.MessageExecutionContext{AgentKBSelectionMode: "all"},
		},
		{Role: "user", Content: "my own question"},
		{Role: "assistant", ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"ordinary-kb"}}},
		{
			Role: "assistant", AgentID: "none-agent", AgentTenantID: 12,
			ExecutionContext: types.MessageExecutionContext{AgentKBSelectionMode: "none"},
		},
	} {
		assertRetiredReadUnchanged(t, message, func() error { return history.CheckMessage(ctx, message) })
	}
}

func TestNextcloudRetiredHistoryDoesNotLoadMissingSources(t *testing.T) {
	history := &NextcloudHistoryGuard{knowledge: retiredPanicKnowledge{}, sources: retiredPanicSources{}}
	for _, message := range []*types.Message{
		{Role: "assistant", KnowledgeReferences: types.References{
			{KnowledgeID: "removed-document", KnowledgeChannel: types.ConnectorTypeNextcloud},
		}},
		{Role: "assistant", ExecutionContext: types.MessageExecutionContext{
			KnowledgeBaseIDs: []string{"nextcloud-kb"},
		}},
	} {
		assertRetiredReadUnchanged(t, message, func() error {
			return history.CheckMessage(context.Background(), message)
		})
	}
}

func TestNextcloudRetiredHistoryLiveReadsDoNotInvokeSource(t *testing.T) {
	history := &NextcloudHistoryGuard{knowledge: retiredPanicKnowledge{}, sources: retiredPanicSources{}}
	message := &types.Message{Role: "assistant", ExecutionContext: types.MessageExecutionContext{
		KnowledgeBaseIDs: []string{"kb-1"},
	}}
	for _, refs := range []types.References{nil, {{KnowledgeID: "live-document"}}} {
		message.KnowledgeReferences = refs
		assertRetiredReadUnchanged(t, message, func() error {
			return history.CheckLiveMessage(context.Background(), message)
		})
	}
	message.ExecutionContext.KnowledgeBaseIDs = []string{"ordinary-kb"}
	message.KnowledgeReferences = nil
	assertRetiredReadUnchanged(t, message, func() error {
		return history.CheckLiveMessage(context.Background(), message)
	})
}

func TestNextcloudRetiredHistorySavedAndLiveShareCompatibilityContract(t *testing.T) {
	sources := retiredPanicSources{}
	guard := &NextcloudHistoryGuard{sources: sources, instanceSources: sources}
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 12, UserID: "person"})
	message := &types.Message{
		Role: "assistant", AgentID: "agent", AgentTenantID: 12,
		ExecutionContext: types.MessageExecutionContext{AgentKBSelectionMode: "none"},
	}
	for _, synthetic := range []bool{false, true} {
		if synthetic {
			message.AgentSteps = types.AgentSteps{{ToolCalls: []types.ToolCall{
				{ID: types.PipelineToolCallIDPrefix + "step", Name: "search_knowledge"},
			}}}
		}
		assertRetiredReadUnchanged(t, message, func() error { return guard.CheckLiveMessage(ctx, message) })
		assertRetiredReadUnchanged(t, message, func() error { return guard.CheckMessage(ctx, message) })
	}
}

// Retired read adapters must not mutate payloads while authorization stays in
// the active workspace/session resource policy.
func assertRetiredReadUnchanged(t *testing.T, input any, check func() error) {
	t.Helper()
	// Inspect fields directly, including json:"-" provenance and malformed
	// lineage, without invoking serializers or mutating pointer addresses.
	snapshot := spew.ConfigState{
		DisableMethods: true, DisablePointerMethods: true,
		DisablePointerAddresses: true, DisableCapacities: true, SortKeys: true,
	}
	before := snapshot.Sdump(input)
	require.NoError(t, check())
	require.Equal(t, before, snapshot.Sdump(input))
}
