package access

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudSourceLineageBatchRejectsEarlierReadChangedDuringLaterRemote(t *testing.T) {
	guard, ctx, firstProof, single, _ := lineagePolicyFixture(t)
	first := firstProof.Sources[0]
	second := first
	second.FileID = "43"
	second.KnowledgeID = "document-2"
	second.ExternalID = "nextcloud:instance-1:43"
	second.ETag = "etag-43"
	secondKnowledge := *single.knowledge
	secondKnowledge.ID = second.KnowledgeID
	metadata := single.knowledge.GetMetadata()
	metadata["nextcloud_file_id"] = second.FileID
	metadata["external_id"] = second.ExternalID
	metadata["nextcloud_etag"] = second.ETag
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	secondKnowledge.Metadata = types.JSON(encoded)
	snapshots := &reviewLineageSnapshots{
		knowledge: map[string]*types.Knowledge{
			first.KnowledgeID:  single.knowledge,
			second.KnowledgeID: &secondKnowledge,
		},
		identity: map[string]types.SourceIdentity{first.KnowledgeID: first, second.KnowledgeID: second},
	}
	guard.snapshots = snapshots
	secondProof, err := types.NewCompleteSourceLineage(second)
	require.NoError(t, err)
	changed := false
	guard.publication.authorize = func(
		_ context.Context,
		_ *types.DataSourceConfig,
		_,
		_,
		_,
		_ string,
		fileID int64,
	) (nextcloud.AuthorizationDecision, error) {
		if fileID == 43 {
			current := snapshots.identity[first.KnowledgeID]
			current.Revision++
			snapshots.identity[first.KnowledgeID] = current
			changed = true
			return nextcloud.AuthorizationDecision{Allow: true, SourceETag: second.ETag}, nil
		}
		return nextcloud.AuthorizationDecision{Allow: true, SourceETag: first.ETag}, nil
	}
	require.ErrorIs(
		t,
		guard.CheckBatch(ctx, []SourceLineageRead{{Lineage: firstProof}, {Lineage: secondProof}}),
		ErrNextcloudPublicationDenied,
	)
	require.True(t, changed, "later actual source authorization did not commit the earlier revision change")
}

func TestNextcloudSourceLineageBatchKeepsEmptySelectedAgentAuthorization(t *testing.T) {
	guard, ctx, _, _, _ := lineagePolicyFixture(t)
	empty, _ := types.NewCompleteSourceLineage()
	guard.agents = &lineageAgentsStub{allowed: false}
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 99, UserID: "person-1", Role: types.TenantRoleViewer})
	require.ErrorIs(
		t,
		guard.CheckBatch(
			ctx,
			[]SourceLineageRead{{Lineage: empty, Scope: LineageReadScope{AgentID: "agent-1", AgentTenantID: 12}}},
		),
		ErrNextcloudPublicationDenied,
	)
}
