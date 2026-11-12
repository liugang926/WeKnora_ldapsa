package access

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudRetiredLineageBatchDoesNotTriggerRemoteRevisionChanges(t *testing.T) {
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
	reads := []SourceLineageRead{{Lineage: firstProof}, {Lineage: secondProof}}
	assertRetiredReadUnchanged(t, reads, func() error { return guard.CheckBatch(ctx, reads) })
	require.False(t, changed, "retired batch read must not call remote authorization")
	require.Equal(t, first.Revision, snapshots.identity[first.KnowledgeID].Revision)
}

func TestRetiredBatchLeavesSelectedAgentAuthorizationToResourcePolicy(t *testing.T) {
	guard, ctx, _, _, _ := lineagePolicyFixture(t)
	empty, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	guard.agents = &lineageAgentsStub{allowed: false}
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 99, UserID: "person-1", Role: types.TenantRoleViewer})
	scope := LineageReadScope{AgentID: "agent-1", AgentTenantID: 12}
	reads := []SourceLineageRead{{Lineage: empty, Scope: scope}}
	assertRetiredReadUnchanged(t, reads, func() error { return guard.CheckBatch(ctx, reads) })
	require.ErrorIs(t, guard.checkCurrentKBReads(ctx, types.CallerFromContext(ctx), nil, nil, scope),
		ErrNextcloudPublicationDenied)
}
