package access

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type reviewLineageSnapshots struct {
	knowledge map[string]*types.Knowledge
	identity  map[string]types.SourceIdentity
}

func (
	s *reviewLineageSnapshots,
) ResolveNextcloudSourceLineage(_ context.Context, tenant uint64, kb, id string) (
	*types.Knowledge,
	types.SourceIdentity,
	error,
) {
	identity, ok := s.identity[id]
	if !ok || identity.TenantID != tenant || identity.KnowledgeBaseID != kb {
		return nil, types.SourceIdentity{}, fmt.Errorf("missing source")
	}
	return s.knowledge[id], identity, nil
}

func (s *reviewLineageSnapshots) RecheckNextcloudSourceLineage(ctx context.Context, lineage *types.SourceLineage) (
	bool,
	error,
) {
	for _, saved := range lineage.Sources {
		_, current, err := s.ResolveNextcloudSourceLineage(
			ctx,
			saved.TenantID,
			saved.KnowledgeBaseID,
			saved.KnowledgeID,
		)
		if err != nil {
			return false, err
		}
		if current != saved {
			return false, nil
		}
	}
	return true, nil
}

// A later source's slow remote authorization must not hide a local generation
// change to an earlier source in the same dependency set.
func TestNextcloudSourceLineageReviewEarlierSourceChangesDuringLaterRemoteCall(t *testing.T) {
	guard, ctx, saved, single, _ := lineagePolicyFixture(t)
	first := saved.Sources[0]
	second := first
	second.KnowledgeID = "document-2"
	second.FileID = "43"
	second.ExternalID = "nextcloud:instance-1:43"
	second.ETag = "etag-43"
	knowledge2 := *single.knowledge
	knowledge2.ID = second.KnowledgeID
	metadata := single.knowledge.GetMetadata()
	metadata["nextcloud_file_id"] = second.FileID
	metadata["external_id"] = second.ExternalID
	metadata["nextcloud_etag"] = second.ETag
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	knowledge2.Metadata = types.JSON(encoded)
	snapshots := &reviewLineageSnapshots{
		knowledge: map[string]*types.Knowledge{first.KnowledgeID: single.knowledge, second.KnowledgeID: &knowledge2},
		identity:  map[string]types.SourceIdentity{first.KnowledgeID: first, second.KnowledgeID: second},
	}
	guard.snapshots = snapshots
	lineage, err := types.NewCompleteSourceLineage(first, second)
	require.NoError(t, err)
	require.Equal(t, first, lineage.Sources[0], "the changed source must be checked before the delayed source")
	guard.publication.authorize = func(_ context.Context, _ *types.DataSourceConfig, _, _, _, _ string, fileID int64) (
		nextcloud.AuthorizationDecision,
		error,
	) {
		if fileID == 43 {
			current := snapshots.identity[first.KnowledgeID]
			current.Revision++
			snapshots.identity[first.KnowledgeID] = current
			return nextcloud.AuthorizationDecision{Allow: true, SourceETag: second.ETag}, nil
		}
		return nextcloud.AuthorizationDecision{Allow: true, SourceETag: first.ETag}, nil
	}
	require.ErrorIs(t, guard.Check(ctx, lineage, LineageReadScope{}), ErrNextcloudPublicationDenied)
	require.NotEqual(
		t,
		first.Revision,
		snapshots.identity[first.KnowledgeID].Revision,
		("the later remote call must actually change the earlier " +
			"source"))
}

// An identity can become disabled while the source response is in flight,
// although the KB's group-resource policy still inherits workspace access.
func TestNextcloudSourceLineageReviewDirectoryRevokedDuringRemoteCall(t *testing.T) {
	guard, ctx, saved, _, _ := lineagePolicyFixture(t)
	directory := guard.publication.directories.(*publicationDirectoryLookup)
	guard.publication.authorize = func(
		context.Context,
		*types.DataSourceConfig,
		string,
		string,
		string,
		string,
		int64,
	) (
		nextcloud.AuthorizationDecision,
		error,
	) {
		directory.identities[0].Status = types.DirectoryObjectDisabled
		return nextcloud.AuthorizationDecision{Allow: true, SourceETag: saved.Sources[0].ETag}, nil
	}
	require.ErrorIs(t, guard.Check(ctx, saved, LineageReadScope{}), ErrNextcloudPublicationDenied)
	require.Equal(
		t,
		types.DirectoryObjectDisabled,
		directory.identities[0].Status,
		"the remote callback must actually revoke the identity",
	)
}

func TestNextcloudSourceLineageReviewCompleteEmptyRequiresCurrentSharedAgent(t *testing.T) {
	guard, ctx, _, _, _ := lineagePolicyFixture(t)
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 99, UserID: "person-1", Role: types.TenantRoleViewer})
	agents := &reviewLineageAgents{}
	guard.agents = agents
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	// This plain-chat turn still uses the selected shared Agent's configuration
	// and system prompt, even though it consulted no knowledge base.
	scope := LineageReadScope{AgentID: "agent-1", AgentTenantID: 12}
	require.ErrorIs(t, guard.Check(
		ctx,
		lineage,
		scope,
	), ErrNextcloudPublicationDenied, ("revoked Agent must not be admitted by an empty source s" +
		"et"))
	require.Positive(t, agents.calls, "the current Agent grant must be reloaded")
}

type reviewLineageAgents struct {
	calls int
}

func (
	a *reviewLineageAgents,
) GetSharedAgentForTenant(context.Context, uint64, types.TenantRole, string, ...uint64) (*types.CustomAgent, error) {
	a.calls++
	return nil, nil
}

func (
	a *reviewLineageAgents,
) TenantCanAccessKBViaSomeSharedAgent(context.Context, uint64, types.TenantRole, *types.KnowledgeBase) (
	bool,
	error,
) {
	return false, nil
}

type reviewLineageGroups struct {
	onRead func()
}

func (
	g *reviewLineageGroups,
) EffectivePermission(context.Context, uint64, types.ResourceType, string, types.ResourceAction, time.Time) (
	types.EffectiveResourcePermission,
	error,
) {
	g.onRead()
	return types.EffectiveResourcePermission{Allowed: true}, nil
}

func TestNextcloudSourceLineageReviewLocalRechecksFollowFinalGrantLookup(t *testing.T) {
	for _, change := range []string{"generation", "directory identity"} {
		t.Run(change, func(t *testing.T) {
			guard, ctx, saved, snapshots, _ := lineagePolicyFixture(t)
			remoteDone := false
			changed := false
			directory := guard.publication.directories.(*publicationDirectoryLookup)
			guard.publication.authorize = func(
				context.Context,
				*types.DataSourceConfig,
				string,
				string,
				string,
				string,
				int64,
			) (
				nextcloud.AuthorizationDecision,
				error,
			) {
				remoteDone = true
				return nextcloud.AuthorizationDecision{Allow: true, SourceETag: saved.Sources[0].ETag}, nil
			}
			guard.groups = &reviewLineageGroups{onRead: func() {
				if !remoteDone || changed {
					return
				}
				changed = true
				if change == "generation" {
					snapshots.identity.Revision++
				} else {
					directory.identities[0].Status = types.DirectoryObjectDisabled
				}
			}}
			require.ErrorIs(t, guard.Check(ctx, saved, LineageReadScope{}), ErrNextcloudPublicationDenied)
			require.True(t, changed, "the final grant lookup must be exercised")
		})
	}
}

func TestNextcloudSourceLineageReviewOrdinaryAPISharedKBRetainsExistingGrant(t *testing.T) {
	guard, ctx, _, _, _ := lineagePolicyFixture(t)
	guard.groups = nil
	kb := guard.kbs.(*lineageKBStub).kb
	kb.EverHadNextcloudSource = false
	shares := &lineageSharesStub{allowed: true}
	guard.shares = shares
	caller := types.Caller{TenantID: 99, Role: types.TenantRoleViewer}
	ctx = types.WithCaller(ctx, caller)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "api-key-99"})
	ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KnowledgeBaseIDs: types.StringArray{kb.ID}})
	// The existing resource authorizer does not require a web-user ID for this
	// tenant API key's current organization grant and explicit ordinary KB scope.
	_, err := ResolveKB(ctx, KBRequest{Caller: caller}, kb, types.OrgRoleViewer, shares, nil)
	require.NoError(t, err)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	scope := LineageReadScope{Targets: types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		TenantID:        kb.TenantID,
		KnowledgeBaseID: kb.ID,
	}}}
	require.NoError(t, guard.Check(ctx, lineage, scope))
}

type reviewLineageAgentUseDenied struct {
	agentCalls int
}

func (
	g *reviewLineageAgentUseDenied,
) EffectivePermission(
	_ context.Context,
	_ uint64,
	kind types.ResourceType,
	_ string,
	action types.ResourceAction,
	_ time.Time,
) (
	types.EffectiveResourcePermission,
	error,
) {
	if kind == types.GroupResourceTypeAgent {
		g.agentCalls++
		return types.EffectiveResourcePermission{Allowed: false, Action: action}, nil
	}
	return types.EffectiveResourcePermission{Allowed: true, Action: action}, nil
}

func TestNextcloudSourceLineageReviewCompleteEmptyRequiresAgentUseGroupGrant(t *testing.T) {
	for _, callerTenant := range []uint64{12, 99} {
		t.Run(fmt.Sprintf("caller-%d", callerTenant), func(t *testing.T) {
			guard, ctx, _, _, _ := lineagePolicyFixture(t)
			ctx = types.WithCaller(ctx, types.Caller{
				TenantID: callerTenant,
				UserID:   "person-1",
				Role:     types.TenantRoleViewer,
			})
			guard.agents = &lineageAgentsStub{allowed: true, agent: &types.CustomAgent{ID: "agent-1", TenantID: 12}}
			groups := &reviewLineageAgentUseDenied{}
			guard.groups = groups
			lineage, err := types.NewCompleteSourceLineage()
			require.NoError(t, err)
			scope := LineageReadScope{AgentID: "agent-1", AgentTenantID: 12}
			require.ErrorIs(t, guard.Check(ctx, lineage, scope), ErrNextcloudPublicationDenied)
			require.Positive(t, groups.agentCalls, "existing Agent/use directory-resource permission must be checked")
		})
	}
}

func TestNextcloudSourceLineageReviewOrdinaryOrgGrantRemainsIndependentOfAgentKBSelection(t *testing.T) {
	guard, ctx, _, _, _ := lineagePolicyFixture(t)
	kb := guard.kbs.(*lineageKBStub).kb
	kb.EverHadNextcloudSource = false
	caller := types.Caller{TenantID: 99, UserID: "person-1", Role: types.TenantRoleViewer}
	ctx = types.WithCaller(ctx, caller)
	shares := &lineageSharesStub{allowed: true}
	guard.shares = shares
	agent := &types.CustomAgent{ID: "agent-1", TenantID: 12}
	agent.Config.KBSelectionMode = "selected"
	agent.Config.KnowledgeBases = []string{"other-kb"}
	agents := &lineageAgentsStub{allowed: true, agent: agent}
	guard.agents = agents
	_, err := ResolveKB(ctx, KBRequest{
		Caller:              caller,
		AgentID:             agent.ID,
		AgentSourceTenantID: "12",
	}, kb, types.OrgRoleViewer, shares, agents)
	require.NoError(t, err, "current organization KB permission grants read independently of an Agent KB fallback")
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	scope := LineageReadScope{AgentID: agent.ID, AgentTenantID: agent.TenantID, Targets: types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledgeBase,
		TenantID:        kb.TenantID,
		KnowledgeBaseID: kb.ID,
	}}}
	require.NoError(t, guard.Check(ctx, lineage, scope))
}
