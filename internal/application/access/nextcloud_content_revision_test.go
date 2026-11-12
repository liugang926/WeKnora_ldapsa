package access

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudRetiredPublicationDoesNotReloadContentRevision(t *testing.T) {
	guard, ctx, k := publicationFixture(t)
	metadata := k.GetMetadata()
	metadata["nextcloud_content_revision"] = "1"
	raw, err := json.Marshal(metadata)
	require.NoError(t, err)
	k.Metadata = raw
	calls := 0
	guard.authorize = func(context.Context, *types.DataSourceConfig, string, string, string, string, int64,
	) (nextcloud.AuthorizationDecision, error) {
		calls++
		return nextcloud.AuthorizationDecision{SourceContentRevision: "2"}, nil
	}
	assertRetiredReadUnchanged(t, k, func() error { return guard.CheckKnowledge(ctx, k) })
	require.Zero(t, calls)
}

type recoveryClosedSourceLookup struct {
	NextcloudDataSourceLookup
	closed bool
}

func (s *recoveryClosedSourceLookup) InstanceControlsClosed(context.Context, string) (bool, error) {
	return s.closed, nil
}

func TestRetainedSourceControlPolicyRemainsSeparateFromRetiredKnowledgeRead(t *testing.T) {
	guard, ctx, k := publicationFixture(t)
	lookup := &recoveryClosedSourceLookup{NextcloudDataSourceLookup: guard.dataSources, closed: true}
	guard.dataSources = lookup
	require.ErrorIs(t, guard.checkSourceControls(ctx, "instance-1"), ErrNextcloudPublicationDenied)
	lookup.closed = false
	require.NoError(t, guard.checkSourceControls(ctx, "instance-1"))
	calls := 0
	guard.authorize = func(context.Context, *types.DataSourceConfig, string, string, string, string, int64,
	) (nextcloud.AuthorizationDecision, error) {
		calls++
		lookup.closed = true
		return nextcloud.AuthorizationDecision{Allow: true}, nil
	}
	assertRetiredReadUnchanged(t, k, func() error { return guard.CheckKnowledge(ctx, k) })
	require.Zero(t, calls)
	require.False(t, lookup.closed)
}
