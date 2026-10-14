package access

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
)

type publicationDirectoryLookup struct {
	identities       []*types.DirectoryIdentity
	snapshotIdentity *types.DirectoryIdentity
	directory        *types.Directory
	err              error
}

func (l *publicationDirectoryLookup) GetIdentityByUserID(context.Context, string) ([]*types.DirectoryIdentity, error) {
	return l.identities, l.err
}

func (
	l *publicationDirectoryLookup,
) GetLoginSnapshot(context.Context, string, string) (*types.DirectoryLoginSnapshot, error) {
	if l.err != nil {
		return nil, l.err
	}
	if len(l.identities) == 0 {
		return &types.DirectoryLoginSnapshot{Directory: l.directory}, nil
	}
	if l.snapshotIdentity != nil {
		return &types.DirectoryLoginSnapshot{Directory: l.directory, Identity: l.snapshotIdentity}, nil
	}
	return &types.DirectoryLoginSnapshot{Directory: l.directory, Identity: l.identities[0]}, nil
}

type publicationDataSourceLookup struct {
	source *types.DataSource
	err    error
}

func (l *publicationDataSourceLookup) FindByID(context.Context, string) (*types.DataSource, error) {
	return l.source, l.err
}

func publicationFixture(t *testing.T) (*NextcloudPublicationGuard, context.Context, *types.Knowledge) {
	t.Helper()
	userID := "person-1"
	now := time.Now().UTC()
	identity := &types.DirectoryIdentity{
		ID: "identity-1", DirectoryID: "ad-1", ObjectGUID: "00112233-4455-6677-8899-aabbccddeeff",
		UserID: &userID, Status: types.DirectoryObjectActive, SnapshotVersion: 4,
	}
	directory := &types.Directory{
		ID: "ad-1", Protocol: types.DirectoryProtocolAD, Enabled: true,
		SnapshotVersion: 4, LastSuccessfulSyncAt: &now, StaleAfterSeconds: 900,
	}
	config, err := json.Marshal(types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": "https://nextcloud.example"},
		Credentials: map[string]interface{}{"token": "source-token"},
		ResourceIDs: []string{"binding-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	source := &types.DataSource{
		ID: "source-1", TenantID: 12, KnowledgeBaseID: "kb-1",
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
		Config: types.JSON(config),
	}
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "source-1", "external_id": "nextcloud:instance-1:42",
		"source_resource_id": "binding-1", "nextcloud_instance_id": "instance-1",
		"nextcloud_binding_id": "binding-1", "nextcloud_file_id": "42",
		"nextcloud_etag": "etag-42",
	})
	if err != nil {
		t.Fatal(err)
	}
	knowledge := &types.Knowledge{
		TenantID: 12, KnowledgeBaseID: "kb-1", Channel: types.ConnectorTypeNextcloud,
		Metadata: types.JSON(metadata),
	}
	guard := NewNextcloudPublicationGuard(
		&publicationDirectoryLookup{identities: []*types.DirectoryIdentity{identity}, directory: directory},
		&publicationDataSourceLookup{source: source},
	)
	guard.authorize = func(_ context.Context, _ *types.DataSourceConfig,
		instanceID, bindingID, directoryID, objectGUID string, fileID int64,
	) (nextcloud.AuthorizationDecision, error) {
		if instanceID != "instance-1" || bindingID != "binding-1" || directoryID != "ad-1" ||
			objectGUID != identity.ObjectGUID || fileID != 42 {
			t.Fatal("guard sent incorrect source identity")
		}
		return nextcloud.AuthorizationDecision{
			Allow:          true,
			Reason:         "authorized",
			PolicyRevision: "rev-1",
			SourceETag:     "etag-42",
		}, nil
	}
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 12, UserID: userID})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: userID})
	return guard, ctx, knowledge
}

func TestNextcloudPublicationGuardAllowsOnlyFreshAuthorizedIdentity(t *testing.T) {
	guard, ctx, knowledge := publicationFixture(t)
	if err := guard.CheckKnowledge(ctx, knowledge); err != nil {
		t.Fatalf("fresh source allow was denied: %v", err)
	}
	if err := guard.CheckKnowledge(ctx, &types.Knowledge{Channel: "web"}); err != nil {
		t.Fatalf("unrelated knowledge was denied: %v", err)
	}
	if err := guard.CheckKnowledge(ctx, &types.Knowledge{
		Channel: "web", Metadata: types.JSON(`{"ordinary_numeric_field":42}`),
	}); err != nil {
		t.Fatalf("ordinary mixed metadata was denied: %v", err)
	}
}

func TestNextcloudPublicationGuardFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*NextcloudPublicationGuard, *context.Context, *types.Knowledge)
		want   error
	}{
		{"source denied", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.authorize = func(context.Context, *types.DataSourceConfig, string, string, string, string, int64) (
				nextcloud.AuthorizationDecision,
				error,
			) {
				return nextcloud.AuthorizationDecision{Allow: false, Reason: "source_not_readable"}, nil
			}
		}, ErrNextcloudPublicationDenied},
		{"source unavailable", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.authorize = func(context.Context, *types.DataSourceConfig, string, string, string, string, int64) (
				nextcloud.AuthorizationDecision,
				error,
			) {
				return nextcloud.AuthorizationDecision{}, errors.New("network failure")
			}
		}, ErrNextcloudPublicationUnavailable},
		{"source version changed", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.authorize = func(context.Context, *types.DataSourceConfig, string, string, string, string, int64) (
				nextcloud.AuthorizationDecision,
				error,
			) {
				return nextcloud.AuthorizationDecision{
					Allow:      true,
					Reason:     "authorized",
					SourceETag: "new-version",
				}, nil
			}
		}, ErrNextcloudPublicationDenied},
		{"missing imported etag", func(_ *NextcloudPublicationGuard, _ *context.Context, k *types.Knowledge) {
			k.Metadata = types.JSON(("{\"datasource_id\":\"source-1\",\"external_id\":\"nextcloud:in" +
				"stance-1:42\",\"source_resource_id\":\"binding-1\",\"nextclou" +
				"d_instance_id\":\"instance-1\",\"nextcloud_binding_id\":\"bin" +
				"ding-1\",\"nextcloud_file_id\":\"42\"}"))
		}, ErrNextcloudPublicationDenied},
		{"unlinked user", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.directories.(*publicationDirectoryLookup).identities = nil
		}, ErrNextcloudPublicationDenied},
		{"ambiguous identity", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			lookup := g.directories.(*publicationDirectoryLookup)
			lookup.identities = append(lookup.identities, lookup.identities[0])
		}, ErrNextcloudPublicationDenied},
		{"disabled identity", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.directories.(*publicationDirectoryLookup).identities[0].Status = types.DirectoryObjectDisabled
		}, ErrNextcloudPublicationDenied},
		{"stale snapshot", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.directories.(*publicationDirectoryLookup).directory.SnapshotVersion = 5
		}, ErrNextcloudPublicationDenied},
		{"identity changed after lookup", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			lookup := g.directories.(*publicationDirectoryLookup)
			cloned := *lookup.identities[0]
			cloned.Status = types.DirectoryObjectDisabled
			lookup.snapshotIdentity = &cloned
		}, ErrNextcloudPublicationDenied},
		{"directory sync stale", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			last := time.Now().Add(-time.Hour)
			g.directories.(*publicationDirectoryLookup).directory.LastSuccessfulSyncAt = &last
		}, ErrNextcloudPublicationDenied},
		{"failed sync", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.directories.(*publicationDirectoryLookup).directory.LastSyncError = "LDAP unavailable"
		}, ErrNextcloudPublicationDenied},
		{"machine caller", func(_ *NextcloudPublicationGuard, ctx *context.Context, _ *types.Knowledge) {
			*ctx = types.WithCaller(*ctx, types.Caller{TenantID: 12, UserID: "system-12"})
		}, ErrNextcloudPublicationDenied},
		{"embed principal with linked user", func(
			_ *NextcloudPublicationGuard,
			ctx *context.Context,
			_ *types.Knowledge,
		) {
			*ctx = types.WithPrincipal(*ctx, types.Principal{Type: types.PrincipalEmbedChannel, ID: "12:channel-1"})
		}, ErrNextcloudPublicationDenied},
		{"mcp principal with linked user", func(
			_ *NextcloudPublicationGuard,
			ctx *context.Context,
			_ *types.Knowledge,
		) {
			*ctx = types.WithPrincipal(*ctx, types.MCPEndpointPrincipal(12, "endpoint-1"))
		}, ErrNextcloudPublicationDenied},
		{"api principal with linked user", func(
			_ *NextcloudPublicationGuard,
			ctx *context.Context,
			_ *types.Knowledge,
		) {
			*ctx = types.WithPrincipal(*ctx, types.Principal{Type: types.PrincipalAPIExternalUser, ID: "12:person-1"})
		}, ErrNextcloudPublicationDenied},
		{"web principal mismatch", func(_ *NextcloudPublicationGuard, ctx *context.Context, _ *types.Knowledge) {
			*ctx = types.WithPrincipal(*ctx, types.Principal{Type: types.PrincipalWebUser, ID: "someone-else"})
		}, ErrNextcloudPublicationDenied},
		{"api scope on web principal", func(_ *NextcloudPublicationGuard, ctx *context.Context, _ *types.Knowledge) {
			*ctx = types.WithTenantAPIKeyScope(*ctx, types.TenantAPIKeyScope{})
		}, ErrNextcloudPublicationDenied},
		{"uncaptured caller", func(_ *NextcloudPublicationGuard, ctx *context.Context, _ *types.Knowledge) {
			*ctx = context.WithValue(context.Background(), types.UserIDContextKey, "person-1")
		}, ErrNextcloudPublicationDenied},
		{"wrong source tenant", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.dataSources.(*publicationDataSourceLookup).source.TenantID = 99
		}, ErrNextcloudPublicationDenied},
		{"multiple selected bindings", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.dataSources.(*publicationDataSourceLookup).source.Config = types.JSON(
				("{\"type\":\"nextcloud\",\"resource_ids\":[\"binding-1\",\"bindin" +
					"g-2\"]}"))
		}, ErrNextcloudPublicationDenied},
		{"no selected binding", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.dataSources.(*publicationDataSourceLookup).source.Config = types.JSON(
				`{"type":"nextcloud","resource_ids":[]}`,
			)
		}, ErrNextcloudPublicationDenied},
		{"missing source", func(g *NextcloudPublicationGuard, _ *context.Context, _ *types.Knowledge) {
			g.dataSources.(*publicationDataSourceLookup).err = errors.New("database down")
		}, ErrNextcloudPublicationUnavailable},
		{"missing file identity", func(_ *NextcloudPublicationGuard, _ *context.Context, k *types.Knowledge) {
			k.Metadata = types.JSON(`{"datasource_id":"source-1"}`)
		}, ErrNextcloudPublicationDenied},
		{"malformed metadata", func(_ *NextcloudPublicationGuard, _ *context.Context, k *types.Knowledge) {
			k.Metadata = types.JSON(`{"datasource_id":42}`)
		}, ErrNextcloudPublicationDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guard, ctx, knowledge := publicationFixture(t)
			tc.mutate(guard, &ctx, knowledge)
			if err := guard.CheckKnowledge(ctx, knowledge); !errors.Is(err, tc.want) {
				t.Fatalf("CheckKnowledge error = %v, want %v", err, tc.want)
			}
		})
	}
}
