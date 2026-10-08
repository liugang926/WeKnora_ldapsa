package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type collectionDirectory struct {
	identity  *types.DirectoryIdentity
	directory *types.Directory
}

func (s collectionDirectory) GetIdentityByUserID(context.Context, string) ([]*types.DirectoryIdentity, error) {
	return []*types.DirectoryIdentity{s.identity}, nil
}
func (s collectionDirectory) GetLoginSnapshot(context.Context, string, string) (*types.DirectoryLoginSnapshot, error) {
	return &types.DirectoryLoginSnapshot{Identity: s.identity, Directory: s.directory}, nil
}

// The actual signed connector HTTP transport, persisted source staging/build
// receipts and production lineage/publication guards are used together here.
// The fresh AD snapshot and Nextcloud endpoint contain only isolated test data.
func collectionNextcloudHTTPFixture(t *testing.T, change string) (*KnowledgeHandler, *atomic.Int64) {
	t.Helper()
	h, db, store, groups, shares, agents := collectionHTTPFixture(t)
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	calls := &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer isolated-source-secret", r.Header.Get("Authorization"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		switch {
		case strings.HasSuffix(r.URL.Path, "/capabilities"):
			fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance"}`)
		case strings.HasSuffix(r.URL.Path, "/publication-check"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			call := calls.Add(1)
			if change == "deny_excluded_before_filter" || change == "personal_revoke_before_write" && call >= 3 {
				fmt.Fprint(w, `{"allow":false,"reason":"source_not_readable","checked_at":1780000000}`)
				return
			}
			if change == "source_withdraw_during_final_remote" && call == 3 {
				require.NoError(t, repository.NewNextcloudContentLeaseStore(db).RetireKnowledge(context.Background(), repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "source", DataSourceID: "ds", ExternalID: "nextcloud:instance:77"}))
			}
			fmt.Fprint(w, `{"allow":true,"reason":"authorized","policy_revision":"isolated-rev","source_etag":"original-etag","checked_at":1780000000}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	require.NoError(t, db.Exec("DROP TABLE nextcloud_source_versions; DROP TABLE nextcloud_source_revisions").Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &repository.NextcloudSourcePairing{}))
	for _, name := range []string{"000031_nextcloud_source_versions", "000045_nextcloud_source_revisions", "000046_nextcloud_content_leases", "000047_nextcloud_candidate_retry"} {
		script, err := os.ReadFile("../../migrations/sqlite/" + name + ".up.sql")
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(script)).Error)
	}
	config, err := (&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding"}, Settings: map[string]interface{}{"base_url": server.URL}, Credentials: map[string]interface{}{"token": "isolated-source-secret", "key_id": "isolated-key"}}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: config}
	require.NoError(t, db.Create(ds).Error)
	base, sha, binding, err := repository.NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	require.NoError(t, db.Create(&repository.NextcloudSourcePairing{OperationID: "collection-pair", TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds", InstanceID: "instance", BindingID: binding, BaseURL: base, ConfigSHA: sha, PublicationEpoch: 1, KeyID: "isolated-key", State: "active"}).Error)
	metadata, err := json.Marshal(map[string]string{"datasource_id": "ds", "external_id": "nextcloud:instance:77", "nextcloud_instance_id": "instance", "nextcloud_binding_id": "binding", "nextcloud_file_id": "77", "source_resource_id": "binding", "nextcloud_path": "source.md", "nextcloud_etag": ""})
	require.NoError(t, err)
	k := &types.Knowledge{ID: "source", TenantID: 7, KnowledgeBaseID: "kb", Type: "file", Channel: types.ConnectorTypeNextcloud, Title: "source original", FileName: "source.md", FileType: "md", FolderPath: "source/docs", ParseStatus: types.ParseStatusPending, EnableStatus: "disabled", Metadata: types.JSON(metadata), CustomMetadata: types.JSON(`{}`)}
	require.NoError(t, db.Create(k).Error)
	repo := repository.NewKnowledgeRepository(db)
	versions := repo.(interface {
		StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
		PublishNextcloudVersion(context.Context, string) (bool, error)
	})
	require.NoError(t, versions.StageNextcloudVersion(context.Background(), 7, "kb", "ds", "nextcloud:instance:77", "original-etag", k.ID))
	scope := repository.NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID, DataSourceID: "ds", ExternalID: "nextcloud:instance:77"}
	leases := repository.NewNextcloudContentLeaseStore(db)
	lease, err := leases.AcquireKnowledge(context.Background(), scope, repository.NextcloudContentBuildLease, "actual-source-builder", time.Minute)
	require.NoError(t, err)
	ctx := repository.WithNextcloudBuildLease(context.Background(), leases, lease, scope)
	proof, err := store.AdmitNextcloudBuildInputs(ctx, []byte("actual original source artifact"))
	require.NoError(t, err)
	ctx = repository.WithKnowledgeBuildInputProof(ctx, store, proof)
	require.NoError(t, repository.NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{{ID: "source-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "source", Content: "actual original source parsed body", ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready"}}))
	require.NoError(t, db.Where("id='source'").Take(k).Error)
	k.ParseStatus, k.EnableStatus = types.ParseStatusCompleted, "enabled"
	require.NoError(t, repo.UpdateKnowledge(ctx, k))
	published, err := versions.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published)
	user := "user"
	now := time.Now().UTC()
	dir := collectionDirectory{identity: &types.DirectoryIdentity{ID: "identity", DirectoryID: "ad-1", ObjectGUID: "00112233-4455-6677-8899-aabbccddeeff", UserID: &user, Status: types.DirectoryObjectActive, SnapshotVersion: 1}, directory: &types.Directory{ID: "ad-1", Protocol: types.DirectoryProtocolAD, Enabled: true, SnapshotVersion: 1, LastSuccessfulSyncAt: &now, StaleAfterSeconds: 900}}
	publication := access.NewNextcloudPublicationGuard(dir, publicationHandlerSources{source: ds})
	guard := access.NewNextcloudSourceLineageGuard(repo.(access.NextcloudLineageSnapshotLookup), collectionHTTPKB{db: db}, shares, agents, groups, publication)
	ConfigureKnowledgeHandlerCollectionOutput(h, service.NewKnowledgeCollectionOutputAuthorizer(store, guard))
	ConfigureKnowledgeHandlerContentLeaseStore(h, leases)
	return h, calls
}

func TestKnowledgeCollectionNextcloudActualHTTPPositiveAndPersonalRevocation(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		want      int
	}{
		{"list", "/knowledge-bases/kb/knowledge?page_size=1", http.StatusOK},
		{"folders", "/knowledge-bases/kb/knowledge/folders", http.StatusOK},
		{"search", "/knowledge/search?keyword=source", http.StatusOK},
		{"deny_excluded_before_filter", "/knowledge-bases/kb/knowledge?keyword=match&page_size=1", http.StatusForbidden},
		{"personal_revoke_before_write", "/knowledge-bases/kb/knowledge?keyword=match&page_size=1", http.StatusForbidden},
		{"source_withdraw_during_final_remote", "/knowledge-bases/kb/knowledge?keyword=match&page_size=1", http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, calls := collectionNextcloudHTTPFixture(t, tc.name)
			w := serveCollectionHTTP(h, tc.url, false)
			require.Equal(t, tc.want, w.Code, w.Body.String())
			require.Positive(t, calls.Load())
			if tc.name == "folders" {
				require.Contains(t, w.Body.String(), `"path":"source/docs"`)
				require.Contains(t, w.Body.String(), `"total_document_count":3`)
			}
			if tc.want != http.StatusOK {
				require.NotContains(t, w.Body.String(), "match original")
				require.NotContains(t, w.Body.String(), "source original")
			}
		})
	}
}
