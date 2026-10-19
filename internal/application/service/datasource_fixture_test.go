package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	storagefile "github.com/Tencent/WeKnora/internal/application/service/file"
	nextcloudconnector "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// These focused sync tests still cross the real original-configuration purpose
// boundary. Reuse the current production schema/retention installation, then
// admit the test's independent KB/name and exact source/pair through SQL writers.
// No UUID, empty lineage or mock can stand in for that current authority.
func wireNextcloudCurrentInputFixture(
	t *testing.T,
	svc *DataSourceService,
	ds *types.DataSource,
	instanceIDs ...string,
) *gorm.DB {
	t.Helper()
	m := newMachinePipelineFixture(t, false)
	db, store := m.f.db, m.f.store
	ctx := types.WithExecutionTenant(m.f.ctx, ds.TenantID)
	ctx = types.WithCaller(
		ctx,
		types.Caller{TenantID: ds.TenantID, UserID: "enrich-human", Role: types.TenantRoleAdmin},
	)
	require.NoError(t, db.FirstOrCreate(&types.Tenant{ID: ds.TenantID, Status: "active"}, "id = ?", ds.TenantID).Error)
	require.NoError(
		t,
		db.FirstOrCreate(
			&types.TenantMember{
				TenantID: ds.TenantID,
				UserID:   "enrich-human",
				Role:     types.TenantRoleAdmin,
				Status:   types.TenantMemberStatusActive,
			},
			"tenant_id = ? AND user_id = ?",
			ds.TenantID,
			"enrich-human",
		).Error,
	)
	// The generic machine fixture has one unrelated active pair. This test
	// replaces its setup-only pairing for the same KB, never a live production row.
	require.NoError(
		t,
		db.Where("knowledge_base_id = ?", ds.KnowledgeBaseID).Delete(&apprepo.NextcloudSourcePairing{}).Error,
	)
	require.NoError(
		t,
		db.Unscoped().Where("knowledge_base_id = ?", ds.KnowledgeBaseID).Delete(&types.DataSource{}).Error,
	)
	kb := &types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID}
	if svc.kbService != nil {
		given, err := svc.kbService.GetKnowledgeBaseByID(ctx, ds.KnowledgeBaseID)
		require.NoError(t, err)
		if given != nil {
			*kb = *given
		}
	}
	kb.ID, kb.TenantID = ds.KnowledgeBaseID, ds.TenantID
	if kb.Name == "" {
		kb.Name = "Independent sync fixture configuration"
	}
	write := apprepo.WithIndependentKnowledgeBaseNameInput(apprepo.WithIndependentKnowledgeInput(ctx), kb.ID, kb.Name)
	var count int64
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("id = ?", kb.ID).Count(&count).Error)
	if count == 0 {
		require.NoError(t, store.CreateKnowledgeBaseWithInput(write, kb))
	} else {
		var existing types.KnowledgeBase
		require.NoError(t, db.Where("id = ?", kb.ID).Take(&existing).Error)
		kb.EverHadNextcloudSource = existing.EverHadNextcloudSource
		require.NoError(t, store.UpdateKnowledgeBaseWithInput(write, kb))
	}
	svc.kbService = &processSyncKBService{kb: kb}
	config, err := ds.ParseConfig()
	require.NoError(t, err)
	if config == nil || len(config.Settings) == 0 || config.Settings["base_url"] == nil {
		if config == nil {
			config = &types.DataSourceConfig{}
		}
		config.Type = types.ConnectorTypeNextcloud
		config.Settings = map[string]interface{}{"base_url": "https://nextcloud.example.test"}
		config.Credentials = map[string]interface{}{"token": "current-input-fixture", "key_id": "fixture"}
		if len(config.ResourceIDs) == 0 {
			config.ResourceIDs = []string{"binding-1"}
		}
		ds.Config, err = config.ToJSON()
		require.NoError(t, err)
	}
	base, digest, binding, err := apprepo.NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	keyID, _ := config.Credentials["key_id"].(string)
	if keyID == "" {
		keyID = "fixture"
	}
	instance := "instance-1"
	if len(instanceIDs) > 0 {
		instance = instanceIDs[0]
	}
	if cursor, err := ds.ParseSyncCursor(); err == nil && cursor != nil {
		if current, ok := cursor.ConnectorCursor["instance_id"].(string); ok && current != "" {
			instance = current
		}
	}
	if ds.Name == "" {
		ds.Name = "Independent sync fixture source"
	}
	sources := apprepo.NewDataSourceRepository(db)
	pairs := apprepo.NewNextcloudSourcePairingRepository(db)
	apprepo.ConfigureDataSourceNameInputWriters(
		sources,
		pairs,
		func(tx *gorm.DB) *apprepo.KnowledgeInputProvenanceStore {
			current := apprepo.NewKnowledgeInputProvenanceStore(tx)
			apprepo.ConfigureKnowledgeInputActorGroupAccess(
				current,
				NewGroupAccessService(apprepo.NewGroupAccessRepository(tx)),
			)
			return current
		},
	)
	ds.Status = types.DataSourceStatusPaused
	require.NoError(t, sources.Create(apprepo.WithIndependentDataSourceNameInput(ctx, ds.Name), ds))
	require.NoError(t, db.Create(&apprepo.NextcloudSourcePairing{
		OperationID: "sync-fixture-" + ds.ID, TenantID: ds.TenantID,
		KnowledgeBaseID: ds.KnowledgeBaseID, DataSourceID: ds.ID, InstanceID: instance, BindingID: binding,
		BaseURL: base, ConfigSHA: digest, PublicationEpoch: 1, KeyID: keyID, State: "active",
	}).Error)
	require.NoError(
		t,
		db.Model(&types.DataSource{}).Where("id = ?", ds.ID).Update("status", types.DataSourceStatusActive).Error,
	)
	ds.Status = types.DataSourceStatusActive
	svc.sourceInputs, svc.nextcloudSourcePairings = store, pairs
	return db
}

// The mocked knowledge writer does not store bytes; capacity admission itself
// is the real SQL protocol with a real local provider, never an allow mock.
func wireNextcloudCapacityFixture(t *testing.T, svc *DataSourceService, ds *types.DataSource) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "sync-capacity.db")), &gorm.Config{})
	require.NoError(t, err)
	pool, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	require.NoError(
		t,
		db.AutoMigrate(
			&types.KnowledgeBase{},
			&types.Knowledge{},
			&types.StoredResource{},
			&types.ResourceBinding{},
			&types.ResourceAccessGrant{},
		),
	)
	for _, name := range []string{"000065_nextcloud_capacity_admission", "000067_nextcloud_capacity_write_intents"} {
		sql, err := os.ReadFile("../../../migrations/sqlite/" + name + ".up.sql")
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(sql)).Error)
	}
	backend := "sync-fixture-backend"
	kb := &types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID, StorageBackendID: &backend}
	require.NoError(t, db.Create(kb).Error)
	files := storagefile.NewBackendScopedFileService(backend, storagefile.NewLocalFileService(t.TempDir(), ""))
	svc.kbService = &processSyncKBService{kb: kb}
	svc.capacityAdmission = NewNextcloudCapacityAdmission(
		apprepo.NewNextcloudCapacityStore(db),
		func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) { return files, nil },
	)
}

// Obtain the opaque OriginalArtifact from the production connector, including
// its authenticated request and If-Match fetch. Tests cannot manufacture it.
func datasourceOriginalFetchedItem(
	t *testing.T,
	fileName, etag, body string,
) (*types.FetchedItem, *types.DataSourceConfig) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer isolated-sync-token", r.Header.Get("Authorization"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Signature"))
		require.NotEmpty(t, r.Header.Get("X-WeKnora-Nonce"))
		switch {
		case strings.HasSuffix(r.URL.Path, "/capabilities"):
			fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance-1"}`)
		case strings.HasSuffix(r.URL.Path, "/manifest"):
			fmt.Fprintf(
				w,
				`{"generation":"sync-fixture","complete":true,"next_cursor":null,"items":[{"file_id":77,"etag":%q,"name":%q,"path":%q,"mime_type":"text/plain","size":%d,"mtime":1700000000}]}`,
				etag,
				fileName,
				fileName,
				len(body),
			)
		case strings.HasSuffix(r.URL.Path, "/content"):
			require.Equal(t, `"`+etag+`"`, r.Header.Get("If-Match"))
			w.Header().Set("ETag", `"`+etag+`"`)
			fmt.Fprint(w, body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	config := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{"binding-1"},
		Settings:    map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{"token": "isolated-sync-token", "key_id": "sync-fixture"},
	}
	items, err := nextcloudconnector.NewConnector().FetchAll(context.Background(), config, config.ResourceIDs)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = nextcloudconnector.VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
	return &items[0], config
}
