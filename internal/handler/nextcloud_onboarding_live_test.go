package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Local component fixture: actual WK handlers/store/migrations and actual NC
// signed pairing/root APIs. LDAP snapshot/model resources are synthetic. Empty
// roots exercise complete-manifest target transfer without pretending AI work.
func TestNextcloudOnboardingActualNCRuntime(t *testing.T) {
	fixture := os.Getenv("WEKNORA_ONBOARDING_LIVE_FIXTURE")
	if fixture == "" {
		t.Skip("owned NC component fixture required")
	}
	var f struct{ Instance, Directory, GUID, Token7, Token8, BaseURL string }
	raw, e := os.ReadFile(fixture)
	require.NoError(t, e)
	require.NoError(t, json.Unmarshal(raw, &f))
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", f.BaseURL)
	db, e := gorm.Open(
		sqlite.Open(filepath.Join(t.TempDir(), "live.db")+"?_busy_timeout=5000"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	require.NoError(t, e)
	sql, e := db.DB()
	require.NoError(t, e)
	sql.SetMaxOpenConns(1)
	defer func() { require.NoError(t, sql.Close()) }()
	require.NoError(
		t,
		db.AutoMigrate(
			&types.KnowledgeBase{},
			&types.Knowledge{},
			&types.Chunk{},
			&types.DataSource{},
			&types.Model{},
			&types.StorageBackend{},
			&types.TenantMember{},
			&types.ResourceAccessPolicy{},
			&types.ResourceGroupGrant{},
			&types.TenantGroupRoleGrant{},
			&types.PermissionVersion{},
			&types.Directory{},
			&types.DirectoryIdentity{},
			&types.DirectoryGroup{},
			&types.DirectoryGroupMembership{},
			&types.Session{},
			&types.SyncLog{},
			&repo.NextcloudSourcePairing{},
			&repo.NextcloudSourcePairingAbort{},
		),
	)
	require.NoError(t, db.Migrator().DropColumn(&types.Session{}, "nextcloud_scoped"))
	require.NoError(t, db.Exec("CREATE TABLE wiki_pages(tenant_id INTEGER,knowledge_base_id TEXT)").Error)
	require.NoError(
		t,
		db.Exec(
			"CREATE UNIQUE INDEX live_policies ON resource_access_policies(tenant_id,resource_type,resource_id)",
		).Error,
	)
	for _, name := range []string{"000031_nextcloud_source_versions", "000032_nextcloud_event_inbox", "000033_nextcloud_event_dispatch", "000046_nextcloud_content_leases", "000074_nextcloud_onboarding", "000076_nextcloud_directory_migrations", "000078_nextcloud_control_recovery"} {
		m, e := os.ReadFile("../../migrations/sqlite/" + name + ".up.sql")
		require.NoError(t, e)
		require.NoError(t, db.Exec(string(m)).Error)
	}
	require.NoError(
		t,
		db.Exec(
			("CREATE TABLE nextcloud_source_status_epochs(connection_id TEXT PRIMARY KEY" +
				",publication_epoch BIGINT,pair_operation_id TEXT,verified_at BIGINT)"),
		).Error,
	)
	now := time.Now().UTC()
	require.NoError(
		t,
		db.Create(
			&types.Directory{ID: f.Directory, Enabled: true, SnapshotVersion: 1, LastSuccessfulSyncAt: &now},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.DirectoryIdentity{
				ID:              "prelogin-owner",
				DirectoryID:     f.Directory,
				ObjectGUID:      f.GUID,
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 1,
			},
		).Error,
	)
	for n, token := range map[int]string{77: f.Token7, 78: f.Token8} {
		tenant := uint64(n)
		id := strconv.Itoa(n)
		storage := "storage-" + id
		require.NoError(
			t,
			db.Create(
				&types.Model{
					ID:       "embedding-" + id,
					TenantID: tenant,
					Type:     types.ModelTypeEmbedding,
					Status:   types.ModelStatusActive,
				},
			).Error,
		)
		require.NoError(
			t,
			db.Create(
				&types.StorageBackend{
					ID:       storage,
					TenantID: tenant,
					Name:     "local",
					Provider: "local",
					Status:   types.StorageBackendStatusActive,
				},
			).Error,
		)
		require.NoError(
			t,
			db.Create(
				&types.KnowledgeBase{
					ID:               "template-" + id,
					TenantID:         tenant,
					Name:             "Template",
					Type:             "document",
					EmbeddingModelID: "embedding-" + id,
					StorageBackendID: &storage,
					IndexingStrategy: types.DefaultIndexingStrategy(),
				},
			).Error,
		)
		h := sha256.Sum256([]byte(token))
		require.NoError(
			t,
			db.Create(
				&types.NextcloudOnboardingCredential{
					ID:                "live-" + id,
					TokenSHA256:       hex.EncodeToString(h[:]),
					InstanceID:        f.Instance,
					BaseURL:           f.BaseURL,
					TenantID:          tenant,
					DirectoryID:       f.Directory,
					TemplateKBID:      "template-" + id,
					Enabled:           true,
					MaxKnowledgeBases: 4,
				},
			).Error,
		)
		require.NoError(
			t,
			db.Create(
				&types.NextcloudOnboardingDelegation{TenantID: tenant, DirectoryID: f.Directory, ObjectGUID: f.GUID},
			).Error,
		)
	}
	store := repo.NewNextcloudOnboardingStore(db)
	pair := NewNextcloudSourcePairingHandler(repo.NewNextcloudSourcePairingRepository(db), nil)
	h := NewNextcloudOnboardingHandler(store, pair, repo.NewNextcloudEventInboxRepository(db))
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Next()
		if c.Writer.Status() >= 400 {
			t.Logf("controlled HTTP %s %s status=%d", c.Request.Method, c.FullPath(), c.Writer.Status())
		}
	})
	base := "/api/v1/integrations/nextcloud/onboarding"
	r.GET(base+"/candidates", h.Candidates)
	r.POST(base+"/operations", h.Create)
	r.GET(base+"/operations/:operation_id", h.Status)
	r.POST(base+"/operations/:operation_id/pair", h.Pair)
	r.PUT(base+"/operations/:operation_id/grants", h.Grants)
	r.POST(base+"/operations/:operation_id/root-migrations", h.RootMigration)
	r.POST(base+"/operations/:operation_id/target-migrations", h.TargetMigration)
	r.POST(base+"/operations/:operation_id/target-migrations/:migration_id/finalize", h.FinalizeTargetMigration)
	done := make(chan bool, 1)
	r.POST("/fixture/finish", func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer "+f.Token7 {
			c.AbortWithStatus(403)
			return
		}
		uid := "source-owner"
		require.NoError(
			t,
			db.Model(&types.DirectoryIdentity{}).Where("id='prelogin-owner'").Update("user_id", uid).Error,
		)
		var ops []types.NextcloudOnboardingOperation
		require.NoError(t, db.Order("created_at").Find(&ops).Error)
		require.Len(t, ops, 2)
		var migrations []repo.NextcloudTargetMigration
		require.NoError(t, db.Find(&migrations).Error)
		require.Len(t, migrations, 1)
		require.Equal(t, "active", migrations[0].State)
		var roots []repo.NextcloudRootMigration
		require.NoError(t, db.Find(&roots).Error)
		require.Len(t, roots, 1)
		require.Equal(t, "finalized", roots[0].State)
		old, e := store.ScopedMatches(context.Background(), uid, 77, "", time.Now())
		require.NoError(t, e)
		require.Empty(t, old)
		fresh, e := store.ScopedMatches(context.Background(), uid, 78, "", time.Now())
		require.NoError(t, e)
		require.Len(t, fresh, 1)
		var members int64
		require.NoError(t, db.Model(&types.TenantMember{}).Count(&members).Error)
		require.Zero(t, members)
		c.JSON(
			200,
			gin.H{"actual_pairing": true, "root_finalized": true, "target_finalized": true, "source_only_grants": true},
		)
		done <- true
	})
	r.POST("/fixture/abort", func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer "+f.Token7 {
			c.AbortWithStatus(403)
			return
		}
		c.JSON(200, gin.H{"aborted": true})
		done <- false
	})
	listener, e := net.Listen("tcp", "0.0.0.0:8089")
	require.NoError(t, e)
	server := &http.Server{Handler: r, ReadHeaderTimeout: 5 * time.Second}
	defer func() { require.NoError(t, server.Close()) }()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve controlled onboarding fixture: %v", err)
		}
	}()
	ready := fixture + ".ready"
	require.NoError(t, os.WriteFile(ready, []byte("ready"), 0o600))
	defer func() { require.NoError(t, os.Remove(ready)) }()
	select {
	case success := <-done:
		require.True(t, success, "owned NC phase failed; see retained controlled RPC diagnostics")
		t.Log(
			("Actual NC signed prepare/commit + WK scoped reserve/grants, same-root rena" +
				"me exact ACK, cross-tenant new-source transfer with empty stable manifest " +
				"passed; synthetic actor/model snapshot; no tenant membership created"),
		)
	case <-time.After(7 * time.Minute):
		t.Fatal("owned NC coordinator timed out")
	}
}
