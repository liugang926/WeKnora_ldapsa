//go:build sqlite_fts5

package handler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	storagefile "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Uses the producer test's actual database and file artifacts while that
// process keeps its original-body journal and source generation alive.
// The WK login context is controlled; source identity and READ use live NC/AD.
func TestNextcloudProcessedLiveHTTPReadAndRevocation(t *testing.T) {
	path := os.Getenv("WEKNORA_DOCUMENT_GRAPH_LIVE_FIXTURE")
	if path == "" || os.Getenv("WEKNORA_DOCUMENT_GRAPH_HTTP") != "1" {
		t.Skip("owned live producer required")
	}
	var input struct {
		Database        string            `json:"database"`
		FileRoot        string            `json:"file_root"`
		KnowledgeID     string            `json:"knowledge_id"`
		KnowledgeBaseID string            `json:"knowledge_base_id"`
		UserID          string            `json:"user_id"`
		BodySHA256      string            `json:"body_sha256"`
		Environment     map[string]string `json:"environment"`
	}
	raw, err := os.ReadFile(path + ".http-input")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &input))
	for name, value := range input.Environment {
		t.Setenv(name, value)
	}
	sqlite_vec.Auto()
	db, err := gorm.Open(sqlite.Open(input.Database+"?_busy_timeout=5000"), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	defer sqlDB.Close()
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, repository.InstallOriginalBodyRetentionCallbacks(db))
	require.NoError(t, repository.NewOriginalBodyRetentionStore(db).VerifyJournalReady())
	repo := repository.NewKnowledgeRepository(db)
	sources := repository.NewDataSourceRepository(db)
	kbs := collectionHTTPKB{db: db}
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	publication := access.NewNextcloudPublicationGuard(repository.NewDirectoryRepository(db), sources)
	store := repository.NewKnowledgeInputProvenanceStore(db)
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	repository.ConfigureKnowledgeCurrentReadActor(repo, store)
	guard := access.NewNextcloudSourceLineageGuard(repo.(access.NextcloudLineageSnapshotLookup), kbs, nil, nil, groups, publication, store)
	repository.ConfigureKnowledgeOriginalInputSourceVerifier(store, service.NewKnowledgeOriginalInputSourceVerifier(store, guard, sources))
	catalog := service.NewResourceCatalog(repository.NewResourceRepository(db))
	files := storagefile.NewResourceCatalogFileService(storagefile.NewBackendScopedFileService("machine-local", storagefile.NewLocalFileService(input.FileRoot, "")), catalog)
	leases := repository.NewNextcloudContentLeaseStore(db)
	kg, err := service.NewKnowledgeService(nil, repo, nil, kbs, nil, nil, nil, nil, nil, nil, files, nil, catalog, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, publication, leases)
	require.NoError(t, err)
	h := &KnowledgeHandler{kgService: kg, kbService: kbs}
	ConfigureKnowledgeHandlerGroupAccess(h, groups)
	ConfigureKnowledgeHandlerPublicationGuard(h, publication)
	ConfigureKnowledgeHandlerContentLeaseStore(h, leases)
	ConfigureKnowledgeHandlerOutputLineage(h, service.NewKnowledgeOutputAuthorizer(store, guard))
	ConfigureKnowledgeHandlerCollectionOutput(h, service.NewKnowledgeCollectionOutputAuthorizer(store, guard))
	gin.SetMode(gin.TestMode)
	human := true
	router := gin.New()
	router.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := types.WithCaller(c.Request.Context(), types.Caller{TenantID: 7, UserID: input.UserID, Role: types.TenantRoleAdmin})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: input.UserID})
		if !human {
			ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, Role: types.TenantRoleAdmin})
			ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
		}
		ctx = types.WithExecutionTenant(ctx, 7)
		c.Set(types.TenantIDContextKey.String(), uint64(7))
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	router.GET("/knowledge/:id", h.GetKnowledge)
	router.GET("/knowledge/:id/preview", h.PreviewKnowledgeFile)
	router.GET("/knowledge/:id/download", h.DownloadKnowledgeFile)
	router.GET("/knowledge-bases/:id/knowledge", h.ListKnowledge)
	baseline := map[string][]string{}
	for _, table := range []string{"nextcloud_content_leases", "original_body_leases"} {
		var ids []string
		require.NoError(t, db.Table(table).Where("released_at_ms IS NULL AND expires_at_ms > ?", time.Now().UnixMilli()).Pluck("lease_id", &ids).Error)
		baseline[table] = ids
	}
	routes := []struct {
		Name, URL string
		File      bool
	}{
		{"detail", "/knowledge/" + input.KnowledgeID, false},
		{"list", "/knowledge-bases/" + input.KnowledgeBaseID + "/knowledge", false},
		{"preview", "/knowledge/" + input.KnowledgeID + "/preview", true},
		{"download", "/knowledge/" + input.KnowledgeID + "/download", true},
	}
	for _, route := range routes {
		t.Run("before_"+route.Name, func(t *testing.T) {
			w := &closeNotifyRecorder{ResponseRecorder: httptest.NewRecorder()}
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, route.URL, nil))
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			if route.File {
				sum := sha256.Sum256(w.Body.Bytes())
				require.Equal(t, input.BodySHA256, hex.EncodeToString(sum[:]))
				require.Equal(t, "private, no-store", w.Header().Get("Cache-Control"))
			} else {
				require.Contains(t, w.Body.String(), input.KnowledgeID)
			}
		})
	}
	if t.Failed() {
		return
	}
	human = false
	for _, route := range routes {
		t.Run("without_person_"+route.Name, func(t *testing.T) {
			w := &closeNotifyRecorder{ResponseRecorder: httptest.NewRecorder()}
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, route.URL, nil))
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), input.KnowledgeID)
		})
	}
	human = true
	if t.Failed() {
		return
	}
	require.NoError(t, os.WriteFile(path+".http-positive", []byte("ready"), 0600))
	require.Eventually(t, func() bool { _, err := os.Stat(path + ".revoked"); return err == nil }, 60*time.Second, 100*time.Millisecond)
	for _, route := range routes {
		t.Run("revoked_"+route.Name, func(t *testing.T) {
			w := &closeNotifyRecorder{ResponseRecorder: httptest.NewRecorder()}
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, route.URL, nil))
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), input.KnowledgeID)
		})
	}
	if t.Failed() {
		return
	}
	// The producer retains its own output guards; only its pre-existing lease
	// IDs may remain. Newly acquired HTTP leases must all be released.
	require.Eventually(t, func() bool { _, err := os.Stat(path + ".source-checked"); return err == nil }, 30*time.Second, 100*time.Millisecond)
	for table, ids := range baseline {
		query := db.Table(table).Where("released_at_ms IS NULL AND expires_at_ms > ?", time.Now().UnixMilli())
		if len(ids) > 0 {
			query = query.Where("lease_id NOT IN ?", ids)
		}
		var count int64
		require.NoError(t, query.Count(&count).Error)
		require.Zero(t, count, table)
	}
	require.NoError(t, os.WriteFile(path+".http-done", []byte("passed"), 0600))
}
