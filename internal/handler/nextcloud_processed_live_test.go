//go:build sqlite_fts5

package handler

import (
	"context"
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
	"github.com/Tencent/WeKnora/internal/config"
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
// The baseline WK login context is controlled; optional JWT cases use the
// production token and authentication services. Source READ uses live NC/AD.
func TestNextcloudProcessedLiveHTTPReadAndRevocation(t *testing.T) {
	path := os.Getenv("WEKNORA_DOCUMENT_GRAPH_LIVE_FIXTURE")
	if path == "" || os.Getenv("WEKNORA_DOCUMENT_GRAPH_HTTP") != "1" {
		t.Skip("owned live producer required")
	}
	var input struct {
		Database                  string            `json:"database"`
		FileRoot                  string            `json:"file_root"`
		KnowledgeID               string            `json:"knowledge_id"`
		KnowledgeBaseID           string            `json:"knowledge_base_id"`
		HistorySessionID          string            `json:"history_session_id"`
		HistoryVectorKBID         string            `json:"history_vector_kb_id"`
		HistoryVectorKnowledgeID  string            `json:"history_vector_knowledge_id"`
		HistoryVectorEmbeddingID  string            `json:"history_vector_embedding_model_id"`
		HistoryVectorAnswerSHA256 string            `json:"history_vector_answer_sha256"`
		UserID                    string            `json:"user_id"`
		BodySHA256                string            `json:"body_sha256"`
		Environment               map[string]string `json:"environment"`
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
	// Material reads resolve current authority on their existing SQL
	// transaction, using the same factory as the production container.
	repository.ConfigureKnowledgeDisplayInputWriters(store, nil, repo, func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(actual, service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
		return actual
	})
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
	var checkHistorySearch func(bool)
	var jwtRouter *gin.Engine
	var accessToken, refreshToken string
	var jwtAuthenticated bool
	var jwtCaller types.Caller
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_JWT") == "1" {
		// Production token issuance, SQL token/user/directory lookups and Auth.
		// The pre-existing fixture member remains Admin; no new role is granted.
		require.NoError(t, db.AutoMigrate(&types.AuthToken{}))
		users := repository.NewUserRepository(db)
		tokens := repository.NewAuthTokenRepository(db)
		tenants := service.NewTenantService(repository.NewTenantRepository(db), nil)
		members := service.NewTenantMemberService(repository.NewTenantMemberRepository(db), nil, users, tokens)
		enforced := true
		cfg := &config.Config{Tenant: &config.TenantConfig{EnableRBAC: &enforced}}
		userService := service.NewUserService(cfg, users, tokens, tenants, members, nil, repository.NewDirectoryRepository(db), groups)
		user, err := users.GetUserByID(context.Background(), input.UserID)
		require.NoError(t, err)
		accessToken, refreshToken, err = userService.GenerateTokens(context.Background(), user)
		require.NoError(t, err)
		validated, tenantID, err := userService.ValidateToken(context.Background(), accessToken)
		require.NoError(t, err)
		require.Equal(t, input.UserID, validated.ID)
		require.Equal(t, uint64(7), tenantID)
		jwtRouter = gin.New()
		jwtRouter.Use(middleware.ErrorHandler(), middleware.Auth(tenants, userService, members, nil, cfg, groups), func(c *gin.Context) {
			jwtCaller = types.CallerFromContext(c.Request.Context())
			jwtAuthenticated = true
			c.Next()
		})
		jwtRouter.GET("/knowledge/:id", h.GetKnowledge)
		jwtRouter.GET("/knowledge/:id/preview", h.PreviewKnowledgeFile)
		jwtRouter.GET("/knowledge/:id/download", h.DownloadKnowledgeFile)
		jwtRouter.GET("/knowledge-bases/:id/knowledge", h.ListKnowledge)
		if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_SEARCH") == "1" || os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_VECTOR") == "1" {
			var vector *documentHistoryVectorSpec
			if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_VECTOR") == "1" {
				vector = &documentHistoryVectorSpec{KnowledgeBaseID: input.HistoryVectorKBID, KnowledgeID: input.HistoryVectorKnowledgeID, EmbeddingModelID: input.HistoryVectorEmbeddingID, AnswerSHA256: input.HistoryVectorAnswerSHA256}
			}
			checkHistorySearch = addDocumentLiveHistorySearch(t, jwtRouter, db, store, guard, publication, sources, kbs, kg, input.HistorySessionID, accessToken, vector)
		}
	}
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
	sourceSentinels := []string{"Alice", "Bob", "Project Cedar"}
	checkJWT := func(stage string, status int, bearer string, authenticated bool) {
		if jwtRouter == nil {
			return
		}
		for _, route := range routes {
			t.Run("jwt_"+stage+"_"+route.Name, func(t *testing.T) {
				jwtAuthenticated = false
				request := httptest.NewRequest(http.MethodGet, route.URL, nil)
				if bearer != "" {
					request.Header.Set("Authorization", "Bearer "+bearer)
				}
				w := &closeNotifyRecorder{ResponseRecorder: httptest.NewRecorder()}
				jwtRouter.ServeHTTP(w, request)
				require.Equal(t, status, w.Code, w.Body.String())
				require.Equal(t, authenticated, jwtAuthenticated, "verify whether production authentication reached the content guard")
				if authenticated {
					require.Equal(t, input.UserID, jwtCaller.UserID)
					require.Equal(t, uint64(7), jwtCaller.TenantID)
					require.Equal(t, types.TenantRoleAdmin, jwtCaller.Role)
				}
				if status == http.StatusOK {
					if route.File {
						sum := sha256.Sum256(w.Body.Bytes())
						require.Equal(t, input.BodySHA256, hex.EncodeToString(sum[:]))
						require.Equal(t, "private, no-store", w.Header().Get("Cache-Control"))
						for _, sentinel := range sourceSentinels {
							require.Contains(t, w.Body.String(), sentinel)
						}
					} else {
						require.Contains(t, w.Body.String(), input.KnowledgeID)
					}
				} else {
					require.NotContains(t, w.Body.String(), input.KnowledgeID)
					for _, sentinel := range sourceSentinels {
						require.NotContains(t, w.Body.String(), sentinel)
					}
				}
			})
		}
	}
	if checkHistorySearch != nil {
		checkHistorySearch(false)
	}
	checkJWT("before", http.StatusOK, accessToken, true)
	checkJWT("missing", http.StatusUnauthorized, "", false)
	checkJWT("invalid", http.StatusUnauthorized, "invalid-owned-fixture-token", false)
	checkJWT("refresh", http.StatusUnauthorized, refreshToken, false)
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
			for _, sentinel := range sourceSentinels {
				require.NotContains(t, w.Body.String(), sentinel)
			}
		})
	}
	human = true
	if t.Failed() {
		return
	}
	require.NoError(t, os.WriteFile(path+".http-positive", []byte("ready"), 0600))
	revocationMarker := path + ".revoked"
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_VECTOR") == "1" {
		// The two test binaries share one SQLite file. Finish JWT denial
		// requests before the producer's fresh denial requests, so their
		// write transactions do not compete during this sequential profile.
		revocationMarker = path + ".http-revoked"
	}
	require.Eventually(t, func() bool { _, err := os.Stat(revocationMarker); return err == nil }, 60*time.Second, 100*time.Millisecond)
	for _, route := range routes {
		t.Run("revoked_"+route.Name, func(t *testing.T) {
			w := &closeNotifyRecorder{ResponseRecorder: httptest.NewRecorder()}
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, route.URL, nil))
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), input.KnowledgeID)
			for _, sentinel := range sourceSentinels {
				require.NotContains(t, w.Body.String(), sentinel)
			}
		})
	}
	if t.Failed() {
		return
	}
	// Reuse the exact pre-revocation token. Auth must still accept the current
	// active member, while the actual source guard refuses its stale authority.
	checkJWT("source_revoked", http.StatusForbidden, accessToken, true)
	if checkHistorySearch != nil {
		checkHistorySearch(true)
	}
	if t.Failed() {
		return
	}
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_VECTOR") == "1" {
		require.NoError(t, os.WriteFile(path+".http-revocation-checked", []byte("passed"), 0600))
	}
	// The producer retains its own output guards; only its pre-existing lease
	// IDs may remain. Newly acquired HTTP leases must all be released.
	sourceWait := 30 * time.Second
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_SEARCH") == "1" || os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_VECTOR") == "1" {
		sourceWait = 180 * time.Second
	}
	require.Eventually(t, func() bool { _, err := os.Stat(path + ".source-checked"); return err == nil }, sourceWait, 100*time.Millisecond)
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
