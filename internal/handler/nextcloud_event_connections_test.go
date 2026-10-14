package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const testManagedEventPath = "/api/v1/datasource/ds-synthetic/nextcloud-event-connection"

func setupManagedNextcloudConnection(t *testing.T) (*gorm.DB, *gin.Engine, *NextcloudEventConnectionHandler) {
	t.Helper()
	db, router, _ := setupNextcloudEventHTTPTest(t)
	h := prepareManagedNextcloudConnection(t, db, router)
	return db, router, h
}

func prepareManagedNextcloudConnection(t *testing.T, db *gorm.DB, router *gin.Engine) *NextcloudEventConnectionHandler {
	t.Helper()
	createNextcloudEventSyncLogFixture(t, db)
	applyNextcloudEventDispatchMigration(t, db)
	require.NoError(t, db.Exec("ALTER TABLE knowledge_bases ADD COLUMN ever_had_nextcloud_source "+
		"BOOLEAN NOT NULL DEFAULT FALSE").Error)
	require.NoError(t, db.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = TRUE`).Error)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections SET status = 'revoked'`).Error)
	service := &stubDataSourceService{getDataSource: func(_ context.Context, id string) (*types.DataSource, error) {
		var ds types.DataSource
		if err := db.Where("id = ?", id).Take(&ds).Error; err != nil {
			return nil, err
		}
		return &ds, nil
	}}
	kbService := &stubKBServiceForDS{getByID: func(_ context.Context, id string) (*types.KnowledgeBase, error) {
		var kb types.KnowledgeBase
		if err := db.Where("id = ?", id).Take(&kb).Error; err != nil {
			return nil, err
		}
		return &kb, nil
	}}
	h := NewNextcloudEventConnectionHandler(repository.NewNextcloudEventInboxRepository(db),
		NewDataSourceHandler(service, kbService))
	h.inspect = func(context.Context, *types.DataSourceConfig) (nextcloud.PairingIdentity, error) {
		return nextcloud.PairingIdentity{
			InstanceID: testEventInstance, BindingID: testEventBinding,
			BaseURL: "https://nextcloud.example.test", PublicationState: "active", PublicationEpoch: 1,
		}, nil
	}
	router.Use(func(c *gin.Context) {
		if tenantID, ok := c.Request.Context().Value(types.TenantIDContextKey).(uint64); ok {
			c.Set(types.TenantIDContextKey.String(), tenantID)
		}
		c.Next()
	})
	router.POST("/api/v1/datasource/:id/nextcloud-event-connection", h.Pair)
	router.GET("/api/v1/datasource/:id/nextcloud-event-connection", h.Status)
	router.POST("/api/v1/datasource/:id/nextcloud-event-connection/rotate", h.Rotate)
	router.POST("/api/v1/datasource/:id/nextcloud-event-connection/rebind", h.Rebind)
	router.DELETE("/api/v1/datasource/:id/nextcloud-event-connection", h.Revoke)
	return h
}

func createNextcloudEventSyncLogFixture(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.NoError(t, db.Exec(`CREATE TABLE sync_logs (
		id TEXT PRIMARY KEY, data_source_id TEXT NOT NULL,
		-- All callers of this fixture use the synthetic tenant 7.
		tenant_id BIGINT NOT NULL DEFAULT 7,
		status TEXT NOT NULL, started_at TIMESTAMP NOT NULL,
		finished_at TIMESTAMP
	)`).Error)
}

func applyNextcloudEventDispatchMigration(t *testing.T, db *gorm.DB) {
	t.Helper()
	migrationPath := "../../migrations/sqlite/000033_nextcloud_event_dispatch.up.sql"
	if db.Name() == "postgres" {
		migrationPath = "../../migrations/versioned/000114_nextcloud_event_dispatch.up.sql"
	}
	migration, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	var statement strings.Builder
	inFunction, inSQLiteTrigger := false, false
	for _, line := range strings.Split(string(migration), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.HasPrefix(trimmed, "CREATE FUNCTION ") {
			inFunction = true
		}
		if db.Name() == "sqlite" && strings.HasPrefix(trimmed, "CREATE TRIGGER ") {
			inSQLiteTrigger = true
		}
		statement.WriteString(line)
		statement.WriteByte('\n')
		complete := strings.HasSuffix(trimmed, ";")
		if inFunction {
			complete = trimmed == "$nextcloud_sync$;"
		}
		if inSQLiteTrigger {
			complete = trimmed == "END;"
		}
		if complete {
			require.NoError(t, db.Exec(statement.String()).Error)
			statement.Reset()
			inFunction, inSQLiteTrigger = false, false
		}
	}
	require.Empty(t, strings.TrimSpace(statement.String()))
}

func managedConnectionRequest(router *gin.Engine, method, path string, tenantID uint64,
	role types.TenantRole,
) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	ctx := context.WithValue(req.Context(), types.TenantIDContextKey, tenantID)
	ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
	req = req.WithContext(ctx)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	return response
}

func issuedEventCredential(t *testing.T,
	response *httptest.ResponseRecorder,
) repository.NextcloudEventConnectionCredential {
	t.Helper()
	var payload struct {
		repository.NextcloudEventConnectionCredential
		ReceiverURL string `json:"receiver_url"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
	require.Equal(t, nextcloudEventPath, payload.ReceiverURL)
	require.Equal(t, testEventInstance, payload.NextcloudInstanceID)
	require.Equal(t, testEventBinding, payload.BindingID)
	require.Len(t, payload.Secret, 43)
	return payload.NextcloudEventConnectionCredential
}

func TestNextcloudEventConnectionPairRotateRevoke(t *testing.T) {
	db, router, _ := setupManagedNextcloudConnection(t)
	denied := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleViewer)
	require.Equal(t, http.StatusForbidden, denied.Code, denied.Body.String())
	denied = managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 8, types.TenantRoleAdmin)
	require.Equal(t, http.StatusForbidden, denied.Code, denied.Body.String())
	apiKeyRequest := httptest.NewRequest(http.MethodPost, testManagedEventPath, nil)
	apiKeyContext := context.WithValue(apiKeyRequest.Context(), types.TenantIDContextKey, uint64(7))
	apiKeyContext = context.WithValue(apiKeyContext, types.TenantRoleContextKey, types.TenantRoleOwner)
	apiKeyContext = types.WithTenantAPIKeyScope(apiKeyContext, types.TenantAPIKeyScope{FullAccess: true})
	denied = postNextcloudEvent(router, apiKeyRequest.WithContext(apiKeyContext))
	require.Equal(t, http.StatusForbidden, denied.Code, denied.Body.String())
	var count int64
	require.NoError(t, db.Table("nextcloud_event_connections").Where("status = 'active'").Count(&count).Error)
	require.Zero(t, count)

	paired := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusCreated, paired.Code, paired.Body.String())
	first := issuedEventCredential(t, paired)
	pairedBatch := func(after, eventID string) nextcloudEventRequest {
		batch := sampleNextcloudEventBatch(after, eventID)
		batch.ConnectionID = first.ConnectionID
		return batch
	}
	for _, operation := range []struct{ method, path string }{
		{http.MethodGet, testManagedEventPath},
		{http.MethodPost, testManagedEventPath + "/rotate"},
		{http.MethodDelete, testManagedEventPath},
	} {
		viewer := managedConnectionRequest(router, operation.method, operation.path, 7, types.TenantRoleViewer)
		require.Equal(t, http.StatusForbidden, viewer.Code, viewer.Body.String())
	}
	require.NotContains(t, paired.Body.String(), "enc:v1:")
	var ciphertext string
	require.NoError(t, db.Raw("SELECT current_secret_ciphertext FROM "+
		"nextcloud_event_connections WHERE connection_id = ?", first.ConnectionID).Scan(&ciphertext).Error)
	require.True(t, strings.HasPrefix(ciphertext, utils.EncPrefix))
	require.NotContains(t, ciphertext, first.Secret)
	plaintext, err := utils.DecryptStoredSecret(ciphertext)
	require.NoError(t, err)
	require.Equal(t, first.Secret, plaintext)
	status := managedConnectionRequest(router, http.MethodGet, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, status.Code, status.Body.String())
	require.Contains(t, status.Body.String(), `"received_through_event_id":"0"`)
	require.Contains(t, status.Body.String(), `"dispatched_through_event_id":"0"`)
	require.Contains(t, status.Body.String(), `"applied_through_event_id":"0"`)
	require.Contains(t, status.Body.String(), `"backlog_count":0`)
	require.Contains(t, status.Body.String(), `"undispatched_count":0`)
	require.Contains(t, status.Body.String(), `"dispatch_state":"idle"`)
	require.NotContains(t, status.Body.String(), `"secret"`)
	require.NotContains(t, status.Body.String(), first.Secret)
	repeat := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusConflict, repeat.Code, repeat.Body.String())
	require.NotContains(t, repeat.Body.String(), first.Secret)

	batch := pairedBatch("0", "1")
	response := postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, first.Secret, first.KeyID,
		batch, strings.Repeat("a", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	rotatedAt := time.Now().UTC()
	rotated := managedConnectionRequest(router, http.MethodPost, testManagedEventPath+"/rotate", 7,
		types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, rotated.Code, rotated.Body.String())
	second := issuedEventCredential(t, rotated)
	require.Equal(t, first.ConnectionID, second.ConnectionID)
	require.NotEqual(t, first.KeyID, second.KeyID)
	require.NotEqual(t, first.Secret, second.Secret)
	var previous struct {
		PreviousKeyID      string
		PreviousValidUntil time.Time
	}
	require.NoError(t, db.Table("nextcloud_event_connections").
		Select("previous_key_id, previous_valid_until").
		Where("connection_id = ?", first.ConnectionID).Take(&previous).Error)
	require.Equal(t, first.KeyID, previous.PreviousKeyID)
	require.WithinDuration(t, rotatedAt.Add(2*time.Minute), previous.PreviousValidUntil, 2*time.Second)
	batch = pairedBatch("1", "2")
	oldNonce := strings.Repeat("b", 32)
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, first.Secret, first.KeyID,
		batch, oldNonce))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, first.Secret, first.KeyID,
		batch, oldNonce))
	require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
	wrongScope := pairedBatch("2", "3")
	wrongScope.BindingID = "other-binding"
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, first.Secret, first.KeyID,
		wrongScope, strings.Repeat("c", 32)))
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	var originalConfig string
	require.NoError(t, db.Raw("SELECT config FROM data_sources WHERE id = ?",
		"ds-synthetic").Scan(&originalConfig).Error)
	changedConfig := syntheticNextcloudEventDataSourceConfig(t, "https://different.example.test", true)
	require.NoError(t, db.Exec("UPDATE data_sources SET config = ? WHERE id = ?", string(changedConfig),
		"ds-synthetic").Error)
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, first.Secret, first.KeyID,
		pairedBatch("2", "3"), strings.Repeat("5", 32)))
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	require.NoError(t, db.Exec(`UPDATE data_sources SET config = ? WHERE id = ?`, originalConfig, "ds-synthetic").Error)
	batch = pairedBatch("2", "3")
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, second.Secret, second.KeyID,
		batch, strings.Repeat("d", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())

	rotated = managedConnectionRequest(router, http.MethodPost, testManagedEventPath+"/rotate", 7,
		types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, rotated.Code, rotated.Body.String())
	third := issuedEventCredential(t, rotated)
	require.Equal(t, first.ConnectionID, third.ConnectionID)
	batch = pairedBatch("3", "4")
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, first.Secret, first.KeyID,
		batch, strings.Repeat("e", 32)))
	require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, second.Secret, second.KeyID,
		batch, strings.Repeat("f", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	batch = pairedBatch("4", "5")
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, third.Secret, third.KeyID,
		batch, strings.Repeat("1", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())

	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET previous_valid_until = ? WHERE connection_id = ?`,
		time.Now().UTC().Add(-time.Minute), first.ConnectionID).Error)
	batch = pairedBatch("5", "6")
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, second.Secret, second.KeyID,
		batch, strings.Repeat("2", 32)))
	require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, third.Secret, third.KeyID,
		batch, strings.Repeat("3", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())

	revoked := managedConnectionRequest(router, http.MethodDelete, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusNoContent, revoked.Code, revoked.Body.String())
	batch = pairedBatch("6", "7")
	response = postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, third.Secret, third.KeyID,
		batch, strings.Repeat("4", 32)))
	require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
	newPair := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusCreated, newPair.Code, newPair.Body.String())
	require.NotEqual(t, first.ConnectionID, issuedEventCredential(t, newPair).ConnectionID)
}

func TestNextcloudEventConnectionRejectsWrongBindingAndChangedSource(t *testing.T) {
	t.Run("wrong live binding", func(t *testing.T) {
		db, router, h := setupManagedNextcloudConnection(t)
		h.inspect = func(context.Context, *types.DataSourceConfig) (nextcloud.PairingIdentity, error) {
			return nextcloud.PairingIdentity{
				InstanceID: testEventInstance, BindingID: "different-binding",
				BaseURL: "https://nextcloud.example.test",
			}, nil
		}
		response := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
		require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
		var count int64
		require.NoError(t, db.Table("nextcloud_event_connections").Where("status = 'active'").Count(&count).Error)
		require.Zero(t, count)
	})
	for _, change := range []struct {
		name string
		url  string
		cred bool
	}{
		{"endpoint edit", "https://changed.example.test", true},
		{"credential clear", "https://nextcloud.example.test", false},
		{"credential reprovision", "https://nextcloud.example.test", true},
	} {
		t.Run(change.name, func(t *testing.T) {
			db, router, h := setupManagedNextcloudConnection(t)
			h.inspect = func(context.Context, *types.DataSourceConfig) (nextcloud.PairingIdentity, error) {
				config := syntheticNextcloudEventDataSourceConfig(t, change.url, change.cred)
				require.NoError(t, db.Exec("UPDATE data_sources SET config = ? WHERE id = ?",
					string(config), "ds-synthetic").Error)
				return nextcloud.PairingIdentity{
					InstanceID: testEventInstance, BindingID: testEventBinding,
					BaseURL: "https://nextcloud.example.test",
				}, nil
			}
			response := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7,
				types.TenantRoleAdmin)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			var count int64
			require.NoError(t, db.Table("nextcloud_event_connections").Where("status = 'active'").Count(&count).Error)
			require.Zero(t, count)
		})
	}
	for _, invalid := range []string{"marker", "other source"} {
		t.Run(invalid, func(t *testing.T) {
			db, router, _ := setupManagedNextcloudConnection(t)
			if invalid == "marker" {
				require.NoError(t, db.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = FALSE`).Error)
			} else {
				require.NoError(t, db.Exec(`INSERT INTO data_sources
					(id, tenant_id, knowledge_base_id, type, config, deleted_at)
					VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`, "other-source", 7, "kb-synthetic", "rss", "{}").Error)
			}
			response := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7,
				types.TenantRoleAdmin)
			require.Equal(t, http.StatusConflict, response.Code, fmt.Sprintf("%s: %s", invalid, response.Body.String()))
		})
	}
}

func TestNextcloudEventConnectionRevocationSurvivesCredentialDrift(t *testing.T) {
	db, router, _ := setupManagedNextcloudConnection(t)
	paired := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusCreated, paired.Code, paired.Body.String())
	credential := issuedEventCredential(t, paired)
	reprovisioned := syntheticNextcloudEventDataSourceConfig(t, "https://nextcloud.example.test", true)
	require.NoError(t, db.Exec("UPDATE data_sources SET config = ? WHERE id = ?", string(reprovisioned),
		"ds-synthetic").Error)
	rotation := managedConnectionRequest(router, http.MethodPost, testManagedEventPath+"/rotate", 7,
		types.TenantRoleAdmin)
	require.Equal(t, http.StatusConflict, rotation.Code, rotation.Body.String())
	config := syntheticNextcloudEventDataSourceConfig(t, "https://changed.example.test", false)
	require.NoError(t, db.Exec(`UPDATE data_sources SET config = ? WHERE id = ?`, string(config), "ds-synthetic").Error)
	status := managedConnectionRequest(router, http.MethodGet, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, status.Code, status.Body.String())
	require.Contains(t, status.Body.String(), `"status":"source_changed"`)
	require.NotContains(t, status.Body.String(), credential.Secret)
	rotation = managedConnectionRequest(router, http.MethodPost, testManagedEventPath+"/rotate", 7,
		types.TenantRoleAdmin)
	require.Equal(t, http.StatusConflict, rotation.Code, rotation.Body.String())
	revoked := managedConnectionRequest(router, http.MethodDelete, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusNoContent, revoked.Code, revoked.Body.String())
	batch := sampleNextcloudEventBatch("0", "1")
	batch.ConnectionID = credential.ConnectionID
	response := postNextcloudEvent(router, signedNextcloudEventRequestWithKey(t, credential.Secret,
		credential.KeyID, batch, strings.Repeat("e", 32)))
	require.Equal(t, http.StatusUnauthorized, response.Code, response.Body.String())
}

func TestNextcloudEventConnectionPostgresLifecycle(t *testing.T) {
	dsn := os.Getenv("NEXTCLOUD_EVENT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NEXTCLOUD_EVENT_TEST_POSTGRES_DSN for isolated PostgreSQL connection lifecycle test")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal("connect isolated PostgreSQL event test database failed")
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	random := make([]byte, 6)
	_, err = rand.Read(random)
	require.NoError(t, err)
	schema := "nextcloud_event_pair_test_" + hex.EncodeToString(random)
	require.NoError(t, db.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() { _ = db.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	require.NoError(t, db.Exec("SET search_path TO "+schema).Error)
	db, router, _ := prepareNextcloudEventHTTPTest(t, db,
		"../../migrations/versioned/000113_nextcloud_event_inbox.up.sql")
	prepareManagedNextcloudConnection(t, db, router)

	paired := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusCreated, paired.Code, paired.Body.String())
	first := issuedEventCredential(t, paired)
	repeat := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusConflict, repeat.Code, repeat.Body.String())
	rotated := managedConnectionRequest(router, http.MethodPost, testManagedEventPath+"/rotate", 7,
		types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, rotated.Code, rotated.Body.String())
	second := issuedEventCredential(t, rotated)
	require.Equal(t, first.ConnectionID, second.ConnectionID)
	require.NotEqual(t, first.KeyID, second.KeyID)
	batch := sampleNextcloudEventBatch("0", "101")
	batch.ConnectionID = second.ConnectionID
	require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
		signedNextcloudEventRequestWithKey(t, first.Secret, first.KeyID, batch, strings.Repeat("1", 32))).Code)
	require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
		signedNextcloudEventRequestWithKey(t, second.Secret, second.KeyID, batch, strings.Repeat("2", 32))).Code)
	status := managedConnectionRequest(router, http.MethodGet, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusOK, status.Code, status.Body.String())
	require.Contains(t, status.Body.String(), `"received_through_event_id":"101"`)
	require.Contains(t, status.Body.String(), `"dispatched_through_event_id":"0"`)
	require.Contains(t, status.Body.String(), `"applied_through_event_id":"0"`)
	require.Contains(t, status.Body.String(), `"backlog_count":1`)
	require.Contains(t, status.Body.String(), `"undispatched_count":1`)
	require.NotContains(t, status.Body.String(), second.Secret)
	revoked := managedConnectionRequest(router, http.MethodDelete, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusNoContent, revoked.Code, revoked.Body.String())
	newPair := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
	require.Equal(t, http.StatusCreated, newPair.Code, newPair.Body.String())
	require.NotEqual(t, first.ConnectionID, issuedEventCredential(t, newPair).ConnectionID)
}
