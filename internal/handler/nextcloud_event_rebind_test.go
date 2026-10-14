package handler

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupManagedRebindTest(t *testing.T, dialect string) (*gorm.DB, *gin.Engine, *NextcloudEventConnectionHandler) {
	t.Helper()
	if dialect == "sqlite" {
		db, router, h := setupManagedNextcloudConnection(t)
		require.NoError(t, db.AutoMigrate(&repository.NextcloudSourceRotation{}))
		return db, router, h
	}
	dsn := os.Getenv("NEXTCLOUD_EVENT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NEXTCLOUD_EVENT_TEST_POSTGRES_DSN for isolated PostgreSQL rebind test")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	random := make([]byte, 6)
	_, err = rand.Read(random)
	require.NoError(t, err)
	schema := "nextcloud_event_rebind_" + hex.EncodeToString(random)
	require.NoError(t, db.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() { _ = db.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	require.NoError(t, db.Exec("SET search_path TO "+schema).Error)
	db, router, _ := prepareNextcloudEventHTTPTest(t, db,
		"../../migrations/versioned/000113_nextcloud_event_inbox.up.sql")
	h := prepareManagedNextcloudConnection(t, db, router)
	require.NoError(t, db.AutoMigrate(&repository.NextcloudSourceRotation{}))
	return db, router, h
}

func managedRebindRequest(router *gin.Engine, rotationID string, tenantID uint64,
	role types.TenantRole, apiKey bool,
) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]string{"operation_id": rotationID})
	req := httptest.NewRequest(http.MethodPost, testManagedEventPath+"/rebind", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx := context.WithValue(req.Context(), types.TenantIDContextKey, tenantID)
	ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
	if apiKey {
		ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{FullAccess: true})
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req.WithContext(ctx))
	return recorder
}

func finalizeManagedEventSourceRotation(t *testing.T, db *gorm.DB) repository.NextcloudSourceRotation {
	t.Helper()
	var pair repository.NextcloudSourcePairing
	require.NoError(t, db.Where("tenant_id = ? AND datasource_id = ?", 7, "ds-synthetic").Take(&pair).Error)
	// The generic event fixture predates source rotation and names the old
	// source key differently from its data-source config. Align that fixture.
	require.NoError(t, db.Model(&pair).Update("key_id", "default").Error)
	pair.KeyID = "default"
	id := uuid.NewString()
	proposed := repository.NextcloudSourceRotation{
		OperationID: id, PairOperationID: pair.OperationID, TenantID: pair.TenantID,
		KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
		InstanceID: pair.InstanceID, BindingID: pair.BindingID,
		NewKeyID: "rot_" + strings.ReplaceAll(id, "-", ""),
	}
	repo := repository.NewNextcloudSourcePairingRepository(db)
	rotation, created, err := repo.PrepareSourceRotation(context.Background(), proposed, "rotated-source-machine-token")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, repo.SwitchSourceRotation(context.Background(), rotation))
	require.NoError(t, repo.FinalizeSourceRotation(context.Background(), rotation))
	require.NoError(t, db.Where("operation_id = ?", rotation.OperationID).Take(&rotation).Error)
	require.Equal(t, "finalized", rotation.State)
	return rotation
}

func TestNextcloudEventRebindFinalizedRotationHTTPAndRepository(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			db, router, h := setupManagedRebindTest(t, dialect)
			paired := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
			require.Equal(t, http.StatusCreated, paired.Code, paired.Body.String())
			credential := issuedEventCredential(t, paired)
			batch := sampleNextcloudEventBatch("0", "101")
			batch.ConnectionID = credential.ConnectionID
			require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
				signedNextcloudEventRequestWithKey(t, credential.Secret, credential.KeyID,
					batch, strings.Repeat("a", 32))).Code)
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch SET dispatched_id = 101,
				applied_id = 101, target_event_id = 101 WHERE connection_id = ?`, credential.ConnectionID).Error)
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_inbox SET state = 'applied'
				WHERE connection_id = ? AND event_id = 101`, credential.ConnectionID).Error)
			var original struct {
				KeyID      string `gorm:"column:current_key_id"`
				Ciphertext string `gorm:"column:current_secret_ciphertext"`
			}
			require.NoError(t,
				db.Table("nextcloud_event_connections").Select("current_key_id, current_secret_ciphertext").
					Where("connection_id = ?", credential.ConnectionID).Take(&original).Error)
			pending := sampleNextcloudEventBatch("101", "102")
			pending.ConnectionID = credential.ConnectionID
			require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
				signedNextcloudEventRequestWithKey(t, credential.Secret, credential.KeyID,
					pending, strings.Repeat("d", 32))).Code)
			rotation := finalizeManagedEventSourceRotation(t, db)
			originalInspect := h.inspect
			for _, live := range []struct {
				name, state string
				epoch       int64
			}{
				{"withdrawn", "withdrawn", 1},
				{"new publication epoch", "active", 2},
			} {
				t.Run(live.name, func(t *testing.T) {
					h.inspect = func(ctx context.Context,
						cfg *types.DataSourceConfig,
					) (nextcloud.PairingIdentity, error) {
						identity, err := originalInspect(ctx, cfg)
						identity.PublicationState = live.state
						identity.PublicationEpoch = live.epoch
						return identity, err
					}
					response := managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
					require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
				})
			}
			h.inspect = originalInspect
			stale := sampleNextcloudEventBatch("102", "103")
			stale.ConnectionID = credential.ConnectionID
			require.Equal(t, http.StatusForbidden, postNextcloudEvent(router,
				signedNextcloudEventRequestWithKey(t, credential.Secret, credential.KeyID,
					stale, strings.Repeat("b", 32))).Code)
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch SET state = 'blocked',
				last_error_code = 'source_changed' WHERE connection_id = ?`, credential.ConnectionID).Error)
			for _, invalid := range []struct {
				name   string
				id     string
				tenant uint64
				role   types.TenantRole
				apiKey bool
				status int
			}{
				{"viewer", rotation.OperationID, 7, types.TenantRoleViewer, false, http.StatusForbidden},
				{"wrong tenant", rotation.OperationID, 8, types.TenantRoleAdmin, false, http.StatusForbidden},
				{"api key", rotation.OperationID, 7, types.TenantRoleOwner, true, http.StatusForbidden},
				{"wrong rotation", uuid.NewString(), 7, types.TenantRoleAdmin, false, http.StatusConflict},
			} {
				t.Run(invalid.name, func(t *testing.T) {
					response := managedRebindRequest(router, invalid.id, invalid.tenant, invalid.role, invalid.apiKey)
					require.Equal(t, invalid.status, response.Code, response.Body.String())
				})
			}
			for _, state := range []string{"pending", "switched", "aborted"} {
				require.NoError(t, db.Exec(`UPDATE nextcloud_source_rotations SET state = ? WHERE operation_id = ?`,
					state, rotation.OperationID).Error)
				response := managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
				require.Equal(t, http.StatusConflict, response.Code, state+": "+response.Body.String())
			}
			require.NoError(t, db.Exec("UPDATE nextcloud_source_rotations SET state = 'finalized' WHERE "+
				"operation_id = ?",
				rotation.OperationID).Error)
			response := managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), `"rebound":true`)
			require.Contains(t, response.Body.String(), `"received_through_event_id":"102"`)
			require.Contains(t, response.Body.String(), `"dispatched_through_event_id":"101"`)
			require.Contains(t, response.Body.String(), `"applied_through_event_id":"101"`)
			require.NotContains(t, response.Body.String(), credential.Secret)
			var updated struct {
				KeyID      string `gorm:"column:current_key_id"`
				Ciphertext string `gorm:"column:current_secret_ciphertext"`
			}
			require.NoError(t,
				db.Table("nextcloud_event_connections").Select("current_key_id, current_secret_ciphertext").
					Where("connection_id = ?", credential.ConnectionID).Take(&updated).Error)
			require.Equal(t, original, updated)
			again := managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
			require.Equal(t, http.StatusOK, again.Code, again.Body.String())
			require.Contains(t, again.Body.String(), `"rebound":false`)
			require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
				signedNextcloudEventRequestWithKey(t, credential.Secret, credential.KeyID,
					stale, strings.Repeat("c", 32))).Code)
			var state string
			require.NoError(t, db.Raw(`SELECT state FROM nextcloud_event_dispatch WHERE connection_id = ?`,
				credential.ConnectionID).Scan(&state).Error)
			require.Equal(t, "idle", state)
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch SET state = 'blocked',
					last_error_code = 'cursor_missing_manual_review' WHERE connection_id = ?`,
				credential.ConnectionID).Error)
			replayed := managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
			require.Equal(t, http.StatusOK, replayed.Code, replayed.Body.String())
			require.Contains(t, replayed.Body.String(), `"rebound":false`)
			require.Contains(t, replayed.Body.String(), `"dispatch_state":"blocked"`)
			require.Contains(t, replayed.Body.String(), `"last_error_code":"cursor_missing_manual_review"`)
		})
	}
}

func TestNextcloudEventRebindRejectsUncertainDispatch(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			db, router, _ := setupManagedRebindTest(t, dialect)
			paired := managedConnectionRequest(router, http.MethodPost, testManagedEventPath, 7, types.TenantRoleAdmin)
			require.Equal(t, http.StatusCreated, paired.Code, paired.Body.String())
			credential := issuedEventCredential(t, paired)
			rotation := finalizeManagedEventSourceRotation(t, db)
			for _, item := range []struct {
				name, state, errorCode string
				target                 int
			}{
				{"queued", "queued", "", 0},
				{"leased", "leased", "", 0},
				{"retry", "retry", "", 0},
				{"other blocked reason", "blocked", "cursor_missing_manual_review", 0},
				{"unfinished target", "blocked", "source_changed", 1},
			} {
				t.Run(item.name, func(t *testing.T) {
					require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch SET state = ?,
						last_error_code = ?, target_event_id = ? WHERE connection_id = ?`,
						item.state, item.errorCode, item.target, credential.ConnectionID).Error)
					response := managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
					require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
					require.Contains(t, response.Body.String(), "event_dispatch_manual_review_required")
				})
			}
			require.NoError(t, db.Exec(`UPDATE nextcloud_event_dispatch SET state = 'idle',
				last_error_code = '', target_event_id = 0 WHERE connection_id = ?`, credential.ConnectionID).Error)
			require.NoError(t, db.Exec(`INSERT INTO sync_logs(id, data_source_id, status, started_at)
				VALUES ('manual-running', 'ds-synthetic', 'running', CURRENT_TIMESTAMP)`).Error)
			response := managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), "event_dispatch_manual_review_required")
			require.NoError(t, db.Exec(`UPDATE sync_logs SET status = 'success' WHERE id = 'manual-running'`).Error)

			// A finalized record for the wrong tuple or a later unrelated
			// source edit cannot authorize a connection rewrite.
			for _, change := range []struct{ column, value string }{
				{"pair_operation_id", uuid.NewString()},
				{"binding_id", "other-binding"},
				{"old_config_sha256", strings.Repeat("0", 64)},
				{"new_config_sha256", strings.Repeat("f", 64)},
			} {
				var original string
				require.NoError(t,
					db.Raw("SELECT "+change.column+" FROM nextcloud_source_rotations WHERE operation_id = ?",
						rotation.OperationID).Scan(&original).Error)
				require.NoError(t,
					db.Exec("UPDATE nextcloud_source_rotations SET "+change.column+" = ? WHERE operation_id = ?",
						change.value, rotation.OperationID).Error)
				response = managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
				require.Equal(t, http.StatusConflict, response.Code, change.column+": "+response.Body.String())
				require.NoError(t,
					db.Exec("UPDATE nextcloud_source_rotations SET "+change.column+" = ? WHERE operation_id = ?",
						original, rotation.OperationID).Error)
			}
			require.NoError(t, db.Exec(`UPDATE data_sources SET status = 'paused' WHERE id = ?`,
				"ds-synthetic").Error)
			response = managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			require.NoError(t, db.Exec(`UPDATE data_sources SET status = 'active' WHERE id = ?`,
				"ds-synthetic").Error)
			changed := syntheticNextcloudEventDataSourceConfig(t, "https://nextcloud.example.test", true)
			require.NoError(t, db.Exec("UPDATE data_sources SET config = ? WHERE id = ?", string(changed),
				"ds-synthetic").Error)
			response = managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
			require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			require.NoError(t, db.Exec(`UPDATE data_sources SET config = ? WHERE id = ?`,
				string(rotation.NewConfig), "ds-synthetic").Error)
			response = managedRebindRequest(router, rotation.OperationID, 7, types.TenantRoleAdmin, false)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		})
	}
}
