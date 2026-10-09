package handler

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	testEventConnection = "conn_nextcloud_synthetic_001"
	testEventInstance   = "instance_synthetic"
	testEventBinding    = "binding_synthetic"
)

func setupNextcloudEventHTTPTest(t *testing.T) (*gorm.DB, *gin.Engine, string) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "events.db")),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	return prepareNextcloudEventHTTPTest(t, db, "../../migrations/sqlite/000032_nextcloud_event_inbox.up.sql")
}

func prepareNextcloudEventHTTPTest(t *testing.T, db *gorm.DB, migrationPath string) (*gorm.DB, *gin.Engine, string) {
	t.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	t.Setenv("SYSTEM_AES_KEY", base64.RawURLEncoding.EncodeToString(key)[:32])
	secretBytes := make([]byte, 32)
	_, err = rand.Read(secretBytes)
	require.NoError(t, err)
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)
	ciphertext, err := utils.EncryptAESGCM(secret, utils.GetAESKey())
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(ciphertext, utils.EncPrefix))

	migration, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	for _, statement := range strings.Split(string(migration), ";") {
		if strings.TrimSpace(statement) != "" {
			require.NoError(t, db.Exec(statement).Error)
		}
	}
	etagMigration := "../../migrations/sqlite/000049_nextcloud_event_hint_etag.up.sql"
	if strings.Contains(migrationPath, "/versioned/") {
		etagMigration = "../../migrations/versioned/000130_nextcloud_event_hint_etag.up.sql"
	}
	etagSchema, err := os.ReadFile(etagMigration)
	require.NoError(t, err)
	for _, statement := range strings.Split(string(etagSchema), ";") {
		if strings.TrimSpace(statement) != "" {
			require.NoError(t, db.Exec(statement).Error)
		}
	}
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL, deleted_at TIMESTAMP
	)`).Error)
	configColumnType := "TEXT"
	if strings.Contains(migrationPath, "/versioned/") {
		configColumnType = "JSONB"
	}
	require.NoError(t, db.Exec(fmt.Sprintf(`CREATE TABLE data_sources (
		id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		knowledge_base_id TEXT NOT NULL, type TEXT NOT NULL,
		config %s NOT NULL, status TEXT NOT NULL DEFAULT 'active',
		updated_at TIMESTAMP, deleted_at TIMESTAMP
	)`, configColumnType)).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES (?, ?)`, "kb-synthetic", 7).Error)
	config := syntheticNextcloudEventDataSourceConfig(t, "https://nextcloud.example.test", true)
	require.NoError(t, db.Exec(`INSERT INTO data_sources
		(id, tenant_id, knowledge_base_id, type, config)
		VALUES (?, ?, ?, ?, ?)`, "ds-synthetic", 7, "kb-synthetic", "nextcloud",
		string(config)).Error)
	var stored types.DataSource
	require.NoError(t, db.Where("id = ?", "ds-synthetic").Take(&stored).Error)
	baseURL, configSHA256, bindingID, err := repository.NextcloudEventDataSourceIdentity(stored.Config)
	require.NoError(t, err)
	require.Equal(t, testEventBinding, bindingID)
	require.NoError(t, db.AutoMigrate(&repository.NextcloudSourcePairing{}))
	require.NoError(t, db.Create(&repository.NextcloudSourcePairing{
		OperationID: "8c43deee-99c9-412f-9488-a9dfd3b453c9", TenantID: 7,
		KnowledgeBaseID: "kb-synthetic", DataSourceID: "ds-synthetic",
		InstanceID: testEventInstance, BindingID: testEventBinding,
		BaseURL: baseURL, ConfigSHA: configSHA256, PublicationEpoch: 1,
		KeyID: "pair_8c43deee99c9412f9488a9dfd3b453c9", State: "active",
	}).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_connections
		(connection_id, tenant_id, knowledge_base_id, datasource_id,
		nextcloud_instance_id, binding_id, datasource_base_url,
		datasource_config_sha256, status,
		current_key_id, current_secret_ciphertext)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, testEventConnection, 7,
		"kb-synthetic", "ds-synthetic", testEventInstance,
		testEventBinding, baseURL, configSHA256, "active", "current", ciphertext).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_event_checkpoint
		(connection_id, received_id) VALUES (?, 0)`, testEventConnection).Error)

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST(nextcloudEventPath,
		NewNextcloudEventHandler(repository.NewNextcloudEventInboxRepository(db)).Receive)
	return db, router, secret
}

func syntheticNextcloudEventDataSourceConfig(t *testing.T, baseURL string, withCredentials bool) types.JSON {
	t.Helper()
	config := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{testEventBinding},
		Settings:    map[string]interface{}{"base_url": baseURL},
	}
	if withCredentials {
		secret := make([]byte, 32)
		_, err := rand.Read(secret)
		require.NoError(t, err)
		config.Credentials = map[string]interface{}{
			"token": base64.RawURLEncoding.EncodeToString(secret), "key_id": "default",
		}
	}
	blob, err := config.ToJSON()
	require.NoError(t, err)
	return blob
}

func signedNextcloudEventRequest(t *testing.T, secret string, body nextcloudEventRequest, nonce string) *http.Request {
	return signedNextcloudEventRequestWithKey(t, secret, "current", body, nonce)
}

func signedNextcloudEventRequestWithKey(t *testing.T, secret, keyID string, body nextcloudEventRequest,
	nonce string,
) *http.Request {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, nextcloudEventPath, bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Nextcloud-Connection-Id", body.ConnectionID)
	req.Header.Set("X-Nextcloud-Key-Id", keyID)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	req.Header.Set("X-Nextcloud-Timestamp", timestamp)
	req.Header.Set("X-Nextcloud-Nonce", nonce)
	mac := hmac.New(sha256.New, []byte(secret))
	_, err = mac.Write(nextcloudEventCanonicalRequest(encoded, timestamp, nonce, body.ConnectionID, keyID))
	require.NoError(t, err)
	req.Header.Set("X-Nextcloud-Signature", hex.EncodeToString(mac.Sum(nil)))
	return req
}

func sampleNextcloudEventBatch(after, eventID string) nextcloudEventRequest {
	return nextcloudEventRequest{
		ConnectionID: testEventConnection, NextcloudInstanceID: testEventInstance,
		BindingID: testEventBinding, AfterEventID: after,
		Events: []nextcloudEventHintJSON{{EventID: eventID, Type: "upsert", FileID: ptrInt64(41)}},
	}
}

func ptrInt64(value int64) *int64 { return &value }

func postNextcloudEvent(router *gin.Engine, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestNextcloudLegacyUnpairedSourceCannotReceiveEvents(t *testing.T) {
	db, router, secret := setupNextcloudEventHTTPTest(t)
	createNextcloudEventSyncLogFixture(t, db)
	applyNextcloudEventDispatchMigration(t, db)
	require.NoError(t, db.Exec(`DELETE FROM nextcloud_source_pairings WHERE datasource_id = ?`, "ds-synthetic").Error)
	status, err := repository.NewNextcloudEventInboxRepository(db).ConnectionStatus(
		context.Background(), 7, "ds-synthetic")
	require.NoError(t, err)
	require.Equal(t, "source_unpaired", status.Status)
	request := signedNextcloudEventRequest(t, secret, sampleNextcloudEventBatch("0", "1"), strings.Repeat("a", 32))
	response := postNextcloudEvent(router, request)
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	var count int64
	require.NoError(t, db.Table("nextcloud_event_inbox").Count(&count).Error)
	require.Zero(t, count)
}

func TestNextcloudEventInboxReceiptSequenceAndReplay(t *testing.T) {
	db, router, secret := setupNextcloudEventHTTPTest(t)
	first := sampleNextcloudEventBatch("0", "9007199254740993") // exact beyond JS Number precision
	etag, path, relativePath := "signed-etag", "/Published/file.md", "file.md"
	first.Events[0].ETag, first.Events[0].Path, first.Events[0].RelativePath = &etag, &path, &relativePath
	response := postNextcloudEvent(router, signedNextcloudEventRequest(t, secret, first, strings.Repeat("a", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"received_through_event_id":"9007199254740993"`)
	require.Contains(t, response.Body.String(), `"durable_receipt_only":true`)

	// A lost 202 can be retried with a fresh signed nonce; it cannot re-use
	// the original nonce, and an altered hint cannot reuse the event ID.
	response = postNextcloudEvent(router, signedNextcloudEventRequest(t, secret, first, strings.Repeat("b", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	response = postNextcloudEvent(router, signedNextcloudEventRequest(t, secret, first, strings.Repeat("a", 32)))
	require.Equal(t, http.StatusUnauthorized, response.Code)
	changed := first
	changed.Events = []nextcloudEventHintJSON{{
		EventID: first.Events[0].EventID, Type: "metadata",
		FileID: ptrInt64(41),
	}}
	response = postNextcloudEvent(router, signedNextcloudEventRequest(t, secret, changed, strings.Repeat("c", 32)))
	require.Equal(t, http.StatusConflict, response.Code)

	second := sampleNextcloudEventBatch("9007199254740993", "9007199254740995")
	response = postNextcloudEvent(router, signedNextcloudEventRequest(t, secret, second, strings.Repeat("d", 32)))
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	response = postNextcloudEvent(router, signedNextcloudEventRequest(t, secret,
		sampleNextcloudEventBatch("9007199254740996", "9007199254740997"), strings.Repeat("e", 32)))
	require.Equal(t, http.StatusConflict, response.Code)

	var received int64
	require.NoError(t, db.Raw(`SELECT received_id FROM nextcloud_event_checkpoint WHERE connection_id = ?`,
		testEventConnection).Scan(&received).Error)
	require.Equal(t, int64(9007199254740995), received)
	var count int64
	require.NoError(t, db.Table("nextcloud_event_inbox").Count(&count).Error)
	require.Equal(t, int64(2), count)
	var stored struct {
		ETag         string `gorm:"column:etag"`
		Path         string `gorm:"column:path"`
		RelativePath string `gorm:"column:relative_path"`
	}
	require.NoError(t, db.Table("nextcloud_event_inbox").Select("etag", "path", "relative_path").
		Where("connection_id = ? AND event_id = ?", testEventConnection, int64(9007199254740993)).
		Take(&stored).Error)
	require.Equal(t, etag, stored.ETag)
	require.Equal(t, path, stored.Path)
	require.Equal(t, relativePath, stored.RelativePath)
}

func TestNextcloudEventInboxAuthenticationScopeAndDatabaseFailure(t *testing.T) {
	db, router, secret := setupNextcloudEventHTTPTest(t)
	batch := sampleNextcloudEventBatch("0", "21")
	request := signedNextcloudEventRequest(t, secret, batch, strings.Repeat("1", 32))
	request.Header.Set("X-Nextcloud-Signature", strings.Repeat("0", 64))
	require.Equal(t, http.StatusUnauthorized, postNextcloudEvent(router, request).Code)

	otherScope := batch
	otherScope.BindingID = "other-binding"
	require.Equal(t, http.StatusForbidden, postNextcloudEvent(router,
		signedNextcloudEventRequest(t, secret, otherScope, strings.Repeat("2", 32))).Code)

	request = signedNextcloudEventRequest(t, secret, batch, strings.Repeat("3", 32))
	request.Header.Set("X-Nextcloud-Timestamp", strconv.FormatInt(time.Now().Add(-6*time.Minute).Unix(), 10))
	require.Equal(t, http.StatusUnauthorized, postNextcloudEvent(router, request).Code)

	// No 202, nonce consumption, or checkpoint advancement is possible if
	// inbox persistence fails mid-transaction.
	require.NoError(t, db.Exec(`DROP TABLE nextcloud_event_inbox`).Error)
	require.Equal(t, http.StatusServiceUnavailable, postNextcloudEvent(router,
		signedNextcloudEventRequest(t, secret, batch, strings.Repeat("4", 32))).Code)
	var nonceCount, received int64
	require.NoError(t, db.Raw(`SELECT COUNT(*) FROM nextcloud_event_nonces`).Scan(&nonceCount).Error)
	require.NoError(t, db.Raw(`SELECT received_id FROM nextcloud_event_checkpoint WHERE connection_id = ?`,
		testEventConnection).Scan(&received).Error)
	require.Zero(t, nonceCount)
	require.Zero(t, received)
}

func TestNextcloudEventInboxRejectsStaleDataSourceIdentity(t *testing.T) {
	db, router, secret := setupNextcloudEventHTTPTest(t)
	changes := []types.JSON{
		syntheticNextcloudEventDataSourceConfig(t, "https://different.example.test", true),
		syntheticNextcloudEventDataSourceConfig(t, "https://nextcloud.example.test", false),
		syntheticNextcloudEventDataSourceConfig(t, "https://nextcloud.example.test", true),
	}
	for i, config := range changes {
		require.NoError(t, db.Exec(`UPDATE data_sources SET config = ? WHERE id = ?`,
			string(config), "ds-synthetic").Error)
		response := postNextcloudEvent(router, signedNextcloudEventRequest(t, secret,
			sampleNextcloudEventBatch("0", "1"), strings.Repeat(strconv.Itoa(i+5), 32)))
		require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	}
	var received, nonceCount int64
	require.NoError(t, db.Raw(`SELECT received_id FROM nextcloud_event_checkpoint WHERE connection_id = ?`,
		testEventConnection).Scan(&received).Error)
	require.NoError(t, db.Raw(`SELECT COUNT(*) FROM nextcloud_event_nonces`).Scan(&nonceCount).Error)
	require.Zero(t, received)
	require.Zero(t, nonceCount)
}

func TestNextcloudEventInboxRevocationAndPreviousKeyWindow(t *testing.T) {
	db, router, currentSecret := setupNextcloudEventHTTPTest(t)
	oldBytes := make([]byte, 32)
	_, err := rand.Read(oldBytes)
	require.NoError(t, err)
	oldSecret := base64.RawURLEncoding.EncodeToString(oldBytes)
	ciphertext, err := utils.EncryptAESGCM(oldSecret, utils.GetAESKey())
	require.NoError(t, err)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET previous_key_id = ?, previous_secret_ciphertext = ?, previous_valid_until = ?
		WHERE connection_id = ?`, "previous", ciphertext, time.Now().Add(time.Minute),
		testEventConnection).Error)
	first := sampleNextcloudEventBatch("0", "1")
	require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
		signedNextcloudEventRequestWithKey(t, oldSecret, "previous", first,
			strings.Repeat("6", 32))).Code)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET previous_valid_until = ? WHERE connection_id = ?`,
		time.Now().Add(-time.Minute), testEventConnection).Error)
	second := sampleNextcloudEventBatch("1", "2")
	require.Equal(t, http.StatusUnauthorized, postNextcloudEvent(router,
		signedNextcloudEventRequestWithKey(t, oldSecret, "previous", second,
			strings.Repeat("7", 32))).Code)
	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET status = 'revoked' WHERE connection_id = ?`, testEventConnection).Error)
	require.Equal(t, http.StatusUnauthorized, postNextcloudEvent(router,
		signedNextcloudEventRequest(t, currentSecret, second,
			strings.Repeat("8", 32))).Code)
}

func TestNextcloudEventInboxPostgresTransaction(t *testing.T) {
	dsn := os.Getenv("NEXTCLOUD_EVENT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NEXTCLOUD_EVENT_TEST_POSTGRES_DSN for isolated PostgreSQL receiver test")
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
	schema := "nextcloud_event_test_" + hex.EncodeToString(random)
	require.NoError(t, db.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() { _ = db.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	require.NoError(t, db.Exec("SET search_path TO "+schema).Error)
	db, router, secret := prepareNextcloudEventHTTPTest(t, db,
		"../../migrations/versioned/000113_nextcloud_event_inbox.up.sql")

	first := sampleNextcloudEventBatch("0", "101")
	require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
		signedNextcloudEventRequest(t, secret, first, strings.Repeat("a", 32))).Code)
	require.Equal(t, http.StatusAccepted, postNextcloudEvent(router,
		signedNextcloudEventRequest(t, secret, first, strings.Repeat("b", 32))).Code)
	require.Equal(t, http.StatusConflict, postNextcloudEvent(router,
		signedNextcloudEventRequest(t, secret,
			sampleNextcloudEventBatch("102", "103"), strings.Repeat("c", 32))).Code)
	var count, received int64
	require.NoError(t, db.Table("nextcloud_event_inbox").Count(&count).Error)
	require.NoError(t, db.Raw(`SELECT received_id FROM nextcloud_event_checkpoint WHERE connection_id = ?`,
		testEventConnection).Scan(&received).Error)
	require.Equal(t, int64(1), count)
	require.Equal(t, int64(101), received)
}
