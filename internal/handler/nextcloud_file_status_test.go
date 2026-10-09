package handler

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func signedNextcloudFileStatusRequest(secret, nonce, fileID, etag string) *http.Request {
	query := "connection_id=" + testEventConnection + "&file_id=" + fileID + "&source_etag=" + etag
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	request := httptest.NewRequest(http.MethodGet, nextcloudFileStatusPath+"?"+query, nil)
	request.Header.Set("X-Nextcloud-Connection-Id", testEventConnection)
	request.Header.Set("X-Nextcloud-Key-Id", "current")
	request.Header.Set("X-Nextcloud-Timestamp", timestamp)
	request.Header.Set("X-Nextcloud-Nonce", nonce)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(nextcloudFileStatusCanonicalRequest(query, timestamp, nonce, testEventConnection, "current"))
	request.Header.Set("X-Nextcloud-Signature", hex.EncodeToString(mac.Sum(nil)))
	return request
}

func readNextcloudFileStatus(router http.Handler, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func installNextcloudFileStatusVersion(t *testing.T, db *gorm.DB, state, parseStatus, enabled, etag string) {
	t.Helper()
	require.NoError(t, db.Exec(`ALTER TABLE knowledges ADD COLUMN file_type TEXT NOT NULL DEFAULT ''`).Error)
	require.NoError(t, db.Exec(`ALTER TABLE knowledges ADD COLUMN error_message TEXT NOT NULL DEFAULT ''`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE chunks (
		id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL, knowledge_base_id TEXT NOT NULL,
		knowledge_id TEXT NOT NULL, chunk_type TEXT NOT NULL, content TEXT NOT NULL,
		is_enabled BOOLEAN NOT NULL, index_status TEXT NOT NULL, deleted_at TIMESTAMP
	)`).Error)
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "ds-synthetic", "external_id": "nextcloud:" + testEventInstance + ":77",
		"nextcloud_instance_id": testEventInstance, "nextcloud_binding_id": testEventBinding,
		"nextcloud_file_id": "77", "nextcloud_target_etag": etag,
		"nextcloud_etag": map[bool]string{true: etag, false: ""}[state == "published"],
	})
	require.NoError(t, err)
	require.NoError(t, db.Exec("INSERT INTO knowledges\n\t\t(id, tenant_id, knowledge_base_id, "+
		"channel, parse_status, enable_status, file_type, "+
		"metadata)\n\t\tVALUES (?, 7, 'kb-synthetic', 'nextcloud', ?, ?, "+
		"'md', ?)", "candidate-77", parseStatus, enabled, string(metadata)).Error)
	if parseStatus == "completed" && enabled == "enabled" {
		require.NoError(t, db.Exec(`INSERT INTO chunks
			(id, tenant_id, knowledge_base_id, knowledge_id, chunk_type, content, is_enabled, index_status)
			VALUES ('chunk-77', 7, 'kb-synthetic', 'candidate-77', 'text', 'indexed text', TRUE, 'ready')`).Error)
	}
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_versions
		(tenant_id, knowledge_base_id, datasource_id, external_id, desired_etag,
		candidate_knowledge_id, state, updated_at)
		VALUES (7, 'kb-synthetic', 'ds-synthetic', ?, ?, 'candidate-77', ?, ?)`,
		"nextcloud:"+testEventInstance+":77", etag, state, time.Now().UTC()).Error)
}

func TestNextcloudSignedFileStatusProvesOnlyCurrentPublishedETag(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	require.NoError(t, db.Exec(`ALTER TABLE nextcloud_source_versions ADD COLUMN updated_at TIMESTAMP`).Error)
	router.GET(nextcloudFileStatusPath,
		NewNextcloudEventHandler(repository.NewNextcloudEventInboxRepository(db)).FileStatus)
	installNextcloudFileStatusVersion(t, db, "published", "completed", "enabled", "etag-77")

	request := signedNextcloudFileStatusRequest(secret, strings.Repeat("a", 32), "77", "etag-77")
	response := readNextcloudFileStatus(router, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	var status repository.NextcloudFileStatus
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &status))
	require.Equal(t, "ready", status.KnowledgeState)
	require.Empty(t, status.FailureCode)
	require.Equal(t, "etag-77", *status.PublishedSourceETag)
	require.NotNil(t, status.KnowledgeReadyAt)
	require.False(t, status.QAAvailable, "file publication does not grant a user's Q&A access")
	require.Equal(t, testEventInstance, status.NextcloudInstanceID)
	require.Equal(t, testEventBinding, status.BindingID)
	require.Equal(t, "ds-synthetic", status.DataSourceID)
	require.Equal(t, "7", status.TenantID)
	requestSignature := request.Header.Get("X-Nextcloud-Signature")
	canonical := "weknora-file-status-hmac-sha256-v1\n" + requestSignature + "\n" +
		func() string { sum := sha256.Sum256(response.Body.Bytes()); return hex.EncodeToString(sum[:]) }() +
		"\n" + testEventConnection + "\ncurrent\n" + strings.Repeat("a", 32)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(canonical))
	require.Equal(t, hex.EncodeToString(mac.Sum(nil)), response.Header().Get("X-WeKnora-Status-Signature"))
	require.Equal(t, http.StatusUnauthorized, readNextcloudFileStatus(router, request).Code,
		"a signed response cannot be replayed under the same request nonce")

	newer := readNextcloudFileStatus(router,
		signedNextcloudFileStatusRequest(secret, strings.Repeat("b", 32), "77", "etag-78"))
	require.Equal(t, http.StatusOK, newer.Code)
	require.NoError(t, json.Unmarshal(newer.Body.Bytes(), &status))
	require.Equal(t, "updating", status.KnowledgeState)
	require.Nil(t, status.KnowledgeReadyAt)
	require.Equal(t, "etag-77", *status.PublishedSourceETag)

	tampered := signedNextcloudFileStatusRequest(secret, strings.Repeat("c", 32), "77", "etag-77")
	tampered.URL.RawQuery = strings.Replace(tampered.URL.RawQuery, "file_id=77", "file_id=78", 1)
	require.Equal(t, http.StatusUnauthorized, readNextcloudFileStatus(router, tampered).Code)
	invalidTarget := signedNextcloudFileStatusRequest(secret, strings.Repeat("d", 32), "77", "etag-77")
	invalidTarget.URL.RawQuery += "&extra=1"
	require.Equal(t, http.StatusBadRequest, readNextcloudFileStatus(router, invalidTarget).Code)

	require.NoError(t, db.Exec("DELETE FROM nextcloud_source_pairings WHERE datasource_id = ?", "ds-synthetic").Error)
	require.Equal(t, http.StatusForbidden, readNextcloudFileStatus(router,
		signedNextcloudFileStatusRequest(secret, strings.Repeat("e", 32), "77", "etag-77")).Code)
}

func TestNextcloudSignedFileStatusRejectsPublishedTextWithoutRetrievableChunk(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	require.NoError(t, db.Exec(`ALTER TABLE nextcloud_source_versions ADD COLUMN updated_at TIMESTAMP`).Error)
	router.GET(nextcloudFileStatusPath,
		NewNextcloudEventHandler(repository.NewNextcloudEventInboxRepository(db)).FileStatus)
	installNextcloudFileStatusVersion(t, db, "published", "completed", "enabled", "etag-77")
	require.NoError(t, db.Exec(`DELETE FROM chunks WHERE id = 'chunk-77'`).Error)

	response := readNextcloudFileStatus(router,
		signedNextcloudFileStatusRequest(secret, strings.Repeat("2", 32), "77", "etag-77"))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var status repository.NextcloudFileStatus
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &status))
	require.Equal(t, "failed", status.KnowledgeState)
	require.Equal(t, "no_retrievable_content", status.FailureCode)
	require.Nil(t, status.KnowledgeReadyAt)
	require.False(t, status.QAAvailable)
	require.NotContains(t, response.Body.String(), "indexed text")
}

func TestNextcloudSignedFileStatusReportsFailedCandidateWithoutReady(t *testing.T) {
	db, router, secret := setupNextcloudDispatchTest(t, false)
	require.NoError(t, db.Exec(`ALTER TABLE nextcloud_source_versions ADD COLUMN updated_at TIMESTAMP`).Error)
	router.GET(nextcloudFileStatusPath,
		NewNextcloudEventHandler(repository.NewNextcloudEventInboxRepository(db)).FileStatus)
	installNextcloudFileStatusVersion(t, db, "staging", "failed", "disabled", "etag-77")
	response := readNextcloudFileStatus(router,
		signedNextcloudFileStatusRequest(secret, strings.Repeat("f", 32), "77", "etag-77"))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var status repository.NextcloudFileStatus
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &status))
	require.Equal(t, "failed", status.KnowledgeState)
	require.Empty(t, status.FailureCode, "raw parser errors must not cross the signed status API")
	require.Nil(t, status.PublishedSourceETag)
	require.Nil(t, status.KnowledgeReadyAt)

	require.NoError(t, db.Exec(`UPDATE knowledges SET error_message = 'no_retrievable_content'
		WHERE id = 'candidate-77'`).Error)
	response = readNextcloudFileStatus(router,
		signedNextcloudFileStatusRequest(secret, strings.Repeat("0", 32), "77", "etag-77"))
	require.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &status))
	require.Equal(t, "no_retrievable_content", status.FailureCode)

	require.NoError(t, db.Exec(`UPDATE knowledges SET metadata = ? WHERE id = 'candidate-77'`,
		"{\"datasource_id\":\"ds-synthetic\",\"external_id\":\"nextcloud:instance"+
			"_synthetic:77\",\"nextcloud_instance_id\":\"instance_synthetic\",\"next"+
			"cloud_binding_id\":\"wrong\",\"nextcloud_file_id\":\"77\",\"nextcloud_tar"+
			"get_etag\":\"etag-77\"}").Error)
	response = readNextcloudFileStatus(router,
		signedNextcloudFileStatusRequest(secret, strings.Repeat("1", 32), "77", "etag-77"))
	require.Equal(t, http.StatusOK, response.Code)
	status = repository.NextcloudFileStatus{}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &status))
	require.Equal(t, "unverified", status.KnowledgeState,
		"a candidate whose provenance no longer matches the pair is not trusted")
	require.Empty(t, status.FailureCode)
}
