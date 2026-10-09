package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudEventRotationGraceStatusAndFileStatus(t *testing.T) {
	db, router, oldSecret := setupNextcloudDispatchTest(t, false)
	repo := repository.NewNextcloudEventInboxRepository(db)
	handler := NewNextcloudEventHandler(repo)
	router.GET(nextcloudEventStatusPath, handler.Status)
	router.GET(nextcloudFileStatusPath, handler.FileStatus)
	require.NoError(t, db.Exec(`ALTER TABLE knowledge_bases
		ADD COLUMN ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT FALSE`).Error)
	require.NoError(t, db.Exec(`UPDATE knowledge_bases SET ever_had_nextcloud_source = TRUE`).Error)
	var source types.DataSource
	require.NoError(t, db.Where("id = ?", "ds-synthetic").Take(&source).Error)
	baseURL, configSHA, bindingID, err := repository.NextcloudEventDataSourceIdentity(source.Config)
	require.NoError(t, err)
	identity := repository.NextcloudEventPairingIdentity{
		TenantID: 7, KnowledgeBaseID: "kb-synthetic", DatasourceID: "ds-synthetic",
		NextcloudInstanceID: testEventInstance, BindingID: bindingID,
		DatasourceBaseURL: baseURL, DatasourceConfigSHA: configSHA,
	}
	signedGET := func(path, query, keyID, secret, nonce string,
		canonical func(string, string, string, string, string) []byte,
	) *http.Request {
		timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
		request := httptest.NewRequest(http.MethodGet, path+"?"+query, nil)
		request.Header.Set("X-Nextcloud-Connection-Id", testEventConnection)
		request.Header.Set("X-Nextcloud-Key-Id", keyID)
		request.Header.Set("X-Nextcloud-Timestamp", timestamp)
		request.Header.Set("X-Nextcloud-Nonce", nonce)
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(canonical(query, timestamp, nonce, testEventConnection, keyID))
		request.Header.Set("X-Nextcloud-Signature", hex.EncodeToString(mac.Sum(nil)))
		return request
	}
	statusRequest := func(keyID, secret, nonce string) *http.Request {
		return signedGET(nextcloudEventStatusPath, "connection_id="+testEventConnection,
			keyID, secret, nonce, nextcloudEventStatusCanonicalRequest)
	}
	fileRequest := func(keyID, secret, nonce string) *http.Request {
		return signedGET(nextcloudFileStatusPath,
			"connection_id="+testEventConnection+"&file_id=77&source_etag=etag-77",
			keyID, secret, nonce, nextcloudFileStatusCanonicalRequest)
	}
	read := func(request *http.Request) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		return recorder
	}
	priorNonce := strings.Repeat("a", 32)
	require.Equal(t, http.StatusOK, read(statusRequest("current", oldSecret, priorNonce)).Code)
	replacement, err := repo.Rotate(context.Background(), identity)
	require.NoError(t, err)
	require.Equal(t, testEventConnection, replacement.ConnectionID)
	require.Equal(t, http.StatusUnauthorized, read(statusRequest("current", oldSecret, priorNonce)).Code,
		"rotation must retain the shared replay nonce table")
	require.Equal(t, http.StatusOK, read(statusRequest("current", oldSecret, strings.Repeat("b", 32))).Code)
	require.Equal(t, http.StatusOK, read(statusRequest(replacement.KeyID, replacement.Secret,
		strings.Repeat("c", 32))).Code)
	oldFileNonce := strings.Repeat("d", 32)
	oldFileRequest := fileRequest("current", oldSecret, oldFileNonce)
	oldFileResponse := read(oldFileRequest)
	require.Equal(t, http.StatusOK, oldFileResponse.Code, oldFileResponse.Body.String())
	bodyHash := sha256.Sum256(oldFileResponse.Body.Bytes())
	responseCanonical := "weknora-file-status-hmac-sha256-v1\n" +
		oldFileRequest.Header.Get("X-Nextcloud-Signature") + "\n" +
		hex.EncodeToString(bodyHash[:]) + "\n" + testEventConnection + "\ncurrent\n" + oldFileNonce
	mac := hmac.New(sha256.New, []byte(oldSecret))
	_, _ = mac.Write([]byte(responseCanonical))
	require.Equal(t, hex.EncodeToString(mac.Sum(nil)), oldFileResponse.Header().Get("X-WeKnora-Status-Signature"))
	require.Equal(t, http.StatusOK, read(fileRequest(replacement.KeyID, replacement.Secret, strings.Repeat("e",
		32))).Code)

	require.NoError(t, db.Exec(`UPDATE nextcloud_event_connections
		SET previous_valid_until = ? WHERE connection_id = ?`,
		time.Now().UTC().Add(-time.Second), testEventConnection).Error)
	require.Equal(t, http.StatusUnauthorized, read(statusRequest("current", oldSecret, strings.Repeat("f", 32))).Code)
	require.Equal(t, http.StatusUnauthorized, read(fileRequest("current", oldSecret, strings.Repeat("1", 32))).Code)
	require.Equal(t, http.StatusOK, read(statusRequest(replacement.KeyID, replacement.Secret,
		strings.Repeat("2", 32))).Code)
	require.Equal(t, http.StatusOK, read(fileRequest(replacement.KeyID, replacement.Secret, strings.Repeat("3",
		32))).Code)
	require.NoError(t, repo.Revoke(context.Background(), 7, "ds-synthetic"))
	require.Equal(t, http.StatusUnauthorized, read(statusRequest(replacement.KeyID, replacement.Secret,
		strings.Repeat("4", 32))).Code)
	require.Equal(t, http.StatusUnauthorized, read(fileRequest(replacement.KeyID, replacement.Secret,
		strings.Repeat("5", 32))).Code)
	var previousCount int64
	require.NoError(t, db.Raw(`SELECT COUNT(*) FROM nextcloud_event_connections
		WHERE connection_id = ? AND previous_key_id IS NOT NULL`, testEventConnection).
		Scan(&previousCount).Error)
	require.Zero(t, previousCount)
}
