package handler

import (
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
	"github.com/stretchr/testify/require"
)

func signedNextcloudEventStatusRequest(secret, nonce string, when time.Time) *http.Request {
	query := "connection_id=" + testEventConnection
	timestamp := strconv.FormatInt(when.Unix(), 10)
	request := httptest.NewRequest(http.MethodGet, nextcloudEventStatusPath+"?"+query, nil)
	request.Header.Set("X-Nextcloud-Connection-Id", testEventConnection)
	request.Header.Set("X-Nextcloud-Key-Id", "current")
	request.Header.Set("X-Nextcloud-Timestamp", timestamp)
	request.Header.Set("X-Nextcloud-Nonce", nonce)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(nextcloudEventStatusCanonicalRequest(query, timestamp, nonce, testEventConnection, "current"))
	request.Header.Set("X-Nextcloud-Signature", hex.EncodeToString(mac.Sum(nil)))
	return request
}

func testNextcloudSignedEventStatus(t *testing.T, usePostgres bool) {
	t.Helper()
	db, router, secret := setupNextcloudDispatchTest(t, usePostgres)
	router.GET(nextcloudEventStatusPath,
		NewNextcloudEventHandler(repository.NewNextcloudEventInboxRepository(db)).Status)
	read := func(request *http.Request) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		return recorder
	}

	// Receipt is durable but must not be reported as application.
	receiveDispatchHint(t, router, secret, "0", "41", "a")
	replayedAcrossMethods := read(signedNextcloudEventStatusRequest(secret, strings.Repeat("a", 32), time.Now()))
	require.Equal(t, http.StatusUnauthorized, replayedAcrossMethods.Code)

	valid := signedNextcloudEventStatusRequest(secret, strings.Repeat("b", 32), time.Now())
	first := read(valid)
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	require.Equal(t, "no-store", first.Header().Get("Cache-Control"))
	require.Contains(t, first.Body.String(), `"received_through_event_id":"41"`)
	require.Contains(t, first.Body.String(), `"applied_through_event_id":"0"`)
	require.Contains(t, first.Body.String(), `"backlog_count":1`)
	require.NotContains(t, first.Body.String(), secret)
	require.Equal(t, http.StatusUnauthorized, read(valid).Code, "status nonce cannot be replayed")

	invalid := signedNextcloudEventStatusRequest(secret, strings.Repeat("c", 32), time.Now())
	signature := invalid.Header.Get("X-Nextcloud-Signature")
	invalid.Header.Set("X-Nextcloud-Signature", strings.Repeat("0", 64))
	require.Equal(t, http.StatusUnauthorized, read(invalid).Code)
	invalid.Header.Set("X-Nextcloud-Signature", signature)
	require.Equal(t, http.StatusOK, read(invalid).Code, "invalid signature must not consume nonce")

	stale := signedNextcloudEventStatusRequest(secret, strings.Repeat("d", 32), time.Now().Add(-6*time.Minute))
	require.Equal(t, http.StatusUnauthorized, read(stale).Code)
	tampered := signedNextcloudEventStatusRequest(secret, strings.Repeat("e", 32), time.Now())
	tampered.URL.RawQuery += "&extra=1"
	require.Equal(t, http.StatusBadRequest, read(tampered).Code)

	// A valid connection secret alone cannot read a changed source's status.
	require.NoError(t, db.Exec(`DELETE FROM nextcloud_source_pairings
		WHERE datasource_id = ?`, "ds-synthetic").Error)
	var before int64
	require.NoError(t, db.Table("nextcloud_event_nonces").Count(&before).Error)
	require.Equal(t, http.StatusForbidden,
		read(signedNextcloudEventStatusRequest(secret, strings.Repeat("f", 32), time.Now())).Code)
	var after int64
	require.NoError(t, db.Table("nextcloud_event_nonces").Count(&after).Error)
	require.Equal(t, before, after, "scope failure must not consume nonce")
}

func TestNextcloudSignedEventStatusSQLite(t *testing.T) {
	testNextcloudSignedEventStatus(t, false)
}

func TestNextcloudSignedEventStatusPostgres(t *testing.T) {
	testNextcloudSignedEventStatus(t, true)
}
