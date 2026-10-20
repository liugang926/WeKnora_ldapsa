package handler

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/stretchr/testify/require"
)

func TestNextcloudStateV2RequestCrossLanguageFixture(t *testing.T) {
	raw, err := os.ReadFile("../application/repository/testdata/nextcloud-stage-status-v2-signatures.json")
	require.NoError(t, err)
	var fixture map[string]string
	require.NoError(t, json.Unmarshal(raw, &fixture))
	key := repository.NextcloudStateSigningKey([]byte(fixture["secret"]))
	mac := hmac.New(sha256.New, key)
	mac.Write(
		nextcloudFileStateV2CanonicalRequest(
			fixture["query"],
			fixture["timestamp"],
			fixture["nonce"],
			"connection-0000001",
			fixture["key_id"],
		),
	)
	require.Equal(t, fixture["request_signature"], hex.EncodeToString(mac.Sum(nil)))
}
