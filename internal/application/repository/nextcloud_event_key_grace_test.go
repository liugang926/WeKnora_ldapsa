package repository

import (
	"crypto/hmac"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestNextcloudEventPreviousKeyExpiryUsesValidationClock(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	oldSecret := strings.Repeat("o", 43)
	newSecret := strings.Repeat("n", 43)
	oldCiphertext, err := utils.EncryptAESGCM(oldSecret, utils.GetAESKey())
	require.NoError(t, err)
	newCiphertext, err := utils.EncryptAESGCM(newSecret, utils.GetAESKey())
	require.NoError(t, err)
	previousID := "old"
	// The incoming request passed timestamp checks before waiting for the row
	// lock. Its saved Now cannot extend the prior key after the lock is taken.
	expired := time.Now().UTC().Add(-time.Second)
	connection := nextcloudEventConnection{
		CurrentKeyID: "new", CurrentSecretCiphertext: newCiphertext,
		PreviousKeyID: &previousID, PreviousSecretCiphertext: &oldCiphertext,
		PreviousValidUntil: &expired,
	}
	canonical := []byte("signed request")
	signed := func(secret, keyID string) SignedNextcloudEventBatch {
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(canonical)
		return SignedNextcloudEventBatch{
			KeyID: keyID, CanonicalRequest: canonical, Signature: mac.Sum(nil),
			Now: expired.Add(-time.Minute),
		}
	}
	require.ErrorIs(t, verifyNextcloudEventSignature(connection, signed(
		oldSecret,
		"old",
	)), ErrNextcloudEventUnauthorized)
	_, err = nextcloudEventResponseSecret(connection, "old")
	require.ErrorIs(t, err, ErrNextcloudEventUnauthorized)
	require.NoError(t, verifyNextcloudEventSignature(connection, signed(newSecret, "new")))
	responseSecret, err := nextcloudEventResponseSecret(connection, "new")
	require.NoError(t, err)
	require.Equal(t, newSecret, string(responseSecret))
}
