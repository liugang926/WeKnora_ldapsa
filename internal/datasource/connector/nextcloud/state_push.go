package nextcloud

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// PushProcessingState reuses the connector's operator-approved fixed origin
// and no-redirect transport. Status HMAC uses a purpose-derived event key,
// never a content bearer, personal token, or model output.
func PushProcessingState(
	ctx context.Context,
	ds *types.DataSourceConfig,
	connection, keyID, eventID string,
	key, body []byte,
) error {
	cfg, err := parseConfig(ds)
	if err != nil {
		return err
	}
	cli := newClient(cfg)
	cli.http.Timeout = 10 * time.Second
	target := cli.endpointURL("/status", nil)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	nonceBytes := make([]byte, 16)
	if _, err := rand.Read(nonceBytes); err != nil {
		return err
	}
	nonce := hex.EncodeToString(nonceBytes)
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	sum := sha256.Sum256(body)
	canonical := StatePushCanonical(
		req.URL.EscapedPath(),
		hex.EncodeToString(sum[:]),
		timestamp,
		nonce,
		connection,
		keyID,
	)
	mac := hmac.New(sha256.New, key)
	mac.Write(canonical)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-WeKnora-Connection-Id", connection)
	req.Header.Set("X-WeKnora-Key-Id", keyID)
	req.Header.Set("X-WeKnora-Timestamp", timestamp)
	req.Header.Set("X-WeKnora-Nonce", nonce)
	req.Header.Set("X-WeKnora-Signature", hex.EncodeToString(mac.Sum(nil)))
	response, err := cli.http.Do(req)
	if err != nil {
		return errors.New("state_delivery_failed")
	}
	defer closeNextcloudResponseBody(ctx, response.Body)
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(raw) > 4096 || response.StatusCode < 200 || response.StatusCode > 299 {
		return errors.New("state_delivery_failed")
	}
	var ack struct {
		EventID   string `json:"event_id"`
		Status    string `json:"status"`
		Persisted bool   `json:"persisted"`
	}
	if json.Unmarshal(raw, &ack) != nil || ack.EventID != eventID || !ack.Persisted || ack.Status != "accepted" {
		return errors.New("state_ack_unverified")
	}
	return nil
}

func StatePushCanonical(path, bodyHash, timestamp, nonce, connection, keyID string) []byte {
	return []byte(
		strings.Join(
			[]string{
				"weknora-state-push-hmac-sha256-v2",
				http.MethodPost,
				path,
				"",
				bodyHash,
				timestamp,
				nonce,
				connection,
				keyID,
			},
			"\n",
		),
	)
}
