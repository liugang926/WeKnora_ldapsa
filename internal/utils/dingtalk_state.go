package utils

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

const dingTalkStateMaxAge = 10 * time.Minute

// DingTalkStatePayload is the signed DingTalk authorization state carried in
// the redirect URL and validated on callback. It deliberately mirrors
// OIDCStatePayload (nonce + issued-at) but is a distinct type so an OIDC
// state token can never be replayed as a DingTalk state and vice versa.
type DingTalkStatePayload struct {
	Nonce    string `json:"nonce"`
	IssuedAt int64  `json:"iat"`
}

var (
	dingTalkStateSecretOnce sync.Once
	dingTalkStateSecret     string
)

func dingTalkStateSigningKey() string {
	dingTalkStateSecretOnce.Do(func() {
		if envSecret := strings.TrimSpace(os.Getenv("JWT_SECRET")); envSecret != "" {
			dingTalkStateSecret = envSecret + ":dingtalk-state"
			return
		}
		randomBytes := make([]byte, 32)
		if _, err := rand.Read(randomBytes); err != nil {
			panic(fmt.Sprintf("failed to generate DingTalk state signing key: %v", err))
		}
		dingTalkStateSecret = base64.StdEncoding.EncodeToString(randomBytes)
	})
	return dingTalkStateSecret
}

// SignDingTalkState returns a tamper-evident state token:
// base64url(payload).base64url(hmac).
func SignDingTalkState(payload *DingTalkStatePayload) (string, error) {
	if payload == nil {
		return "", errors.New("dingtalk state payload is required")
	}
	if strings.TrimSpace(payload.Nonce) == "" {
		return "", errors.New("dingtalk state nonce is required")
	}
	if payload.IssuedAt == 0 {
		payload.IssuedAt = time.Now().Unix()
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal dingtalk state: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(dingTalkStateSigningKey()))
	mac.Write(raw)
	sig := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(raw) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// VerifyDingTalkState validates the HMAC and freshness of a state token.
func VerifyDingTalkState(raw string) (*DingTalkStatePayload, error) {
	raw = strings.TrimSpace(raw)
	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return nil, errors.New("invalid dingtalk state format")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("decode dingtalk state payload: %w", err)
	}
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode dingtalk state signature: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(dingTalkStateSigningKey()))
	mac.Write(payloadBytes)
	if !hmac.Equal(mac.Sum(nil), sigBytes) {
		return nil, errors.New("dingtalk state signature mismatch")
	}
	var payload DingTalkStatePayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal dingtalk state: %w", err)
	}
	if strings.TrimSpace(payload.Nonce) == "" {
		return nil, errors.New("state.nonce is required")
	}
	if payload.IssuedAt == 0 {
		return nil, errors.New("state.iat is required")
	}
	issuedAt := time.Unix(payload.IssuedAt, 0)
	if time.Since(issuedAt) > dingTalkStateMaxAge || time.Until(issuedAt) > time.Minute {
		return nil, errors.New("dingtalk state expired or invalid timestamp")
	}
	return &payload, nil
}
