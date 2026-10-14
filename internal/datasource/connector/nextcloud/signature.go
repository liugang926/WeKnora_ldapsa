package nextcloud

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
)

const machineSignatureVersion = "weknora-hmac-sha256-v1"

var (
	machineKeyIDPattern    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	machineQueryKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
)

func validMachineKeyID(id string) bool { return machineKeyIDPattern.MatchString(id) }

// canonicalMachineQuery uses form decoding, RFC 3986 encoding and bytewise
// sorting by encoded key then value. Repeated pairs are retained.
func canonicalMachineQuery(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if strings.Contains(raw, ";") {
		return "", fmt.Errorf("ambiguous machine query separator")
	}
	type pair struct{ key, value string }
	pairs := make([]pair, 0)
	seenKeys := make(map[string]bool)
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		key, value, _ := strings.Cut(part, "=")
		decodedKey, err := url.QueryUnescape(key)
		if err != nil {
			return "", err
		}
		if !machineQueryKeyPattern.MatchString(decodedKey) {
			return "", fmt.Errorf("invalid machine query key")
		}
		if seenKeys[decodedKey] {
			return "", fmt.Errorf("duplicate machine query key")
		}
		seenKeys[decodedKey] = true
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return "", err
		}
		encode := func(input string) string {
			return strings.ReplaceAll(url.QueryEscape(input), "+", "%20")
		}
		pairs = append(pairs, pair{encode(decodedKey), encode(decodedValue)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key != pairs[j].key {
			return pairs[i].key < pairs[j].key
		}
		return pairs[i].value < pairs[j].value
	})
	parts := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		parts = append(parts, pair.key+"="+pair.value)
	}
	return strings.Join(parts, "&"), nil
}

func machineCanonicalRequest(method, rawPath, rawQuery string, body []byte,
	timestamp, nonce, keyID string,
) (string, error) {
	query, err := canonicalMachineQuery(rawQuery)
	if err != nil {
		return "", err
	}
	bodyHash := sha256.Sum256(body)
	return strings.Join([]string{
		machineSignatureVersion,
		strings.ToUpper(method),
		rawPath,
		query,
		hex.EncodeToString(bodyHash[:]),
		timestamp,
		nonce,
		keyID,
	}, "\n"), nil
}

func signMachineRequest(req *http.Request, cfg config, body []byte) error {
	if !validMachineKeyID(cfg.keyID) {
		return fmt.Errorf("invalid Nextcloud machine key ID")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("generate Nextcloud request nonce: %w", err),
			fmt.Sprintf("generate Nextcloud request nonce: %s", apperrors.PublicMessage(err)))
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := hex.EncodeToString(random[:])
	canonical, err := machineCanonicalRequest(req.Method, req.URL.EscapedPath(), req.URL.RawQuery,
		body, timestamp, nonce, cfg.keyID)
	if err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("canonicalize Nextcloud request: %w", err),
			fmt.Sprintf("canonicalize Nextcloud request: %s", apperrors.PublicMessage(err)))
	}
	key := sha256.Sum256([]byte(cfg.token))
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte(canonical))
	req.Header.Set("Authorization", "Bearer "+cfg.token)
	req.Header.Set("X-WeKnora-Key-Id", cfg.keyID)
	req.Header.Set("X-WeKnora-Timestamp", timestamp)
	req.Header.Set("X-WeKnora-Nonce", nonce)
	req.Header.Set("X-WeKnora-Signature", hex.EncodeToString(mac.Sum(nil)))
	return nil
}
