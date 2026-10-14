package nextcloud

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestMachineCanonicalVector(t *testing.T) {
	canonical, err := machineCanonicalRequest(
		"post",
		"/index.php/apps/integration_weknora/api/v1/bindings/dev-published/authorize",
		"b=two+words&a=1&empty",
		[]byte(`{"file_id":42}`), "1780000000", "00112233445566778899aabbccddeeff", "default",
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := strings.Join([]string{
		"weknora-hmac-sha256-v1", "POST",
		"/index.php/apps/integration_weknora/api/v1/bindings/dev-published/authorize",
		"a=1&b=two%20words&empty=",
		"f4581e98d2ce1acdc3da81a39c21b618110a7e120535e15d1a3410bebf684b71",
		"1780000000", "00112233445566778899aabbccddeeff", "default",
	}, "\n")
	if canonical != expected {
		t.Fatalf("canonical = %q, want %q", canonical, expected)
	}
	key := sha256.Sum256([]byte("paired-secret"))
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte(canonical))
	if got := hex.EncodeToString(mac.Sum(
		nil)); got != "204a5da882a867f96220bab1e17d1ed2df2503c288a290fa8c17b3262bb7cc01" {
		t.Fatalf("signature = %s", got)
	}
}

func TestMachineCanonicalQueryRejectsAmbiguousKeys(t *testing.T) {
	for _, query := range []string{"x=1&x=2", "x=1&%78=2", "x=%broken", "a.b=1", "x=1;y=2"} {
		if _, err := canonicalMachineQuery(query); err == nil {
			t.Fatalf("accepted ambiguous query %q", query)
		}
	}
}

func TestMachineSignRequestBindsBodyPathAndFreshNonce(t *testing.T) {
	cfg := config{token: "paired-secret", keyID: "default"}
	request, err := http.NewRequest(http.MethodPost,
		"https://files.example/index.php/apps/integration_weknora/api/v1/bindings"+
			"/a/authorize?z=2&a=1",
		strings.NewReader(`{"file_id":42}`))
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"file_id":42}`)
	if err := signMachineRequest(request, cfg, body); err != nil {
		t.Fatal(err)
	}
	if request.Header.Get("Authorization") != "Bearer paired-secret" ||
		request.Header.Get("X-WeKnora-Key-Id") != "default" ||
		!regexp.MustCompile(`^[a-f0-9]{32}$`).MatchString(request.Header.Get("X-WeKnora-Nonce")) {
		t.Fatal("missing or malformed machine authentication headers")
	}
	timestamp := request.Header.Get("X-WeKnora-Timestamp")
	parsed, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || time.Now().Unix()-parsed > 5 || parsed-time.Now().Unix() > 5 {
		t.Fatalf("timestamp = %s: %v", timestamp, err)
	}
	canonical, err := machineCanonicalRequest(request.Method, request.URL.EscapedPath(),
		request.URL.RawQuery, body, timestamp, request.Header.Get("X-WeKnora-Nonce"), "default")
	if err != nil {
		t.Fatal(err)
	}
	key := sha256.Sum256([]byte(cfg.token))
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte(canonical))
	if !hmac.Equal(mac.Sum(nil), mustDecodeHex(t, request.Header.Get("X-WeKnora-Signature"))) {
		t.Fatal("signed header does not match method, URL and body")
	}
	first := request.Header.Get("X-WeKnora-Nonce")
	if err := signMachineRequest(request, cfg, body); err != nil {
		t.Fatal(err)
	}
	if request.Header.Get("X-WeKnora-Nonce") == first {
		t.Fatal("reused nonce")
	}
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestMachineKeyIDConfiguration(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", "https://files.example")
	source := &types.DataSourceConfig{
		Settings:    map[string]interface{}{"base_url": "https://files.example"},
		Credentials: map[string]interface{}{"token": "paired-secret", "key_id": "new-key.2"},
	}
	cfg, err := parseConfig(source)
	if err != nil || cfg.keyID != "new-key.2" {
		t.Fatalf("key ID = %q, err = %v", cfg.keyID, err)
	}
	source.Credentials["key_id"] = "unsafe/key"
	if _, err := parseConfig(source); err == nil {
		t.Fatal("accepted malformed key ID")
	}
}
