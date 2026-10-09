package nextcloud

import (
	"context"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

// Run with NEXTCLOUD_LIVE_BASE_URL and NEXTCLOUD_LIVE_TOKEN against the local
// Compose stack. It proves the Go-signed GET and POST cross the PHP verifier.
func TestLiveSignedNextcloudMachineRequests(t *testing.T) {
	base := os.Getenv("NEXTCLOUD_LIVE_BASE_URL")
	token := os.Getenv("NEXTCLOUD_LIVE_TOKEN")
	if base == "" || token == "" {
		t.Skip("local Nextcloud Compose stack not configured")
	}
	allowHTTPTestOrigin(t, base)
	source := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": base},
		Credentials: map[string]interface{}{"token": token},
	}
	cfg, err := parseConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := newClient(cfg).capabilities(context.Background())
	if err != nil || capabilities.InstanceID == "" {
		t.Fatalf("signed capability request failed: %+v, %v", capabilities, err)
	}
	decision, err := AuthorizeCurrentFile(context.Background(), source,
		capabilities.InstanceID, "dev-published", "unmapped-live-test",
		"00112233-4455-6677-8899-aabbccddeeff", 77)
	if err != nil || decision.Allow || decision.Reason != "identity_unmapped_or_disabled" {
		t.Fatalf("signed authorization POST failed: %+v, %v", decision, err)
	}
}
