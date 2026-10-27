package runtime

import (
	"context"
	"strings"
	"testing"
)

func TestValidateDeploymentCapabilitiesNotRequired(t *testing.T) {
	t.Setenv(RequireAnyDocSelfTestEnv, "")
	if err := ValidateDeploymentCapabilities(context.Background()); err != nil {
		t.Fatalf("non-strict startup failed: %v", err)
	}
	readiness := GetDeploymentReadiness()
	if !readiness.Ready || readiness.Required || readiness.Checked || readiness.Reason != "not_required" {
		t.Fatalf("unexpected readiness in non-strict mode: %+v", readiness)
	}
}

func TestValidateDeploymentCapabilitiesStrictModeFailsClosed(t *testing.T) {
	t.Setenv(RequireAnyDocSelfTestEnv, "1")
	err := ValidateDeploymentCapabilities(context.Background())
	readiness := GetDeploymentReadiness()
	if !readiness.Required || !readiness.Checked || readiness.Ready {
		t.Fatalf("strict mode did not preserve an unready snapshot: %+v", readiness)
	}
	if err == nil || !strings.Contains(err.Error(), "mandatory deployment capability failed") {
		t.Fatalf("strict mode error = %v, want mandatory capability failure", err)
	}
}
