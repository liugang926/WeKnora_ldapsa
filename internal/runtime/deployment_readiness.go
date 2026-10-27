package runtime

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser/anydoc"
	"github.com/Tencent/WeKnora/internal/logger"
)

// RequireAnyDocSelfTestEnv enables the mandatory production parser probe.
const RequireAnyDocSelfTestEnv = "WEKNORA_REQUIRE_ANYDOC_SELFTEST"

// DeploymentReadiness is the process-local mandatory-capability snapshot. The
// production image checks it before binding the HTTP port, so a missing parser
// cannot be mistaken for a healthy deployment.
type DeploymentReadiness struct {
	Required      bool      `json:"required"`
	Checked       bool      `json:"checked"`
	Ready         bool      `json:"ready"`
	AnyDocVersion string    `json:"anydoc_version,omitempty"`
	PDF           bool      `json:"pdf"`
	DOCX          bool      `json:"docx"`
	Reason        string    `json:"reason,omitempty"`
	CheckedAt     time.Time `json:"checked_at,omitempty"`
}

var deploymentReadiness = struct {
	sync.RWMutex
	value DeploymentReadiness
}{}

// ValidateDeploymentCapabilities runs the mandatory startup probes. It is a
// no-op outside strict production mode, so local builds without the Rust
// archive remain useful while production builds fail closed.
func ValidateDeploymentCapabilities(ctx context.Context) error {
	required := strings.TrimSpace(os.Getenv(RequireAnyDocSelfTestEnv)) == "1"
	next := DeploymentReadiness{
		Required:  required,
		Checked:   required,
		Ready:     !required,
		Reason:    "not_required",
		CheckedAt: time.Now().UTC(),
	}
	if required {
		result, err := anydoc.SelfTest()
		next.AnyDocVersion = result.Version
		next.PDF = result.Checks["pdf"]
		next.DOCX = result.Checks["docx"]
		if err != nil {
			next.Ready = false
			next.Reason = err.Error()
		} else {
			next.Ready = true
			next.Reason = ""
		}
	}
	deploymentReadiness.Lock()
	deploymentReadiness.value = next
	deploymentReadiness.Unlock()
	if next.Ready {
		logger.Infof(ctx, "[deployment-readiness] required=%t anydoc=%s pdf=%t docx=%t",
			next.Required, next.AnyDocVersion, next.PDF, next.DOCX)
		return nil
	}
	return fmt.Errorf("mandatory deployment capability failed: %s", next.Reason)
}

// GetDeploymentReadiness returns a copy safe for concurrent HTTP reads.
func GetDeploymentReadiness() DeploymentReadiness {
	deploymentReadiness.RLock()
	defer deploymentReadiness.RUnlock()
	return deploymentReadiness.value
}
