//go:build sqlite_fts5

package service

import (
	"os"
	"testing"

	"gorm.io/gorm"
)

// The current module marker requires its actual onboarding ledger, even for
// a pre-existing direct member's first login. Missing inventory must refuse.
func installDocumentLiveAuthSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_JWT") != "1" {
		return
	}
	installDocumentOnboardingSchema(t, db)
}
