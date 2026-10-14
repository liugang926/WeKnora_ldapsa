//go:build sqlite_fts5

package service

import (
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The current module marker requires its actual onboarding ledger, even for
// a pre-existing direct member's first login. Missing inventory must refuse.
func installDocumentLiveAuthSchema(t *testing.T, db *gorm.DB) {
	t.Helper()
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_JWT") != "1" {
		return
	}
	require.NoError(t, db.AutoMigrate(&types.AuthToken{}, &types.WikiPage{}))
	ddl, err := os.ReadFile("../../../migrations/sqlite/000074_nextcloud_onboarding.up.sql")
	require.NoError(t, err)
	// Session AutoMigrate already installed this column. All production
	// onboarding tables, constraints and triggers remain byte-for-byte.
	statement := "ALTER TABLE sessions ADD COLUMN nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE;"
	require.True(t, db.Migrator().HasColumn(&types.Session{}, "nextcloud_scoped"))
	require.Equal(t, 1, strings.Count(string(ddl), statement))
	require.NoError(t, db.Exec(strings.Replace(string(ddl), statement, "", 1)).Error)
}
