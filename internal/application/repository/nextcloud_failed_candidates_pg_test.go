package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

// The real stack can use a locale collation where '~' sorts before digits.
// This test must run against PostgreSQL rather than SQLite's BINARY collation.
func TestPostgresNextcloudFailedCandidateListUnderLocaleCollation(t *testing.T) {
	knowledge, db := nextcloudVersionPGTestRepo(t)
	require.NoError(t, db.AutoMigrate(&nextcloudCandidateRetryJob{}))
	var oldUpperBoundIncludesFile bool
	err := db.Raw(`SELECT 'nextcloud:instance:77' < 'nextcloud:instance:~' COLLATE "en-x-icu"`).
		Scan(&oldUpperBoundIncludesFile).Error
	if err != nil {
		t.Skip("PostgreSQL en-x-icu collation is unavailable")
	}
	require.False(t, oldUpperBoundIncludesFile,
		"this collation must reproduce the old upper-bound defect")
	require.NoError(t, db.Exec(`ALTER TABLE nextcloud_source_versions
		ALTER COLUMN external_id TYPE text COLLATE "en-x-icu"`).Error)
	candidate := insertNextcloudVersionTestKnowledge(t, db, "failed-77", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, knowledge.StageNextcloudVersion(context.Background(), 7, "kb", "ds",
		"nextcloud:instance:77", "etag-77", candidate.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", candidate.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	var pair NextcloudSourcePairing
	require.NoError(t, db.Where("datasource_id = ?", "ds").Take(&pair).Error)
	items, next, err := NewNextcloudSourcePairingRepository(db).
		ListFailedCandidates(context.Background(), pair, 25, "")
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, items, 1)
	require.Equal(t, "77", items[0].FileID)
}
