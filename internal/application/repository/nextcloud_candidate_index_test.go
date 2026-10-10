package repository

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudCandidateKnowledgeQueriesUsePartialIndex(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:"+uuid.NewString()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	schema, err := os.ReadFile("../../../migrations/sqlite/000031_nextcloud_source_versions.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(schema)).Error)
	for i, candidate := range []string{"", "candidate"} {
		require.NoError(t, db.Table("nextcloud_source_versions").Create(map[string]interface{}{
			"tenant_id": 7, "knowledge_base_id": "kb", "datasource_id": "ds",
			"external_id": i, "candidate_knowledge_id": candidate, "state": "published",
		}).Error)
	}
	for _, filterState := range []bool{false, true} {
		query := withNextcloudCandidateKnowledgeID(db.Table("nextcloud_source_versions"), "candidate")
		if filterState {
			query = query.Where("state IN ?", []string{"staging", "published"})
		}
		statement := query.Session(&gorm.Session{DryRun: true}).Find(&[]nextcloudSourceVersion{}).Statement
		var plan []struct{ Detail string }
		require.NoError(t, db.Raw("EXPLAIN QUERY PLAN "+statement.SQL.String(), statement.Vars...).Scan(&plan).Error)
		require.Len(t, plan, 1)
		require.Contains(t, plan[0].Detail, "SEARCH")
		require.Contains(t, plan[0].Detail, "idx_nextcloud_source_versions_candidate")
		var row nextcloudSourceVersion
		require.NoError(t, query.Take(&row).Error)
		require.Equal(t, "candidate", row.CandidateKnowledgeID)
	}
	query := withNextcloudCandidateKnowledgeID(db.Table("nextcloud_source_versions"), "")
	statement := query.Session(&gorm.Session{DryRun: true}).Find(&[]nextcloudSourceVersion{}).Statement
	require.False(t, strings.Contains(statement.SQL.String(), "<>"))
	var row nextcloudSourceVersion
	require.NoError(t, query.Take(&row).Error)
	require.Empty(t, row.CandidateKnowledgeID)
}
