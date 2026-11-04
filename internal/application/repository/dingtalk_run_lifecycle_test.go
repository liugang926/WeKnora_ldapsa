package repository

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestFinalizeDingTalkRunCurrentError(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		result       types.SyncResult
		cursor       types.JSON
		cleared      bool
	}{
		{"success", types.SyncLogStatusSuccess, types.SyncResult{Total: 18, Created: 18}, types.JSON(`{"version":1}`), true},
		{"zero-success-without-cursor", types.SyncLogStatusSuccess, types.SyncResult{}, nil, true},
		{"failed", types.SyncLogStatusFailed, types.SyncResult{Failed: 1}, nil, false},
		{"partial", types.SyncLogStatusPartial, types.SyncResult{Failed: 1}, nil, false},
		{"canceled", types.SyncLogStatusCanceled, types.SyncResult{}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "notice.db")), &gorm.Config{})
			require.NoError(t, err)
			pool, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pool.Close()) })
			require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.SyncLog{}, &dingTalkRunClaim{}))
			require.NoError(t, db.Exec("CREATE TABLE dingtalk_source_input_revisions (datasource_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)").Error)
			ds := &types.DataSource{ID: "source", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeDingTalk, Status: types.DataSourceStatusActive, ErrorMessage: "previous import failed"}
			require.NoError(t, db.Create(ds).Error)
			require.NoError(t, db.Exec("INSERT INTO dingtalk_source_input_revisions VALUES (?, ?)", ds.ID, 1).Error)
			finished := time.Now().Add(-time.Hour).UTC()
			history := &types.SyncLog{ID: "historical-failure", DataSourceID: ds.ID, TenantID: ds.TenantID, Status: types.SyncLogStatusFailed, ErrorMessage: "historical failure", ItemsFailed: 1, FinishedAt: &finished}
			require.NoError(t, db.Create(history).Error)
			var historicalBefore types.SyncLog
			require.NoError(t, db.Where("id = ?", history.ID).Take(&historicalBefore).Error)
			require.NoError(t, db.Create(&types.SyncLog{ID: "current-run", DataSourceID: ds.ID, TenantID: ds.TenantID, Status: types.SyncLogStatusRunning}).Error)
			store := NewKnowledgeInputProvenanceStore(db)
			ctx, err := store.ClaimDingTalkRun(context.Background(), ds, "current-run", "current-task", 0, time.Minute)
			require.NoError(t, err)
			require.NoError(t, store.FinalizeDingTalkRun(ctx, tc.status, &tc.result, tc.cursor))
			var actual types.DataSource
			require.NoError(t, db.Where("id = ?", ds.ID).Take(&actual).Error)
			if tc.cleared {
				require.Empty(t, actual.ErrorMessage)
			} else {
				require.Equal(t, ds.ErrorMessage, actual.ErrorMessage)
			}
			var historicalAfter types.SyncLog
			require.NoError(t, db.Where("id = ?", history.ID).Take(&historicalAfter).Error)
			require.Equal(t, historicalBefore, historicalAfter)
		})
	}
}
