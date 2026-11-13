package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestDingTalkLegacyScheduleFailsOnceBeforeDocumentFetch(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "schedule.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.SyncLog{}))
	require.NoError(t, db.Exec("ALTER TABLE sync_logs DROP COLUMN dingtalk_schedule_grant").Error)
	for _, filename := range []string{
		"000090_dingtalk_publication.up.sql", "000091_dingtalk_lifecycle.up.sql",
		"000093_dingtalk_schedule_authorization.up.sql",
	} {
		migration, err := os.ReadFile("../../../migrations/sqlite/" + filename)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(migration)).Error)
	}
	require.NoError(t, db.Exec(`CREATE TABLE dingtalk_source_input_revisions (
        datasource_id TEXT PRIMARY KEY, revision INTEGER NOT NULL)`).Error)
	ds := &types.DataSource{
		ID: "finance", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeDingTalk,
		Status: types.DataSourceStatusActive, SyncSchedule: "0 30 4 * * *",
	}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Exec("INSERT INTO dingtalk_source_input_revisions VALUES (?, 1)", ds.ID).Error)
	logs := repository.NewSyncLogRepository(db)
	svc := &DataSourceService{
		dsRepo: repository.NewDataSourceRepository(db), syncLogRepo: logs,
		kbService:    &processSyncKBService{kb: &types.KnowledgeBase{ID: "kb", TenantID: 7}},
		sourceInputs: repository.NewKnowledgeInputProvenanceStore(db),
	}
	start := time.Now().UTC()
	ctx := types.WithTaskExecutionID(context.Background(), "dssync:finance:"+start.Format("200601021504"))
	log := &types.SyncLog{
		DataSourceID: ds.ID, TenantID: 7, Status: types.SyncLogStatusRunning,
		RecoveryTrigger: "schedule",
		QueueTaskID:     "dssync:finance:" + start.Format("200601021504"), StartedAt: start,
	}
	require.NoError(t, logs.Create(ctx, log))
	payload := types.DataSourceSyncPayload{DataSourceID: ds.ID, TenantID: 7, SyncLogID: log.ID, Trigger: "schedule"}
	run := func(p types.DataSourceSyncPayload) error {
		raw, err := json.Marshal(p)
		require.NoError(t, err)
		return svc.ProcessSync(ctx, asynq.NewTask(types.TypeDataSourceSync, raw))
	}
	forged := payload
	forged.Initiator.UserID = "claimed-editor"
	require.ErrorIs(t, run(forged), asynq.SkipRetry)
	stored, err := logs.FindByID(ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusRunning, stored.Status)
	err = run(payload)
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.ErrorIs(t, err, repository.ErrDingTalkScheduleAuthorizationRequired)
	stored, err = logs.FindByID(ctx, log.ID)
	require.NoError(t, err)
	require.Equal(t, types.SyncLogStatusFailed, stored.Status)
	require.Zero(t, stored.ItemsFailed)
	require.Zero(t, stored.ItemsTotal)
	require.Equal(t, repository.ErrDingTalkScheduleAuthorizationRequired.Error(), stored.ErrorMessage)
	var actual types.DataSource
	require.NoError(t, db.First(&actual, "id = ?", ds.ID).Error)
	require.Equal(t, stored.ErrorMessage, actual.ErrorMessage)
	require.Empty(t, actual.LastSyncCursor)
}
