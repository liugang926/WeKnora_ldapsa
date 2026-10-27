package service

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

func setupNextcloudStallDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "stall.db")), &gorm.Config{})
	require.NoError(t, err)
	for _, statement := range []string{
		`CREATE TABLE knowledges (
			id TEXT PRIMARY KEY,
			tenant_id INTEGER NOT NULL,
			knowledge_base_id TEXT NOT NULL,
			channel TEXT NOT NULL,
			parse_status TEXT NOT NULL,
			error_message TEXT NOT NULL DEFAULT '',
			pending_subtasks_count INTEGER NOT NULL DEFAULT 0,
			summary_status TEXT NOT NULL DEFAULT '',
			updated_at DATETIME NOT NULL,
			deleted_at DATETIME
		)`,
		`CREATE TABLE nextcloud_source_versions (
			tenant_id INTEGER NOT NULL,
			knowledge_base_id TEXT NOT NULL,
			datasource_id TEXT NOT NULL,
			external_id TEXT NOT NULL,
			candidate_knowledge_id TEXT NOT NULL,
			state TEXT NOT NULL
		)`,
		`CREATE TABLE data_sources (
			id TEXT PRIMARY KEY,
			tenant_id INTEGER NOT NULL,
			knowledge_base_id TEXT NOT NULL,
			type TEXT NOT NULL,
			status TEXT NOT NULL,
			deleted_at DATETIME
		)`,
		`CREATE TABLE nextcloud_source_pairings (
			tenant_id INTEGER NOT NULL,
			knowledge_base_id TEXT NOT NULL,
			datasource_id TEXT NOT NULL,
			state TEXT NOT NULL
		)`,
		`CREATE TABLE task_pending_ops (
			task_type TEXT NOT NULL,
			scope TEXT NOT NULL,
			op TEXT NOT NULL,
			dedup_key TEXT NOT NULL
		)`,
	} {
		require.NoError(t, db.Exec(statement).Error)
	}
	return db
}

func insertNextcloudStall(t *testing.T, db *gorm.DB, updatedAt time.Time) {
	t.Helper()
	require.NoError(t, db.Exec(`INSERT INTO knowledges
		(id,tenant_id,knowledge_base_id,channel,parse_status,pending_subtasks_count,updated_at)
		VALUES ('candidate-stalled',7,'kb-1',?,?,33,?)`,
		types.ConnectorTypeNextcloud, types.ParseStatusFinalizing, updatedAt).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_versions
		(tenant_id,knowledge_base_id,datasource_id,external_id,candidate_knowledge_id,state)
		VALUES (7,'kb-1','ds-1','nextcloud:instance:77','candidate-stalled','staging')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO data_sources
		(id,tenant_id,knowledge_base_id,type,status) VALUES ('ds-1',7,'kb-1',?,'active')`,
		types.ConnectorTypeNextcloud).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_pairings
		(tenant_id,knowledge_base_id,datasource_id,state) VALUES (7,'kb-1','ds-1','active')`).Error)
}

func TestNextcloudFinalizingWatchdogFailsOnlyWithoutExecutableWork(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_FINALIZING_STALL_THRESHOLD", "5m")
	db := setupNextcloudStallDB(t)
	insertNextcloudStall(t, db, time.Now().UTC().Add(-10*time.Minute))
	service := newHousekeepingSvcWithInspector(db, fakeTaskInspector{})

	service.recoverStalledNextcloudFinalizing(context.Background(), time.Now().UTC())

	var row struct {
		Status    string
		Pending   int
		ErrorText string
	}
	require.NoError(t, db.Table("knowledges").
		Select("parse_status AS status, pending_subtasks_count AS pending, error_message AS error_text").
		Where("id = 'candidate-stalled'").Take(&row).Error)
	require.Equal(t, types.ParseStatusFailed, row.Status)
	require.Zero(t, row.Pending)
	require.Equal(t, "nextcloud_finalizing_stalled_without_queue_task", row.ErrorText)

	var datasourceStatus string
	require.NoError(t, db.Table("data_sources").Select("status").
		Where("id = 'ds-1'").Scan(&datasourceStatus).Error)
	require.Equal(t, types.DataSourceStatusActive, datasourceStatus,
		"a single stalled file must not fail the whole data source")
}

func TestNextcloudFinalizingWatchdogKeepsQueuedWork(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_FINALIZING_STALL_THRESHOLD", "5m")
	db := setupNextcloudStallDB(t)
	insertNextcloudStall(t, db, time.Now().UTC().Add(-10*time.Minute))
	service := newHousekeepingSvcWithInspector(db, fakeTaskInspector{
		queued: map[string]bool{"candidate-stalled": true},
	})

	service.recoverStalledNextcloudFinalizing(context.Background(), time.Now().UTC())

	var status string
	require.NoError(t, db.Table("knowledges").Select("parse_status").
		Where("id = 'candidate-stalled'").Scan(&status).Error)
	require.Equal(t, types.ParseStatusFinalizing, status)
}
