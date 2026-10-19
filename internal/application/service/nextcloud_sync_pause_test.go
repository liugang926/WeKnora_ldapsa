package service

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNextcloudSyncPausedCurrentSourceAdmission(t *testing.T) {
	enabled := true
	ds := &types.DataSource{ID: "source", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, SyncEnabled: &enabled}
	log := &types.SyncLog{ID: "log", DataSourceID: ds.ID, TenantID: 7, Status: types.SyncLogStatusRunning}
	svc := &DataSourceService{dsRepo: newKBDeleteDSRepo("kb", ds), syncLogRepo: &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{"log": log}}}
	payload := types.DataSourceSyncPayload{DataSourceID: ds.ID, TenantID: 7, SyncLogID: log.ID}
	stopped, err := svc.checkNextcloudSyncActive(context.Background(), payload)
	require.NoError(t, err)
	require.False(t, stopped)
	enabled = false
	stopped, err = svc.checkNextcloudSyncActive(context.Background(), payload)
	require.True(t, stopped)
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.Equal(t, types.DataSourceStatusActive, ds.Status, "content pause does not revoke source authorization or change pairing state")
	enabled = true
	stopped, err = svc.checkNextcloudSyncActive(context.Background(), payload)
	require.NoError(t, err)
	require.False(t, stopped)
	payload.TenantID = 8
	_, err = svc.checkNextcloudSyncActive(context.Background(), payload)
	require.Error(t, err)
	svc.dsRepo = nil
	_, err = svc.checkNextcloudSyncActive(context.Background(), payload)
	require.Error(t, err, "missing guard denies without panic")
}
