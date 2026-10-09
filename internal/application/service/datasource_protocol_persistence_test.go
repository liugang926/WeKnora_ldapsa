package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type protocolBatchFailureConnector struct {
	datasource.Connector
	err error
}

func (protocolBatchFailureConnector) Type() string { return types.ConnectorTypeFeishu }

func (c *protocolBatchFailureConnector) FetchAll(
	context.Context, *types.DataSourceConfig, []string,
) ([]types.FetchedItem, error) {
	return nil, c.err
}

func (c *protocolBatchFailureConnector) FetchIncremental(
	context.Context, *types.DataSourceConfig, *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	return nil, nil, c.err
}

type protocolStreamingFailureConnector struct {
	recordingStreamConnector
	err error
}

func (c *protocolStreamingFailureConnector) FetchStream(
	context.Context, *types.DataSourceConfig, *types.SyncCursor, datasource.StreamHandler,
) (*types.SyncCursor, error) {
	return nil, c.err
}

func protocolPersistenceService(t *testing.T, connectorType string) (
	*DataSourceService, *gorm.DB, *types.DataSource, *types.SyncLog,
) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "protocol-persistence.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.SyncLog{}))
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, tenant_id INTEGER NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT 0
	)`).Error)
	require.NoError(t, db.Exec(
		"INSERT INTO knowledge_bases (id, tenant_id) VALUES ('protocol-kb', 7)",
	).Error)
	cursor := types.JSON(`{"connector_cursor":{"marker":"unchanged"}}`)
	ds := &types.DataSource{
		ID: "protocol-ds", TenantID: 7, KnowledgeBaseID: "protocol-kb", Name: "protocol source",
		Type: connectorType, Status: types.DataSourceStatusActive, SyncMode: types.SyncModeIncremental,
		Config: types.JSON(`{"type":"feishu"}`), LastSyncCursor: cursor,
	}
	log := &types.SyncLog{
		ID: "protocol-log", DataSourceID: ds.ID, TenantID: ds.TenantID,
		Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC(),
	}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Create(log).Error)
	svc := &DataSourceService{
		dsRepo: apprepo.NewDataSourceRepository(db), syncLogRepo: apprepo.NewSyncLogRepository(db),
		tenantRepo: &processSyncTenantRepo{tenant: &types.Tenant{ID: 7}}, tagService: &processSyncTagService{},
		kbService: &processSyncKBService{kb: &types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: 7}},
	}
	return svc, db, ds, log
}

func assertPersistedProtocolFailure(t *testing.T, db *gorm.DB, dsID, logID string, originalCursor types.JSON) {
	t.Helper()
	var storedDS types.DataSource
	var storedLog types.SyncLog
	require.NoError(t, db.Where("id = ?", dsID).Take(&storedDS).Error)
	require.NoError(t, db.Where("id = ?", logID).Take(&storedLog).Error)
	const legacy = "Fetch failed: Nextcloud fixture failure"
	assert.Equal(t, legacy, storedDS.ErrorMessage, "the persisted data-source public bytes must remain unchanged")
	assert.Equal(t, legacy, storedLog.ErrorMessage, "the persisted sync-log public bytes must remain unchanged")
	require.Equal(t, types.DataSourceStatusError, storedDS.Status)
	require.Equal(t, types.SyncLogStatusFailed, storedLog.Status)
	require.Equal(t, originalCursor, storedDS.LastSyncCursor, "a failed fetch must not advance the cursor")
}

func TestStreamingProtocolFailureKeepsRealSQLitePublicMessages(t *testing.T) {
	svc, db, ds, log := protocolPersistenceService(t, types.ConnectorTypeNextcloud)
	originalCursor := append(types.JSON(nil), ds.LastSyncCursor...)
	cause := errors.New("nextcloud fixture failure")
	fetchError := apperrors.NewProtocolError(cause, "Nextcloud fixture failure")
	connector := &protocolStreamingFailureConnector{err: fetchError}
	err := svc.processSyncStreaming(context.Background(), connector, ds, log,
		&types.DataSourceConfig{}, types.DataSourceSyncPayload{}, false)
	require.ErrorIs(t, err, cause)
	require.Same(t, fetchError, err, "only public persistence may change; return the original typed failure")
	assertPersistedProtocolFailure(t, db, ds.ID, log.ID, originalCursor)
}

func TestBatchProtocolFailureKeepsRealSQLitePublicMessages(t *testing.T) {
	svc, db, ds, log := protocolPersistenceService(t, types.ConnectorTypeFeishu)
	originalCursor := append(types.JSON(nil), ds.LastSyncCursor...)
	cause := errors.New("nextcloud fixture failure")
	fetchError := apperrors.NewProtocolError(cause, "Nextcloud fixture failure")
	registry := datasource.NewConnectorRegistry()
	require.NoError(t, registry.Register(&protocolBatchFailureConnector{err: fetchError}))
	svc.connectorRegistry = registry
	payload, err := json.Marshal(types.DataSourceSyncPayload{
		DataSourceID: ds.ID, SyncLogID: log.ID, TenantID: ds.TenantID,
	})
	require.NoError(t, err)
	err = svc.ProcessSync(context.Background(), asynq.NewTask("protocol-persistence", payload))
	require.ErrorIs(t, err, cause)
	require.Same(t, fetchError, err, "only public persistence may change; return the original typed failure")
	assertPersistedProtocolFailure(t, db, ds.ID, log.ID, originalCursor)
}
