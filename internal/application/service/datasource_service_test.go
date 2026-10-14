package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	nextcloudconnector "github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func nextcloudBaselineTestDB(t *testing.T) (*gorm.DB, *apprepo.NextcloudEventInboxRepository) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "baseline.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_source_versions (
		tenant_id INTEGER, knowledge_base_id TEXT, datasource_id TEXT,
		external_id TEXT, state TEXT)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (
		id TEXT PRIMARY KEY, tenant_id INTEGER, knowledge_base_id TEXT,
		channel TEXT, metadata TEXT)`).Error)
	return db, apprepo.NewNextcloudEventInboxRepository(db)
}

func nextcloudBaselineTestRepo(t *testing.T) *apprepo.NextcloudEventInboxRepository {
	t.Helper()
	_, repo := nextcloudBaselineTestDB(t)
	return repo
}

func TestProcessSyncCancelsWhenKnowledgeBaseDeleted(t *testing.T) {
	ds := &types.DataSource{
		ID:              "ds-1",
		TenantID:        1,
		KnowledgeBaseID: "kb-deleted",
		Type:            types.ConnectorTypeRSS,
		Status:          types.DataSourceStatusActive,
	}
	dsRepo := newKBDeleteDSRepo("kb-deleted", ds)
	syncLog := &types.SyncLog{
		ID:           "log-1",
		DataSourceID: ds.ID,
		TenantID:     ds.TenantID,
		Status:       types.SyncLogStatusRunning,
		StartedAt:    time.Now().UTC(),
	}
	syncLogRepo := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{syncLog.ID: syncLog}}

	svc := &DataSourceService{
		dsRepo:      dsRepo,
		syncLogRepo: syncLogRepo,
		kbService:   &processSyncKBService{getErr: apprepo.ErrKnowledgeBaseNotFound},
	}

	payload, err := json.Marshal(types.DataSourceSyncPayload{
		DataSourceID: ds.ID,
		TenantID:     ds.TenantID,
		SyncLogID:    syncLog.ID,
	})
	require.NoError(t, err)

	err = svc.ProcessSync(context.Background(), asynq.NewTask(types.TypeDataSourceSync, payload))
	require.NoError(t, err)

	updated := syncLogRepo.logs[syncLog.ID]
	require.NotNil(t, updated)
	assert.Equal(t, types.SyncLogStatusCanceled, updated.Status)
	assert.Equal(t, "knowledge base has been deleted", updated.ErrorMessage)
	require.NotNil(t, updated.FinishedAt)
}

type processSyncKBService struct {
	getErr error
	kb     *types.KnowledgeBase
}

func (s *processSyncKBService) CreateKnowledgeBase(context.Context, *types.KnowledgeBase) (*types.KnowledgeBase, error) {
	return nil, nil
}

func (s *processSyncKBService) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, s.getErr
}

func (s *processSyncKBService) GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, s.getErr
}

func (s *processSyncKBService) GetKnowledgeBasesByIDsOnly(context.Context, []string) ([]*types.KnowledgeBase, error) {
	return nil, nil
}

func (s *processSyncKBService) FillKnowledgeBaseCounts(context.Context, *types.KnowledgeBase) error {
	return nil
}

func (s *processSyncKBService) ListKnowledgeBases(context.Context) ([]*types.KnowledgeBase, error) {
	return nil, nil
}

func (s *processSyncKBService) ListKnowledgeBasesByTenantID(context.Context, uint64) ([]*types.KnowledgeBase, error) {
	return nil, nil
}

func (s *processSyncKBService) UpdateKnowledgeBase(
	context.Context, string, string, string, *types.KnowledgeBaseConfig,
) (*types.KnowledgeBase, error) {
	return nil, nil
}
func (s *processSyncKBService) DeleteKnowledgeBase(context.Context, string) error { return nil }
func (s *processSyncKBService) TogglePinKnowledgeBase(context.Context, string) (*types.KnowledgeBase, error) {
	return nil, nil
}

func (s *processSyncKBService) HybridSearch(context.Context, string, types.SearchParams) ([]*types.SearchResult, error) {
	return nil, nil
}

func (s *processSyncKBService) GetQueryEmbedding(context.Context, string, string) ([]float32, error) {
	return nil, nil
}

func (s *processSyncKBService) ResolveEmbeddingModelKeys(context.Context, []*types.KnowledgeBase) map[string]string {
	return nil
}

func (s *processSyncKBService) CopyKnowledgeBase(context.Context, string, string) (*types.KnowledgeBase, *types.KnowledgeBase, error) {
	return nil, nil, nil
}

func (s *processSyncKBService) DuplicateKnowledgeBase(context.Context, string) (*types.KnowledgeBase, error) {
	return nil, nil
}
func (s *processSyncKBService) GetRepository() interfaces.KnowledgeBaseRepository { return nil }
func (s *processSyncKBService) ProcessKBDelete(context.Context, *asynq.Task) error {
	return nil
}

var _ interfaces.KnowledgeBaseService = (*processSyncKBService)(nil)

type processSyncSyncLogRepo struct {
	logs   map[string]*types.SyncLog
	nextID string
}

type nextcloudUncertainTestQueue struct {
	taskID            string
	acceptedTaskID    string
	acceptBeforeError bool
}

func (q *nextcloudUncertainTestQueue) Enqueue(_ *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	for _, opt := range opts {
		if opt.Type() == asynq.TaskIDOpt {
			q.taskID, _ = opt.Value().(string)
		}
	}
	if q.acceptBeforeError {
		q.acceptedTaskID = q.taskID
	}
	return nil, errors.New("synthetic enqueue response lost")
}

type nextcloudLateSuccessQueue struct {
	logs             *processSyncSyncLogRepo
	taskID           string
	failBeforeReturn bool
}

func (q *nextcloudLateSuccessQueue) Enqueue(_ *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	for _, opt := range opts {
		if opt.Type() == asynq.TaskIDOpt {
			q.taskID, _ = opt.Value().(string)
		}
	}
	if q.failBeforeReturn {
		for _, log := range q.logs.logs {
			log.Status = types.SyncLogStatusFailed
			log.ErrorMessage = "sync_task_absent_before_worker_start"
			now := time.Now().UTC()
			log.FinishedAt = &now
		}
	}
	return &asynq.TaskInfo{ID: q.taskID}, nil
}

func TestManualNextcloudSyncLateEnqueueReceipt(t *testing.T) {
	for _, tc := range []struct {
		name             string
		failBeforeReturn bool
	}{
		{name: "current admission remains accepted"},
		{name: "recovery won before late queue reply", failBeforeReturn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := &types.DataSource{
				ID: "nextcloud-manual", TenantID: 1,
				KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
				Status: types.DataSourceStatusActive,
			}
			logs := &processSyncSyncLogRepo{logs: make(map[string]*types.SyncLog), nextID: "late-receipt-log"}
			queue := &nextcloudLateSuccessQueue{logs: logs, failBeforeReturn: tc.failBeforeReturn}
			svc := &DataSourceService{
				dsRepo:      newKBDeleteDSRepo("kb-1", ds),
				syncLogRepo: logs, taskEnqueuer: queue,
			}
			admitted, err := svc.ManualSync(context.Background(), ds.ID)
			require.Equal(t, "dssync:late-receipt-log", queue.taskID)
			require.NotNil(t, admitted)
			if tc.failBeforeReturn {
				require.ErrorIs(t, err, datasource.ErrSyncEnqueueUncertain)
				require.Equal(t, types.SyncLogStatusFailed, admitted.Status)
				require.Equal(t, "sync_task_absent_before_worker_start", admitted.ErrorMessage)
			} else {
				require.NoError(t, err)
				require.Equal(t, types.SyncLogStatusRunning, admitted.Status)
			}
		})
	}
}

func TestManualNextcloudSyncUncertainEnqueueRetainsAdmission(t *testing.T) {
	for _, tc := range []struct {
		name              string
		acceptBeforeError bool
	}{
		{"failed_before_accept", false},
		{"accepted_reply_lost", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds := &types.DataSource{
				ID: "nextcloud-manual", TenantID: 1,
				KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
				Status: types.DataSourceStatusActive,
			}
			logs := &processSyncSyncLogRepo{logs: make(map[string]*types.SyncLog), nextID: "stable-manual-log"}
			queue := &nextcloudUncertainTestQueue{acceptBeforeError: tc.acceptBeforeError}
			svc := &DataSourceService{
				dsRepo:      newKBDeleteDSRepo("kb-1", ds),
				syncLogRepo: logs, taskEnqueuer: queue,
			}
			admitted, err := svc.ManualSync(context.Background(), ds.ID)
			require.ErrorIs(t, err, datasource.ErrSyncEnqueueUncertain)
			require.Len(t, logs.logs, 1)
			require.NotNil(t, admitted)
			require.Equal(t, "stable-manual-log", admitted.ID)
			require.Equal(t, "dssync:stable-manual-log", queue.taskID)
			if tc.acceptBeforeError {
				require.Equal(t, queue.taskID, queue.acceptedTaskID)
			} else {
				require.Empty(t, queue.acceptedTaskID)
			}
			require.Equal(t, types.SyncLogStatusRunning, admitted.Status)
			require.Equal(t, datasource.NextcloudSyncEnqueueUncertain, admitted.ErrorMessage)
		})
	}
}

func TestProcessSyncNextcloudTerminalLogCannotRestart(t *testing.T) {
	ds := &types.DataSource{
		ID: "nextcloud-terminal", TenantID: 1,
		KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive,
	}
	log := &types.SyncLog{
		ID: "terminated-log", DataSourceID: ds.ID,
		TenantID: ds.TenantID, Status: types.SyncLogStatusFailed,
	}
	logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
	svc := &DataSourceService{dsRepo: newKBDeleteDSRepo("kb-1", ds), syncLogRepo: logs}
	payload, err := json.Marshal(types.DataSourceSyncPayload{
		DataSourceID: ds.ID,
		TenantID:     ds.TenantID, SyncLogID: log.ID, Trigger: "manual",
	})
	require.NoError(t, err)
	err = svc.ProcessSync(context.Background(), asynq.NewTask(types.TypeDataSourceSync, payload))
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.Equal(t, types.SyncLogStatusFailed, log.Status)
}

func TestProcessSyncUnpairedLegacyNextcloudStopsBeforeFetch(t *testing.T) {
	db, _ := nextcloudBaselineTestDB(t)
	require.NoError(t, db.AutoMigrate(&apprepo.NextcloudSourcePairing{}))
	ds := &types.DataSource{
		ID: "legacy-unpaired", TenantID: 1,
		KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive,
	}
	log := &types.SyncLog{
		ID: "old-running-log", DataSourceID: ds.ID,
		TenantID: ds.TenantID, Status: types.SyncLogStatusRunning,
	}
	logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
	svc := &DataSourceService{
		dsRepo: newKBDeleteDSRepo("kb-1", ds), syncLogRepo: logs,
		nextcloudSourcePairings: apprepo.NewNextcloudSourcePairingRepository(db),
	}
	payload, err := json.Marshal(types.DataSourceSyncPayload{
		DataSourceID: ds.ID,
		TenantID:     ds.TenantID, SyncLogID: log.ID, Trigger: "manual",
	})
	require.NoError(t, err)
	err = svc.ProcessSync(context.Background(), asynq.NewTask(types.TypeDataSourceSync, payload))
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.Equal(t, types.SyncLogStatusCanceled, log.Status)
	require.Contains(t, log.ErrorMessage, "not paired")
}

func TestProcessSyncRejectsUnclaimedNextcloudManualBeforeSourceIO(t *testing.T) {
	ds := &types.DataSource{
		ID: "nextcloud-claim", TenantID: 7,
		KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive,
	}
	for _, tc := range []struct {
		name, trigger, taskID string
		version               int
	}{
		{"unknown_version", "manual", "dssync:claim-log", 2},
		{"wrong_trigger", "schedule", "dssync:claim-log", 1},
		{"wrong_task", "manual", "dssync:wrong", 1},
		{"unknown_trigger", "legacy", "dssync:claim-log", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &types.SyncLog{
				ID: "claim-log", DataSourceID: ds.ID, TenantID: ds.TenantID,
				Status: types.SyncLogStatusRunning, RecoveryVersion: tc.version,
				RecoveryTrigger: "manual", QueueTaskID: "dssync:claim-log",
			}
			logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
			// kbService is deliberately nil: reaching it means source work began
			// without an exact claim.
			svc := &DataSourceService{dsRepo: newKBDeleteDSRepo("kb", ds), syncLogRepo: logs}
			payload, err := json.Marshal(types.DataSourceSyncPayload{
				DataSourceID: ds.ID,
				TenantID:     ds.TenantID, SyncLogID: log.ID, Trigger: tc.trigger,
			})
			require.NoError(t, err)
			err = svc.ProcessSync(types.WithTaskExecutionID(context.Background(), tc.taskID),
				asynq.NewTask(types.TypeDataSourceSync, payload))
			require.ErrorIs(t, err, asynq.SkipRetry)
			require.Nil(t, log.WorkerStartedAt)
		})
	}
}

func TestProcessSyncNextcloudRetryAfterTransientFailure(t *testing.T) {
	config, err := (&types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{"binding-1"},
		Settings:    map[string]interface{}{"base_url": "https://nextcloud.example.test"},
		Credentials: map[string]interface{}{"token": "synthetic-token"},
	}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "nextcloud-retry", TenantID: 1,
		KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive, Config: config,
		LastSyncCursor: types.JSON(`{"connector_cursor":`),
	}
	log := &types.SyncLog{
		ID: "retry-log", DataSourceID: ds.ID,
		TenantID: ds.TenantID, Status: types.SyncLogStatusRunning,
		RecoveryVersion: 1, RecoveryTrigger: "manual", QueueTaskID: "dssync:retry-log",
	}
	logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
	registry := datasource.NewConnectorRegistry()
	require.NoError(t, registry.Register(nextcloudconnector.NewConnector()))
	svc := &DataSourceService{
		dsRepo:      newKBDeleteDSRepo("kb-1", ds),
		syncLogRepo: logs, connectorRegistry: registry,
		kbService: &processSyncKBService{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 1}},
	}
	payload, err := json.Marshal(types.DataSourceSyncPayload{
		DataSourceID: ds.ID,
		TenantID:     ds.TenantID, SyncLogID: log.ID, Trigger: "manual",
	})
	require.NoError(t, err)
	task := asynq.NewTask(types.TypeDataSourceSync, payload)
	firstCtx := types.WithTaskRetryMetadata(types.WithTaskExecutionID(context.Background(), log.QueueTaskID), 0, 2)
	err = svc.ProcessSync(firstCtx, task)
	require.ErrorContains(t, err, "baseline verifier is unavailable")
	require.Equal(t, types.SyncLogStatusRunning, log.Status)
	require.NotNil(t, log.WorkerStartedAt)
	require.False(t, log.WorkerActive)
	require.Empty(t, log.WorkerAttemptToken)

	svc.nextcloudEventInbox = nextcloudBaselineTestRepo(t)
	secondCtx := types.WithTaskRetryMetadata(types.WithTaskExecutionID(context.Background(), log.QueueTaskID), 1, 2)
	err = svc.ProcessSync(secondCtx, task)
	require.ErrorIs(t, err, apprepo.ErrNextcloudSourceCursor)
	require.NotErrorIs(t, err, asynq.SkipRetry)
	require.Equal(t, types.SyncLogStatusFailed, log.Status)
}

func TestProcessSyncNextcloudMalformedCursorFailsClosed(t *testing.T) {
	for _, full := range []bool{true, false} {
		t.Run(map[bool]string{true: "full", false: "incremental"}[full], func(t *testing.T) {
			config, err := (&types.DataSourceConfig{
				Type:        types.ConnectorTypeNextcloud,
				ResourceIDs: []string{"binding-1"},
				Settings:    map[string]interface{}{"base_url": "https://nextcloud.example.test"},
				Credentials: map[string]interface{}{"token": "synthetic-token"},
			}).ToJSON()
			require.NoError(t, err)
			ds := &types.DataSource{
				ID: "nextcloud-bad-cursor", TenantID: 1,
				KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
				Status: types.DataSourceStatusActive, Config: config,
				LastSyncCursor: types.JSON(`{"connector_cursor":`),
			}
			log := &types.SyncLog{
				ID: "bad-cursor-log", DataSourceID: ds.ID,
				TenantID: ds.TenantID, Status: types.SyncLogStatusRunning,
				RecoveryVersion: 1, RecoveryTrigger: "manual", QueueTaskID: "dssync:bad-cursor-log",
			}
			logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
			registry := datasource.NewConnectorRegistry()
			require.NoError(t, registry.Register(nextcloudconnector.NewConnector()))
			svc := &DataSourceService{
				dsRepo:      newKBDeleteDSRepo("kb-1", ds),
				syncLogRepo: logs, connectorRegistry: registry,
				kbService:           &processSyncKBService{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 1}},
				nextcloudEventInbox: nextcloudBaselineTestRepo(t),
			}
			payload, err := json.Marshal(types.DataSourceSyncPayload{
				DataSourceID: ds.ID,
				TenantID:     ds.TenantID, SyncLogID: log.ID, ForceFull: full, Trigger: "manual",
			})
			require.NoError(t, err)
			err = svc.ProcessSync(types.WithTaskExecutionID(context.Background(), log.QueueTaskID),
				asynq.NewTask(types.TypeDataSourceSync, payload))
			require.ErrorIs(t, err, apprepo.ErrNextcloudSourceCursor)
			require.Equal(t, types.SyncLogStatusFailed, log.Status)
			require.Equal(t, `{"connector_cursor":`, string(ds.LastSyncCursor))
		})
	}
}

func TestProcessSyncNextcloudLostCursorWithHistoryFailsClosed(t *testing.T) {
	for _, scenario := range []string{"manual_version", "schedule_legacy_knowledge"} {
		t.Run(scenario, func(t *testing.T) {
			db, baselineRepo := nextcloudBaselineTestDB(t)
			if scenario == "manual_version" {
				require.NoError(t, db.Exec(`INSERT INTO nextcloud_source_versions
					(tenant_id, knowledge_base_id, datasource_id, external_id, state)
					VALUES (1, 'kb-1', 'nextcloud-lost-cursor', 'nextcloud:instance-1:41', 'published')`).Error)
			} else {
				require.NoError(t, db.Exec(`INSERT INTO knowledges
					(id, tenant_id, knowledge_base_id, channel, metadata)
					VALUES ('old-knowledge', 1, 'kb-1', 'nextcloud', ?)`,
					`{"datasource_id":"nextcloud-lost-cursor","external_id":"nextcloud:instance-1:41"}`).Error)
			}
			config, err := (&types.DataSourceConfig{
				Type:        types.ConnectorTypeNextcloud,
				ResourceIDs: []string{"binding-1"},
			}).ToJSON()
			require.NoError(t, err)
			ds := &types.DataSource{
				ID: "nextcloud-lost-cursor", TenantID: 1,
				KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
				Status: types.DataSourceStatusActive, Config: config,
			}
			log := &types.SyncLog{
				ID: "lost-cursor-log", DataSourceID: ds.ID,
				TenantID: ds.TenantID, Status: types.SyncLogStatusRunning,
				RecoveryVersion: 1, QueueTaskID: "dssync:lost-cursor-log",
			}
			logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
			registry := datasource.NewConnectorRegistry()
			require.NoError(t, registry.Register(nextcloudconnector.NewConnector()))
			svc := &DataSourceService{
				dsRepo:      newKBDeleteDSRepo("kb-1", ds),
				syncLogRepo: logs, connectorRegistry: registry,
				kbService:           &processSyncKBService{kb: &types.KnowledgeBase{ID: "kb-1", TenantID: 1}},
				nextcloudEventInbox: baselineRepo,
			}
			trigger := "manual"
			if scenario == "schedule_legacy_knowledge" {
				trigger = "schedule"
			}
			log.RecoveryTrigger = trigger
			payload, err := json.Marshal(types.DataSourceSyncPayload{
				DataSourceID: ds.ID,
				TenantID:     ds.TenantID, SyncLogID: log.ID, ForceFull: true, Trigger: trigger,
			})
			require.NoError(t, err)
			err = svc.ProcessSync(types.WithTaskExecutionID(context.Background(), log.QueueTaskID),
				asynq.NewTask(types.TypeDataSourceSync, payload))
			require.ErrorIs(t, err, apprepo.ErrNextcloudSourceCursor)
			require.Equal(t, types.SyncLogStatusFailed, log.Status)
			require.Empty(t, ds.LastSyncCursor)
		})
	}
}

func (r *processSyncSyncLogRepo) MarkNextcloudEnqueueUncertain(_ context.Context, logID, dsID string,
	tenantID uint64, _, _ string,
) (bool, error) {
	log := r.logs[logID]
	if log == nil || log.DataSourceID != dsID || log.TenantID != tenantID ||
		log.Status != types.SyncLogStatusRunning || log.WorkerStartedAt != nil {
		return false, nil
	}
	log.ErrorMessage = datasource.NextcloudSyncEnqueueUncertain
	return true, nil
}

func (r *processSyncSyncLogRepo) ClaimNextcloudSyncStart(_ context.Context, logID, dsID string,
	tenantID uint64, trigger, taskID string, retried int,
) (string, error) {
	log := r.logs[logID]
	if log == nil || log.DataSourceID != dsID || log.TenantID != tenantID ||
		log.Status != types.SyncLogStatusRunning || log.RecoveryVersion != 1 ||
		log.RecoveryTrigger != trigger || log.QueueTaskID != taskID || log.WorkerActive ||
		(retried == 0 && log.WorkerStartedAt != nil) ||
		(retried > 0 && log.WorkerStartedAt == nil) {
		return "", nil
	}
	now := time.Now().UTC()
	if log.WorkerStartedAt == nil {
		log.WorkerStartedAt = &now
	}
	log.WorkerActive = true
	log.WorkerAttemptToken = fmt.Sprintf("attempt-%d", retried)
	return log.WorkerAttemptToken, nil
}

func (r *processSyncSyncLogRepo) FinishNextcloudSyncAttempt(_ context.Context, logID, dsID string,
	tenantID uint64, trigger, taskID, token string, terminal bool, message string,
) (bool, error) {
	log := r.logs[logID]
	if log == nil || log.DataSourceID != dsID || log.TenantID != tenantID ||
		log.Status != types.SyncLogStatusRunning || log.RecoveryVersion != 1 ||
		log.RecoveryTrigger != trigger || log.QueueTaskID != taskID ||
		!log.WorkerActive || log.WorkerAttemptToken != token {
		return false, nil
	}
	log.WorkerActive = false
	log.WorkerAttemptToken = ""
	log.ErrorMessage = message
	if terminal {
		log.Status = types.SyncLogStatusFailed
		now := time.Now().UTC()
		log.FinishedAt = &now
	}
	return true, nil
}

func (r *processSyncSyncLogRepo) Create(_ context.Context, log *types.SyncLog) error {
	if log.ID == "" {
		log.ID = r.nextID
		if log.ID == "" {
			log.ID = uuid.NewString()
		}
	}
	if log.RecoveryVersion == 1 && log.QueueTaskID == "" {
		log.QueueTaskID = datasource.NextcloudSyncTaskID(log.ID)
	}
	r.logs[log.ID] = log
	return nil
}

func (r *processSyncSyncLogRepo) FindByID(_ context.Context, id string) (*types.SyncLog, error) {
	log, ok := r.logs[id]
	if !ok {
		return nil, errors.New("sync log not found")
	}
	return log, nil
}

func (r *processSyncSyncLogRepo) FindByDataSource(context.Context, string, int, int) ([]*types.SyncLog, error) {
	return nil, nil
}

func (r *processSyncSyncLogRepo) FindLatest(context.Context, string) (*types.SyncLog, error) {
	return nil, nil
}

func (r *processSyncSyncLogRepo) HasRunningSync(context.Context, string) (bool, error) {
	return false, nil
}

func (r *processSyncSyncLogRepo) Update(_ context.Context, log *types.SyncLog) error {
	r.logs[log.ID] = log
	return nil
}

func (r *processSyncSyncLogRepo) UpdateResult(_ context.Context, log *types.SyncLog) error {
	return r.Update(context.Background(), log)
}

func (r *processSyncSyncLogRepo) CancelPendingByDataSource(context.Context, string) error {
	return nil
}
func (r *processSyncSyncLogRepo) CleanupOldLogs(context.Context, int) error { return nil }

func TestAllFetchedItemsFailedError(t *testing.T) {
	err := allFetchedItemsFailedError(&types.SyncResult{
		Total:  2,
		Failed: 2,
		Errors: []types.SyncItemError{{Message: "doc one: export failed"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "all fetched items failed during sync (2/2)")
	assert.Contains(t, err.Error(), "doc one: export failed")
}

func TestAllFetchedItemsFailedErrorIgnoresPartialFailure(t *testing.T) {
	err := allFetchedItemsFailedError(&types.SyncResult{
		Total:   3,
		Created: 1,
		Failed:  2,
	})
	require.NoError(t, err)
}

func TestAllFetchedItemsFailedErrorIgnoresSkippedItems(t *testing.T) {
	err := allFetchedItemsFailedError(&types.SyncResult{
		Total:   3,
		Skipped: 3,
	})
	require.NoError(t, err)
}

func TestAllFetchedItemsFailedErrorTruncatesLongDetail(t *testing.T) {
	err := allFetchedItemsFailedError(&types.SyncResult{
		Total:  1,
		Failed: 1,
		Errors: []types.SyncItemError{{Message: strings.Repeat("x", 600)}},
	})
	require.Error(t, err)
	assert.LessOrEqual(t, len(err.Error()), 560)
	assert.Contains(t, err.Error(), "...")
}

const deletedItemConnectorType = "test-sync-deletion"

type deletedItemConnector struct{}

func (deletedItemConnector) Type() string { return deletedItemConnectorType }
func (deletedItemConnector) Validate(context.Context, *types.DataSourceConfig) error {
	return nil
}

func (deletedItemConnector) ListResources(context.Context, *types.DataSourceConfig, string) ([]types.Resource, error) {
	return nil, nil
}

func (deletedItemConnector) ResolveResourceAncestors(
	context.Context, *types.DataSourceConfig, []string,
) ([]string, error) {
	return nil, nil
}

func (deletedItemConnector) FetchAll(context.Context, *types.DataSourceConfig, []string) ([]types.FetchedItem, error) {
	return []types.FetchedItem{{
		ExternalID:       "file:gone",
		SourceResourceID: "folder:1",
		IsDeleted:        true,
	}}, nil
}

func (deletedItemConnector) FetchIncremental(
	context.Context, *types.DataSourceConfig, *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	items, err := (deletedItemConnector{}).FetchAll(context.Background(), nil, nil)
	return items, nil, err
}

type processSyncTenantRepo struct {
	interfaces.TenantRepository
	tenant *types.Tenant
}

func (r *processSyncTenantRepo) GetTenantByID(context.Context, uint64) (*types.Tenant, error) {
	return r.tenant, nil
}

type processSyncTagService struct {
	interfaces.KnowledgeTagService
	ctx context.Context
}

func (s *processSyncTagService) FindOrCreateTagByName(
	ctx context.Context,
	_ string,
	_ string,
) (*types.KnowledgeTag, error) {
	s.ctx = ctx
	return nil, nil
}

type deletionLookupKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	knowledge         *types.Knowledge
	lookupErr         error
	metadataUpdates   []map[string]string // metadata persisted via UpdateKnowledge
	metadataUpdateErr error
	hardDeleted       []string
	hardDeleteErr     error
	tenantID          uint64
	knowledgeBaseID   string
	dataSourceID      string
	externalID        string
	stageErr          error
	stageCalls        int
	admitErr          error
	admitCalls        int
	tombstoneErr      error
	tombstoneCalls    int
}

func (
	r *deletionLookupKnowledgeRepo,
) AdmitNextcloudCandidateRetry(context.Context, uint64, string, string, string, string) error {
	r.admitCalls++
	return r.admitErr
}

func (
	r *deletionLookupKnowledgeRepo,
) StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error {
	r.stageCalls++
	return r.stageErr
}

func (
	r *deletionLookupKnowledgeRepo,
) StageNextcloudVersionWithSource(
	context.Context,
	uint64,
	string,
	string,
	string,
	string,
	string,
	string,
	map[string]string,
) error {
	r.stageCalls++
	return r.stageErr
}

func (r *deletionLookupKnowledgeRepo) PublishNextcloudVersion(context.Context, string) (bool, error) {
	return false, nil
}

func (r *deletionLookupKnowledgeRepo) TombstoneNextcloudVersion(context.Context, uint64, string, string, string) error {
	r.tombstoneCalls++
	return r.tombstoneErr
}

func (r *deletionLookupKnowledgeRepo) UpdateKnowledge(_ context.Context, knowledge *types.Knowledge) error {
	if r.metadataUpdateErr != nil {
		return r.metadataUpdateErr
	}
	metadata := map[string]string{}
	if len(knowledge.Metadata) > 0 {
		if err := json.Unmarshal(knowledge.Metadata, &metadata); err != nil {
			return err
		}
	}
	r.metadataUpdates = append(r.metadataUpdates, metadata)
	return nil
}

func (r *deletionLookupKnowledgeRepo) FindByDataSourceExternalID(
	_ context.Context, tenantID uint64, knowledgeBaseID, dataSourceID, externalID string,
) (*types.Knowledge, error) {
	if r.lookupErr != nil {
		return nil, r.lookupErr
	}
	r.tenantID = tenantID
	r.knowledgeBaseID = knowledgeBaseID
	r.dataSourceID = dataSourceID
	r.externalID = externalID
	return r.knowledge, nil
}

func (r *deletionLookupKnowledgeRepo) HardDeleteKnowledge(_ context.Context, _ uint64, id string) error {
	if r.hardDeleteErr != nil {
		return r.hardDeleteErr
	}
	r.hardDeleted = append(r.hardDeleted, id)
	return nil
}

func (r *deletionLookupKnowledgeRepo) HardDeleteKnowledgeList(_ context.Context, _ uint64, ids []string) error {
	for _, id := range ids {
		if err := r.HardDeleteKnowledge(context.Background(), 0, id); err != nil {
			return err
		}
	}
	return nil
}

// scopedDeletionRepo models two data sources sharing the same external_id.
type scopedDeletionRepo struct {
	interfaces.KnowledgeRepository
	live        map[string]*types.Knowledge
	hardDeleted []string
}

func (r *scopedDeletionRepo) FindByDataSourceExternalID(
	_ context.Context, _ uint64, _, dataSourceID, externalID string,
) (*types.Knowledge, error) {
	if r.live == nil {
		return nil, nil
	}
	return r.live[dataSourceID+"|"+externalID], nil
}

func (r *scopedDeletionRepo) HardDeleteKnowledge(_ context.Context, _ uint64, id string) error {
	r.hardDeleted = append(r.hardDeleted, id)
	return nil
}

func (r *scopedDeletionRepo) HardDeleteKnowledgeList(_ context.Context, _ uint64, ids []string) error {
	r.hardDeleted = append(r.hardDeleted, ids...)
	return nil
}

// keyedDeletionRepo maps external_id to live knowledge for multi-item sync tests.
type keyedDeletionRepo struct {
	interfaces.KnowledgeRepository
	items         map[string]*types.Knowledge
	hardDeleted   []string
	hardDeleteErr error
	tombstoneErr  error
}

func (
	r *keyedDeletionRepo,
) AdmitNextcloudCandidateRetry(context.Context, uint64, string, string, string, string) error {
	return nil
}

func (
	r *keyedDeletionRepo,
) StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error {
	return nil
}

func (
	r *keyedDeletionRepo,
) StageNextcloudVersionWithSource(
	context.Context,
	uint64,
	string,
	string,
	string,
	string,
	string,
	string,
	map[string]string,
) error {
	return nil
}

func (r *keyedDeletionRepo) PublishNextcloudVersion(context.Context, string) (bool, error) {
	return false, nil
}

func (r *keyedDeletionRepo) TombstoneNextcloudVersion(context.Context, uint64, string, string, string) error {
	return r.tombstoneErr
}

func (r *keyedDeletionRepo) FindByDataSourceExternalID(
	_ context.Context, _ uint64, _, _, externalID string,
) (*types.Knowledge, error) {
	if r.items == nil {
		return nil, nil
	}
	return r.items[externalID], nil
}

func (r *keyedDeletionRepo) HardDeleteKnowledge(_ context.Context, _ uint64, id string) error {
	if r.hardDeleteErr != nil {
		return r.hardDeleteErr
	}
	r.hardDeleted = append(r.hardDeleted, id)
	return nil
}

func (r *keyedDeletionRepo) HardDeleteKnowledgeList(_ context.Context, _ uint64, ids []string) error {
	for _, id := range ids {
		if err := r.HardDeleteKnowledge(context.Background(), 0, id); err != nil {
			return err
		}
	}
	return nil
}

// syncDeletionHarness wires a DataSourceService around a connector that always
// reports one deleted item, with overridable lookup/delete fakes.
type syncDeletionHarness struct {
	ds            *types.DataSource
	syncLogID     string
	syncLogRepo   *processSyncSyncLogRepo
	knowledgeRepo *deletionLookupKnowledgeRepo
	knowledgeSvc  *sweepFakeKS
	svc           *DataSourceService
}

// newSyncDeletionHarness builds a full-sync ProcessSync fixture. Passing nil
// for repo/ks selects the happy-path defaults: an existing knowledge item and
// no lookup/delete errors.
func newSyncDeletionHarness(
	t *testing.T, syncDeletions bool, dsID, logID string,
	repo *deletionLookupKnowledgeRepo, ks *sweepFakeKS,
) *syncDeletionHarness {
	t.Helper()
	configJSON, err := (&types.DataSourceConfig{Type: deletedItemConnectorType}).ToJSON()
	require.NoError(t, err)

	ds := &types.DataSource{
		ID:              dsID,
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		Name:            "Sync Deletion",
		Type:            deletedItemConnectorType,
		Config:          configJSON,
		SyncMode:        types.SyncModeFull,
		Status:          types.DataSourceStatusActive,
		SyncDeletions:   syncDeletions,
	}
	syncLog := &types.SyncLog{
		ID:           logID,
		DataSourceID: ds.ID,
		TenantID:     ds.TenantID,
		Status:       types.SyncLogStatusRunning,
		StartedAt:    time.Now().UTC(),
	}
	if repo == nil {
		repo = &deletionLookupKnowledgeRepo{knowledge: &types.Knowledge{ID: "knowledge-gone"}}
	}
	if ks == nil {
		ks = &sweepFakeKS{repo: repo}
	}
	syncLogRepo := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{syncLog.ID: syncLog}}
	registry := datasource.NewConnectorRegistry()
	require.NoError(t, registry.Register(deletedItemConnector{}))

	return &syncDeletionHarness{
		ds:            ds,
		syncLogID:     syncLog.ID,
		syncLogRepo:   syncLogRepo,
		knowledgeRepo: repo,
		knowledgeSvc:  ks,
		svc: &DataSourceService{
			dsRepo:            newKBDeleteDSRepo(ds.KnowledgeBaseID, ds),
			syncLogRepo:       syncLogRepo,
			knowledgeService:  ks,
			kbService:         &processSyncKBService{kb: &types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID}},
			connectorRegistry: registry,
			tenantRepo:        &processSyncTenantRepo{tenant: &types.Tenant{ID: ds.TenantID}},
			tagService:        &processSyncTagService{},
		},
	}
}

// run executes a full sync and returns the persisted sync log plus the error
// ProcessSync returned (non-nil when every fetched item failed).
func (h *syncDeletionHarness) run(t *testing.T) (*types.SyncLog, error) {
	t.Helper()
	payload, err := json.Marshal(types.DataSourceSyncPayload{
		DataSourceID: h.ds.ID,
		TenantID:     h.ds.TenantID,
		SyncLogID:    h.syncLogID,
		ForceFull:    true,
	})
	require.NoError(t, err)
	err = h.svc.ProcessSync(context.Background(), asynq.NewTask(types.TypeDataSourceSync, payload))

	updated := h.syncLogRepo.logs[h.syncLogID]
	require.NotNil(t, updated)
	return updated, err
}

// deletionFailedCount extracts the SyncResult.deletion_failed counter without
// referencing the SyncResult type, so these tests stay independent of the
// field's commit.
func deletionFailedCount(t *testing.T, log *types.SyncLog) int {
	t.Helper()
	var counters struct {
		DeletionFailed int `json:"deletion_failed"`
	}
	require.NoError(t, json.Unmarshal(log.Result, &counters))
	return counters.DeletionFailed
}

// TestProcessSync_SyncDeletionsDeletesMatchingKnowledge verifies that a
// deleted source item removes the matching KB knowledge (counted as Deleted),
// and that the lookup is scoped to tenant, KB, data source and external ID.
// TestIngestItem_URLCreationAttachesDataSourceMetadata verifies that a
// URL-only item gets its datasource scoping keys attached right after
// creation, so a later source-side deletion can find it via
// FindByDataSourceExternalID (CreateKnowledgeFromURL itself persists no
// metadata).
func TestIngestItem_URLCreationAttachesDataSourceMetadata(t *testing.T) {
	ds := &types.DataSource{ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1"}
	repo := &deletionLookupKnowledgeRepo{}
	ks := &sweepFakeKS{repo: repo, createURLKnowledge: &types.Knowledge{ID: "url-knowledge-1"}}
	svc := &DataSourceService{knowledgeService: ks}

	isUpdate, err := svc.ingestItem(context.Background(), ds, &types.FetchedItem{
		ExternalID: "url:1",
		URL:        "https://example.com/doc",
	}, nil)
	require.NoError(t, err)
	assert.False(t, isUpdate)
	require.Len(t, repo.metadataUpdates, 1)
	assert.Equal(t, ds.ID, repo.metadataUpdates[0]["datasource_id"])
	assert.Equal(t, "url:1", repo.metadataUpdates[0]["external_id"])
}

// TestIngestItem_PersistsSourceUpdatedAt verifies that the connector-supplied
// last-modified time reaches the persisted metadata as an RFC3339 UTC string,
// and that an item without one gets no key rather than a zero time.
func TestIngestItem_PersistsSourceUpdatedAt(t *testing.T) {
	ds := &types.DataSource{ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1"}
	edited := time.Date(2023, 4, 5, 6, 7, 8, 0, time.FixedZone("CST", 8*3600))

	repo := &deletionLookupKnowledgeRepo{}
	ks := &sweepFakeKS{repo: repo, createURLKnowledge: &types.Knowledge{ID: "url-knowledge-1"}}
	svc := &DataSourceService{knowledgeService: ks}
	created := time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err := svc.ingestItem(context.Background(), ds, &types.FetchedItem{
		ExternalID: "url:1",
		URL:        "https://example.com/doc",
		UpdatedAt:  edited,
		CreatedAt:  created,
	}, nil)
	require.NoError(t, err)
	require.Len(t, repo.metadataUpdates, 1)
	assert.Equal(t, "2023-04-04T22:07:08Z", repo.metadataUpdates[0]["source_updated_at"])
	assert.Equal(t, "2021-01-02T03:04:05Z", repo.metadataUpdates[0]["source_created_at"])

	repo = &deletionLookupKnowledgeRepo{}
	ks = &sweepFakeKS{repo: repo, createURLKnowledge: &types.Knowledge{ID: "url-knowledge-2"}}
	svc = &DataSourceService{knowledgeService: ks}
	_, err = svc.ingestItem(context.Background(), ds, &types.FetchedItem{
		ExternalID: "url:2",
		URL:        "https://example.com/doc2",
	}, nil)
	require.NoError(t, err)
	require.Len(t, repo.metadataUpdates, 1)
	_, present := repo.metadataUpdates[0]["source_updated_at"]
	assert.False(t, present)
	_, present = repo.metadataUpdates[0]["source_created_at"]
	assert.False(t, present)
}

func TestProcessSync_SyncDeletionsDeletesMatchingKnowledge(t *testing.T) {
	h := newSyncDeletionHarness(t, true, "ds-delete-characterization", "log-delete-characterization", nil, nil)
	updated, err := h.run(t)
	require.NoError(t, err)

	assert.Equal(t, 1, updated.ItemsDeleted)
	assert.Equal(t, []string{"knowledge-gone"}, h.knowledgeSvc.deleted)
	assert.Equal(t, h.ds.TenantID, h.knowledgeRepo.tenantID)
	assert.Equal(t, h.ds.KnowledgeBaseID, h.knowledgeRepo.knowledgeBaseID)
	assert.Equal(t, h.ds.ID, h.knowledgeRepo.dataSourceID)
	assert.Equal(t, "file:gone", h.knowledgeRepo.externalID)
}

// TestProcessSync_SyncDeletionsDisabledSkipsDeletion verifies that with
// SyncDeletions off the item is neither deleted nor counted (Deleted=0,
// Skipped=0, no DeleteKnowledge call).
func TestProcessSync_SyncDeletionsDisabledSkipsDeletion(t *testing.T) {
	h := newSyncDeletionHarness(t, false, "ds-delete-disabled", "log-delete-disabled", nil, nil)
	updated, err := h.run(t)
	require.NoError(t, err)

	assert.Empty(t, h.knowledgeSvc.deleted)
	assert.Equal(t, 0, updated.ItemsDeleted)
	assert.Equal(t, 0, updated.ItemsSkipped)
}

// TestProcessSync_SyncDeletionsAlreadyGoneCountsSkipped verifies the
// idempotent path: the source item reports deletion but no KB knowledge
// matches, so the item counts as Skipped and nothing is deleted.
func TestProcessSync_SyncDeletionsAlreadyGoneCountsSkipped(t *testing.T) {
	h := newSyncDeletionHarness(t, true, "ds-delete-gone", "log-delete-gone", &deletionLookupKnowledgeRepo{}, nil)
	updated, err := h.run(t)
	require.NoError(t, err)

	assert.Empty(t, h.knowledgeSvc.deleted)
	assert.Equal(t, 0, updated.ItemsDeleted)
	assert.Equal(t, 1, updated.ItemsSkipped)
}

// TestProcessSync_SyncDeletionsLookupFailureCountsFailed verifies that a
// failing scoped lookup surfaces as a Failed item with the
// deletion_lookup_failed code, increments DeletionFailed, and never calls
// DeleteKnowledge.
func TestProcessSync_SyncDeletionsLookupFailureCountsFailed(t *testing.T) {
	repo := &deletionLookupKnowledgeRepo{lookupErr: errors.New("lookup failed")}
	h := newSyncDeletionHarness(t, true, "ds-delete-lookup-fail", "log-delete-lookup-fail", repo, nil)
	updated, err := h.run(t)
	require.Error(t, err)

	assert.Empty(t, h.knowledgeSvc.deleted)
	assert.Equal(t, 0, updated.ItemsDeleted)
	assert.Equal(t, 1, updated.ItemsFailed)
	result, err := updated.ParseResult()
	require.NoError(t, err)
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "deletion_lookup_failed", result.Errors[0].Code)
	assert.Equal(t, 1, deletionFailedCount(t, updated))
}

// TestProcessSync_SyncDeletionsDeleteFailureCountsFailed verifies that a
// failing DeleteKnowledge call surfaces as a Failed item with the
// deletion_failed code and increments DeletionFailed without counting
// the item as Deleted.
func TestProcessSync_SyncDeletionsDeleteFailureCountsFailed(t *testing.T) {
	h := newSyncDeletionHarness(t, true, "ds-delete-fail", "log-delete-fail", nil, nil)
	h.knowledgeSvc.deleteErr = errors.New("delete failed")
	updated, err := h.run(t)
	require.Error(t, err)

	assert.Equal(t, []string{"knowledge-gone"}, h.knowledgeSvc.deleted)
	assert.Equal(t, 0, updated.ItemsDeleted)
	assert.Equal(t, 1, updated.ItemsFailed)
	result, err := updated.ParseResult()
	require.NoError(t, err)
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "deletion_failed", result.Errors[0].Code)
	assert.Equal(t, 1, deletionFailedCount(t, updated))
}

func TestProcessSync_SyncDeletionsHardDeletesRow(t *testing.T) {
	h := newSyncDeletionHarness(t, true, "ds-delete-hard", "log-delete-hard", nil, nil)
	updated, err := h.run(t)
	require.NoError(t, err)

	assert.Equal(t, 1, updated.ItemsDeleted)
	assert.Equal(t, []string{"knowledge-gone"}, h.knowledgeSvc.deleted)
	assert.Equal(t, []string{"knowledge-gone"}, h.knowledgeRepo.hardDeleted)
}

func TestApplyFetchedItem_SyncDeletionScopedPerDataSource(t *testing.T) {
	repo := &scopedDeletionRepo{live: map[string]*types.Knowledge{
		"ds-a|file:shared": {ID: "knowledge-a"},
		"ds-b|file:shared": {ID: "knowledge-b"},
	}}
	ks := &sweepFakeKS{repo: repo}
	svc := &DataSourceService{knowledgeService: ks}

	result := &types.SyncResult{}
	dsA := &types.DataSource{
		ID: "ds-a", TenantID: 1, KnowledgeBaseID: "kb-1", SyncDeletions: true,
	}
	svc.applyFetchedItem(context.Background(), dsA, &types.FetchedItem{
		ExternalID: "file:shared",
		IsDeleted:  true,
	}, nil, result)

	assert.Equal(t, 1, result.Deleted)
	assert.Equal(t, []string{"knowledge-a"}, ks.deleted)
	assert.Equal(t, []string{"knowledge-a"}, repo.hardDeleted)
	assert.NotContains(t, ks.deleted, "knowledge-b")
}

type mixedSyncConnector struct{}

func (mixedSyncConnector) Type() string { return "test-sync-mixed" }
func (mixedSyncConnector) Validate(context.Context, *types.DataSourceConfig) error {
	return nil
}

func (mixedSyncConnector) ListResources(context.Context, *types.DataSourceConfig, string) ([]types.Resource, error) {
	return nil, nil
}

func (mixedSyncConnector) ResolveResourceAncestors(
	context.Context, *types.DataSourceConfig, []string,
) ([]string, error) {
	return nil, nil
}

func (mixedSyncConnector) FetchAll(context.Context, *types.DataSourceConfig, []string) ([]types.FetchedItem, error) {
	return []types.FetchedItem{
		{ExternalID: "file:gone", IsDeleted: true},
		{ExternalID: "file:new", Content: []byte("hello"), FileName: "new.txt"},
	}, nil
}

func (mixedSyncConnector) FetchIncremental(
	context.Context, *types.DataSourceConfig, *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	items, err := (mixedSyncConnector{}).FetchAll(context.Background(), nil, nil)
	return items, nil, err
}

type nextcloudCursorMixedConnector struct{ mixedSyncConnector }

func (nextcloudCursorMixedConnector) Type() string { return types.ConnectorTypeNextcloud }

func (nextcloudCursorMixedConnector) FetchIncremental(
	ctx context.Context, config *types.DataSourceConfig, _ *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	items, err := (mixedSyncConnector{}).FetchAll(ctx, config, nil)
	items[1].Metadata = map[string]string{"nextcloud_etag": "etag-new"}
	return items, &types.SyncCursor{ConnectorCursor: map[string]interface{}{
		"instance_id": "instance-1", "files": map[string]interface{}{"binding-1": map[string]interface{}{}},
		"marker": "new",
	}}, err
}

func TestProcessSync_NextcloudPartialFailureDoesNotAdvanceCursor(t *testing.T) {
	for _, test := range []struct {
		name            string
		trigger         string
		deleteFail      bool
		wantMarker      string
		recoveryVersion int
	}{
		{
			name:            "failed deletion retains checkpoint",
			trigger:         "manual",
			deleteFail:      true,
			wantMarker:      "old",
			recoveryVersion: 1,
		},
		{name: "successful deletion advances checkpoint", trigger: "manual", wantMarker: "new", recoveryVersion: 1},
		{
			name:            "legacy queued manual sync completes after upgrade",
			trigger:         "manual",
			wantMarker:      "new",
			recoveryVersion: 0,
		},
		{
			name:            "legacy queued empty trigger completes after upgrade",
			trigger:         "",
			wantMarker:      "new",
			recoveryVersion: 0,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			configJSON, err := (&types.DataSourceConfig{
				Type:        types.ConnectorTypeNextcloud,
				ResourceIDs: []string{"binding-1"},
			}).ToJSON()
			require.NoError(t, err)
			oldCursor, err := (&types.SyncCursor{ConnectorCursor: map[string]interface{}{
				"instance_id": "instance-1", "files": map[string]interface{}{"binding-1": map[string]interface{}{}},
				"marker": "old",
			}}).ToJSON()
			require.NoError(t, err)
			ds := &types.DataSource{
				ID: "ds-nextcloud", TenantID: 1, KnowledgeBaseID: "kb-1",
				Type: types.ConnectorTypeNextcloud, Config: configJSON,
				SyncMode: types.SyncModeIncremental, Status: types.DataSourceStatusActive,
				SyncDeletions: true, LastSyncCursor: oldCursor,
			}
			syncLog := &types.SyncLog{
				ID: "log-nextcloud", DataSourceID: ds.ID, TenantID: ds.TenantID,
				Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC(),
				RecoveryVersion: test.recoveryVersion,
			}
			if test.recoveryVersion == 1 {
				syncLog.RecoveryTrigger = "manual"
				syncLog.QueueTaskID = "dssync:log-nextcloud"
			}
			repo := &keyedDeletionRepo{items: map[string]*types.Knowledge{
				"file:gone": {ID: "knowledge-gone"},
			}}
			if test.deleteFail {
				repo.tombstoneErr = errors.New("tombstone failed")
			}
			registry := datasource.NewConnectorRegistry()
			require.NoError(t, registry.Register(nextcloudCursorMixedConnector{}))
			svc := &DataSourceService{
				dsRepo:           newKBDeleteDSRepo(ds.KnowledgeBaseID, ds),
				syncLogRepo:      &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{syncLog.ID: syncLog}},
				knowledgeService: &sweepFakeKS{repo: repo},
				kbService: &processSyncKBService{kb: &types.KnowledgeBase{
					ID:       ds.KnowledgeBaseID,
					TenantID: ds.TenantID,
				}},
				connectorRegistry:   registry,
				tenantRepo:          &processSyncTenantRepo{tenant: &types.Tenant{ID: ds.TenantID}},
				tagService:          &processSyncTagService{},
				nextcloudEventInbox: nextcloudBaselineTestRepo(t),
			}
			payload, err := json.Marshal(types.DataSourceSyncPayload{
				DataSourceID: ds.ID, TenantID: ds.TenantID, SyncLogID: syncLog.ID, Trigger: test.trigger,
			})
			require.NoError(t, err)
			ctx := context.Background()
			if test.recoveryVersion == 1 {
				ctx = types.WithTaskExecutionID(ctx, syncLog.QueueTaskID)
			}
			require.NoError(t, svc.ProcessSync(ctx, asynq.NewTask(types.TypeDataSourceSync, payload)))
			require.NotEqual(t, types.SyncLogStatusRunning, syncLog.Status)
			stored, err := ds.ParseSyncCursor()
			require.NoError(t, err)
			require.NotNil(t, stored)
			assert.Equal(t, test.wantMarker, stored.ConnectorCursor["marker"])
		})
	}
}

func TestProcessSync_SyncDeletionsPartialWhenMixedResults(t *testing.T) {
	configJSON, err := (&types.DataSourceConfig{Type: "test-sync-mixed"}).ToJSON()
	require.NoError(t, err)

	ds := &types.DataSource{
		ID: "ds-mixed", TenantID: 1, KnowledgeBaseID: "kb-1",
		Type: "test-sync-mixed", Config: configJSON,
		SyncMode: types.SyncModeFull, Status: types.DataSourceStatusActive,
		SyncDeletions: true,
	}
	syncLog := &types.SyncLog{
		ID: "log-mixed", DataSourceID: ds.ID, TenantID: ds.TenantID,
		Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC(),
	}
	repo := &keyedDeletionRepo{
		items:         map[string]*types.Knowledge{"file:gone": {ID: "knowledge-gone"}},
		hardDeleteErr: errors.New("hard delete failed"),
	}
	ks := &sweepFakeKS{repo: repo}
	syncLogRepo := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{syncLog.ID: syncLog}}
	registry := datasource.NewConnectorRegistry()
	require.NoError(t, registry.Register(mixedSyncConnector{}))

	svc := &DataSourceService{
		dsRepo:            newKBDeleteDSRepo(ds.KnowledgeBaseID, ds),
		syncLogRepo:       syncLogRepo,
		knowledgeService:  ks,
		kbService:         &processSyncKBService{kb: &types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID}},
		connectorRegistry: registry,
		tenantRepo:        &processSyncTenantRepo{tenant: &types.Tenant{ID: ds.TenantID}},
		tagService:        &processSyncTagService{},
	}

	payload, err := json.Marshal(types.DataSourceSyncPayload{
		DataSourceID: ds.ID, TenantID: ds.TenantID, SyncLogID: syncLog.ID, ForceFull: true,
	})
	require.NoError(t, err)
	err = svc.ProcessSync(context.Background(), asynq.NewTask(types.TypeDataSourceSync, payload))
	require.NoError(t, err, "partial failure must not fail the whole sync")

	updated := syncLogRepo.logs[syncLog.ID]
	require.NotNil(t, updated)
	assert.Equal(t, types.SyncLogStatusPartial, updated.Status)
	assert.Equal(t, 1, updated.ItemsFailed)
	assert.Equal(t, 1, updated.ItemsCreated)
	assert.Contains(t, updated.ErrorMessage, "deletion failure(s) will only retry on the next full sync")
}

func TestIngestItem_URLCreationMetadataAttachFailure(t *testing.T) {
	ds := &types.DataSource{ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1"}
	repo := &deletionLookupKnowledgeRepo{metadataUpdateErr: errors.New("db unavailable")}
	ks := &sweepFakeKS{repo: repo, createURLKnowledge: &types.Knowledge{ID: "url-knowledge-1"}}
	svc := &DataSourceService{knowledgeService: ks}

	_, err := svc.ingestItem(context.Background(), ds, &types.FetchedItem{
		ExternalID: "url:1",
		URL:        "https://example.com/doc",
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attach datasource metadata")
}

func TestNextcloudOrdinarySyncSkipsFailedSameETagBeforeCreate(t *testing.T) {
	repo := &deletionLookupKnowledgeRepo{admitErr: apprepo.ErrNextcloudCandidateRetryNotDue}
	knowledge := &sweepFakeKS{repo: repo}
	svc := &DataSourceService{knowledgeService: knowledge}
	result := &types.SyncResult{}
	svc.applyFetchedItem(context.Background(), &types.DataSource{
		ID: "ds", TenantID: 7, KnowledgeBaseID: "kb", Type: types.ConnectorTypeNextcloud,
	}, &types.FetchedItem{
		ExternalID: "nextcloud:instance:77", FileName: "file.md",
		Content: []byte("identical bytes"), Metadata: map[string]string{"nextcloud_etag": "same-etag"},
	},
		nil, result)
	require.Equal(t, 1, result.Skipped)
	require.Zero(t, result.Failed)
	require.Equal(t, 1, repo.admitCalls)
	require.Zero(t, repo.stageCalls)
	require.Empty(t, knowledge.events, "no parser job or stored file is created")
}

func TestIngestItem_NextcloudFailureRetainsOldKnowledge(t *testing.T) {
	for _, tc := range []struct {
		name            string
		lookupErr       error
		createErr       error
		stageErr        error
		failedCandidate bool
	}{
		{name: "lookup", lookupErr: errors.New("lookup unavailable")},
		{name: "create", createErr: errors.New("storage unavailable")},
		{name: "enqueue failure", failedCandidate: true},
		{name: "stage", stageErr: errors.New("database unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &deletionLookupKnowledgeRepo{
				knowledge: &types.Knowledge{ID: "old-version"},
				lookupErr: tc.lookupErr,
				stageErr:  tc.stageErr,
			}
			ks := &sweepFakeKS{repo: repo, createErr: tc.createErr}
			if tc.failedCandidate {
				ks.createdKnowledge = &types.Knowledge{ID: "new-knowledge", ParseStatus: types.ParseStatusFailed}
			}
			svc := &DataSourceService{knowledgeService: ks}
			ds := &types.DataSource{
				ID: "ds-nextcloud", TenantID: 1, KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud,
			}
			updated, err := svc.ingestItem(context.Background(), ds, &types.FetchedItem{
				ExternalID: "nextcloud:instance:77", FileName: "document.txt", Content: []byte("new bytes"),
				Metadata: map[string]string{"nextcloud_etag": "new-etag"},
			}, nil)
			require.Error(t, err)
			assert.Equal(t, tc.lookupErr == nil, updated)
			assert.Empty(t, ks.deleted)
			assert.Empty(t, repo.hardDeleted)
			if tc.failedCandidate {
				assert.Zero(t, repo.stageCalls, "failed enqueue must not become a desired version")
			}
			if tc.lookupErr != nil {
				assert.NotContains(t, ks.events, "create:document.txt")
			}
		})
	}
}

type nextcloudChannelCaptureKnowledgeService struct {
	*sweepFakeKS
	channel string
}

func (s *nextcloudChannelCaptureKnowledgeService) CreateKnowledgeFromFile(
	_ context.Context, _ string, _ *multipart.FileHeader, _ map[string]string,
	_ *bool, _ string, _ []string, channel string,
	_ *types.KnowledgeProcessOverrides,
) (*types.Knowledge, error) {
	s.channel = channel
	return &types.Knowledge{ID: "new-knowledge"}, nil
}

func TestIngestItem_NextcloudConnectorCannotRelabelProvenance(t *testing.T) {
	knowledge := &nextcloudChannelCaptureKnowledgeService{
		sweepFakeKS: &sweepFakeKS{repo: &deletionLookupKnowledgeRepo{}},
	}
	svc := &DataSourceService{knowledgeService: knowledge}
	_, err := svc.ingestItem(context.Background(), &types.DataSource{
		ID: "nextcloud-source", TenantID: 1, KnowledgeBaseID: "kb-1",
		Type: types.ConnectorTypeNextcloud,
	}, &types.FetchedItem{
		ExternalID: "nextcloud:instance:77", FileName: "document.txt",
		Content: []byte("current bytes"), Metadata: map[string]string{"channel": "local", "nextcloud_etag": "etag-1"},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, types.ConnectorTypeNextcloud, knowledge.channel)
}
