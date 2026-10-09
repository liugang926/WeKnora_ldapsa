package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingDSRepo captures UpdateSyncState calls so checkpoint persistence can
// be asserted without a database.
type recordingDSRepo struct {
	kbDeleteDSRepo
	updated            []*types.DataSource
	eventFailures      []*types.DataSource
	rejectEventFailure bool
	casUpdates         []*types.DataSource
	casExpected        []string
	rejectCAS          bool
}

func (r *recordingDSRepo) UpdateSyncState(_ context.Context, ds *types.DataSource) error {
	// Snapshot the fields a checkpoint is expected to persist.
	cp := *ds
	r.updated = append(r.updated, &cp)
	return nil
}

func (r *recordingDSRepo) UpdateNextcloudRetryableEventFailure(_ context.Context, ds *types.DataSource) (bool, error) {
	cp := *ds
	r.eventFailures = append(r.eventFailures, &cp)
	return !r.rejectEventFailure, nil
}

func (r *recordingDSRepo) UpdateNextcloudSyncStateCAS(_ context.Context, ds *types.DataSource, expectedStatus string) (
	bool,
	error,
) {
	cp := *ds
	r.casUpdates = append(r.casUpdates, &cp)
	r.casExpected = append(r.casExpected, expectedStatus)
	return !r.rejectCAS, nil
}

func makeConnectorCursor(t *testing.T, spaceNodeTimes map[string]map[string]string) types.JSON {
	t.Helper()
	inner := map[string]interface{}{"space_node_times": spaceNodeTimes}
	b, err := json.Marshal(&types.SyncCursor{ConnectorCursor: inner})
	require.NoError(t, err)
	return types.JSON(b)
}

// A fresh full sync (ForceFull, first attempt) must ignore any recorded cursor
// and re-fetch everything. A retry of that same task (attempt > 0) must instead
// resume from the checkpointed cursor so it converges instead of restarting.
func TestStreamStartCursor_ForceFullFirstAttemptDropsCursor(t *testing.T) {
	ds := &types.DataSource{
		LastSyncCursor: makeConnectorCursor(t, map[string]map[string]string{"space1": {"nt1": "100"}}),
	}

	fresh, err := streamStartCursor(ds, true /*forceFull*/, 0 /*attempt*/)
	require.NoError(t, err)
	assert.Nil(t, fresh, "fresh ForceFull must drop the cursor to re-fetch everything")

	retry, err := streamStartCursor(ds, true /*forceFull*/, 1 /*attempt*/)
	require.NoError(t, err)
	require.NotNil(t, retry, "a retried ForceFull must resume from the checkpoint")
	assert.NotNil(t, retry.ConnectorCursor["space_node_times"])
}

// Incremental sync always resumes from the recorded cursor regardless of attempt.
func TestStreamStartCursor_IncrementalKeepsCursor(t *testing.T) {
	ds := &types.DataSource{
		LastSyncCursor: makeConnectorCursor(t, map[string]map[string]string{"space1": {"nt1": "100"}}),
	}
	cur, err := streamStartCursor(ds, false /*forceFull*/, 0)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.NotNil(t, cur.ConnectorCursor["space_node_times"])
}

func newStreamHandler(svc *DataSourceService, ds *types.DataSource, result *types.SyncResult, syncLog *types.SyncLog) *streamSyncHandler {
	return &streamSyncHandler{svc: svc, ds: ds, result: result, syncLog: syncLog}
}

// Emit routes items through the same classification as the batch loop: deleted
// items count into result.Deleted (the actual deletion, scoping and failure
// counters are covered by the ProcessSync tests) and connector-reported
// failures (an item carrying only a Metadata["error"]) land in result.Failed
// with a message — never silently lost.
func TestStreamHandler_EmitClassifiesDeletedAndFailed(t *testing.T) {
	ds := &types.DataSource{
		ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1",
		Type: types.ConnectorTypeFeishu, SyncDeletions: true,
	}
	result := &types.SyncResult{}
	knowledgeRepo := &deletionLookupKnowledgeRepo{knowledge: &types.Knowledge{ID: "knowledge-gone"}}
	knowledgeSvc := &sweepFakeKS{repo: knowledgeRepo}
	h := newStreamHandler(&DataSourceService{knowledgeService: knowledgeSvc}, ds, result, &types.SyncLog{})

	require.NoError(t, h.Emit(context.Background(), types.FetchedItem{ExternalID: "gone", IsDeleted: true}))
	require.NoError(t, h.Emit(context.Background(), types.FetchedItem{
		ExternalID: "bad", Title: "Broken Doc",
		Metadata: map[string]string{"error": "export failed"},
	}))

	assert.Equal(t, 1, result.Deleted)
	assert.Equal(t, 1, result.Failed)
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "Broken Doc", result.Errors[0].Title)
	assert.Contains(t, result.Errors[0].Message, "export failed")
}

// A canceled context aborts the stream: Emit returns the context error so the
// connector stops fetching instead of burning API budget on a doomed run.
func TestStreamHandler_EmitAbortsOnCanceledContext(t *testing.T) {
	h := newStreamHandler(&DataSourceService{}, &types.DataSource{}, &types.SyncResult{}, &types.SyncLog{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := h.Emit(ctx, types.FetchedItem{ExternalID: "x", Content: []byte("data"), FileName: "x.md"})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// Checkpoint persists the connector cursor onto the data source so a crash
// after it keeps the progress made so far.
func TestStreamHandler_CheckpointPersistsCursor(t *testing.T) {
	dsRepo := &recordingDSRepo{}
	svc := &DataSourceService{dsRepo: dsRepo, syncLogRepo: &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{}}}
	ds := &types.DataSource{ID: "ds-1"}
	result := &types.SyncResult{Created: 3}
	syncLog := &types.SyncLog{ID: "log-1"}
	h := newStreamHandler(svc, ds, result, syncLog)

	cursor := &types.SyncCursor{ConnectorCursor: map[string]interface{}{
		"space_node_times": map[string]map[string]string{"space1": {"nt1": "100"}},
	}}
	require.NoError(t, h.Checkpoint(context.Background(), cursor))

	require.Len(t, dsRepo.updated, 1)
	assert.NotEmpty(t, dsRepo.updated[0].LastSyncCursor, "checkpoint must persist the cursor JSON")
}

type recordingStreamConnector struct {
	streamCalled bool
	fullCalled   bool
	fullCursor   *types.SyncCursor
}

func (recordingStreamConnector) Type() string { return "recording" }

func (recordingStreamConnector) Validate(context.Context, *types.DataSourceConfig) error {
	return nil
}

func (recordingStreamConnector) ListResources(
	context.Context, *types.DataSourceConfig, string,
) ([]types.Resource, error) {
	return nil, nil
}

func (recordingStreamConnector) ResolveResourceAncestors(
	context.Context, *types.DataSourceConfig, []string,
) ([]string, error) {
	return nil, nil
}

func (recordingStreamConnector) FetchAll(
	context.Context, *types.DataSourceConfig, []string,
) ([]types.FetchedItem, error) {
	return nil, nil
}

func (recordingStreamConnector) FetchIncremental(
	context.Context, *types.DataSourceConfig, *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	return nil, nil, nil
}

func (r *recordingStreamConnector) FetchStream(
	context.Context, *types.DataSourceConfig, *types.SyncCursor, datasource.StreamHandler,
) (*types.SyncCursor, error) {
	r.streamCalled = true
	return nil, nil
}

type recordingFullStreamConnector struct {
	recordingStreamConnector
}

func (r *recordingFullStreamConnector) FetchFullStream(
	_ context.Context, _ *types.DataSourceConfig, cursor *types.SyncCursor, _ datasource.StreamHandler,
) (*types.SyncCursor, error) {
	r.fullCalled = true
	r.fullCursor = cursor
	return cursor, nil
}

var (
	_ datasource.StreamingConnector     = (*recordingStreamConnector)(nil)
	_ datasource.FullStreamingConnector = (*recordingFullStreamConnector)(nil)
)

func TestStreamingFetchUsesFullStreamBaseline(t *testing.T) {
	start := &types.SyncCursor{ConnectorCursor: map[string]interface{}{"phase": "dropped"}}
	baseline := &types.SyncCursor{ConnectorCursor: map[string]interface{}{"phase": "stored"}}
	cfg := &types.DataSourceConfig{}

	full := &recordingFullStreamConnector{}
	_, err := streamingFetch(context.Background(), full, cfg, true, start, baseline, nil)
	require.NoError(t, err)
	if !full.fullCalled || full.streamCalled || full.fullCursor != baseline {
		t.Fatalf("full connector full=%v stream=%v cursor=%#v", full.fullCalled, full.streamCalled, full.fullCursor)
	}

	plain := &recordingStreamConnector{}
	_, err = streamingFetch(context.Background(), plain, cfg, true, start, baseline, nil)
	require.NoError(t, err)
	if !plain.streamCalled {
		t.Fatal("plain streaming connector must keep FetchStream on force-full")
	}
}

func nextcloudStreamDeletion() types.FetchedItem {
	return types.FetchedItem{
		ExternalID: "nextcloud:instance-1:41", SourceResourceID: "binding-1", IsDeleted: true,
		Metadata: map[string]string{
			"nextcloud_instance_id": "instance-1", "nextcloud_binding_id": "binding-1",
			"nextcloud_file_id": "41",
		},
	}
}

func nextcloudStreamCursor(t *testing.T, marker string) *types.SyncCursor {
	t.Helper()
	return &types.SyncCursor{ConnectorCursor: map[string]interface{}{
		"instance_id": "instance-1", "files": map[string]interface{}{"binding-1": map[string]interface{}{}},
		"marker": marker,
	}}
}

func TestNextcloudRetryOnlyEmitsClaimedFileAndRetainsSharedCursor(t *testing.T) {
	oldJSON, err := nextcloudStreamCursor(t, "old").ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1",
		Type: types.ConnectorTypeNextcloud, LastSyncCursor: oldJSON,
	}
	payload := types.DataSourceSyncPayload{
		Trigger:                  "nextcloud_candidate_retry",
		NextcloudRetryExternalID: "nextcloud:instance-1:41",
	}
	h := &streamSyncHandler{
		svc: &DataSourceService{}, ds: ds, result: &types.SyncResult{},
		payload: payload, nextcloudObserved: true,
		nextcloudInstanceID: "instance-1", nextcloudBindingID: "binding-1",
	}
	neighbor := types.FetchedItem{
		ExternalID: "nextcloud:instance-1:88", SourceResourceID: "binding-1",
		FileName: "changed.md", Content: []byte("new B"), Metadata: map[string]string{
			"nextcloud_instance_id": "instance-1", "nextcloud_binding_id": "binding-1",
			"nextcloud_file_id": "88", "nextcloud_etag": "changed-b",
		},
	}
	require.NoError(t, h.Emit(context.Background(), neighbor))
	require.Zero(t, h.result.Total)
	require.Zero(t, h.result.Created)
	neighbor.IsDeleted = true
	require.NoError(t, h.Emit(context.Background(), neighbor))
	require.Zero(t, h.result.Deleted)
	require.NoError(t, storeCompletedStreamCursor(ds, payload, nextcloudStreamCursor(t, "new")))
	stored, err := ds.ParseSyncCursor()
	require.NoError(t, err)
	require.Equal(t, "old", stored.ConnectorCursor["marker"],
		"B must remain visible to the next ordinary source sync")
	require.Nil(t, ds.LastSyncAt)
}

func TestNextcloudCandidateRetryPreStreamFailureKeepsSourceActive(t *testing.T) {
	log := &types.SyncLog{ID: "retry-log", Status: types.SyncLogStatusRunning}
	ordinaryLog := &types.SyncLog{ID: "ordinary-log", Status: types.SyncLogStatusRunning}
	logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{
		log.ID: log, ordinaryLog.ID: ordinaryLog,
	}}
	svc := &DataSourceService{dsRepo: &recordingDSRepo{}, syncLogRepo: logs}
	ds := &types.DataSource{
		ID: "ds", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive,
	}
	ctx := context.WithValue(context.Background(), nextcloudCandidateRetryRunKey{}, true)
	svc.recordPreStreamSyncFailure(ctx, ds, log, "temporary upstream 503", false)
	require.Equal(t, types.SyncLogStatusFailed, log.Status)
	require.Equal(t, types.DataSourceStatusActive, ds.Status)
	require.Empty(t, ds.ErrorMessage)
	ordinary := &types.DataSource{
		ID: "ds-ordinary", Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive,
	}
	svc.recordPreStreamSyncFailure(context.Background(), ordinary, ordinaryLog,
		"ordinary source failure", false)
	require.Equal(t, types.DataSourceStatusError, ordinary.Status)
	require.Equal(t, "ordinary source failure", ordinary.ErrorMessage)
}

func TestNextcloudCandidateRetryFailureKeepsSourceActive(t *testing.T) {
	oldJSON, err := nextcloudStreamCursor(t, "old").ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1",
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
		LastSyncCursor: oldJSON,
	}
	log := &types.SyncLog{ID: "retry-log", DataSourceID: ds.ID, Status: types.SyncLogStatusRunning}
	logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
	dsRepo := &recordingDSRepo{}
	svc := &DataSourceService{dsRepo: dsRepo, syncLogRepo: logs}
	ctx := context.WithValue(context.Background(), nextcloudCandidateRetryRunKey{}, true)
	svc.updateSyncRunResult(ctx, ds, log, &types.SyncResult{}, nil,
		types.SyncLogStatusFailed, "source_fetch_failed", false)
	require.Equal(t, types.SyncLogStatusFailed, log.Status)
	require.Equal(t, types.DataSourceStatusActive, ds.Status)
	require.Empty(t, dsRepo.updated)
	stored, err := ds.ParseSyncCursor()
	require.NoError(t, err)
	require.Equal(t, "old", stored.ConnectorCursor["marker"])
}

func TestNextcloudEventFailureKeepsSourceActive(t *testing.T) {
	for _, scenario := range []struct {
		name, trigger string
		cause         error
		wantStatus    string
	}{
		{"event_412", "nextcloud_event", datasource.ErrRetryableSource, types.DataSourceStatusActive},
		{"event_401", "nextcloud_event", datasource.ErrInvalidCredentials, types.DataSourceStatusError},
		{"event_unclassified", "nextcloud_event", errors.New("source unavailable"), types.DataSourceStatusError},
		{"manual_412", "manual", datasource.ErrRetryableSource, types.DataSourceStatusError},
		{"schedule_412", "schedule", datasource.ErrRetryableSource, types.DataSourceStatusError},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			oldCursor, err := nextcloudStreamCursor(t, "old").ToJSON()
			require.NoError(t, err)
			ds := &types.DataSource{
				ID: "ds", TenantID: 1, Type: types.ConnectorTypeNextcloud,
				Status: types.DataSourceStatusActive, LastSyncCursor: oldCursor,
			}
			log := &types.SyncLog{
				ID: "log", DataSourceID: ds.ID,
				Status: types.SyncLogStatusRunning,
			}
			logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
			dsRepo := &recordingDSRepo{}
			svc := &DataSourceService{dsRepo: dsRepo, syncLogRepo: logs}
			ctx := context.Background()
			if scenario.trigger == "nextcloud_event" {
				ctx = context.WithValue(ctx, nextcloudEventRunKey{}, true)
			}
			ctx = context.WithValue(ctx, nextcloudEventFailureKey{}, scenario.cause)
			svc.updateSyncRunResult(ctx, ds, log, &types.SyncResult{}, nil,
				types.SyncLogStatusFailed, "Fetch failed: Nextcloud content returned status 412", false)
			require.Equal(t, types.SyncLogStatusFailed, log.Status)
			require.NotNil(t, log.FinishedAt)
			require.Equal(t, "Fetch failed: Nextcloud content returned status 412", log.ErrorMessage)
			if scenario.wantStatus == types.DataSourceStatusActive {
				require.Len(t, dsRepo.eventFailures, 1)
				require.Empty(t, dsRepo.updated)
			} else {
				require.Empty(t, dsRepo.eventFailures)
				require.Len(t, dsRepo.casUpdates, 1)
				require.Equal(t, types.DataSourceStatusActive, dsRepo.casExpected[0])
				require.Empty(t, dsRepo.updated)
			}
			require.Equal(t, scenario.wantStatus, ds.Status)
			require.Equal(t, oldCursor, ds.LastSyncCursor)
		})
	}
}

type recordingEventSyncLogRepo struct {
	processSyncSyncLogRepo
	resultUpdates int
}

func (r *recordingEventSyncLogRepo) UpdateResult(ctx context.Context, log *types.SyncLog) error {
	r.resultUpdates++
	return r.processSyncSyncLogRepo.UpdateResult(ctx, log)
}

func TestNextcloudEventPauseCASStillReleasesSyncLog(t *testing.T) {
	ds := &types.DataSource{
		ID: "ds", TenantID: 1, Type: types.ConnectorTypeNextcloud,
		Status: types.DataSourceStatusActive,
	}
	log := &types.SyncLog{ID: "log", DataSourceID: ds.ID, Status: types.SyncLogStatusRunning}
	logs := &recordingEventSyncLogRepo{processSyncSyncLogRepo: processSyncSyncLogRepo{
		logs: map[string]*types.SyncLog{log.ID: log},
	}}
	dsRepo := &recordingDSRepo{rejectEventFailure: true}
	svc := &DataSourceService{dsRepo: dsRepo, syncLogRepo: logs}
	ctx := context.WithValue(context.Background(), nextcloudEventRunKey{}, true)
	ctx = context.WithValue(ctx, nextcloudEventFailureKey{}, datasource.ErrRetryableSource)
	svc.updateSyncRunResult(ctx, ds, log, &types.SyncResult{}, nil,
		types.SyncLogStatusFailed, "Fetch failed: Nextcloud content returned status 412", false)
	require.Len(t, dsRepo.eventFailures, 1)
	require.Empty(t, dsRepo.updated)
	require.Equal(t, 1, logs.resultUpdates)
	require.Equal(t, types.SyncLogStatusFailed, logs.logs[log.ID].Status)
	require.NotNil(t, logs.logs[log.ID].FinishedAt)
}

func TestNextcloudSyncCompletionCASPreservesAdminStatus(t *testing.T) {
	for _, scenario := range []struct {
		name, initialStatus, runStatus, wantSourceStatus, wantLogStatus string
		rejectCAS                                                       bool
		isEvent                                                         bool
	}{
		{
			"event success after pause", types.DataSourceStatusActive, types.SyncLogStatusSuccess,
			types.DataSourceStatusActive, types.SyncLogStatusCanceled, true, true,
		},
		{
			"event 401 after pause", types.DataSourceStatusActive, types.SyncLogStatusFailed,
			types.DataSourceStatusError, types.SyncLogStatusFailed, true, true,
		},
		{
			"event 403 after pause", types.DataSourceStatusActive, types.SyncLogStatusFailed,
			types.DataSourceStatusError, types.SyncLogStatusFailed, true, true,
		},
		{
			"manual error retry succeeds", types.DataSourceStatusError, types.SyncLogStatusSuccess,
			types.DataSourceStatusActive, types.SyncLogStatusSuccess, false, false,
		},
		{
			"scheduled error retry succeeds", types.DataSourceStatusError, types.SyncLogStatusSuccess,
			types.DataSourceStatusActive, types.SyncLogStatusSuccess, false, false,
		},
		{
			"manual started paused succeeds", types.DataSourceStatusPaused, types.SyncLogStatusSuccess,
			types.DataSourceStatusPaused, types.SyncLogStatusSuccess, false, false,
		},
		{
			"manual resumed while running", types.DataSourceStatusPaused, types.SyncLogStatusSuccess,
			types.DataSourceStatusPaused, types.SyncLogStatusCanceled, true, false,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ds := &types.DataSource{
				ID: "ds", TenantID: 1, Type: types.ConnectorTypeNextcloud,
				Status: scenario.initialStatus, LastSyncCursor: types.JSON(`{"marker":"new"}`),
			}
			log := &types.SyncLog{ID: "log", DataSourceID: ds.ID, Status: types.SyncLogStatusRunning}
			logs := &recordingEventSyncLogRepo{processSyncSyncLogRepo: processSyncSyncLogRepo{
				logs: map[string]*types.SyncLog{log.ID: log},
			}}
			dsRepo := &recordingDSRepo{rejectCAS: scenario.rejectCAS}
			svc := &DataSourceService{dsRepo: dsRepo, syncLogRepo: logs}
			ctx := context.Background()
			if scenario.isEvent {
				ctx = context.WithValue(ctx, nextcloudEventRunKey{}, true)
				if scenario.runStatus == types.SyncLogStatusFailed {
					ctx = context.WithValue(ctx, nextcloudEventFailureKey{}, datasource.ErrInvalidCredentials)
				}
			}
			message := ""
			if scenario.runStatus == types.SyncLogStatusFailed {
				message = "Fetch failed: Nextcloud returned status 401 or 403"
			}
			svc.updateSyncRunResult(ctx, ds, log, &types.SyncResult{}, nil,
				scenario.runStatus, message, scenario.initialStatus == types.DataSourceStatusPaused)
			require.Len(t, dsRepo.casUpdates, 1)
			require.Equal(t, scenario.initialStatus, dsRepo.casExpected[0])
			require.Equal(t, scenario.wantSourceStatus, dsRepo.casUpdates[0].Status)
			require.Empty(t, dsRepo.updated)
			require.Empty(t, dsRepo.eventFailures)
			require.Equal(t, 1, logs.resultUpdates)
			require.Equal(t, scenario.wantLogStatus, log.Status)
			require.NotNil(t, log.FinishedAt)
			if scenario.rejectCAS && scenario.runStatus == types.SyncLogStatusSuccess {
				require.Contains(t, log.ErrorMessage, "cursor not committed")
			}
		})
	}
}

func TestNextcloudPreStreamFailureCASReleasesLog(t *testing.T) {
	for _, scenario := range []struct {
		name, initialStatus, desiredStatus string
		rejectCAS                          bool
	}{
		{"active source", types.DataSourceStatusActive, types.DataSourceStatusError, false},
		{"pause after worker read", types.DataSourceStatusActive, types.DataSourceStatusError, true},
		{"manual started paused", types.DataSourceStatusPaused, types.DataSourceStatusPaused, false},
		{"error source retry", types.DataSourceStatusError, types.DataSourceStatusError, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ds := &types.DataSource{
				ID: "ds", TenantID: 1, Type: types.ConnectorTypeNextcloud,
				Status: scenario.initialStatus,
			}
			log := &types.SyncLog{ID: "log", DataSourceID: ds.ID, Status: types.SyncLogStatusRunning}
			logs := &recordingEventSyncLogRepo{processSyncSyncLogRepo: processSyncSyncLogRepo{
				logs: map[string]*types.SyncLog{log.ID: log},
			}}
			dsRepo := &recordingDSRepo{rejectCAS: scenario.rejectCAS}
			svc := &DataSourceService{dsRepo: dsRepo, syncLogRepo: logs}
			svc.recordPreStreamSyncFailure(context.Background(), ds, log, "invalid configuration",
				scenario.initialStatus == types.DataSourceStatusPaused)
			require.Len(t, dsRepo.casUpdates, 1)
			require.Equal(t, scenario.initialStatus, dsRepo.casExpected[0])
			require.Equal(t, scenario.desiredStatus, dsRepo.casUpdates[0].Status)
			require.Empty(t, dsRepo.updated)
			require.Equal(t, 1, logs.resultUpdates)
			require.Equal(t, types.SyncLogStatusFailed, log.Status)
			require.NotNil(t, log.FinishedAt)
		})
	}
}

func TestNextcloudEventPreStreamFailureStillStopsSource(t *testing.T) {
	for _, trigger := range []string{"nextcloud_event", "manual", "schedule"} {
		t.Run(trigger, func(t *testing.T) {
			ds := &types.DataSource{
				ID: "ds", TenantID: 1, Type: types.ConnectorTypeNextcloud,
				Status: types.DataSourceStatusActive,
			}
			log := &types.SyncLog{
				ID: "log", DataSourceID: ds.ID,
				Status: types.SyncLogStatusRunning,
			}
			logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
			dsRepo := &recordingDSRepo{}
			svc := &DataSourceService{dsRepo: dsRepo, syncLogRepo: logs}
			ctx := context.Background()
			if trigger == "nextcloud_event" {
				ctx = context.WithValue(ctx, nextcloudEventRunKey{}, true)
			}
			svc.recordPreStreamSyncFailure(ctx, ds, log, "temporary source failure", false)
			require.Equal(t, types.SyncLogStatusFailed, log.Status)
			require.NotNil(t, log.FinishedAt)
			require.Equal(t, types.DataSourceStatusError, ds.Status)
			require.Equal(t, "temporary source failure", ds.ErrorMessage)
			require.Len(t, dsRepo.casUpdates, 1)
			require.Empty(t, dsRepo.updated)
		})
	}
}

func TestNextcloudStreamHandlerRejectsFailedItemAndPartialCheckpoint(t *testing.T) {
	repo := &keyedDeletionRepo{tombstoneErr: errors.New("tombstone failed")}
	dsRepo := &recordingDSRepo{}
	log := &types.SyncLog{ID: "log-1", Status: types.SyncLogStatusRunning}
	svc := &DataSourceService{
		dsRepo: dsRepo, knowledgeService: &sweepFakeKS{repo: repo},
		syncLogRepo: &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}},
	}
	ds := &types.DataSource{ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud}
	h := &streamSyncHandler{
		svc: svc, ds: ds, result: &types.SyncResult{}, syncLog: log,
		payload: types.DataSourceSyncPayload{SyncLogID: log.ID},
	}
	require.NoError(t, h.ObserveNextcloudIdentity(context.Background(), "instance-1", "binding-1"))
	require.ErrorContains(t, h.Emit(context.Background(), nextcloudStreamDeletion()), "could not be processed")
	require.Equal(t, 1, h.result.Failed)
	require.ErrorContains(t, h.Checkpoint(context.Background(), nextcloudStreamCursor(t, "new")),
		"no resumable partial cursor")
	require.Empty(t, dsRepo.updated)
}

func TestNextcloudStreamHandlerRechecksRunningLogBeforeItem(t *testing.T) {
	repo := &keyedDeletionRepo{}
	log := &types.SyncLog{ID: "log-1", Status: types.SyncLogStatusRunning}
	svc := &DataSourceService{
		knowledgeService: &sweepFakeKS{repo: repo},
		syncLogRepo:      &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}},
	}
	ds := &types.DataSource{ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeNextcloud}
	h := &streamSyncHandler{
		svc: svc, ds: ds, result: &types.SyncResult{}, syncLog: log,
		payload: types.DataSourceSyncPayload{SyncLogID: log.ID},
	}
	require.NoError(t, h.ObserveNextcloudIdentity(context.Background(), "instance-1", "binding-1"))
	log.Status = types.SyncLogStatusCanceled
	require.ErrorIs(t, h.Emit(context.Background(), nextcloudStreamDeletion()), asynq.SkipRetry)
	require.True(t, h.nextcloudGuardStopped)
	require.Zero(t, h.result.Total)
}

func TestNextcloudStreamHandlerStagesVersionBeforeAcceptingItem(t *testing.T) {
	for _, failStage := range []bool{false, true} {
		t.Run(map[bool]string{false: "staged", true: "stage failed"}[failStage], func(t *testing.T) {
			repo := &deletionLookupKnowledgeRepo{}
			if failStage {
				repo.stageErr = errors.New("stage unavailable")
			}
			log := &types.SyncLog{ID: "log-1", Status: types.SyncLogStatusRunning}
			svc := &DataSourceService{
				knowledgeService: &sweepFakeKS{repo: repo},
				syncLogRepo:      &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}},
			}
			ds := &types.DataSource{
				ID:              "ds-1",
				TenantID:        1,
				KnowledgeBaseID: "kb-1",
				Type:            types.ConnectorTypeNextcloud,
			}
			h := &streamSyncHandler{
				svc: svc, ds: ds, result: &types.SyncResult{}, syncLog: log,
				payload: types.DataSourceSyncPayload{SyncLogID: log.ID},
			}
			require.NoError(t, h.ObserveNextcloudIdentity(context.Background(), "instance-1", "binding-1"))
			item := types.FetchedItem{
				ExternalID: "nextcloud:instance-1:41", SourceResourceID: "binding-1",
				FileName: "Notes.md", Content: []byte("hello"), Metadata: map[string]string{
					"nextcloud_instance_id": "instance-1", "nextcloud_binding_id": "binding-1",
					"nextcloud_file_id": "41", "nextcloud_etag": "etag-1",
				},
			}
			err := h.Emit(context.Background(), item)
			require.Equal(t, 1, repo.stageCalls)
			if failStage {
				require.ErrorContains(t, err, "could not be processed")
				require.Equal(t, 1, h.result.Failed)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, h.result.Created)
			}
		})
	}
}

type nextcloudStreamingTestConnector struct {
	recordingStreamConnector
	emit         bool
	afterObserve func()
	cursor       *types.SyncCursor
}

type nextcloudFailingStreamConnector struct {
	recordingStreamConnector
	fetchErr error
}

func (c *nextcloudFailingStreamConnector) FetchStream(
	context.Context, *types.DataSourceConfig, *types.SyncCursor, datasource.StreamHandler,
) (*types.SyncCursor, error) {
	return nil, c.fetchErr
}

func TestProcessSyncStreamingNextcloudRetryableEventFailureKeepsSourceActive(t *testing.T) {
	for _, scenario := range []struct {
		name, trigger string
		cause         error
		wantStatus    string
	}{
		{"stale_event", "nextcloud_event", datasource.ErrRetryableSource, types.DataSourceStatusActive},
		{"unauthorized_event", "nextcloud_event", datasource.ErrInvalidCredentials, types.DataSourceStatusError},
		{"manual_stale", "manual", datasource.ErrRetryableSource, types.DataSourceStatusError},
		{"schedule_stale", "schedule", datasource.ErrRetryableSource, types.DataSourceStatusError},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			oldCursor, err := nextcloudStreamCursor(t, "old").ToJSON()
			require.NoError(t, err)
			ds := &types.DataSource{
				ID: "ds", TenantID: 1, KnowledgeBaseID: "kb",
				Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
				LastSyncCursor: oldCursor,
			}
			log := &types.SyncLog{
				ID: "log", DataSourceID: ds.ID,
				Status: types.SyncLogStatusRunning,
			}
			dsRepo := &recordingDSRepo{}
			svc := &DataSourceService{
				dsRepo:      dsRepo,
				syncLogRepo: &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}},
				tenantRepo:  &processSyncTenantRepo{tenant: &types.Tenant{ID: 1}},
				tagService:  &processSyncTagService{},
			}
			connector := &nextcloudFailingStreamConnector{fetchErr: scenario.cause}
			err = svc.processSyncStreaming(context.Background(), connector, ds, log,
				&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding-1"}},
				types.DataSourceSyncPayload{SyncLogID: log.ID, Trigger: scenario.trigger}, false)
			require.ErrorIs(t, err, scenario.cause)
			require.Equal(t, types.SyncLogStatusFailed, log.Status)
			require.Equal(t, scenario.wantStatus, ds.Status)
			if scenario.wantStatus == types.DataSourceStatusActive {
				require.Len(t, dsRepo.eventFailures, 1)
				require.Empty(t, dsRepo.updated)
			} else {
				require.Empty(t, dsRepo.eventFailures)
				require.Len(t, dsRepo.casUpdates, 1)
				require.Equal(t, types.DataSourceStatusActive, dsRepo.casExpected[0])
				require.Empty(t, dsRepo.updated)
			}
			require.Equal(t, oldCursor, ds.LastSyncCursor)
		})
	}
}

func (c *nextcloudStreamingTestConnector) FetchStream(
	ctx context.Context, _ *types.DataSourceConfig, _ *types.SyncCursor, h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	observer := h.(interface {
		ObserveNextcloudIdentity(context.Context, string, string) error
	})
	if err := observer.ObserveNextcloudIdentity(ctx, "instance-1", "binding-1"); err != nil {
		return nil, err
	}
	if c.afterObserve != nil {
		c.afterObserve()
	}
	if c.emit {
		if err := h.Emit(ctx, nextcloudStreamDeletion()); err != nil {
			return nil, err
		}
	}
	return c.cursor, nil
}

func TestProcessSyncStreamingNextcloudCursorOnlyAfterSuccessfulItems(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := "success"
		if failure {
			name = "failed deletion"
		}
		t.Run(name, func(t *testing.T) {
			oldJSON, err := nextcloudStreamCursor(t, "old").ToJSON()
			require.NoError(t, err)
			repo := &keyedDeletionRepo{}
			if failure {
				repo.tombstoneErr = errors.New("tombstone failed")
			}
			ds := &types.DataSource{
				ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1",
				Type: types.ConnectorTypeNextcloud, LastSyncCursor: oldJSON,
			}
			log := &types.SyncLog{ID: "log-1", Status: types.SyncLogStatusRunning}
			dsRepo := &recordingDSRepo{}
			svc := &DataSourceService{
				dsRepo: dsRepo, knowledgeService: &sweepFakeKS{repo: repo},
				syncLogRepo: &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}},
				tenantRepo:  &processSyncTenantRepo{tenant: &types.Tenant{ID: 1}},
				tagService:  &processSyncTagService{},
			}
			connector := &nextcloudStreamingTestConnector{emit: true, cursor: nextcloudStreamCursor(t, "new")}
			err = svc.processSyncStreaming(context.Background(), connector, ds, log,
				&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding-1"}},
				types.DataSourceSyncPayload{SyncLogID: log.ID}, false)
			if failure {
				require.ErrorContains(t, err, "could not be processed")
				require.Equal(t, types.SyncLogStatusFailed, log.Status)
			} else {
				require.NoError(t, err)
				require.Equal(t, types.SyncLogStatusSuccess, log.Status)
			}
			stored, err := ds.ParseSyncCursor()
			require.NoError(t, err)
			if failure {
				require.Equal(t, "old", stored.ConnectorCursor["marker"])
			} else {
				require.Equal(t, "new", stored.ConnectorCursor["marker"])
			}
		})
	}
}

func TestProcessSyncStreamingNextcloudRechecksEmptyScanBeforeCursor(t *testing.T) {
	oldJSON, err := nextcloudStreamCursor(t, "old").ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1",
		Type: types.ConnectorTypeNextcloud, LastSyncCursor: oldJSON,
	}
	log := &types.SyncLog{ID: "log-1", Status: types.SyncLogStatusRunning}
	svc := &DataSourceService{
		dsRepo:      &recordingDSRepo{},
		syncLogRepo: &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}},
		tenantRepo:  &processSyncTenantRepo{tenant: &types.Tenant{ID: 1}},
		tagService:  &processSyncTagService{},
	}
	connector := &nextcloudStreamingTestConnector{
		cursor:       nextcloudStreamCursor(t, "new"),
		afterObserve: func() { log.Status = types.SyncLogStatusCanceled },
	}
	err = svc.processSyncStreaming(context.Background(), connector, ds, log,
		&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding-1"}},
		types.DataSourceSyncPayload{SyncLogID: log.ID}, false)
	require.ErrorIs(t, err, asynq.SkipRetry)
	require.Equal(t, types.SyncLogStatusCanceled, log.Status)
	stored, err := ds.ParseSyncCursor()
	require.NoError(t, err)
	require.Equal(t, "old", stored.ConnectorCursor["marker"])
}

func TestProcessSyncStreamingNextcloudRejectsDifferentCursorInstance(t *testing.T) {
	oldJSON, err := nextcloudStreamCursor(t, "old").ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds-1", TenantID: 1, KnowledgeBaseID: "kb-1",
		Type: types.ConnectorTypeNextcloud, LastSyncCursor: oldJSON,
	}
	log := &types.SyncLog{ID: "log-1", Status: types.SyncLogStatusRunning}
	svc := &DataSourceService{
		dsRepo:      &recordingDSRepo{},
		syncLogRepo: &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}},
		tenantRepo:  &processSyncTenantRepo{tenant: &types.Tenant{ID: 1}},
		tagService:  &processSyncTagService{},
	}
	bad := nextcloudStreamCursor(t, "new")
	bad.ConnectorCursor["instance_id"] = "cloned-instance"
	connector := &nextcloudStreamingTestConnector{cursor: bad}
	err = svc.processSyncStreaming(context.Background(), connector, ds, log,
		&types.DataSourceConfig{Type: types.ConnectorTypeNextcloud, ResourceIDs: []string{"binding-1"}},
		types.DataSourceSyncPayload{SyncLogID: log.ID}, false)
	require.ErrorContains(t, err, "cursor identity or inventory differs")
	require.Equal(t, types.SyncLogStatusFailed, log.Status)
	stored, err := ds.ParseSyncCursor()
	require.NoError(t, err)
	require.Equal(t, "old", stored.ConnectorCursor["marker"])
}
