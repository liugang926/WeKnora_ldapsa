package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type datasourceReplacementChunks struct {
	replaceFileChunks
	deleted int
	err     error
}

func (c *datasourceReplacementChunks) DeleteByKnowledgeList(context.Context, uint64, []string) error {
	c.deleted++
	return c.err
}

type datasourceReplacementTenant struct{ interfaces.TenantRepository }

func (datasourceReplacementTenant) AdjustStorageUsed(context.Context, uint64, int64) error {
	return nil
}

type datasourceReplacementQueue struct {
	t       *testing.T
	repo    interfaces.KnowledgeRepository
	oldID   string
	inputs  *repository.KnowledgeInputProvenanceStore
	fetched []byte
	queued  int
}

func (q *datasourceReplacementQueue) Enqueue(task *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	if task.Type() == types.TypeDocumentProcess {
		var payload types.DocumentProcessPayload
		require.NoError(q.t, json.Unmarshal(task.Payload(), &payload))
		_, err := q.repo.GetKnowledgeByID(context.Background(), 7, q.oldID)
		require.Error(q.t, err, "retirement must commit before a worker can start")
		created, err := q.repo.GetKnowledgeByID(context.Background(), 7, payload.KnowledgeID)
		require.NoError(q.t, err, "replacement must commit before a worker can start")
		require.Equal(q.t, "ds-1", created.GetMetadata()["datasource_id"])
		require.Equal(q.t, "doc-1", created.GetMetadata()["external_id"])
		if created.Type == "file" {
			require.NotNil(q.t, payload.OriginalInput)
			request := types.KnowledgeBuildInputCaptureRequest{
				TenantID: created.TenantID, KnowledgeBaseID: created.KnowledgeBaseID, KnowledgeID: created.ID,
				Expected: payload.OriginalInput,
			}
			snapshot, err := q.inputs.CaptureKnowledgeBuildInputs(context.Background(), request)
			require.NoError(q.t, err)
			require.NoError(q.t, repository.KnowledgeInputArtifactMatches(snapshot.Proof, q.fetched))
		}
		q.queued++
	}
	return &asynq.TaskInfo{ID: "task-1", Queue: types.QueueDefault}, nil
}

type singleFileConnector struct {
	conflictSkipConnector
	kind string
	item types.FetchedItem
}

func (c singleFileConnector) Type() string { return c.kind }

func (c singleFileConnector) FetchAll(
	context.Context, *types.DataSourceConfig, []string,
) ([]types.FetchedItem, error) {
	return []types.FetchedItem{c.item}, nil
}

type replacementSyncHarness struct {
	*replaceFileHarness
	source *types.DataSource
	queue  *datasourceReplacementQueue
}

func (h *replacementSyncHarness) sync(t *testing.T, item *types.FetchedItem) (*types.SyncLog, error) {
	t.Helper()
	h.queue.fetched = item.Content
	return syncConnectorFile(h.ctx, t, h.svc, h.source, item)
}

func syncConnectorFile(ctx context.Context, t *testing.T, ks *knowledgeService,
	source *types.DataSource, item *types.FetchedItem,
) (*types.SyncLog, error) {
	t.Helper()
	registry := datasource.NewConnectorRegistry()
	require.NoError(t, registry.Register(singleFileConnector{kind: source.Type, item: *item}))
	log := &types.SyncLog{
		ID: "file-sync", DataSourceID: source.ID, TenantID: source.TenantID,
		Status: types.SyncLogStatusRunning, StartedAt: time.Now().UTC(),
	}
	logs := &processSyncSyncLogRepo{logs: map[string]*types.SyncLog{log.ID: log}}
	svc := &DataSourceService{
		dsRepo:      newKBDeleteDSRepo(source.KnowledgeBaseID, source),
		syncLogRepo: logs, connectorRegistry: registry, knowledgeService: ks,
		kbService: ks.kbService, tagService: &processSyncTagService{},
		tenantRepo: &processSyncTenantRepo{tenant: ctx.Value(types.TenantInfoContextKey).(*types.Tenant)},
	}
	task := enrichmentTask(t, types.TypeDataSourceSync, types.DataSourceSyncPayload{
		DataSourceID: source.ID, TenantID: source.TenantID, SyncLogID: log.ID,
		ForceFull: true, Trigger: "manual", Initiator: types.TaskInitiatorFromContext(ctx),
	})
	err := svc.ProcessSync(context.Background(), task)
	return logs.logs[log.ID], err
}

func datasourceReplacementFixture(t *testing.T) (
	*replacementSyncHarness, *gorm.DB, *datasourceReplacementChunks, *datasourceReplacementQueue,
) {
	t.Helper()
	return datasourceReplacementFixtureForConnector(t, conflictSkipConnectorType)
}

func datasourceReplacementFixtureForConnector(t *testing.T, connectorType string) (
	*replacementSyncHarness, *gorm.DB, *datasourceReplacementChunks, *datasourceReplacementQueue,
) {
	t.Helper()
	h := newReplaceFileHarness(t)
	f := newEnrichmentTestFixture(t, false)
	inputs, db := f.store, f.db
	caller := types.CallerFromContext(f.ctx)
	h.ctx = context.WithValue(f.ctx, types.UserIDContextKey, caller.UserID)
	h.ctx = context.WithValue(h.ctx, types.TenantInfoContextKey, &types.Tenant{ID: caller.TenantID})
	require.NotEmpty(t, types.TaskInitiatorFromContext(h.ctx).UserID)
	h.svc.groupAccess = NewGroupAccessService(repository.NewGroupAccessRepository(db))
	require.NoError(t, db.AutoMigrate(&types.DataSource{}))
	cfg, err := (&types.DataSourceConfig{Type: connectorType}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds-1", TenantID: 7, KnowledgeBaseID: "kb", Type: connectorType,
		Config: cfg, SyncMode: types.SyncModeFull, Status: types.DataSourceStatusActive,
	}
	require.NoError(t, db.Create(ds).Error)
	h.original.KnowledgeBaseID, h.original.Channel = ds.KnowledgeBaseID, ds.Type
	h.original.Metadata = types.JSON(`{"datasource_id":"ds-1","external_id":"doc-1"}`)
	_, err = inputs.CreateKnowledgeWithInput(
		repository.WithIndependentKnowledgeInput(h.ctx), &h.original, []byte(replaceFileOldContent),
	)
	require.NoError(t, err)
	h.svc.repo = repository.NewKnowledgeRepository(db)
	var kb types.KnowledgeBase
	require.NoError(t, db.First(&kb, "id = ?", ds.KnowledgeBaseID).Error)
	h.svc.kbService = &processSyncKBService{kb: &kb}
	h.svc.knowledgeInputs, h.svc.knowledgeInputWriter = inputs, inputs
	chunks := &datasourceReplacementChunks{}
	h.svc.chunkRepo = chunks
	h.svc.tenantRepo = datasourceReplacementTenant{}
	queue := &datasourceReplacementQueue{t: t, repo: h.svc.repo, oldID: h.original.ID, inputs: inputs}
	h.svc.task = queue
	return &replacementSyncHarness{h, ds, queue}, db, chunks, queue
}

func TestDataSourceReplacementRejectsUnfetchedItem(t *testing.T) {
	h, _, chunks, queue := datasourceReplacementFixture(t)
	svc := &DataSourceService{knowledgeService: h.svc}
	_, err := svc.ingestItem(h.ctx, h.source, &types.FetchedItem{
		ExternalID: "doc-1", Content: []byte("new content"), FileName: "doc.md",
	}, nil)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	require.Zero(t, queue.queued)
	require.Zero(t, chunks.deleted)
	require.Zero(t, h.store.saved)
	require.Empty(t, h.store.deleted)
	_, err = h.svc.repo.GetKnowledgeByID(h.ctx, 7, h.original.ID)
	require.NoError(t, err)
}

func TestDataSourceReplacementPreservesOldResourcesOnFailure(t *testing.T) {
	for _, stage := range []string{"validation", "json", "storage", "insert"} {
		t.Run(stage, func(t *testing.T) {
			h, db, chunks, queue := datasourceReplacementFixture(t)
			item := &types.FetchedItem{ExternalID: "doc-1", Content: []byte("new content"), FileName: "doc.md"}
			switch stage {
			case "validation":
				item.FileName = "doc.unsupported"
			case "json":
				item.FileName, item.Content = "doc.json", []byte("invalid JSON")
			case "storage":
				h.store.saveErr = errors.New("storage unavailable")
			case "insert":
				require.NoError(t, db.Exec(`CREATE TRIGGER fail_replacement BEFORE INSERT ON knowledges
					BEGIN SELECT RAISE(FAIL, 'forced insert failure'); END`).Error)
			}
			log, runErr := h.sync(t, item)
			switch stage {
			case "validation", "json":
				wantCode := "unsupported_file_type"
				if stage == "json" {
					wantCode = "invalid_json_content"
					require.NoError(t, runErr)
					require.Equal(t, types.SyncLogStatusPartial, log.Status)
				}
				var result types.SyncResult
				require.NoError(t, json.Unmarshal(log.Result, &result))
				require.Len(t, result.Errors, 1)
				require.Equal(t, wantCode, result.Errors[0].Code)
			case "storage":
				require.ErrorIs(t, runErr, h.store.saveErr)
			case "insert":
				var ingestErr *dataSourceIngestError
				require.ErrorAs(t, runErr, &ingestErr)
				require.ErrorContains(t, ingestErr.Unwrap(), "forced insert failure")
			}
			require.Equal(t, 1, log.ItemsFailed, log.ErrorMessage)
			require.Zero(t, log.ItemsCreated)
			require.Zero(t, log.ItemsUpdated)
			old, err := h.svc.repo.FindByDataSourceExternalID(
				h.ctx, h.source.TenantID, h.source.KnowledgeBaseID, h.source.ID, item.ExternalID,
			)
			require.NoError(t, err)
			require.NotNil(t, old)
			require.Equal(t, h.original.ID, old.ID)
			require.Equal(t, h.original.FileHash, old.FileHash)
			require.Equal(t, h.original.FilePath, old.FilePath)
			require.Equal(t, types.ParseStatusCompleted, old.ParseStatus)
			require.Zero(t, chunks.deleted)
			require.Zero(t, queue.queued)
			for _, path := range h.store.deleted {
				require.NotEqual(t, h.original.FilePath, path)
			}
			if stage == "insert" {
				require.Equal(t, []string{"new/file.md"}, h.store.deleted, "discard only the staged new file")
			}
		})
	}
}

func TestDataSourceReplacementCommitsBeforeEnqueueAndCleanup(t *testing.T) {
	for _, kind := range []string{"file", "url", "file_url"} {
		t.Run(kind, func(t *testing.T) {
			h, _, chunks, queue := datasourceReplacementFixture(t)
			tenant := h.ctx.Value(types.TenantInfoContextKey).(*types.Tenant)
			tenant.StorageQuota, tenant.StorageUsed = 100, 100
			require.NoError(t, h.svc.repo.UpdateKnowledgeColumn(h.ctx, h.original.ID, "storage_size", int64(10)))
			item := &types.FetchedItem{ExternalID: "doc-1", FileName: "doc.md"}
			switch kind {
			case "file":
				item.Content = []byte("new content")
			case "url":
				item.URL, item.FileName = "https://example.com/doc", ""
			case "file_url":
				item.URL = "https://example.com/doc.md"
			}
			log, err := h.sync(t, item)
			require.NoError(t, err)
			require.Equal(t, 1, log.ItemsUpdated, log.ErrorMessage)
			require.Equal(t, 1, queue.queued)
			require.Equal(t, 1, chunks.deleted)
			require.Contains(t, h.store.deleted, h.original.FilePath)
			created, err := h.svc.repo.FindByDataSourceExternalID(h.ctx, 7, h.source.KnowledgeBaseID, "ds-1", "doc-1")
			require.NoError(t, err)
			require.NotNil(t, created)
			require.Equal(t, kind, created.Type)
			require.NotEqual(t, h.original.ID, created.ID)
		})
	}
}

func TestDataSourceReplacementCleanupFailureKeepsCommittedReplacement(t *testing.T) {
	h, db, chunks, queue := datasourceReplacementFixture(t)
	chunks.err = errors.New("index cleanup unavailable")
	_, err := h.sync(t, &types.FetchedItem{
		ExternalID: "doc-1", Content: []byte("new content"), FileName: "doc.md",
	})
	require.NoError(t, err)
	require.Equal(t, 1, queue.queued)
	require.NotContains(t, h.store.deleted, h.original.FilePath)
	var retired types.Knowledge
	require.NoError(t, db.Unscoped().First(&retired, "id = ?", h.original.ID).Error)
	require.True(t, retired.DeletedAt.Valid)
	created, err := h.svc.repo.FindByDataSourceExternalID(h.ctx, 7, h.source.KnowledgeBaseID, "ds-1", "doc-1")
	require.NoError(t, err)
	require.NotNil(t, created)
}

type uncertainReplacementInputs struct {
	*repository.KnowledgeInputProvenanceStore
	committed bool
	ackErr    error
}

func (r *uncertainReplacementInputs) ReplaceKnowledgeWithInput(
	ctx context.Context, before, after *types.Knowledge, tags []string, artifact any,
) (*types.KnowledgeBuildInputProof, error) {
	_, err := r.KnowledgeInputProvenanceStore.ReplaceKnowledgeWithInput(ctx, before, after, tags, artifact)
	if err != nil {
		return nil, err
	}
	r.committed = true
	return nil, r.ackErr
}

func TestDataSourceReplacementRetainsFileOnUncertainCommit(t *testing.T) {
	for _, failReads := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit confirmed", true: "reads unavailable"}[failReads], func(t *testing.T) {
			h, db, chunks, queue := datasourceReplacementFixture(t)
			inputs := &uncertainReplacementInputs{
				KnowledgeInputProvenanceStore: queue.inputs,
				ackErr:                        errors.New("commit acknowledgement lost"),
			}
			h.svc.knowledgeInputWriter = inputs
			if failReads {
				err := db.Callback().Query().Before("gorm:query").Register("test:lost_reads", func(tx *gorm.DB) {
					if inputs.committed {
						_ = tx.AddError(errors.New("database unavailable"))
					}
				})
				require.NoError(t, err)
			}
			_, err := h.sync(t, &types.FetchedItem{
				ExternalID: "doc-1", Content: []byte("new content"), FileName: "doc.md",
			})
			require.True(t, inputs.committed)
			require.NotContains(t, h.store.deleted, "new/file.md", "a committed row may reference this file")
			if failReads {
				require.ErrorIs(t, err, inputs.ackErr)
				require.Zero(t, queue.queued)
				require.Zero(t, chunks.deleted)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, queue.queued)
				require.Equal(t, 1, chunks.deleted)
			}
		})
	}
}
