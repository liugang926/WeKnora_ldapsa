package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
)

type dingTalkFilePublicationKey struct{}
type dingTalkFilePublication struct {
	store   *repository.KnowledgeInputProvenanceStore
	rebuild bool
	result  *repository.DingTalkPublicationResult
}

// A durable backend must support exact task ID lookups. A Lite no-op inspector
// cannot establish enqueue acceptance or the end of a previous attempt.
type dingTalkQueueInspector interface {
	LookupKnowledgeQueueTask(context.Context, string) (*asynq.TaskInfo, error)
	DingTalkTasksQuiescent(context.Context, string) (bool, error)
}

const dingTalkRunTTL = 2 * time.Minute

// dingTalkProgressFlushInterval throttles live sync_log progress writes so a
// 200-document sync cannot hammer the database on every item.
const dingTalkProgressFlushInterval = 5 * time.Second

// syncProgressWriter turns connector fetch reports and import-loop outcomes
// into throttled sync_log progress updates (items_total plus an in-flight
// progress payload inside the result column). Terminal state remains owned by
// FinalizeDingTalkRun, which overwrites these values authoritatively.
type syncProgressWriter struct {
	repo   interfaces.SyncLogRepository
	logID  string
	mu     sync.Mutex
	last   time.Time
	total  int
	phase  string
	fields map[string]any
}

func newSyncProgressWriter(repo interfaces.SyncLogRepository, logID string) *syncProgressWriter {
	return &syncProgressWriter{repo: repo, logID: logID, phase: "fetch"}
}

// reportFetch records connector-side enumeration/processing counts.
func (w *syncProgressWriter) reportFetch(p datasource.FetchProgress) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.total = max(w.total, p.Enumerated)
	payload := map[string]any{"phase": "fetch", "fetched": p.Processed, "enumerated": p.Enumerated}
	w.fields = map[string]any{
		"items_total": w.total,
		"result":      types.JSON(mustMarshalJSON(payload)),
	}
	w.flushLocked()
	w.mu.Unlock()
}

// reportImport records import-loop counters (authoritative item outcomes so
// far, with fetch-phase failures carried in failed).
func (w *syncProgressWriter) reportImport(total, created, updated, skipped, failed, done int) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.phase = "import"
	payload := map[string]any{"phase": "import", "done": done, "total": total}
	w.fields = map[string]any{
		"items_total":   total,
		"items_created": created,
		"items_updated": updated,
		"items_skipped": skipped,
		"items_failed":  failed,
		"result":        types.JSON(mustMarshalJSON(payload)),
	}
	w.flushLocked()
	w.mu.Unlock()
}

func (w *syncProgressWriter) flushLocked() {
	now := time.Now()
	if now.Sub(w.last) < dingTalkProgressFlushInterval {
		return
	}
	w.last = now
	// Progress writes are best-effort; a failed flush must never fail the sync.
	_ = w.repo.UpdateProgress(context.Background(), w.logID, w.fields)
}

func mustMarshalJSON(v any) []byte {
	encoded, err := json.Marshal(v)
	if err != nil {
		return []byte(`{}`)
	}
	return encoded
}

// isSerializationConflict reports Postgres serialization/concurrent-update
// failures that are safe to retry immediately.
func isSerializationConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLSTATE 40001") ||
		strings.Contains(msg, "could not serialize access") ||
		strings.Contains(msg, "concurrent update")
}

func dingTalkSyncFailure(result *types.SyncResult, title, code string, err error) {
	result.Failed++
	recordSyncError(result, types.SyncItemError{Title: title, Code: code,
		Message: "DingTalk synchronization could not complete; retry and check the server log"})
	logger.Errorf(context.Background(), "DingTalk sync stage=%s: %v", code, err)
}

// dingTalkCancelMessage renders the finalize error_message for a canceled
// run. The dominant cause is a concurrent configuration update invalidating
// the run claim (RenewDingTalkRun/ErrDingTalkRunExpired): without this the
// sync page used to show a bare "canceled" with no explanation.
func dingTalkCancelMessage(cause error) string {
	if cause == nil {
		return ""
	}
	if errors.Is(cause, repository.ErrDingTalkRunExpired) {
		return "同步已取消：数据源配置已更新（下个调度窗口将使用新配置重试）"
	}
	return "同步已取消：" + cause.Error()
}

func (s *DataSourceService) processDingTalkSync(ctx context.Context, ds *types.DataSource, kb *types.KnowledgeBase, payload types.DataSourceSyncPayload) (runErr error) {
	if s.sourceInputs == nil {
		return types.ErrSourceLineageUnknown
	}
	taskID, _ := asynq.GetTaskID(ctx)
	if taskID == "" {
		taskID, _ = types.TaskExecutionIDFromContext(ctx)
	}
	retry, ok := asynq.GetRetryCount(ctx)
	if !ok {
		retry, _, _ = types.TaskRetryMetadataFromContext(ctx)
	}
	claimCtx, err := s.sourceInputs.ClaimDingTalkRun(ctx, ds, payload.SyncLogID, taskID, retry, dingTalkRunTTL)
	if err != nil {
		return err
	} // No claim: never finalize someone else's log.
	ctx, cancel := context.WithCancelCause(claimCtx)
	done := make(chan struct{})
	go func(heartbeatCtx context.Context) {
		defer close(done)
		ticker := time.NewTicker(dingTalkRunTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := s.sourceInputs.RenewDingTalkRun(heartbeatCtx, dingTalkRunTTL); err != nil {
					cancel(err)
					return
				}
			}
		}
	}(ctx)
	result := &types.SyncResult{}
	status := types.SyncLogStatusFailed
	progress := newSyncProgressWriter(s.syncLogRepo, payload.SyncLogID)
	var cursorJSON types.JSON
	defer func() {
		cause := context.Cause(ctx)
		cancel(nil)
		<-done
		finalMessage := ""
		if cause != nil {
			runErr = errors.Join(runErr, cause)
			status = types.SyncLogStatusCanceled
			finalMessage = dingTalkCancelMessage(cause)
		}
		if runErr != nil && result.Failed == 0 {
			dingTalkSyncFailure(result, "", "dingtalk_sync_failed", runErr)
		}
		finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(claimCtx), 10*time.Second)
		defer finishCancel()
		runErr = errors.Join(runErr, s.sourceInputs.FinalizeDingTalkRun(finishCtx, status, result, cursorJSON, finalMessage))
	}()

	knowledge, ok := s.knowledgeService.(*knowledgeService)
	if !ok || knowledge.knowledgeInputs != s.sourceInputs {
		return errors.New("DingTalk sync requires the configured knowledge input store")
	}
	if _, ok := knowledge.taskInspector.(dingTalkQueueInspector); !ok {
		return errors.New("DingTalk sync requires a durable queue inspector")
	}
	connector, err := s.connectorRegistry.Get(ds.Type)
	if err != nil {
		return err
	}
	config, err := ds.ParseConfig()
	if err != nil {
		return err
	}
	config.MultimodalEnabled = kb.IsMultimodalEnabled()
	config.ProcessingPolicyDigest = types.GraphDigest(struct {
		Process types.EffectiveProcessConfig
		Index   types.IndexingStrategy
		Summary string
	}{ResolveProcessConfig(kb, nil), kb.IndexingStrategy, kb.SummaryModelID})
	tenant, err := s.tenantRepo.GetTenantByID(ctx, ds.TenantID)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, tenant)
	ctx, err = s.sourceInputs.BeginDingTalkPublicationFetch(ctx, ds, payload.SyncLogID)
	if err != nil {
		return err
	}
	if err = s.sourceInputs.ReconcileDingTalkDeletedPublications(ctx); err != nil {
		return err
	}
	ctx, err = s.sourceInputs.BeginDingTalkPublicationFetch(ctx, ds, payload.SyncLogID)
	if err != nil {
		return err
	}
	cursor, err := ds.ParseSyncCursor()
	if err != nil {
		return err
	}
	// Live fetch progress: the connector reports enumerated/processed counts
	// through the context so the sync page can render a percentage during
	// the (potentially hour-long) download phase. The values only wrap the
	// existing context chain; cancellation semantics are unchanged.
	ctx = datasource.WithFetchProgress(ctx, progress.reportFetch)
	full, tombstones, err := s.sourceInputs.DingTalkRefetchState(ctx)
	if err != nil {
		return err
	}
	localDeletion, err := dingtalk.CursorContainsAnyDocument(cursor, tombstones)
	if err != nil {
		return err
	}
	work, err := s.sourceInputs.ListDingTalkWork(ctx)
	if err != nil {
		return err
	}
	for _, intent := range work {
		if intent.Kind == "process" {
			full = true
		}
	}
	full = full || localDeletion || payload.ForceFull || ds.SyncMode == types.SyncModeFull
	var items []types.FetchedItem
	var nextCursor *types.SyncCursor
	if full {
		fetcher, ok := connector.(datasource.FullSyncWithCursor)
		if !ok {
			return errors.New("DingTalk connector must support full fetch with inventory")
		}
		items, nextCursor, err = fetcher.FetchAllFromCursor(ctx, config, config.ResourceIDs, cursor)
	} else {
		items, nextCursor, err = connector.FetchIncremental(ctx, config, cursor)
	}
	partial := false
	if err != nil {
		var partialErr *datasource.PartialFetchError
		if !errors.As(err, &partialErr) {
			return err
		}
		partial = true
		dingTalkSyncFailure(result, "", "dingtalk_fetch_partial", err)
	}
	result.Total = len(items)
	tagIDs := s.resolveAutoTagIDs(ctx, ds)
	selected := map[string]*types.Knowledge{}
	importDone := 0
	for i := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		item := &items[i]
		if item.IsDeleted {
			importDone++
			continue
		}
		if failure := item.Metadata["error"]; failure != "" {
			dingTalkSyncFailure(result, item.Title, "dingtalk_document_fetch_failed", errors.New(failure))
			importDone++
			progress.reportImport(result.Total, result.Created, result.Updated, result.Skipped, result.Failed, importDone)
			continue
		}
		k, outcome, err := s.ingestDingTalkItem(ctx, ds, item, tagIDs)
		if err != nil && isSerializationConflict(err) {
			// Postgres SERIALIZABLE / concurrent-update conflicts are a normal
			// part of ingesting a large batch while enrichment fan-out writes
			// the same rows. A short bounded retry rescues the item instead
			// of failing a 200-document sync over a transient collision.
			recovered := false
			for attempt := 1; attempt <= 3; attempt++ {
				select {
				case <-ctx.Done():
					recovered = false
				case <-time.After(time.Duration(attempt) * 250 * time.Millisecond):
				}
				k, outcome, err = s.ingestDingTalkItem(ctx, ds, item, tagIDs)
				if err == nil || !isSerializationConflict(err) {
					recovered = err == nil
					break
				}
			}
			if !recovered && err != nil {
				dingTalkSyncFailure(result, item.Title, "dingtalk_import_failed", err)
				importDone++
				progress.reportImport(result.Total, result.Created, result.Updated, result.Skipped, result.Failed, importDone)
				continue
			}
		}
		if err != nil {
			dingTalkSyncFailure(result, item.Title, "dingtalk_import_failed", err)
			importDone++
			progress.reportImport(result.Total, result.Created, result.Updated, result.Skipped, result.Failed, importDone)
			continue
		}
		selected[item.ExternalID] = k
		switch outcome {
		case "created":
			result.Created++
		case "replaced":
			result.Updated++
		default:
			result.Skipped++
		}
		importDone++
		progress.reportImport(result.Total, result.Created, result.Updated, result.Skipped, result.Failed, importDone)
	}
	// A partial traversal is never evidence of absence. Successful complete
	// connector tombstones retain its existing union-of-selections semantics.
	if !partial {
		for _, item := range items {
			if !item.IsDeleted {
				continue
			}
			required, retireErr := s.sourceInputs.DingTalkRetirementRequired(ctx, item.ExternalID)
			if retireErr == nil && required {
				retireErr = s.sourceInputs.RetireDingTalkDocument(ctx, item.ExternalID, true)
			}
			if err := retireErr; err != nil {
				dingTalkSyncFailure(result, item.Title, "dingtalk_retire_failed", err)
				result.DeletionFailed++
			} else if required {
				result.Deleted++
			} else {
				result.Skipped++
			}
		}
	}
	// Retirement changes the publication snapshot. A new Begin here is used
	// only for durable work loading; it does not rebind or reuse fetched items.
	ctx, err = s.sourceInputs.BeginDingTalkPublicationFetch(ctx, ds, payload.SyncLogID)
	if err != nil {
		return err
	}
	if err := s.consumeDingTalkWork(ctx, knowledge, kb, selected); err != nil {
		dingTalkSyncFailure(result, "", "dingtalk_processing_handoff_failed", err)
	}
	if result.Failed > 0 {
		return datasource.ErrSyncFailed
	}
	if nextCursor == nil {
		return errors.New("DingTalk complete fetch returned no inventory")
	}
	cursorJSON, err = nextCursor.ToJSON()
	if err != nil {
		return err
	}
	status = types.SyncLogStatusSuccess
	return nil
}

func (s *DataSourceService) ingestDingTalkItem(ctx context.Context, ds *types.DataSource, item *types.FetchedItem, tags []string) (*types.Knowledge, string, error) {
	bound, err := s.sourceInputs.BindDingTalkOriginalArtifact(ctx, item)
	if err != nil {
		return nil, "", err
	}
	rebuild, err := s.sourceInputs.DingTalkConfigurationChanged(bound, item.ExternalID)
	if err != nil {
		return nil, "", err
	}
	publication := &dingTalkFilePublication{store: s.sourceInputs, rebuild: rebuild}
	bound = context.WithValue(bound, dingTalkFilePublicationKey{}, publication)
	metadata := make(map[string]string, len(item.Metadata)+4)
	for key, value := range item.Metadata {
		metadata[key] = value
	}
	if !item.UpdatedAt.IsZero() {
		metadata["source_updated_at"] = item.UpdatedAt.UTC().Format(time.RFC3339)
	}
	metadata["datasource_id"], metadata["external_id"] = ds.ID, item.ExternalID
	metadata["source_resource_id"], metadata["channel"] = item.SourceResourceID, types.ConnectorTypeDingTalk
	file, err := bytesToFileHeader(item.Content, item.FileName)
	if err != nil {
		return nil, "", err
	}
	k, err := s.knowledgeService.CreateKnowledgeFromFile(bound, ds.KnowledgeBaseID, file, metadata, nil, item.FileName, tags, types.ConnectorTypeDingTalk, nil)
	if err != nil {
		return k, "", err
	}
	if publication.result == nil {
		return nil, "", types.ErrSourceLineageUnknown
	}
	return k, publication.result.Outcome, nil
}

func (s *DataSourceService) consumeDingTalkWork(ctx context.Context, knowledge *knowledgeService, kb *types.KnowledgeBase, selected map[string]*types.Knowledge) error {
	work, err := s.sourceInputs.ListDingTalkWork(ctx)
	if err != nil {
		return err
	}
	var failures error
	for _, intent := range work {
		if err := ctx.Err(); err != nil {
			return errors.Join(failures, err)
		}
		if intent.Kind == "process" {
			current := selected[intent.ExternalID]
			if current == nil || current.ID != intent.KnowledgeID {
				continue
			}
		}
		k, proof, err := s.sourceInputs.LoadDingTalkWork(ctx, intent)
		if err == nil && intent.Kind == "process" {
			err = knowledge.enqueueDingTalkWork(ctx, s.sourceInputs, kb, k, proof, intent)
		} else if err == nil && intent.Kind == "cleanup" {
			err = knowledge.cleanupDingTalkRetiredKnowledge(ctx, s.sourceInputs, intent, kb, k)
			if err == nil {
				err = s.sourceInputs.SettleDingTalkCleanupCapacity(ctx, intent)
			}
			if err == nil {
				var refreshed []repository.DingTalkWorkIntent
				refreshed, err = s.sourceInputs.ListDingTalkWork(ctx)
				found := false
				for _, latest := range refreshed {
					if latest.ID == intent.ID {
						intent, found = latest, true
						break
					}
				}
				if err == nil && !found {
					err = repository.ErrDingTalkPublicationConflict
				}
				if err == nil {
					_, _, err = s.sourceInputs.LoadDingTalkWork(ctx, intent)
				}
				if err == nil {
					err = s.sourceInputs.FinishDingTalkWork(ctx, intent, true)
				}
			}
		}
		if err != nil {
			failures = errors.Join(failures, err)
		}
	}
	return failures
}

func (s *knowledgeService) enqueueDingTalkWork(ctx context.Context, store *repository.KnowledgeInputProvenanceStore, kb *types.KnowledgeBase, k *types.Knowledge, proof *types.KnowledgeBuildInputProof, intent repository.DingTalkWorkIntent) error {
	inspector, ok := s.taskInspector.(dingTalkQueueInspector)
	if !ok {
		return errors.New("durable DingTalk queue inspector unavailable")
	}
	reader, err := s.resolveFileServiceForPath(ctx, kb, k.FilePath).GetFile(ctx, k.FilePath)
	if err != nil {
		return err
	}
	input, readErr := io.ReadAll(reader)
	if err = errors.Join(readErr, reader.Close()); err != nil {
		return err
	}
	ctx, err = s.resumeKnowledgeInput(ctx, proof, input)
	if err != nil {
		return err
	}
	overrides, err := k.ProcessOverrides()
	if err != nil {
		return err
	}
	eff := ResolveProcessConfig(kb, overrides)
	count := eff.QuestionGenerationConfig.QuestionCount
	if count <= 0 {
		count = 3
	}
	payload := types.DocumentProcessPayload{OriginalInput: proof, TenantID: k.TenantID, KnowledgeID: k.ID,
		KnowledgeBaseID: k.KnowledgeBaseID, FilePath: k.FilePath, FileName: k.FileName, FileType: k.FileType,
		EnableMultimodel: eff.EnableMultimodel, EnableQuestionGeneration: eff.QuestionGenerationConfig.Enabled,
		QuestionCount: count, Language: types.LanguageFromContextOrDefault(ctx), Initiator: types.TaskInitiatorFromContext(ctx)}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	// Retry only after the original document and all enrichment work have
	// ended. The legacy probe alone swallows scan errors, so require the strict
	// optional backend capability as well. Missing tasks are checked in every
	// live queue state; an archived/completed task must match this exact proof.
	if intent.State == "done" && k.ParseStatus == types.ParseStatusFailed {
		queued, err := s.taskInspector.HasQueuedTasksForKnowledge(ctx, k.ID)
		if err != nil {
			return err
		}
		if queued {
			return nil
		}
		quiet, err := inspector.DingTalkTasksQuiescent(ctx, k.ID)
		if err != nil {
			return err
		}
		if !quiet {
			return nil
		}
		oldKey := repository.DingTalkQueueKey(intent)
		old, err := inspector.LookupKnowledgeQueueTask(ctx, oldKey)
		if err != nil {
			return err
		}
		if old != nil && (!sameDingTalkDocumentTask(old, oldKey, payload) ||
			(old.State != asynq.TaskStateArchived && old.State != asynq.TaskStateCompleted)) {
			return errors.New("previous DingTalk processing task is not confirmed terminal")
		}
		intent, err = store.RetryDingTalkProcessing(ctx, intent, true)
		if err != nil {
			return err
		}
	}
	key := repository.DingTalkQueueKey(intent)
	task, err := newKnowledgeQueueTask(ctx, s.knowledgeInputs, types.TypeDocumentProcess, raw,
		documentProcessTaskOptions(s.config, asynq.TaskID(key))...)
	if err != nil {
		return err
	}
	existing, err := inspector.LookupKnowledgeQueueTask(ctx, key)
	if err != nil {
		return err
	}
	if existing == nil {
		info, enqueueErr := s.task.Enqueue(task)
		if enqueueErr != nil {
			// Even an uncertain acknowledgement is resolved only by the same
			// persisted key, never by allocating another task.
			existing, err = inspector.LookupKnowledgeQueueTask(ctx, key)
			if err != nil {
				return errors.Join(enqueueErr, err)
			}
			if existing == nil {
				return enqueueErr
			}
		} else {
			if info == nil || info.ID != key {
				return errors.New("DingTalk queue did not acknowledge the requested task ID")
			}
			existing = info
		}
	}
	if !sameDingTalkDocumentTask(existing, key, payload) {
		return errors.New("DingTalk queue task identity conflict")
	}
	if intent.State == "done" {
		return nil
	}
	return store.FinishDingTalkWork(ctx, intent, true)
}

func sameDingTalkDocumentTask(info *asynq.TaskInfo, key string, expected types.DocumentProcessPayload) bool {
	if info == nil || info.ID != key || info.Type != types.TypeDocumentProcess || info.Queue != types.QueueDefault {
		return false
	}
	var actual types.DocumentProcessPayload
	if json.Unmarshal(info.Payload, &actual) != nil {
		return false
	}
	// Ignore request tracing fields; admission, actor, bytes location and every
	// processing control must agree before accepting an existing stable ID.
	actual.RequestId, expected.RequestId = "", ""
	return reflect.DeepEqual(actual, expected)
}

func (s *knowledgeService) failDingTalkProcessing(ctx context.Context, k *types.Knowledge, code string) {
	store, ok := s.knowledgeInputs.(*repository.KnowledgeInputProvenanceStore)
	if !ok {
		logger.Error(ctx, "DingTalk processing failure store unavailable")
		return
	}
	if err := store.SetDingTalkProcessingFailure(ctx, k, code); err != nil {
		logger.Errorf(ctx, "DingTalk processing failure state could not be persisted: %v", err)
	}
}
