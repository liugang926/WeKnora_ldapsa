package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// NextcloudGraphBackend defines the nextcloud graph backend value.
const NextcloudGraphBackend = "scoped_sql_v1"

type nextcloudGraphPlan struct {
	TenantID                     uint64 `gorm:"primaryKey"`
	KnowledgeID                  string `gorm:"primaryKey"`
	Attempt                      int    `gorm:"primaryKey"`
	KnowledgeBaseID              string
	Expected                     int
	Enabled, Required            bool
	ConfigDigest, ManifestDigest string
	CreatedAt                    int64
}

func (nextcloudGraphPlan) TableName() string { return "nextcloud_graph_plans" }

type nextcloudGraphTask struct {
	OperationID                  string `gorm:"primaryKey"`
	TenantID                     uint64
	KnowledgeBaseID, KnowledgeID string
	Attempt                      int
	ChunkID                      string
	BuildEpoch                   int64
	ConfigDigest, State          string
	ConfigRevision               uint64
	PayloadJSON, QueueState      string
	QueueAttempts                int
	QueueNextAt                  int64
	Persisted                    bool
	Finalized                    bool
	ErrorCode                    string
	CreatedAt                    int64
	CompletedAt                  *int64
}

func (nextcloudGraphTask) TableName() string { return "nextcloud_graph_tasks" }

type nextcloudGraphRecord struct {
	OperationID                                         string `gorm:"primaryKey"`
	TenantID                                            uint64
	KnowledgeBaseID, KnowledgeID, Backend, BuildLeaseID string
	Generation                                          int64
	BodyJSON, BodySHA256                                string
	NodeCount, EdgeCount                                int
	CommittedAt                                         int64
}

func (nextcloudGraphRecord) TableName() string { return "nextcloud_graph_records" }

// privateGraphBody preserves original receipts alongside the real nodes and
// relationships. Model JSON cannot populate these fields.
type privateGraphBody struct {
	Graph      *types.GraphData
	Receipt    *types.GraphSourceReceipt
	Nodes      []*types.GraphSourceReceipt
	Edges      []*types.GraphSourceReceipt
	Attributes [][]string
	Chunks     [][]string
}

// NextcloudGraphOperationID returns the nextcloud graph operation id result.
func NextcloudGraphOperationID(tenant uint64, knowledge string, attempt int, chunk string) string {
	return types.GraphDigest(struct {
		Backend   string
		Tenant    uint64
		Knowledge string
		Attempt   int
		Chunk     string
	}{NextcloudGraphBackend, tenant, knowledge, attempt, chunk})
}

func nextcloudGraphConfig(kb *types.KnowledgeBase) string {
	return types.GraphDigest(struct {
		Index   types.IndexingStrategy
		Extract *types.ExtractConfig
		Summary string
	}{kb.IndexingStrategy, kb.ExtractConfig, kb.SummaryModelID})
}

// PlanGraph returns the plan graph result.
func (s *NextcloudProcessingStateStore) PlanGraph(
	ctx context.Context,
	k *types.Knowledge,
	attempt int,
	chunks []*types.Chunk,
	enabled, required bool,
) error {
	if k == nil || k.Channel != types.ConnectorTypeNextcloud {
		return nil
	}
	if attempt < 1 || !NextcloudBuildLeasePresent(ctx, k.TenantID, k.KnowledgeBaseID, k.ID) {
		return ErrNextcloudContentLeaseDenied
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockNextcloudGraphKnowledgeBase(tx, k.TenantID, k.KnowledgeBaseID); err != nil {
			return err
		}
		if err := ValidateNextcloudBuildWrite(ctx, tx, k.TenantID, k.KnowledgeBaseID, k.ID); err != nil {
			return err
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id=? AND tenant_id=?", k.KnowledgeBaseID, k.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		if enabled != kb.IsGraphEnabled() || required != kb.IndexingStrategy.GraphRequired {
			return ErrKnowledgeInputChanged
		}
		configRevision, err := knowledgeConfigurationControlRevision(tx, k.KnowledgeBaseID)
		if err != nil {
			return err
		}
		lease, scope, err := NextcloudBuildInputLease(ctx)
		if err != nil {
			return err
		}
		allowed, err := NextcloudBuildControlsAllowed(ctx, tx, k.TenantID, k.KnowledgeBaseID, scope.DataSourceID, k.ID)
		if err != nil {
			return err
		}
		if !allowed {
			return ErrNextcloudContentLeaseDenied
		}
		var prior []nextcloudGraphTask
		if err := tx.Where("tenant_id=? AND knowledge_id=? AND attempt=?", k.TenantID, k.ID, attempt).
			Find(&prior).
			Error; err != nil {
			return err
		}
		ids := map[string]bool{}
		if enabled {
			for _, chunk := range chunks {
				if chunk == nil || chunk.TenantID != k.TenantID || chunk.KnowledgeID != k.ID ||
					chunk.KnowledgeBaseID != k.KnowledgeBaseID ||
					chunk.ID == "" ||
					ids[chunk.ID] {
					return ErrKnowledgeInputChanged
				}
				ids[chunk.ID] = true
			}
		}
		manifestIDs := make([]string, 0, len(ids))
		for id := range ids {
			manifestIDs = append(manifestIDs, id)
		}
		sort.Strings(manifestIDs)
		manifestDigest := types.GraphDigest(manifestIDs)
		var plan nextcloudGraphPlan
		planErr := tx.Where("tenant_id=? AND knowledge_id=? AND attempt=?", k.TenantID, k.ID, attempt).Take(&plan).Error
		if planErr == nil {
			if plan.ManifestDigest != manifestDigest || plan.ConfigDigest != nextcloudGraphConfig(&kb) ||
				plan.Expected != len(ids) ||
				plan.Enabled != enabled ||
				plan.Required != required {
				return ErrKnowledgeInputChanged
			}
			return nil
		}
		if !errors.Is(planErr, gorm.ErrRecordNotFound) {
			return planErr
		}
		plan = nextcloudGraphPlan{
			TenantID:        k.TenantID,
			KnowledgeID:     k.ID,
			Attempt:         attempt,
			KnowledgeBaseID: k.KnowledgeBaseID,
			Expected:        len(ids),
			Enabled:         enabled,
			Required:        required,
			ConfigDigest:    nextcloudGraphConfig(&kb),
			ManifestDigest:  manifestDigest,
			CreatedAt:       time.Now().UTC().Unix(),
		}
		if err := tx.Create(&plan).Error; err != nil {
			return err
		}
		if len(prior) > 0 {
			if len(prior) != len(ids) {
				return ErrKnowledgeInputChanged
			}
			for _, task := range prior {
				if !ids[task.ChunkID] || task.BuildEpoch != lease.Epoch ||
					task.ConfigDigest != nextcloudGraphConfig(&kb) {
					return ErrKnowledgeInputChanged
				}
			}
			return nil
		}
		for id := range ids {
			task := nextcloudGraphTask{
				OperationID:     NextcloudGraphOperationID(k.TenantID, k.ID, attempt, id),
				TenantID:        k.TenantID,
				KnowledgeBaseID: k.KnowledgeBaseID,
				KnowledgeID:     k.ID,
				Attempt:         attempt,
				ChunkID:         id,
				BuildEpoch:      lease.Epoch,
				ConfigDigest:    nextcloudGraphConfig(&kb),
				ConfigRevision:  configRevision,
				State:           "pending",
				QueueState:      "unprepared",
				CreatedAt:       time.Now().UTC().Unix(),
			}
			if err := tx.Create(&task).Error; err != nil {
				return err
			}
		}
		return CaptureNextcloudKnowledgeStateInTx(ctx, tx, k.ID)
	})
}

func validateNextcloudGraphTask(ctx context.Context, tx *gorm.DB, task nextcloudGraphTask) error {
	if !NextcloudBuildLeasePresent(ctx, task.TenantID, task.KnowledgeBaseID, task.KnowledgeID) {
		return ErrNextcloudContentLeaseDenied
	}
	if err := lockNextcloudGraphKnowledgeBase(tx, task.TenantID, task.KnowledgeBaseID); err != nil {
		return err
	}
	if err := ValidateNextcloudBuildWrite(ctx, tx, task.TenantID, task.KnowledgeBaseID, task.KnowledgeID); err != nil {
		return err
	}
	lease, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil || lease.Epoch != task.BuildEpoch {
		return ErrNextcloudContentLeaseDenied
	}
	allowed, err := NextcloudBuildControlsAllowed(
		ctx,
		tx,
		task.TenantID,
		task.KnowledgeBaseID,
		scope.DataSourceID,
		task.KnowledgeID,
	)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrNextcloudContentLeaseDenied
	}
	var attempt int
	if err := tx.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id=?", task.KnowledgeID).
		Select("COALESCE(MAX(attempt),0)").
		Scan(&attempt).
		Error; err != nil {
		return err
	}
	if attempt != task.Attempt {
		return ErrKnowledgeInputChanged
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id=? AND tenant_id=?", task.KnowledgeBaseID, task.TenantID).Take(&kb).Error; err != nil {
		return err
	}
	revision, err := knowledgeConfigurationControlRevision(tx, task.KnowledgeBaseID)
	if err != nil {
		return err
	}
	if revision != task.ConfigRevision {
		return ErrKnowledgeInputChanged
	}
	if !kb.IsGraphEnabled() || nextcloudGraphConfig(&kb) != task.ConfigDigest {
		return ErrKnowledgeInputChanged
	}
	return nil
}

// StartGraph starts graph.
func (s *NextcloudProcessingStateStore) StartGraph(
	ctx context.Context,
	tenant uint64,
	knowledge string,
	attempt int,
	chunk string,
) (bool, error) {
	done := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task nextcloudGraphTask
		if err := tx.Where("operation_id=?", NextcloudGraphOperationID(tenant, knowledge, attempt, chunk)).
			Take(&task).
			Error; err != nil {
			return err
		}
		if err := validateNextcloudGraphTask(ctx, tx, task); err != nil {
			return err
		}
		if task.Persisted && (task.State == "succeeded" || task.State == "succeeded_empty") {
			var record nextcloudGraphRecord
			if err := tx.Where(
				"operation_id=? AND backend=? AND generation=?",
				task.OperationID,
				NextcloudGraphBackend,
				task.BuildEpoch,
			).
				Take(&record).
				Error; err != nil {
				return err
			}
			if recoverySHA([]byte(record.BodyJSON)) != record.BodySHA256 {
				return types.ErrGraphSourceReceiptUnknown
			}
			done = true
			return nil
		}
		if task.State == "cancelled" || task.State == "skipped" {
			return ErrNextcloudContentLeaseDenied
		}
		if err := tx.Model(&task).
			Updates(map[string]any{"state": "running", "error_code": "", "completed_at": nil}).
			Error; err != nil {
			return err
		}
		return CaptureNextcloudKnowledgeStateInTx(ctx, tx, knowledge)
	})
	return done, err
}

// FailGraph returns the fail graph result.
func (s *NextcloudProcessingStateStore) FailGraph(
	ctx context.Context,
	tenant uint64,
	knowledge string,
	attempt int,
	chunk, code string,
	terminal bool,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task nextcloudGraphTask
		if err := tx.Where("operation_id=?", NextcloudGraphOperationID(tenant, knowledge, attempt, chunk)).
			Take(&task).
			Error; err != nil {
			return err
		}
		if err := validateNextcloudGraphTask(ctx, tx, task); err != nil {
			return err
		}
		if task.Persisted {
			return nil
		}
		// Never copy model errors, endpoint URLs or stack traces into state.
		allowed := map[string]bool{
			"graph_enqueue_failed":     true,
			"graph_input_unverified":   true,
			"graph_extraction_failed":  true,
			"graph_persistence_failed": true,
		}
		if !allowed[code] {
			code = "graph_extraction_failed"
		}
		state := "pending"
		var completed *int64
		if terminal {
			state = "failed"
			now := time.Now().UTC().Unix()
			completed = &now
		}
		if err := tx.Model(&task).
			Updates(map[string]any{"state": state, "error_code": code, "completed_at": completed}).
			Error; err != nil {
			return err
		}
		return CaptureNextcloudKnowledgeStateInTx(ctx, tx, knowledge)
	})
}

func readNextcloudGraphState(
	tx *gorm.DB,
	k *types.Knowledge,
	attempt int,
	enabled, required bool,
) (NextcloudGraphState, error) {
	out := NextcloudGraphState{Enabled: enabled, Required: required, State: "not_enabled"}
	if !enabled {
		return out, nil
	}
	out.State = "pending"
	var tasks []nextcloudGraphTask
	if err := tx.Where("tenant_id=? AND knowledge_id=? AND attempt=?", k.TenantID, k.ID, attempt).
		Find(&tasks).
		Error; err != nil {
		return out, err
	}
	var plan nextcloudGraphPlan
	planErr := tx.Where("tenant_id=? AND knowledge_id=? AND attempt=?", k.TenantID, k.ID, attempt).Take(&plan).Error
	if planErr == nil {
		var currentKB types.KnowledgeBase
		if err := tx.Where("id=? AND tenant_id=?", k.KnowledgeBaseID, k.TenantID).Take(&currentKB).Error; err != nil {
			return out, err
		}
		if plan.ConfigDigest != nextcloudGraphConfig(&currentKB) || plan.Enabled != enabled ||
			plan.Required != required {
			out.Expected = plan.Expected
			out.Pending = plan.Expected
			out.State = "unverified"
			out.ErrorCode = "graph_configuration_changed"
			return out, nil
		}
		out.Expected = plan.Expected
		if plan.Expected == 0 {
			out.State = "skipped"
			out.ErrorCode = "graph_no_applicable_input"
			return out, nil
		}
		if len(tasks) != plan.Expected {
			out.State = "unverified"
			out.Pending = plan.Expected
			out.ErrorCode = "graph_task_manifest_unverified"
			return out, nil
		}
	} else if !errors.Is(planErr, gorm.ErrRecordNotFound) {
		return out, planErr
	}
	if errors.Is(planErr, gorm.ErrRecordNotFound) {
		out.State = "blocked"
		out.ErrorCode = "graph_task_manifest_pending"
		return out, nil
	}
	if len(tasks) == 0 {
		out.State = "blocked"
		out.ErrorCode = "graph_task_manifest_pending"
		return out, nil
	}
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ChunkID)
	}
	sort.Strings(ids)
	if types.GraphDigest(ids) != plan.ManifestDigest {
		out.State = "unverified"
		out.Pending = plan.Expected
		out.ErrorCode = "graph_task_manifest_unverified"
		return out, nil
	}
	empty := true
	latest := int64(0)
	for _, task := range tasks {
		switch task.State {
		case "succeeded", "succeeded_empty":
			if !task.Persisted {
				out.Pending++
				out.State = "unverified"
				out.ErrorCode = "graph_persistence_unverified"
				continue
			}
			var record nextcloudGraphRecord
			recordErr := tx.Where(
				"operation_id=? AND backend=? AND generation=?",
				task.OperationID,
				NextcloudGraphBackend,
				task.BuildEpoch,
			).
				Take(&record).
				Error
			validRecord := recordErr == nil
			if validRecord {
				_, recordErr = decodeScopedGraphRecord(record)
				validRecord = recordErr == nil
				if validRecord {
					graph, _ := decodeScopedGraphRecord(record)
					store := NewKnowledgeInputProvenanceStore(tx)
					_, recordErr = store.captureKnowledgeBuildSnapshotInTx(
						tx,
						types.KnowledgeBuildInputCaptureRequest{
							Projection:      types.KnowledgeBuildProjectionGraph,
							TenantID:        k.TenantID,
							KnowledgeBaseID: k.KnowledgeBaseID,
							KnowledgeID:     k.ID,
							ChunkIDs:        graph.SourceReceipt.OriginalInput.ChunkIDs,
							Expected:        graph.SourceReceipt.OriginalInput,
						},
						true,
					)
					validRecord = recordErr == nil
				}
			}
			if !validRecord {
				out.Pending++
				out.State = "unverified"
				out.ErrorCode = "graph_persistence_unverified"
				continue
			}
			out.Succeeded++
			if task.State != "succeeded_empty" {
				empty = false
			}
		case "failed":
			out.Failed++
			out.ErrorCode = task.ErrorCode
		case "skipped":
			out.Skipped++
		case "cancelled":
			out.Cancelled++
		default:
			out.Pending++
			if task.State == "running" && out.State != "unverified" {
				out.State = "running"
			}
		}
		if task.CompletedAt != nil && *task.CompletedAt > latest {
			latest = *task.CompletedAt
		}
	}
	if out.Pending == 0 {
		switch {
		case out.Succeeded == out.Expected:
			out.State = "succeeded"
			if empty {
				out.State = "succeeded_empty"
			}
			out.Persisted = true
		case out.Succeeded > 0 && out.Failed > 0:
			out.State = "partial_failed"
		case out.Failed > 0:
			out.State = "failed"
		case out.Cancelled > 0:
			out.State = "cancelled"
		default:
			out.State = "skipped"
		}
		if latest > 0 {
			out.CompletedAt = &latest
		}
	}
	return out, nil
}

// ScopedSQLGraphRepository is a real graph backend for Nextcloud documents.
// Provider material and the local ACK share one DB transaction with the exact
// persistent generation fence. This removes the external check/commit gap.
// Ordinary graphs continue to use their configured Neo4j adapter.
type ScopedSQLGraphRepository struct {
	db       *gorm.DB
	fallback interfaces.RetrieveGraphRepository
}

// NewScopedSQLGraphRepository constructs scoped sqlgraph repository.
func NewScopedSQLGraphRepository(db *gorm.DB, fallback interfaces.RetrieveGraphRepository) *ScopedSQLGraphRepository {
	return &ScopedSQLGraphRepository{db: db, fallback: fallback}
}

// AddGraph returns the add graph result.
func (s *ScopedSQLGraphRepository) AddGraph(
	ctx context.Context,
	namespace types.NameSpace,
	graphs []*types.GraphData,
) error {
	operation, source := types.GraphWriteOperationFromContext(ctx)
	if !source {
		for _, graph := range graphs {
			if graph != nil && graph.SourceReceipt != nil && graph.SourceReceipt.SourceLineage != nil {
				for _, identity := range graph.SourceReceipt.SourceLineage.Sources {
					if identity.Provider == types.ConnectorTypeNextcloud {
						return ErrNextcloudContentLeaseDenied
					}
				}
			}
		}
		if s.fallback == nil {
			return types.ErrGraphSourceReceiptUnknown
		}
		return s.fallback.AddGraph(ctx, namespace, graphs)
	}
	if len(graphs) != 1 {
		return types.ErrGraphSourceReceiptUnknown
	}
	graph := graphs[0]
	if _, err := types.ValidateGraphMaterial(graph); err != nil {
		return err
	}
	if err := types.CheckGraphDataReceipt(namespace, graph); err != nil {
		return err
	}
	if err := types.CheckGraphWriteAuthorization(ctx, graph.SourceReceipt); err != nil {
		return err
	}
	body := privateGraphBody{Graph: graph, Receipt: graph.SourceReceipt}
	for _, node := range graph.Node {
		body.Nodes = append(body.Nodes, node.SourceReceipt)
		body.Attributes = append(body.Attributes, node.Attributes)
		body.Chunks = append(body.Chunks, node.Chunks)
	}
	for _, edge := range graph.Relation {
		body.Edges = append(body.Edges, edge.SourceReceipt)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task nextcloudGraphTask
		if err := tx.Where("operation_id=?", operation.ID).Take(&task).Error; err != nil {
			return err
		}
		if operation.ID != NextcloudGraphOperationID(
			task.TenantID,
			task.KnowledgeID,
			operation.Attempt,
			operation.ChunkID,
		) ||
			task.Attempt != operation.Attempt ||
			task.ChunkID != operation.ChunkID ||
			namespace.KnowledgeBase != task.KnowledgeBaseID ||
			namespace.Knowledge != task.KnowledgeID ||
			graph.SourceReceipt.TenantID != task.TenantID ||
			graph.SourceReceipt.BuildGeneration != uint64(task.BuildEpoch) {
			return types.ErrGraphSourceReceiptUnknown
		}
		// This takes the same KB/exact locks as retirement and physical GC. A
		// provider commit after retirement cannot succeed, even on another worker.
		if err := validateNextcloudGraphTask(ctx, tx, task); err != nil {
			return err
		}
		if _, err := validateKnowledgeInputWrite(
			ctx,
			tx,
			task.TenantID,
			task.KnowledgeBaseID,
			task.KnowledgeID,
		); err != nil {
			return err
		}
		var old nextcloudGraphRecord
		err := tx.Where("operation_id=?", operation.ID).Take(&old).Error
		if err == nil {
			if old.BodySHA256 != recoverySHA(raw) || old.Generation != task.BuildEpoch ||
				old.Backend != NextcloudGraphBackend {
				return types.ErrGraphSourceReceiptUnknown
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		now := time.Now().UTC().Unix()
		lease, _, err := NextcloudBuildInputLease(ctx)
		if err != nil {
			return err
		}
		record := nextcloudGraphRecord{
			OperationID:     operation.ID,
			TenantID:        task.TenantID,
			KnowledgeBaseID: task.KnowledgeBaseID,
			KnowledgeID:     task.KnowledgeID,
			Backend:         NextcloudGraphBackend,
			BuildLeaseID:    lease.ID,
			Generation:      task.BuildEpoch,
			BodyJSON:        string(raw),
			BodySHA256:      recoverySHA(raw),
			NodeCount:       len(graph.Node),
			EdgeCount:       len(graph.Relation),
			CommittedAt:     now,
		}
		if err := tx.Create(&record).Error; err != nil {
			return err
		}
		if err := RecordNextcloudDerivedArtifactInTx(
			ctx,
			tx,
			task.KnowledgeID,
			task.KnowledgeBaseID,
			"scoped_sql_graph",
			operation.ID,
			0,
		); err != nil {
			return err
		}
		state := "succeeded"
		if len(graph.Node) == 0 && len(graph.Relation) == 0 {
			state = "succeeded_empty"
		}
		if err := tx.Model(&task).
			Updates(map[string]any{"state": state, "persisted": true, "error_code": "", "completed_at": now}).
			Error; err != nil {
			return err
		}
		var v nextcloudSourceVersion
		if err := withNextcloudCandidateKnowledgeID(tx, task.KnowledgeID).Take(&v).Error; err != nil {
			return err
		}
		return captureNextcloudStateInTx(ctx, tx, v.TenantID, v.KnowledgeBaseID, v.DataSourceID, v.ExternalID, true)
	})
}

// DelGraph returns the del graph result.
func (s *ScopedSQLGraphRepository) DelGraph(ctx context.Context, namespaces []types.NameSpace) error {
	// Source graph deletion belongs to the existing exact GC claim protocol.
	for _, namespace := range namespaces {
		var count int64
		if s.db.Migrator().HasTable("nextcloud_graph_records") {
			query := s.db.WithContext(ctx).
				Model(&nextcloudGraphRecord{}).
				Where("knowledge_base_id=?", namespace.KnowledgeBase)
			if namespace.Knowledge != "" {
				query = query.Where("knowledge_id=?", namespace.Knowledge)
			}
			if err := query.Count(&count).Error; err != nil {
				return err
			}
			if count > 0 {
				return ErrNextcloudContentGCUncovered
			}
		}
	}
	if s.fallback == nil {
		return nil
	}
	return s.fallback.DelGraph(ctx, namespaces)
}

// SearchNode returns the search node result.
func (s *ScopedSQLGraphRepository) SearchNode(
	ctx context.Context,
	namespace types.NameSpace,
	names []string,
) (*types.GraphData, error) {
	var rows []nextcloudGraphRecord
	if s.db.Migrator().HasTable("nextcloud_graph_records") {
		q := s.db.WithContext(ctx).Where("knowledge_base_id=?", namespace.KnowledgeBase)
		if namespace.Knowledge != "" {
			q = q.Where("knowledge_id=?", namespace.Knowledge)
		}
		if err := q.Order("operation_id").Limit(4097).Find(&rows).Error; err != nil {
			return nil, err
		}
	}
	if len(rows) == 0 {
		if s.fallback == nil {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		return s.fallback.SearchNode(ctx, namespace, names)
	}
	if len(rows) > 4096 {
		return nil, types.ErrSourceLineageLimit
	}
	lineage, _ := types.NewCompleteSourceLineage()
	out := &types.GraphData{SourceLineage: lineage}
	// Typed graph readers validate original input receipts and acquire caller
	// read leases before exposing this internal retrieval projection.
	for _, row := range rows {
		if row.Backend != NextcloudGraphBackend || recoverySHA([]byte(row.BodyJSON)) != row.BodySHA256 {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		var body privateGraphBody
		if err := json.Unmarshal([]byte(row.BodyJSON), &body); err != nil {
			return nil, err
		}
		if body.Graph == nil || body.Receipt == nil || len(body.Nodes) != len(body.Graph.Node) ||
			len(body.Edges) != len(body.Graph.Relation) ||
			len(body.Attributes) != len(body.Graph.Node) ||
			len(body.Chunks) != len(body.Graph.Node) {
			return nil, types.ErrGraphSourceReceiptUnknown
		}
		graph := body.Graph
		graph.SourceReceipt = body.Receipt
		graph.SourceLineage = body.Receipt.SourceLineage
		graph.WireDigest = body.Receipt.WireDigest
		for i, node := range graph.Node {
			node.SourceReceipt = body.Nodes[i]
			node.Attributes = body.Attributes[i]
			node.Chunks = body.Chunks[i]
		}
		for i, edge := range graph.Relation {
			edge.SourceReceipt = body.Edges[i]
		}
		ns := types.NameSpace{KnowledgeBase: row.KnowledgeBaseID, Knowledge: row.KnowledgeID}
		if _, err := types.ValidateGraphMaterial(graph); err != nil {
			return nil, err
		}
		if err := types.CheckGraphDataReceipt(ns, graph); err != nil {
			return nil, err
		}
		var err error
		out.SourceLineage, err = types.UnionSourceLineage(out.SourceLineage, graph.SourceLineage)
		if err != nil {
			return nil, err
		}
		out.InputReceipts = append(out.InputReceipts, graph.SourceReceipt)
		matches := map[string]bool{}
		for _, node := range graph.Node {
			for _, name := range names {
				if stringsContains(node.Name, name) {
					matches[node.Name] = true
				}
			}
		}
		for _, edge := range graph.Relation {
			if matches[edge.Node1] || matches[edge.Node2] {
				out.Relation = append(out.Relation, edge)
				matches[edge.Node1] = true
				matches[edge.Node2] = true
			}
		}
		for _, node := range graph.Node {
			if matches[node.Name] {
				out.Node = append(out.Node, node)
			}
		}
	}
	return out, nil
}

func stringsContains(a, b string) bool {
	for i := 0; i+len(b) <= len(a); i++ {
		if a[i:i+len(b)] == b {
			return true
		}
	}
	return false
}

type nextcloudGraphReadyStackKey struct{}

func nextcloudRequiredGraphReady(tx *gorm.DB, k *types.Knowledge) (bool, error) {
	if k == nil {
		return false, nil
	}
	ctx := tx.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}
	previous, _ := ctx.Value(nextcloudGraphReadyStackKey{}).(map[string]bool)
	key := types.GraphDigest(struct {
		Tenant        uint64
		KB, Knowledge string
	}{k.TenantID, k.KnowledgeBaseID, k.ID})
	if previous[key] || len(previous) >= 64 {
		return false, nil
	}
	stack := map[string]bool{}
	for scope, seen := range previous {
		stack[scope] = seen
	}
	stack[key] = true
	tx = tx.WithContext(context.WithValue(ctx, nextcloudGraphReadyStackKey{}, stack))

	if !tx.Migrator().HasTable("nextcloud_graph_tasks") {
		return true, nil
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id=? AND tenant_id=?", k.KnowledgeBaseID, k.TenantID).Take(&kb).Error; err != nil {
		return false, err
	}
	if !kb.IndexingStrategy.GraphRequired {
		return true, nil
	}
	var attempt int
	if err := tx.Model(&types.KnowledgeProcessingSpan{}).
		Where("knowledge_id=?", k.ID).
		Select("COALESCE(MAX(attempt),0)").
		Scan(&attempt).
		Error; err != nil {
		return false, err
	}
	state, err := readNextcloudGraphState(tx, k, attempt, kb.IsGraphEnabled(), true)
	return state.Persisted && (state.State == "succeeded" || state.State == "succeeded_empty"), err
}

var (
	_ interfaces.RetrieveGraphRepository = (*ScopedSQLGraphRepository)(nil)
	_ types.NextcloudProcessingLedger    = (*NextcloudProcessingStateStore)(nil)
)

func decodeScopedGraphRecord(row nextcloudGraphRecord) (*types.GraphData, error) {
	if row.Backend != NextcloudGraphBackend || recoverySHA([]byte(row.BodyJSON)) != row.BodySHA256 {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	var body privateGraphBody
	if err := json.Unmarshal([]byte(row.BodyJSON), &body); err != nil {
		return nil, err
	}
	if body.Graph == nil || body.Receipt == nil || len(body.Nodes) != len(body.Graph.Node) ||
		len(body.Edges) != len(body.Graph.Relation) ||
		len(body.Attributes) != len(body.Graph.Node) ||
		len(body.Chunks) != len(body.Graph.Node) ||
		body.Receipt.TenantID != row.TenantID ||
		body.Receipt.BuildGeneration != uint64(row.Generation) {
		return nil, types.ErrGraphSourceReceiptUnknown
	}
	graph := body.Graph
	graph.SourceReceipt = body.Receipt
	graph.SourceLineage = body.Receipt.SourceLineage
	graph.WireDigest = body.Receipt.WireDigest
	for i, node := range graph.Node {
		node.SourceReceipt = body.Nodes[i]
		node.Attributes = body.Attributes[i]
		node.Chunks = body.Chunks[i]
	}
	for i, edge := range graph.Relation {
		edge.SourceReceipt = body.Edges[i]
	}
	if _, err := types.ValidateGraphMaterial(graph); err != nil {
		return nil, err
	}
	if err := types.CheckGraphDataReceipt(
		types.NameSpace{KnowledgeBase: row.KnowledgeBaseID, Knowledge: row.KnowledgeID},
		graph,
	); err != nil {
		return nil, err
	}
	return graph, nil
}

// RecordGraphPayload records graph payload.
func (s *NextcloudProcessingStateStore) RecordGraphPayload(
	ctx context.Context,
	tenant uint64,
	knowledge string,
	attempt int,
	chunk string,
	payload []byte,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task nextcloudGraphTask
		if err := tx.Where("operation_id=?", NextcloudGraphOperationID(tenant, knowledge, attempt, chunk)).
			Take(&task).
			Error; err != nil {
			return err
		}
		if err := validateNextcloudGraphTask(ctx, tx, task); err != nil {
			return err
		}
		if task.PayloadJSON != "" {
			if task.PayloadJSON != string(payload) {
				return ErrKnowledgeInputChanged
			}
			return nil
		}
		var wire types.ExtractChunkPayload
		if json.Unmarshal(payload, &wire) != nil || wire.TenantID != tenant || wire.KnowledgeID != knowledge ||
			wire.Attempt != attempt ||
			wire.ChunkID != chunk ||
			wire.OriginalInput == nil ||
			wire.OriginalInput.BuildGeneration != uint64(task.BuildEpoch) {
			return ErrKnowledgeInputChanged
		}
		return tx.Model(&task).Updates(map[string]any{"payload_json": string(payload), "queue_state": "pending"}).Error
	})
}

// MarkGraphQueued returns the mark graph queued result.
func (s *NextcloudProcessingStateStore) MarkGraphQueued(
	ctx context.Context,
	tenant uint64,
	knowledge string,
	attempt int,
	chunk string,
	accepted bool,
) error {
	state := "pending"
	if accepted {
		state = "queued"
	}
	return s.db.WithContext(ctx).
		Model(&nextcloudGraphTask{}).
		Where("operation_id=? AND payload_json<>''", NextcloudGraphOperationID(tenant, knowledge, attempt, chunk)).
		Update("queue_state", state).
		Error
}

// NextcloudGraphQueueIntent represents nextcloud graph queue intent repository data.
type NextcloudGraphQueueIntent struct {
	OperationID string
	Payload     []byte
}

// PendingGraphQueueIntents returns pending graph queue intents.
func (s *NextcloudProcessingStateStore) PendingGraphQueueIntents(
	ctx context.Context,
	now time.Time,
) ([]NextcloudGraphQueueIntent, error) {
	var tasks []nextcloudGraphTask
	// Reject permanently stale queue heads in the bounded selection itself,
	// so retired or superseded tasks cannot occupy every compensation page.
	query := s.db.WithContext(ctx).
		Where(("queue_state='pending' AND payload_json<>'' AND queue_next_at<=? AND state " +
			"IN ('pending','running')"), now.Unix()).
		Where(("EXISTS(SELECT 1 FROM nextcloud_content_fences f JOIN nextcloud_content_fen" +
			"ces kb ON kb.tenant_id=f.tenant_id AND kb.knowledge_base_id=f.knowledge_ba" +
			"se_id AND kb.knowledge_id='' JOIN nextcloud_source_versions v ON v.tenant_" +
			"id=f.tenant_id AND v.knowledge_base_id=f.knowledge_base_id AND v.datasourc" +
			"e_id=f.datasource_id AND v.external_id=f.external_id JOIN data_sources d O" +
			"N d.id=v.datasource_id AND d.tenant_id=v.tenant_id AND d.knowledge_base_id" +
			"=v.knowledge_base_id WHERE f.tenant_id=nextcloud_graph_tasks.tenant_id AND" +
			" f.knowledge_base_id=nextcloud_graph_tasks.knowledge_base_id AND f.knowled" +
			"ge_id=nextcloud_graph_tasks.knowledge_id AND f.state='open' AND kb.state='" +
			"open' AND f.epoch=nextcloud_graph_tasks.build_epoch AND v.candidate_knowle" +
			"dge_id=f.knowledge_id AND v.state IN ('staging','published') AND d.status=" +
			"'active' AND d.deleted_at IS NULL)")).
		Where(("attempt=(SELECT MAX(attempt) FROM knowledge_processing_spans WHERE knowled" +
			"ge_id=nextcloud_graph_tasks.knowledge_id) AND config_revision=(SELECT revi" +
			"sion FROM knowledge_configuration_control_versions WHERE knowledge_base_id" +
			"=nextcloud_graph_tasks.knowledge_base_id)"))
	if err := query.Order("created_at,operation_id").Limit(100).Find(&tasks).Error; err != nil {
		return nil, err
	}
	intents := []NextcloudGraphQueueIntent{}
	for _, task := range tasks {
		err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var kb types.KnowledgeBase
			if err := tx.Where("id=? AND tenant_id=?", task.KnowledgeBaseID, task.TenantID).
				Take(&kb).
				Error; err != nil {
				return err
			}
			candidate, ready, err := (&knowledgeRepository{db: tx}).publicationCandidateMode(
				tx,
				task.KnowledgeID,
				false,
				true,
			)
			if err != nil {
				return err
			}
			allowed, err := NextcloudBuildControlsAllowed(
				ctx,
				tx,
				task.TenantID,
				task.KnowledgeBaseID,
				candidate.version.DataSourceID,
				task.KnowledgeID,
			)
			if err != nil {
				return err
			}
			if !allowed {
				return ErrNextcloudContentLeaseDenied
			}
			if !ready || candidate.version.CandidateKnowledgeID != task.KnowledgeID ||
				nextcloudGraphConfig(&kb) != task.ConfigDigest {
				return ErrNextcloudContentLeaseDenied
			}
			var fence nextcloudContentFence
			if err := tx.Where(("tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND state='open' AN" +
				"D epoch=?"), task.TenantID, task.KnowledgeBaseID, task.KnowledgeID, task.BuildEpoch).
				Take(&fence).
				Error; err != nil {
				return err
			}
			return nil
		})
		if err == nil {
			intents = append(
				intents,
				NextcloudGraphQueueIntent{OperationID: task.OperationID, Payload: []byte(task.PayloadJSON)},
			)
		}
	}
	return intents, nil
}

// FinishGraphEnqueue finishes graph enqueue.
func (s *NextcloudProcessingStateStore) FinishGraphEnqueue(
	ctx context.Context,
	operation string,
	accepted bool,
	now time.Time,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var task nextcloudGraphTask
		if err := tx.Where("operation_id=? AND queue_state='pending'", operation).Take(&task).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if accepted {
			return tx.Model(&task).Update("queue_state", "queued").Error
		}
		attempts := task.QueueAttempts + 1
		delay := int64(5)
		for i := 1; i < attempts && delay < 300; i++ {
			delay *= 2
		}
		if delay > 300 {
			delay = 300
		}
		return tx.Model(&task).
			Updates(map[string]any{
				"queue_attempts": attempts,
				"queue_next_at":  now.Unix() + delay,
				"error_code":     "graph_enqueue_failed",
			}).
			Error
	})
}

func lockNextcloudGraphKnowledgeBase(tx *gorm.DB, tenant uint64, kb string) error {
	result := tx.Exec(
		"UPDATE knowledge_bases SET ever_had_nextcloud_source=ever_had_nextcloud_source WHERE tenant_id=? AND id=?",
		tenant,
		kb,
	)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrNextcloudContentLeaseDenied
	}
	return nil
}

// UnpreparedGraphChunks recovers a crash after the frozen plan/finalizing
// handoff but before payload persistence. It never reconstructs inputs from a
// model response: the retrying parser must capture the actual current proof.
func (s *NextcloudProcessingStateStore) UnpreparedGraphChunks(
	ctx context.Context,
	tenant uint64,
	knowledge string,
	attempt int,
) ([]string, error) {
	result := []string{}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var tasks []nextcloudGraphTask
		if err := tx.Where(("tenant_id=? AND knowledge_id=? AND attempt=? AND state='pending' AND queue" +
			"_state='unprepared' AND payload_json='' AND finalized=FALSE"), tenant, knowledge, attempt).
			Order("chunk_id").
			Find(&tasks).
			Error; err != nil {
			return err
		}
		for _, task := range tasks {
			if err := validateNextcloudGraphTask(ctx, tx, task); err != nil {
				return err
			}
			result = append(result, task.ChunkID)
		}
		return nil
	})
	return result, err
}
