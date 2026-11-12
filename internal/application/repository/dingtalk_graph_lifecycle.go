package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DingTalkGraphTaskType identifies durable graph tasks.
const DingTalkGraphTaskType = "dingtalk:graph"

// DingTalkGraphRecord persists graph output and its source receipts.
type DingTalkGraphRecord struct {
	SkipReason  string                    `json:"skip_reason,omitempty"`
	Outcome     string                    `json:"outcome,omitempty"`
	FailureCode string                    `json:"failure_code,omitempty"`
	Payload     types.ExtractChunkPayload `json:"payload"`
	Graph       *types.GraphData          `json:"-"`
}

// GraphData intentionally hides receipts from public JSON. This private SQL
// envelope persists them explicitly; replay must never reconstruct provenance
// from a bare graph body.
type dingTalkGraphOutput struct {
	Body       *types.GraphData            `json:"body"`
	Receipt    *types.GraphSourceReceipt   `json:"receipt"`
	WireDigest string                      `json:"wire_digest"`
	Lineage    *types.SourceLineage        `json:"lineage"`
	Inputs     []*types.GraphSourceReceipt `json:"inputs"`
	Nodes      []*types.GraphSourceReceipt `json:"nodes"`
	Relations  []*types.GraphSourceReceipt `json:"relations"`
}

// MarshalJSON includes private graph receipts in persisted output.
func (r DingTalkGraphRecord) MarshalJSON() ([]byte, error) {
	type plain DingTalkGraphRecord
	var output *dingTalkGraphOutput
	if r.Graph != nil {
		g := r.Graph
		output = &dingTalkGraphOutput{
			Body:       g,
			Receipt:    g.SourceReceipt,
			WireDigest: g.WireDigest,
			Lineage:    g.SourceLineage,
			Inputs:     g.InputReceipts,
		}
		for _, n := range g.Node {
			output.Nodes = append(output.Nodes, n.SourceReceipt)
		}
		for _, e := range g.Relation {
			output.Relations = append(output.Relations, e.SourceReceipt)
		}
	}
	return json.Marshal(struct {
		plain
		Output *dingTalkGraphOutput `json:"graph,omitempty"`
	}{plain(r), output})
}

// UnmarshalJSON restores and validates persisted graph receipts.
func (r *DingTalkGraphRecord) UnmarshalJSON(raw []byte) error {
	type plain DingTalkGraphRecord
	var wire struct {
		plain
		Output *dingTalkGraphOutput `json:"graph,omitempty"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	*r = DingTalkGraphRecord(wire.plain)
	if o := wire.Output; o != nil {
		if o.Body == nil || len(o.Body.Node) != len(o.Nodes) || len(o.Body.Relation) != len(o.Relations) {
			return types.ErrGraphSourceReceiptUnknown
		}
		r.Graph = o.Body
		r.Graph.SourceReceipt = o.Receipt
		r.Graph.WireDigest = o.WireDigest
		r.Graph.SourceLineage = o.Lineage
		r.Graph.InputReceipts = o.Inputs
		for i, n := range r.Graph.Node {
			if n == nil {
				return types.ErrGraphSourceReceiptUnknown
			}
			n.SourceReceipt = o.Nodes[i]
		}
		for i, e := range r.Graph.Relation {
			if e == nil {
				return types.ErrGraphSourceReceiptUnknown
			}
			e.SourceReceipt = o.Relations[i]
		}
	}
	return nil
}

type dingTalkGraphContextKey struct{}

// WithDingTalkGraphTask attaches the authenticated graph payload.
func WithDingTalkGraphTask(ctx context.Context, p types.ExtractChunkPayload) context.Context {
	return context.WithValue(ctx, dingTalkGraphContextKey{}, p)
}

// DingTalkGraphTaskID derives a stable graph task identity.
func DingTalkGraphTaskID(p types.ExtractChunkPayload) string {
	if p.OriginalInput == nil {
		return ""
	}
	return types.GraphDigest([]any{
		p.TenantID,
		p.OriginalInput.KnowledgeBaseID,
		p.KnowledgeID,
		p.OriginalInput.BuildGeneration,
		p.Attempt,
		"graph",
		p.ChunkID,
	})
}

// All lifecycle mutations lock the document before its operation. This also
// serializes finalizing handoff, reparse and duplicate settlement.
func lockDingTalkGraph(ctx context.Context, tx *gorm.DB, p types.ExtractChunkPayload) error {
	proof := KnowledgeBuildInputProofFromContext(ctx)
	if proof == nil || p.OriginalInput == nil || p.Attempt <= 0 || p.ChunkID == "" ||
		proof.AdmissionID != p.OriginalInput.AdmissionID || proof.BuildGeneration != p.OriginalInput.BuildGeneration ||
		p.TenantID != proof.TenantID || p.KnowledgeID != proof.KnowledgeID {
		return ErrKnowledgeInputChanged
	}
	var k types.Knowledge
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"id=? AND tenant_id=? AND knowledge_base_id=?",
		p.KnowledgeID,
		p.TenantID,
		proof.KnowledgeBaseID).Take(&k).Error; err != nil {
		return err
	}
	if k.Channel != types.ConnectorTypeDingTalk ||
		(k.ParseStatus != types.ParseStatusProcessing &&
			k.ParseStatus != types.ParseStatusFinalizing &&
			k.ParseStatus != types.ParseStatusCompleted) {
		return ErrKnowledgeInputChanged
	}
	var latest int
	if err := tx.Model(&types.KnowledgeProcessingSpan{}).Where(
		"knowledge_id=? AND kind=?",
		p.KnowledgeID,
		types.SpanKindRoot).Select("COALESCE(MAX(attempt),0)").Scan(
		&latest).Error; err != nil {
		return err
	}
	if latest != p.Attempt {
		return ErrKnowledgeInputChanged
	}
	_, err := validateKnowledgeInputWrite(ctx, tx, p.TenantID, proof.KnowledgeBaseID, p.KnowledgeID)
	return err
}

func loadDingTalkGraph(tx *gorm.DB, p types.ExtractChunkPayload) (*types.TaskPendingOp, *DingTalkGraphRecord, error) {
	var op types.TaskPendingOp
	if p.GraphTaskID == "" || p.GraphTaskID != DingTalkGraphTaskID(p) {
		return nil, nil, ErrKnowledgeInputChanged
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id=? AND task_type=? AND scope=? AND scope_id=? AND dedup_key=?",
		p.TenantID,
		DingTalkGraphTaskType,
		"knowledge",
		p.KnowledgeID,
		p.GraphTaskID).Take(&op).Error; err != nil {
		return nil, nil, err
	}
	var saved DingTalkGraphRecord
	// Delivery mode is not part of the immutable task input identity.
	p.GraphSettlementOnly = false
	if json.Unmarshal(op.Payload, &saved) != nil || types.GraphDigest(saved.Payload) != types.GraphDigest(p) {
		return nil, nil, ErrKnowledgeInputChanged
	}
	return &op, &saved, nil
}

// Payloads, graph slots and the optional Wiki slot are published atomically.
// This only admits a NEW processing handoff, never historical finalizing work.
func (r *knowledgeRepository) SeedDingTalkGraphFinalizing(
	ctx context.Context,
	id string,
	count int,
	tasks []types.ExtractChunkPayload,
	wiki *types.TaskPendingOp) (bool,
	error,
) {
	if len(tasks) == 0 || count < len(tasks) {
		return false, errors.New("invalid graph plan")
	}
	if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
		return false, err
	}
	changed := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockDingTalkGraph(ctx, tx, tasks[0]); err != nil {
			return err
		}
		result := tx.Model(&types.Knowledge{}).Where(
			"id=? AND parse_status=?",
			id,
			types.ParseStatusProcessing).Updates(map[string]any{
			"parse_status":           types.ParseStatusFinalizing,
			"pending_subtasks_count": count,
			"updated_at":             time.Now(),
			"error_message":          "",
		})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		seen := map[string]bool{}
		for _, p := range tasks {
			if p.KnowledgeID != id || p.GraphTaskID != DingTalkGraphTaskID(p) || seen[p.GraphTaskID] {
				return ErrKnowledgeInputChanged
			}
			seen[p.GraphTaskID] = true
			if err := lockDingTalkGraph(ctx, tx, p); err != nil {
				return err
			}
			raw, err := json.Marshal(DingTalkGraphRecord{Payload: p})
			if err != nil {
				return err
			}
			op := types.TaskPendingOp{
				TenantID:   p.TenantID,
				TaskType:   DingTalkGraphTaskType,
				Scope:      "knowledge",
				ScopeID:    id,
				Op:         "planned",
				DedupKey:   p.GraphTaskID,
				Payload:    raw,
				EnqueuedAt: time.Now(),
			}
			if err := tx.Create(&op).Error; err != nil {
				return err
			}
		}
		if wiki != nil {
			if wiki.TenantID != tasks[0].TenantID ||
				wiki.ScopeID != tasks[0].OriginalInput.KnowledgeBaseID ||
				wiki.TaskType != "wiki:ingest" {
				return ErrKnowledgeInputChanged
			}
			if err := tx.Create(wiki).Error; err != nil {
				return err
			}
		}
		changed = true
		return nil
	})
	return changed, err
}

func (r *knowledgeRepository) DingTalkGraphState(
	ctx context.Context,
	p types.ExtractChunkPayload) (string,
	*types.GraphData,
	string,
	error,
) {
	var state, skipReason string
	var graph *types.GraphData
	if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
		return "", nil, "", err
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockDingTalkGraph(ctx, tx, p); err != nil {
			return err
		}
		op, saved, err := loadDingTalkGraph(tx, p)
		if err != nil {
			return err
		}
		state, graph, skipReason = op.Op, saved.Graph, saved.SkipReason
		if state == "settled" && saved.Outcome != "" {
			state = saved.Outcome
		}
		return nil
	})
	return state, graph, skipReason, err
}

// Prepared output is immutable: racing deliveries use the first persisted
// graph and its receipts, so an uncertain Neo4j acknowledgement can be replayed.
func (r *knowledgeRepository) PrepareDingTalkGraph(
	ctx context.Context,
	p types.ExtractChunkPayload,
	graph *types.GraphData) (*types.GraphData,
	error,
) {
	if graph == nil {
		return nil, errors.New("missing graph output")
	}
	if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
		return nil, err
	}
	var savedGraph *types.GraphData
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockDingTalkGraph(ctx, tx, p); err != nil {
			return err
		}
		op, saved, err := loadDingTalkGraph(tx, p)
		if err != nil {
			return err
		}
		if saved.Graph != nil {
			savedGraph = saved.Graph
			return nil
		}
		if op.Op != "planned" && op.Op != "queued" {
			return errors.New("graph task already terminal")
		}
		saved.Graph = graph
		raw, err := json.Marshal(saved)
		if err != nil {
			return err
		}
		savedGraph = graph
		return tx.Model(op).Updates(map[string]any{"op": "result_prepared", "payload": raw}).Error
	})
	return savedGraph, err
}

func (r *knowledgeRepository) MarkDingTalkGraphOutcome(
	ctx context.Context,
	p types.ExtractChunkPayload,
	success bool,
	failureCode,
	skipReason string,
) error {
	if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
		return err
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockDingTalkGraph(ctx, tx, p); err != nil {
			return err
		}
		op, saved, err := loadDingTalkGraph(tx, p)
		if err != nil {
			return err
		}
		if op.Op == "settled" ||
			op.Op == "result_committed" ||
			op.Op == "terminal_failed" ||
			op.Op == "result_skipped" {
			return nil
		}
		state := "terminal_failed"
		if skipReason != "" {
			if success ||
				(skipReason != "graph_backend_disabled" &&
					skipReason != "extract_disabled") ||
				(op.Op != "planned" &&
					op.Op != "queued" &&
					op.Op != "result_prepared") {
				return errors.New("invalid graph skip outcome")
			}
			state = "result_skipped"
			saved.SkipReason = skipReason
		}
		if success {
			if op.Op != "result_prepared" {
				return errors.New("graph output not prepared")
			}
			state = "result_committed"
		}
		saved.Outcome = state
		if state == "terminal_failed" {
			saved.FailureCode = failureCode
		}
		raw, err := json.Marshal(saved)
		if err != nil {
			return err
		}
		return tx.Model(op).Updates(map[string]any{"op": state, "payload": raw}).Error
	})
}

// Called inside FinalizeSubtask's transaction, before any counter change.
func settleDingTalkGraphInTx(ctx context.Context, tx *gorm.DB, p types.ExtractChunkPayload) (bool, error) {
	if err := lockDingTalkGraph(ctx, tx, p); err != nil {
		return false, err
	}
	op, _, err := loadDingTalkGraph(tx, p)
	if err != nil {
		return false, err
	}
	if op.Op == "settled" {
		return false, nil
	}
	if op.Op != "result_committed" && op.Op != "terminal_failed" && op.Op != "result_skipped" {
		return false, fmt.Errorf("graph settlement lacks terminal evidence: %s", op.Op)
	}
	result := tx.Model(op).Where("op=?", op.Op).Update("op", "settled")
	return result.RowsAffected == 1, result.Error
}

// PendingDingTalkGraphs returns recoverable rows not tied to the lifetime of an Asynq task. ClaimedAt is
// only a delivery throttle, never evidence of execution or completion.
func (s *KnowledgeInputProvenanceStore) PendingDingTalkGraphs(
	ctx context.Context,
	id string) ([]types.TaskPendingOp,
	error,
) {
	var rows []types.TaskPendingOp
	q := s.db.WithContext(ctx).Where("task_type=? AND op <> ? AND (claimed_at IS NULL OR claimed_at < ?)",
		DingTalkGraphTaskType,
		"settled",
		time.Now().Add(-2*time.Minute))
	if id != "" {
		q = q.Where("scope_id=?", id)
	}
	err := q.Order("claimed_at ASC NULLS FIRST, id ASC").Limit(100).Find(&rows).Error
	return rows, err
}

// NoteDingTalkGraphDelivery records a delivery attempt without settling work.
func (s *KnowledgeInputProvenanceStore) NoteDingTalkGraphDelivery(ctx context.Context, id int64, delivered bool) error {
	updates := map[string]any{"claimed_at": time.Now()}
	if delivered {
		updates["op"] = gorm.Expr("CASE WHEN op = 'planned' THEN 'queued' ELSE op END")
	}
	return s.db.WithContext(ctx).Model(&types.TaskPendingOp{}).Where(
		"id=? AND task_type=?",
		id,
		DingTalkGraphTaskType).Updates(updates).Error
}

// ExcludeDurableDingTalkGraphPlans is a guard on BOTH candidate reads and the
// final reset UPDATE. It protects only counters completely explained by current
// graph/Wiki work, not a document merely containing one old graph record.
func ExcludeDurableDingTalkGraphPlans(db *gorm.DB) *gorm.DB {
	// Some isolated legacy fixtures predate provenance tables. Real schema174
	// has all of these; absence cannot be treated as positive durable evidence.
	if db.Name() == "sqlite" &&
		(!db.Migrator().HasTable("task_pending_ops") ||
			!db.Migrator().HasTable("knowledge_input_material_receipts") ||
			!db.Migrator().HasTable("knowledge_input_admissions")) {
		return db
	}
	field := func(alias, path string) string {
		if db.Name() == "postgres" {
			return alias + ".payload::jsonb #>> '{" + path + "}'"
		}
		return "CAST(json_extract(" + alias + ".payload, '$." + strings.ReplaceAll(path, ",", ".") + "') AS TEXT)"
	}
	latest := "(SELECT COALESCE(MAX(sp.attempt),0) FROM knowledge_processing_spans sp " +
		"WHERE sp.knowledge_id=knowledges.id AND sp.kind='root')"
	graph := fmt.Sprintf(`(SELECT COUNT(DISTINCT p.dedup_key) FROM task_pending_ops p
`+
		` JOIN knowledge_input_material_receipts mr ON`+
		` mr.row_kind='knowledge' AND mr.row_id=knowledges.id
 JOIN`+
		` knowledge_input_admissions a ON a.admission_id=mr.admission_id
`+
		` WHERE p.task_type='dingtalk:graph' AND p.scope='knowledge' AND`+
		` p.scope_id=knowledges.id AND p.tenant_id=knowledges.tenant_id
`+
		` AND p.op IN`+
		` ('planned','queued','result_prepared','result_committed','termin`+
		`al_failed','result_skipped')
 AND %s=CAST(%s AS TEXT) AND`+
		` %s=a.admission_id AND %s=CAST(a.build_generation AS TEXT)
 AND`+
		` a.tenant_id=knowledges.tenant_id AND`+
		` a.knowledge_base_id=knowledges.knowledge_base_id AND`+
		` %s=knowledges.knowledge_base_id)`,
		field("p", "payload,attempt"), latest,
		field("p", "payload,original_input_proof,admission_id"),
		field("p", "payload,original_input_proof,build_generation"),
		field("p", "payload,original_input_proof,knowledge_base_id"))
	wiki := fmt.Sprintf(`(SELECT COUNT(DISTINCT w.dedup_key) FROM task_pending_ops w
`+
		` JOIN knowledge_input_material_receipts mr ON`+
		` mr.row_kind='knowledge' AND mr.row_id=knowledges.id
 JOIN`+
		` knowledge_input_admissions a ON a.admission_id=mr.admission_id
`+
		` WHERE w.task_type='wiki:ingest' AND w.scope='knowledge_base'`+
		` AND w.scope_id=knowledges.knowledge_base_id AND`+
		` w.tenant_id=knowledges.tenant_id AND w.dedup_key=knowledges.id`+
		` AND w.op='ingest'
 AND %s=CAST(%s AS TEXT) AND`+
		` %s=a.admission_id AND %s=CAST(a.build_generation AS TEXT))`,
		field("w", "attempt"), latest,
		field("w", "original_input,admission_id"),
		field("w", "original_input,build_generation"))
	return db.Where("NOT (knowledges.channel = ? AND knowledges.parse_status = ? AND "+
		"knowledges.pending_subtasks_count > 0 AND "+graph+
		" > 0 AND knowledges.pending_subtasks_count = "+graph+" + "+wiki+")",
		types.ConnectorTypeDingTalk,
		types.ParseStatusFinalizing)
}
