package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const DingTalkRecoveryTask = "dingtalk:document-recovery"

// DingTalkRecoveryPlan is an exact, read-only material snapshot. It is not an
// authorization token: the worker restores and rechecks the original actor.
type DingTalkRecoveryPlan struct {
	Version         int    `json:"version"`
	Action          string `json:"action"`
	TenantID        uint64 `json:"tenant_id"`
	KnowledgeBaseID string `json:"knowledge_base_id"`
	DataSourceID    string `json:"data_source_id"`
	KnowledgeID     string `json:"knowledge_id"`
	Attempt         int    `json:"attempt"`
	Status          string `json:"status"`
	SourceRevision  uint64 `json:"source_revision"`
	AdmissionID     string `json:"admission_id"`
	BuildGeneration uint64 `json:"build_generation"`
	InputDigest     string `json:"input_digest"`
	ConfigDigest    string `json:"config_digest"`
	MaterialDigest  string `json:"material_digest"`
	Digest          string `json:"digest"`
}

type dingTalkRecoveryIndex struct {
	ID                                                       uint64
	SourceID, ChunkID, KnowledgeID, KnowledgeBaseID, Content string
	SourceType, Dimension                                    int
	IsEnabled                                                bool
	VectorDigest                                             string
}

// PlanDingTalkRecovery reads actual PostgreSQL index rows, not counts. content
// is the same title/context projection used by the real indexing service.
func (s *KnowledgeInputProvenanceStore) PlanDingTalkRecovery(ctx context.Context, tenant uint64, kb, source, id, action string, content func(*types.Knowledge, *types.Chunk) string) (*DingTalkRecoveryPlan, error) {
	return s.planDingTalkRecovery(s.db.WithContext(ctx), tenant, kb, source, id, action, content)
}
func (s *KnowledgeInputProvenanceStore) planDingTalkRecovery(db *gorm.DB, tenant uint64, kb, source, id, action string, content func(*types.Knowledge, *types.Chunk) string) (*DingTalkRecoveryPlan, error) {
	if tenant == 0 || kb == "" || source == "" || id == "" || (action != "resume-wiki" && action != "reparse") {
		return nil, errors.New("exact recovery scope and action required")
	}
	var k types.Knowledge
	if err := db.Where("id=? AND tenant_id=? AND knowledge_base_id=?", id, tenant, kb).Take(&k).Error; err != nil {
		return nil, err
	}
	if k.ParseStatus != types.ParseStatusFailed || k.Channel != types.ConnectorTypeDingTalk || k.GetMetadata()["datasource_id"] != source {
		return nil, errors.New("recovery requires exact failed DingTalk document")
	}
	var ds types.DataSource
	if err := db.Where("id=? AND tenant_id=? AND knowledge_base_id=?", source, tenant, kb).Take(&ds).Error; err != nil {
		return nil, err
	}
	if ds.Type != types.ConnectorTypeDingTalk || ds.Status == types.DataSourceStatusPaused || ds.Status == types.DataSourceStatusDeleted {
		return nil, ErrKnowledgeInputChanged
	}
	var a knowledgeInputAdmission
	if err := db.Table("knowledge_input_admissions AS a").Select("a.*").Joins("JOIN knowledge_input_material_receipts r ON r.admission_id=a.admission_id AND r.row_kind='knowledge' AND r.row_id=a.knowledge_id").Where("a.tenant_id=? AND a.knowledge_base_id=? AND a.knowledge_id=?", tenant, kb, id).Take(&a).Error; err != nil {
		return nil, err
	}
	if a.OriginKind != dingTalkOriginalKind {
		return nil, ErrKnowledgeInputChanged
	}
	if err := verifyDingTalkOriginalReceipt(db, &k, &a); err != nil {
		return nil, err
	}
	var r dingTalkOriginalReceipt
	if err := db.Where("admission_id=?", a.AdmissionID).Take(&r).Error; err != nil {
		return nil, err
	}
	var revision uint64
	if err := db.Table("dingtalk_source_input_revisions").Select("revision").Where("datasource_id=?", source).Scan(&revision).Error; err != nil {
		return nil, err
	}
	// The receipt records the historical fetch revision. Current configuration
	// is checked semantically below; encrypted config rewrites can advance the
	// source revision without changing that configuration. Bind the current
	// revision into the plan so any later source update still invalidates apply.
	if revision == 0 {
		return nil, ErrKnowledgeInputChanged
	}
	var facts dingtalk.OriginalFetchedArtifactInput
	if err := json.Unmarshal([]byte(r.Facts), &facts); err != nil {
		return nil, err
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return nil, err
	}
	if err = dingtalk.VerifyOriginalArtifactConfiguration(cfg, facts); err != nil {
		return nil, err
	}
	var publication dingTalkPublication
	if err = db.Where("tenant_id=? AND knowledge_base_id=? AND data_source_id=? AND external_id=?", tenant, kb, source, facts.DocumentID).Take(&publication).Error; err != nil {
		return nil, err
	}
	if publication.Tombstone || publication.KnowledgeID != id || publication.ItemDigest != facts.ItemDigest {
		return nil, ErrKnowledgeInputChanged
	}
	var attempt int
	if err = db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id=? AND kind=?", id, types.SpanKindRoot).Select("COALESCE(MAX(attempt),0)").Scan(&attempt).Error; err != nil {
		return nil, err
	}
	if attempt <= 0 {
		return nil, errors.New("missing original parse attempt")
	}
	var ops []types.TaskPendingOp
	if err = db.Where("(task_type=? AND scope_id=? AND dedup_key=?) OR (task_type=? AND scope_id=? AND dedup_key=?)", "wiki:ingest", kb, id, "wiki:settled", id, strconv.Itoa(attempt)).Find(&ops).Error; err != nil {
		return nil, err
	}
	if len(ops) != 0 {
		return nil, errors.New("existing Wiki operation/settlement must use normal worker")
	}
	var outstanding int64
	if err = db.Model(&DingTalkWorkIntent{}).Where("knowledge_id=? AND state<>?", id, "done").Count(&outstanding).Error; err != nil {
		return nil, err
	}
	if outstanding != 0 {
		return nil, errors.New("source work intent still pending; wait for its normal worker")
	}
	var chunks []*types.Chunk
	if err = db.Where("knowledge_id=? AND tenant_id=? AND knowledge_base_id=?", id, tenant, kb).Order("id").Find(&chunks).Error; err != nil {
		return nil, err
	}
	var indexes []dingTalkRecoveryIndex
	vectorDigest := "CAST(embedding AS TEXT)"
	if db.Dialector.Name() == "postgres" {
		vectorDigest = "md5(embedding::text)"
	}
	if err = db.Table("embeddings").Select("id,source_id,source_type,chunk_id,knowledge_id,knowledge_base_id,content,dimension,is_enabled,"+vectorDigest+" AS vector_digest").Where("knowledge_id=? AND knowledge_base_id=?", id, kb).Order("id").Scan(&indexes).Error; err != nil {
		return nil, err
	}
	if action == "resume-wiki" {
		if content == nil || len(chunks) == 0 || k.PendingSubtasksCount != 0 || (k.SummaryStatus != types.SummaryStatusCompleted && k.SummaryStatus != types.SummaryStatusNone) {
			return nil, errors.New("Wiki-only recovery lacks a terminal parse; use reparse")
		}
		for _, c := range chunks {
			if err := verifyKnowledgeInputMaterial(db, &a, &k, c); err != nil {
				return nil, err
			}
		}
		for _, stage := range []string{types.StageDocReader, types.StageChunking, types.StageEmbedding, types.StagePostProcess} {
			var n int64
			if err := db.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id=? AND attempt=? AND name=? AND status=?", id, attempt, stage, types.SpanStatusDone).Count(&n).Error; err != nil {
				return nil, err
			}
			if n != 1 {
				return nil, errors.New("original parse stage incomplete; use reparse")
			}
		}
		valid := map[string]bool{}
		for _, c := range chunks {
			if c.ChunkType != types.ChunkTypeText || !c.IsEnabled {
				continue
			}
			for _, i := range indexes {
				if i.SourceType == 0 && i.SourceID == c.ID && i.ChunkID == c.ID && i.IsEnabled && i.Dimension > 0 && i.VectorDigest != "" && i.Content == content(&k, c) {
					valid[c.ID] = true
				}
			}
			if !valid[c.ID] {
				return nil, errors.New("original chunk index incomplete; use reparse")
			}
		}
		if len(valid) == 0 {
			return nil, errors.New("no authenticated text index; use reparse")
		}
	}
	// Including all rows and immutable receipts binds the plan to actual outputs;
	// nothing from this material is printed to the maintenance operator.
	material, err := KnowledgeInputDigest(struct {
		K      types.Knowledge
		R      dingTalkOriginalReceipt
		P      dingTalkPublication
		C      []*types.Chunk
		I      []dingTalkRecoveryIndex
		Config types.JSON
	}{k, r, publication, chunks, indexes, ds.Config})
	if err != nil {
		return nil, err
	}
	p := &DingTalkRecoveryPlan{Version: 1, Action: action, TenantID: tenant, KnowledgeBaseID: kb, DataSourceID: source, KnowledgeID: id, Attempt: attempt, Status: k.ParseStatus, SourceRevision: revision, AdmissionID: a.AdmissionID, BuildGeneration: a.BuildGeneration, InputDigest: a.InputDigest, ConfigDigest: a.ConfigDigest, MaterialDigest: material}
	p.Digest, err = KnowledgeInputDigest(p)
	return p, err
}

// ApplyDingTalkWikiRecovery is the only state mutation. The original actor and
// material proof are mandatory; source/document locks plus the attempt CAS
// fence state changes against publication and reparses. The receipt survives op consumption, preserving failed history.
func (s *KnowledgeInputProvenanceStore) ApplyDingTalkWikiRecovery(ctx context.Context, p *DingTalkRecoveryPlan, op *types.TaskPendingOp, content func(*types.Knowledge, *types.Chunk) string) error {
	if p == nil || p.Action != "resume-wiki" || op == nil {
		return ErrKnowledgeInputChanged
	}
	if err := PrecheckKnowledgeBuildWrite(ctx); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ds types.DataSource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", p.DataSourceID).Take(&ds).Error; err != nil {
			return err
		}
		var k types.Knowledge
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", p.KnowledgeID).Take(&k).Error; err != nil {
			return err
		}
		var receipt types.TaskPendingOp
		err := tx.Where("task_type=? AND scope=? AND scope_id=? AND dedup_key=?", DingTalkRecoveryTask, "knowledge", p.KnowledgeID, p.Digest).Take(&receipt).Error
		if err == nil {
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		fresh, err := s.planDingTalkRecovery(tx, p.TenantID, p.KnowledgeBaseID, p.DataSourceID, p.KnowledgeID, p.Action, content)
		if err != nil {
			return err
		}
		if fresh.Digest != p.Digest {
			return errors.New("recovery plan changed; generate a fresh plan")
		}
		if _, err = validateKnowledgeInputWrite(ctx, tx, p.TenantID, p.KnowledgeBaseID, p.KnowledgeID); err != nil {
			return err
		}
		var saved struct {
			KnowledgeID   string                          `json:"knowledge_id"`
			Attempt       int                             `json:"attempt"`
			OriginalInput *types.KnowledgeBuildInputProof `json:"original_input"`
		}
		if json.Unmarshal(op.Payload, &saved) != nil || saved.KnowledgeID != p.KnowledgeID || saved.Attempt != p.Attempt || saved.OriginalInput == nil || saved.OriginalInput.AdmissionID != p.AdmissionID {
			return ErrKnowledgeInputChanged
		}
		if op.TaskType != "wiki:ingest" || op.Scope != "knowledge_base" || op.ScopeID != p.KnowledgeBaseID || op.TenantID != p.TenantID || op.DedupKey != p.KnowledgeID {
			return ErrKnowledgeInputChanged
		}
		updated := tx.Model(&types.Knowledge{}).Where("id=? AND parse_status=? AND pending_subtasks_count=0 AND (SELECT COALESCE(MAX(attempt),0) FROM knowledge_processing_spans WHERE knowledge_id=? AND kind=?)=?", p.KnowledgeID, types.ParseStatusFailed, p.KnowledgeID, types.SpanKindRoot, p.Attempt).Updates(map[string]any{"parse_status": types.ParseStatusFinalizing, "pending_subtasks_count": 1, "updated_at": time.Now()})
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected != 1 {
			return ErrKnowledgeInputChanged
		}
		if err = tx.Create(op).Error; err != nil {
			return err
		}
		payload, _ := json.Marshal(p)
		return tx.Create(&types.TaskPendingOp{TenantID: p.TenantID, TaskType: DingTalkRecoveryTask, Scope: "knowledge", ScopeID: p.KnowledgeID, Op: "resumed", DedupKey: p.Digest, Payload: payload, EnqueuedAt: time.Now()}).Error
	})
}

func (p *DingTalkRecoveryPlan) Validate() error {
	if p == nil {
		return ErrKnowledgeInputChanged
	}
	copy := *p
	copy.Digest = ""
	d, err := KnowledgeInputDigest(&copy)
	if err != nil {
		return err
	}
	if p.Version != 1 || p.Digest != d {
		return fmt.Errorf("invalid recovery plan digest")
	}
	return nil
}

// DingTalkRecoveryWasApplied recognizes only this exact committed recovery.
// It is used to retry a lost Redis trigger, never to add another counter slot.
func (s *KnowledgeInputProvenanceStore) DingTalkRecoveryWasApplied(ctx context.Context, p *DingTalkRecoveryPlan) (bool, error) {
	var n int64
	err := s.db.WithContext(ctx).Model(&types.TaskPendingOp{}).Where("task_type=? AND scope=? AND scope_id=? AND dedup_key=?", DingTalkRecoveryTask, "knowledge", p.KnowledgeID, p.Digest).Count(&n).Error
	return n == 1, err
}
