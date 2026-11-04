package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDingTalkPublicationConflict = errors.New("DingTalk publication changed; refetch required")

type dingTalkPublicationKey struct{}
type dingTalkPublicationSnapshot struct {
	Fetch       dingTalkFetch
	Generations map[string]uint64
}
type dingTalkPublication struct {
	TenantID        uint64 `gorm:"primaryKey"`
	KnowledgeBaseID string `gorm:"primaryKey"`
	DataSourceID    string `gorm:"primaryKey"`
	ExternalID      string `gorm:"primaryKey"`
	Generation      uint64
	KnowledgeID     string
	ItemDigest      string
	Tombstone       bool
}

func (dingTalkPublication) TableName() string { return "dingtalk_document_publications" }

// DingTalkWorkIntent is a durable processing/cleanup fact, never authorization.
// Version is an acknowledgement CAS token. Workers must use the existing actor
// and build proof checks before performing any operation.
type DingTalkWorkIntent struct {
	ID                                                           string `gorm:"primaryKey"`
	TenantID                                                     uint64
	KnowledgeBaseID, DataSourceID, ExternalID, KnowledgeID, Kind string
	Generation, Version                                          uint64
	State                                                        string
	ProcessAttempt                                               uint64
	CapacitySettled                                              bool
}

func (DingTalkWorkIntent) TableName() string { return "dingtalk_work_intents" }

type DingTalkPublicationResult struct {
	Outcome            string
	Knowledge          *types.Knowledge
	Proof              *types.KnowledgeBuildInputProof
	Generation         uint64
	RetiredKnowledgeID string
}

// BeginDingTalkPublicationFetch must precede the real connector fetch. The
// snapshot is process-local and cannot be reconstructed from request metadata.
func (s *KnowledgeInputProvenanceStore) BeginDingTalkPublicationFetch(ctx context.Context, ds *types.DataSource, run string) (context.Context, error) {
	ctx, err := s.BeginDingTalkOriginalFetch(ctx, ds, run)
	if err != nil {
		return ctx, err
	}
	f := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	snapshot := dingTalkPublicationSnapshot{Fetch: f, Generations: map[string]uint64{}}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := checkDingTalkRun(tx, f); err != nil {
			return err
		}
		var rows []dingTalkPublication
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ?", f.Tenant, f.KB, f.Source).Find(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			snapshot.Generations[row.ExternalID] = row.Generation
		}
		return nil
	})
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, dingTalkPublicationKey{}, snapshot), nil
}

func lockDingTalkPublicationSource(tx *gorm.DB, f dingTalkFetch) error {
	var ds types.DataSource
	// Lock source exclusively FIRST. Never upgrade a shared lock after admission.
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", f.Source, f.Tenant, f.KB).Take(&ds).Error; err != nil {
		return err
	}
	return checkDingTalkRun(tx, f)
}

func (s *KnowledgeInputProvenanceStore) PublishDingTalkOriginal(ctx context.Context, k *types.Knowledge, input []byte) (*DingTalkPublicationResult, error) {
	snapshot, ok := ctx.Value(dingTalkPublicationKey{}).(dingTalkPublicationSnapshot)
	f, present := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	origin, hasOrigin := ctx.Value(knowledgeInputOriginKey{}).(knowledgeInputOrigin)
	if !ok || !present || snapshot.Fetch.Invocation != f.Invocation || !hasOrigin || origin.kind != dingTalkOriginalKind || origin.lineage.RequireComplete() != nil || k == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if k.ID == "" {
		k.ID = uuid.NewString()
	}
	var result *DingTalkPublicationResult
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockDingTalkPublicationSource(tx, f); err != nil {
			return err
		}
		artifact, err := s.checkDingTalkOriginalCreation(ctx, tx, k, input)
		if err != nil {
			return err
		}
		// Preserve the ordinary admission actor rejection even on a dedup hit.
		if ctx.Value(types.CallerContextKey) != nil && ctx.Value(types.PrincipalContextKey) != nil {
			if _, err := captureKnowledgeActorSelector(ctx, ""); err != nil {
				return err
			}
		}
		var old dingTalkPublication
		query := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ?", f.Tenant, f.KB, f.Source, artifact.Fact.DocumentID)
		err = query.Take(&old).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		found := err == nil
		var oldKnowledge types.Knowledge
		if found && !old.Tombstone {
			if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", old.KnowledgeID, f.Tenant, f.KB).Take(&oldKnowledge).Error; err != nil {
				return err
			}
			// The old row supplies identity, never authority for newly fetched bytes.
			// A failed legacy reparse can leave its generated material inconsistent.
			if _, err := loadDingTalkPublishedOriginal(tx, old); err != nil {
				return err
			}
			if old.ItemDigest == artifact.Fact.ItemDigest {
				a, _, lineage, err := s.admissionInTx(tx, &oldKnowledge, nil)
				if err == nil {
					err = verifyKnowledgeInputMaterial(tx, a, &oldKnowledge, nil)
				}
				if err == nil {
					local, localErr := isDingTalkLocalReparseAdmission(tx, a)
					if localErr != nil {
						return localErr
					}
					if a.OriginKind != dingTalkOriginalKind && !local {
						return ErrKnowledgeInputChanged
					}
					result = &DingTalkPublicationResult{Outcome: "unchanged", Knowledge: &oldKnowledge, Generation: old.Generation, Proof: &types.KnowledgeBuildInputProof{Version: 1, TenantID: f.Tenant, KnowledgeBaseID: f.KB, KnowledgeID: old.KnowledgeID, AdmissionID: a.AdmissionID, BuildGeneration: a.BuildGeneration, InputDigest: a.InputDigest, ConfigDigest: a.ConfigDigest, SourceLineage: lineage}}
					return nil
				}
				// Only a known obsolete local input/material is repairable here.
				// Storage, authorization and unknown-identity errors remain errors.
				if !errors.Is(err, ErrKnowledgeInputChanged) {
					return err
				}
			}

		}
		if snapshot.Generations[artifact.Fact.DocumentID] != old.Generation {
			return ErrDingTalkPublicationConflict
		}
		// Refuse legacy/untracked active rows rather than silently laundering them.
		var count int64
		q := tx.Model(&types.Knowledge{}).Where("tenant_id = ? AND knowledge_base_id = ? AND metadata->>'datasource_id' = ? AND metadata->>'external_id' = ?", f.Tenant, f.KB, f.Source, artifact.Fact.DocumentID)
		if found {
			q = q.Where("id <> ?", old.KnowledgeID)
		}
		if err := q.Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return ErrDingTalkPublicationConflict
		}
		proof, err := s.createKnowledgeWithInputInTx(ctx, tx, k, input, origin)
		if err != nil {
			return err
		}
		next := dingTalkPublication{TenantID: f.Tenant, KnowledgeBaseID: f.KB, DataSourceID: f.Source, ExternalID: artifact.Fact.DocumentID, Generation: old.Generation + 1, KnowledgeID: k.ID, ItemDigest: artifact.Fact.ItemDigest}
		result = &DingTalkPublicationResult{Outcome: "created", Knowledge: k, Proof: proof, Generation: next.Generation}
		if found {
			if !old.Tombstone {
				if err := tx.Model(&types.Knowledge{}).Where("id = ? AND tenant_id = ?", old.KnowledgeID, f.Tenant).Update("deleted_at", time.Now().UTC()).Error; err != nil {
					return err
				}
				result.Outcome = "replaced"
				result.RetiredKnowledgeID = old.KnowledgeID
				cleanup := DingTalkWorkIntent{ID: uuid.NewString(), TenantID: f.Tenant, KnowledgeBaseID: f.KB, DataSourceID: f.Source, ExternalID: next.ExternalID, KnowledgeID: old.KnowledgeID, Kind: "cleanup", Generation: old.Generation, Version: 1, State: "pending", ProcessAttempt: 1}
				if err := tx.Create(&cleanup).Error; err != nil {
					return err
				}
			}
			if err := dingTalkCAS(tx.Model(&dingTalkPublication{}).Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ? AND generation = ?", f.Tenant, f.KB, f.Source, artifact.Fact.DocumentID, old.Generation).Updates(map[string]any{"generation": next.Generation, "knowledge_id": next.KnowledgeID, "item_digest": next.ItemDigest, "tombstone": false})); err != nil {
				return err
			}
		} else if err := tx.Create(&next).Error; err != nil {
			return err
		}
		return tx.Create(&DingTalkWorkIntent{ID: uuid.NewString(), TenantID: f.Tenant, KnowledgeBaseID: f.KB, DataSourceID: f.Source, ExternalID: next.ExternalID, KnowledgeID: k.ID, Kind: "process", Generation: next.Generation, Version: 1, State: "pending", ProcessAttempt: 1}).Error
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ListDingTalkWork requires the current, already-authorized source/run context.
// Completed enqueue intents with failed/pending knowledge remain discoverable,
// including crashes after commit or before durable queue acknowledgement.
func (s *KnowledgeInputProvenanceStore) ListDingTalkWork(ctx context.Context) ([]DingTalkWorkIntent, error) {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok {
		return nil, types.ErrSourceLineageUnknown
	}
	var rows []DingTalkWorkIntent
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := checkDingTalkRun(tx, f); err != nil {
			return err
		}
		return tx.Table("dingtalk_work_intents AS i").Select("i.*").Where("i.tenant_id = ? AND i.knowledge_base_id = ? AND i.data_source_id = ?", f.Tenant, f.KB, f.Source).Where("(i.kind = 'cleanup' AND i.state <> 'done') OR (i.kind = 'process' AND EXISTS (SELECT 1 FROM dingtalk_document_publications p JOIN knowledges k ON k.id = p.knowledge_id WHERE p.knowledge_id = i.knowledge_id AND p.generation = i.generation AND k.deleted_at IS NULL AND NOT EXISTS (SELECT 1 FROM knowledge_input_material_receipts m JOIN task_pending_ops l ON l.dedup_key=m.admission_id AND l.task_type='dingtalk:local-reparse' AND l.scope='knowledge' AND l.scope_id=k.id AND l.tenant_id=k.tenant_id WHERE m.row_kind='knowledge' AND m.row_id=k.id) AND (i.state <> 'done' OR k.parse_status IN ('pending','failed'))))").Order("i.kind, i.id").Scan(&rows).Error
	})
	return rows, err
}

// FinishDingTalkWork acknowledges only the exact intent/version observed. It
// intentionally stores no arbitrary error text; retry state is sufficient.
func (s *KnowledgeInputProvenanceStore) FinishDingTalkWork(ctx context.Context, intent DingTalkWorkIntent, success bool) error {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok || intent.TenantID != f.Tenant || intent.KnowledgeBaseID != f.KB || intent.DataSourceID != f.Source {
		return ErrKnowledgeInputChanged
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockDingTalkPublicationSource(tx, f); err != nil {
			return err
		}
		if intent.Kind == "cleanup" && success && !intent.CapacitySettled {
			return ErrDingTalkPublicationConflict
		}
		state := "retry"
		if success {
			state = "done"
		}
		q := tx.Model(&DingTalkWorkIntent{}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ? AND knowledge_id = ? AND kind = ? AND generation = ? AND version = ?", intent.ID, f.Tenant, f.KB, f.Source, intent.ExternalID, intent.KnowledgeID, intent.Kind, intent.Generation, intent.Version)
		if intent.Kind == "process" {
			q = q.Where("EXISTS (SELECT 1 FROM dingtalk_document_publications p WHERE p.knowledge_id = dingtalk_work_intents.knowledge_id AND p.generation = dingtalk_work_intents.generation)")
		}
		r := q.Updates(map[string]any{"state": state, "version": intent.Version + 1})
		if r.Error != nil {
			return r.Error
		}
		if r.RowsAffected != 1 {
			return ErrDingTalkPublicationConflict
		}
		return nil
	})
}

// LoadDingTalkWork resolves only an existing exact intent. Cleanup may read its
// retired row (ordinary Get intentionally cannot); it never resolves by a broad
// external ID. This does not authorize resource deletion or queue execution.
func (s *KnowledgeInputProvenanceStore) LoadDingTalkWork(ctx context.Context, intent DingTalkWorkIntent) (*types.Knowledge, *types.KnowledgeBuildInputProof, error) {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok || intent.TenantID != f.Tenant || intent.KnowledgeBaseID != f.KB || intent.DataSourceID != f.Source {
		return nil, nil, ErrKnowledgeInputChanged
	}
	var k types.Knowledge
	var proof *types.KnowledgeBuildInputProof
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := checkDingTalkRun(tx, f); err != nil {
			return err
		}
		var stored DingTalkWorkIntent
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ?", intent.ID, f.Tenant, f.KB, f.Source).Take(&stored).Error; err != nil {
			return err
		}
		if stored != intent {
			return ErrDingTalkPublicationConflict
		}
		var publication dingTalkPublication
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ?", f.Tenant, f.KB, f.Source, intent.ExternalID).Take(&publication).Error; err != nil {
			return err
		}
		query := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", intent.KnowledgeID, f.Tenant, f.KB)
		if intent.Kind == "cleanup" {
			query = query.Unscoped()
		}
		if err := query.Take(&k).Error; err != nil {
			return err
		}
		meta := k.GetMetadata()
		if k.Channel != types.ConnectorTypeDingTalk || meta["datasource_id"] != f.Source || meta["external_id"] != intent.ExternalID {
			return ErrKnowledgeInputChanged
		}
		if intent.Kind == "cleanup" {
			if !k.DeletedAt.Valid || (!publication.Tombstone && publication.KnowledgeID == k.ID) || publication.Generation <= intent.Generation {
				return ErrKnowledgeInputChanged
			}
			return nil
		}
		if intent.Kind != "process" || publication.Tombstone || publication.KnowledgeID != k.ID || publication.Generation != intent.Generation {
			return ErrKnowledgeInputChanged
		}
		a, _, lineage, err := s.admissionInTx(tx, &k, nil)
		if err != nil {
			return err
		}
		local, err := isDingTalkLocalReparseAdmission(tx, a)
		if err != nil {
			return err
		}
		if a.OriginKind != dingTalkOriginalKind && !local {
			return ErrKnowledgeInputChanged
		}
		proof = &types.KnowledgeBuildInputProof{Version: 1, TenantID: f.Tenant, KnowledgeBaseID: f.KB, KnowledgeID: k.ID, AdmissionID: a.AdmissionID, BuildGeneration: a.BuildGeneration, InputDigest: a.InputDigest, ConfigDigest: a.ConfigDigest, SourceLineage: lineage}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return &k, proof, nil
}
