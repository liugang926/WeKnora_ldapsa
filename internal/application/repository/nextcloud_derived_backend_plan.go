package repository

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
)

type nextcloudDerivedBackendPlan struct {
	TenantID                                               uint64 `gorm:"primaryKey"`
	KnowledgeBaseID, KnowledgeID                           string `gorm:"primaryKey"`
	DataSourceID                                           string `gorm:"column:datasource_id"`
	ExternalID, OriginAdmissionID, BackendPlan, PlanSHA256 string
	CreatedAtMS                                            int64
}

func (nextcloudDerivedBackendPlan) TableName() string { return "nextcloud_derived_backend_plans" }

type nextcloudBackendSelection struct {
	Engines       []string `json:"engines"`
	Graph         bool     `json:"graph"`
	Wiki          bool     `json:"wiki"`
	VectorStoreID string   `json:"vector_store_id"`
}

// Invoked by the actual Composite selection before model or storage IO.
// No caller-supplied ready flag and no current-config restamping of old rows.
func RecordNextcloudDerivedBackendSelection(ctx context.Context, engines []types.RetrieverEngineType) error {
	guard, ok := ctx.Value(nextcloudBuildContextKey{}).(nextcloudBuildContext)
	if !ok {
		return nil
	}
	recorder, ok := guard.validator.(interface {
		RecordDerivedBackendSelection(context.Context, nextcloudBuildContext, []types.RetrieverEngineType) error
	})
	if !ok {
		return ErrNextcloudContentLeaseInvalid
	}
	return recorder.RecordDerivedBackendSelection(ctx, guard, engines)
}
func (s *NextcloudContentLeaseStore) RecordDerivedBackendSelection(ctx context.Context, guard nextcloudBuildContext, engines []types.RetrieverEngineType) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := s.ValidateBuildLeaseInTx(ctx, tx, guard.lease, guard.scope); e != nil {
			return e
		}
		var k types.Knowledge
		var kb types.KnowledgeBase
		if e := tx.Where("id=? AND tenant_id=? AND knowledge_base_id=? AND channel=?", guard.scope.KnowledgeID, guard.scope.TenantID, guard.scope.KnowledgeBaseID, types.ConnectorTypeNextcloud).Take(&k).Error; e != nil {
			return e
		}
		if e := tx.Where("id=? AND tenant_id=?", guard.scope.KnowledgeBaseID, guard.scope.TenantID).Take(&kb).Error; e != nil {
			return e
		}
		selected := nextcloudBackendSelection{Engines: []string{}, Graph: kb.IsGraphEnabled(), Wiki: kb.IsWikiEnabled()}
		overrides, overrideErr := k.ProcessOverrides()
		if overrideErr != nil {
			return overrideErr
		}
		if overrides != nil {
			if overrides.GraphEnabled != nil {
				selected.Graph = *overrides.GraphEnabled
			}
			extractEnabled := kb.ExtractConfig != nil && kb.ExtractConfig.Enabled
			if overrides.ExtractConfig != nil {
				extractEnabled = overrides.ExtractConfig.Enabled
			}
			selected.Graph = selected.Graph && extractEnabled
		}
		if kb.VectorStoreID != nil {
			selected.VectorStoreID = *kb.VectorStoreID
		}
		seen := map[string]bool{}
		for _, engine := range engines {
			value := string(engine)
			if !seen[value] {
				selected.Engines = append(selected.Engines, value)
				seen[value] = true
			}
		}
		sort.Strings(selected.Engines)
		raw, e := json.Marshal(selected)
		if e != nil {
			return e
		}
		digest := recoverySHA(raw)
		var existing nextcloudDerivedBackendPlan
		e = tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", guard.scope.TenantID, guard.scope.KnowledgeBaseID, guard.scope.KnowledgeID).Take(&existing).Error
		if e == nil {
			proof := KnowledgeBuildInputProofFromContext(ctx)
			if existing.PlanSHA256 != digest || existing.DataSourceID != guard.scope.DataSourceID || existing.ExternalID != guard.scope.ExternalID || proof == nil || existing.OriginAdmissionID != proof.AdmissionID {
				return ErrNextcloudContentLeaseDenied
			}
			return nil
		}
		if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		proof := KnowledgeBuildInputProofFromContext(ctx)
		if proof == nil || proof.AdmissionID == "" {
			return ErrKnowledgeInputChanged
		}
		var boundary struct{ InstalledAtMS int64 }
		if e = tx.Table("nextcloud_derived_protocol_boundary").Where("id=1").Take(&boundary).Error; e != nil {
			return e
		}
		var witness struct{ AdmittedAtMS int64 }
		if e = tx.Table("nextcloud_derived_origin_witnesses").Where("admission_id=?", proof.AdmissionID).Take(&witness).Error; e != nil {
			return e
		}
		var origin struct{ CreatedAtMS int64 }
		if e = tx.Table("nextcloud_derived_new_knowledges").Where("knowledge_id=?", k.ID).Take(&origin).Error; e != nil {
			return ErrKnowledgeInputChanged
		}
		if origin.CreatedAtMS < boundary.InstalledAtMS {
			return ErrKnowledgeInputChanged
		}
		if witness.AdmittedAtMS < boundary.InstalledAtMS {
			return ErrKnowledgeInputChanged
		}
		// Historic material already written before a plan cannot be adopted.
		var prior int64
		if e = tx.Table("chunks").Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", k.TenantID, k.KnowledgeBaseID, k.ID).Count(&prior).Error; e != nil {
			return e
		}
		if prior != 0 {
			return ErrKnowledgeInputChanged
		}
		for _, table := range []string{"lite_embeddings", "embeddings"} {
			if tx.Migrator().HasTable(table) {
				if e = tx.Table(table).Where("knowledge_base_id=? AND knowledge_id=?", k.KnowledgeBaseID, k.ID).Count(&prior).Error; e != nil {
					return e
				}
				if prior != 0 {
					return ErrKnowledgeInputChanged
				}
			}
		}
		now, e := nextcloudLeaseNowMS(tx)
		if e != nil {
			return e
		}
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&nextcloudDerivedBackendPlan{TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, DataSourceID: guard.scope.DataSourceID, ExternalID: guard.scope.ExternalID, OriginAdmissionID: proof.AdmissionID, BackendPlan: string(raw), PlanSHA256: digest, CreatedAtMS: now}).Error
	})
}
