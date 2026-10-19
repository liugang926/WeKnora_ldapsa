package repository

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strconv"
)

type nextcloudDerivedArtifact struct {
	TenantID                                          uint64 `gorm:"primaryKey"`
	KnowledgeBaseID, KnowledgeID, ItemKind, ObjectRef string `gorm:"primaryKey"`
	PlanSHA256, IdentitySHA256                        string
	Dimension, WrittenAtMS                            int64
}

func (nextcloudDerivedArtifact) TableName() string { return "nextcloud_derived_write_artifacts" }

// Called in the actual row-write transaction. The immutable identity ledger is
// exhaustive for this protocol's supported local writers; historic rows cannot
// be backfilled from a later config, empty scan, or garbage-collection attempt.
func RecordNextcloudDerivedArtifactInTx(ctx context.Context, tx *gorm.DB, knowledgeID, kbID, kind, ref string, dimension int64) error {
	if !tx.Migrator().HasTable(&nextcloudDerivedArtifact{}) {
		return nil
	}
	var k types.Knowledge
	e := tx.Unscoped().Where("id=? AND knowledge_base_id=?", knowledgeID, kbID).Take(&k).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return CheckNextcloudBuildIdentity(ctx, 0, "", "")
	}
	if e != nil {
		return e
	}
	if k.Channel != types.ConnectorTypeNextcloud {
		return nil
	}
	if ref == "" || !NextcloudBuildLeasePresent(ctx, k.TenantID, kbID, knowledgeID) {
		return ErrNextcloudContentLeaseInvalid
	}
	if e = ValidateNextcloudBuildWrite(ctx, tx, k.TenantID, kbID, knowledgeID); e != nil {
		return e
	}
	var plan nextcloudDerivedBackendPlan
	if e = tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", k.TenantID, kbID, knowledgeID).Take(&plan).Error; e != nil {
		return ErrNextcloudContentGCUncovered
	}
	var backend nextcloudBackendSelection
	if recoverySHA([]byte(plan.BackendPlan)) != plan.PlanSHA256 || json.Unmarshal([]byte(plan.BackendPlan), &backend) != nil {
		return ErrNextcloudContentLeaseDenied
	}
	if kind == "scoped_sql_graph" {
		if !backend.Graph || backend.GraphBackend != NextcloudGraphBackend {
			return ErrNextcloudContentLeaseDenied
		}
	} else if kind != "derived_chunk" {
		expected := string(types.PostgresRetrieverEngineType)
		if kind == "sqlite_embedding" {
			expected = string(types.SQLiteRetrieverEngineType)
		}
		found := false
		for _, v := range backend.Engines {
			if v == expected {
				found = true
			}
		}
		if !found {
			return ErrNextcloudContentLeaseDenied
		}
	}
	identity := strconv.FormatUint(k.TenantID, 10) + "\x00" + kbID + "\x00" + knowledgeID + "\x00" + kind + "\x00" + ref + "\x00" + strconv.FormatInt(dimension, 10) + "\x00" + plan.PlanSHA256
	now, e := nextcloudLeaseNowMS(tx)
	if e != nil {
		return e
	}
	artifact := nextcloudDerivedArtifact{TenantID: k.TenantID, KnowledgeBaseID: kbID, KnowledgeID: knowledgeID, ItemKind: kind, ObjectRef: ref, PlanSHA256: plan.PlanSHA256, IdentitySHA256: recoverySHA([]byte(identity)), Dimension: dimension, WrittenAtMS: now}
	if e = tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&artifact).Error; e != nil {
		return e
	}
	var actual nextcloudDerivedArtifact
	if e = tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND item_kind=? AND object_ref=?", k.TenantID, kbID, knowledgeID, kind, ref).Take(&actual).Error; e != nil {
		return e
	}
	if actual.IdentitySHA256 != artifact.IdentitySHA256 {
		return ErrNextcloudContentLeaseDenied
	}
	return nil
}

// Cross-owner NC chunk pointers are not an ownership transfer. The immutable
// writer ledger keeps this rejection effective even after the original row is
// collected, preventing a late independent writer from reviving a stale ID.
func guardNextcloudDerivedChunkReferences(tx *gorm.DB, chunks []*types.Chunk) error {
	if !tx.Migrator().HasTable(&nextcloudDerivedArtifact{}) {
		return nil
	}
	for _, chunk := range chunks {
		for _, ref := range []string{chunk.ParentChunkID, chunk.PreChunkID, chunk.NextChunkID} {
			if ref == "" {
				continue
			}
			var owners []nextcloudDerivedArtifact
			if e := tx.Where("item_kind='derived_chunk' AND object_ref=?", ref).Find(&owners).Error; e != nil {
				return e
			}
			for _, owner := range owners {
				if owner.TenantID != chunk.TenantID || owner.KnowledgeBaseID != chunk.KnowledgeBaseID || owner.KnowledgeID != chunk.KnowledgeID {
					return ErrNextcloudContentLeaseDenied
				}
			}
		}
	}
	return nil
}
