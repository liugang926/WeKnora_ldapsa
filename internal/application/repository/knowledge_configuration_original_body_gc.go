package repository

import (
	"encoding/json"
	"errors"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func configurationOriginalBodySourcesClosed(tx *gorm.DB, lineageText string, now int64) (bool, int64, error) {
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(lineageText), &lineage) != nil || lineage.RequireComplete() != nil {
		return false, 0, ErrOriginalBodyUnavailable
	}
	closed, deadline := false, int64(0)
	store := NewOriginalBodyRetentionStore(tx)
	for _, source := range lineage.Sources {
		// Only retained positive lifecycle state can close a derived body. Missing
		// rows or a remote authorization error are not a fabricated deletion fact.
		d, current, err := store.bodyKnowledgeDeadline(
			tx,
			source.TenantID,
			source.KnowledgeBaseID,
			source.KnowledgeID,
			"",
			now,
		)
		if err != nil {
			return false, 0, err
		}
		if !current && d >= 0 {
			closed = true
			if d > deadline {
				deadline = d
			}
		}
		if !current && d < 0 {
			return false, 0, ErrOriginalBodyRetained
		}
	}
	return closed, deadline, nil
}

func (s *OriginalBodyRetentionStore) configurationOriginalBodyCurrent(
	tx *gorm.DB,
	head originalBodyFieldHead,
	_ map[string]any,
	now int64,
) (bool, int64, error) {
	if head.AdmissionID == "" {
		return true, 0, nil
	} // Unknown legacy is an explicit retained body.
	var origin knowledgeConfigurationInputOrigin
	if err := bodyInternal(
		tx,
	).Where("origin_id=? AND tenant_id=? AND knowledge_base_id=?", head.AdmissionID, head.TenantID, head.KnowledgeBaseID).
		Take(&origin).
		Error; err != nil {
		return true, 0, err
	}
	if origin.SourceLineage != head.SourceLineage {
		return true, 0, ErrOriginalBodyUnavailable
	}
	if !tx.Migrator().HasTable(&knowledgeConfigurationOriginalBodyManifest{}) {
		return true, 0, ErrOriginalBodyUnavailable
	}
	var manifest knowledgeConfigurationOriginalBodyManifest
	if bodyInternal(tx).Where("origin_id=?", origin.OriginID).Take(&manifest).Error != nil {
		return true, 0, ErrOriginalBodyUnavailable
	}
	digest, err := configurationOriginalBodyManifestDigest(manifest)
	if err != nil || digest != manifest.ManifestSHA256 || manifest.ConfigDigest != origin.InputDigest ||
		manifest.ConfigRevision != origin.ConfigRevision {
		return true, 0, ErrOriginalBodyUnavailable
	}
	var bound []originalBodyFieldHead
	if json.Unmarshal([]byte(manifest.HeadsJSON), &bound) != nil ||
		!configurationOriginalBodyCatalogComplete(manifest, bound, origin) {
		return true, 0, ErrOriginalBodyUnavailable
	}
	actual, err := configurationOriginalBodyHeads(tx, head.TenantID, head.KnowledgeBaseID)
	if err != nil || !sameOriginalBodyHeads(bound, actual) {
		return true, 0, ErrOriginalBodyUnavailable
	}
	if _, err := configurationOriginalBodySavedParents(tx, manifest, actual, origin, false); err != nil {
		return true, 0, err
	}
	found := false
	for _, actual := range bound {
		if actual == head {
			found = true
			break
		}
	}
	if !found {
		return true, 0, ErrOriginalBodyUnavailable
	}
	closed, deadline, err := configurationOriginalBodySourcesClosed(tx, origin.SourceLineage, now)
	if err != nil {
		return true, 0, err
	}
	if closed {
		return false, deadline, nil
	}
	var kb types.KnowledgeBase
	if err := bodyInternal(
		tx,
	).Unscoped().
		Select("id,tenant_id,deleted_at").
		Where("id=? AND tenant_id=?", head.RowKey, head.TenantID).
		Take(&kb).
		Error; err != nil {
		return true, 0, err
	}
	if kb.DeletedAt.Valid {
		return false, kb.DeletedAt.Time.UnixMilli() + 3600000, nil
	}
	return true, 0, nil
}

type configurationOriginalBodyDependentKB struct {
	TenantID        uint64
	KnowledgeBaseID string
}

func configurationOriginalBodyDependentKBs(
	tx *gorm.DB,
	job nextcloudGCJob,
) ([]configurationOriginalBodyDependentKB, error) {
	if !tx.Migrator().HasTable("knowledge_config_input_origins") {
		return nil, nil
	}
	var origins []knowledgeConfigurationInputOrigin
	if err := bodyInternal(tx).Order("tenant_id,knowledge_base_id,config_revision").Find(&origins).Error; err != nil {
		return nil, err
	}
	seen := map[configurationOriginalBodyDependentKB]bool{}
	for _, origin := range origins {
		var lineage types.SourceLineage
		if json.Unmarshal([]byte(origin.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil {
			return nil, ErrOriginalBodyUnavailable
		}
		dependent := false
		for _, source := range lineage.Sources {
			if source.TenantID == job.TenantID && source.KnowledgeBaseID == job.KnowledgeBaseID &&
				source.KnowledgeID == job.KnowledgeID &&
				source.DataSourceID == job.DataSourceID &&
				source.ExternalID == job.ExternalID {
				dependent = true
				break
			}
		}
		if !dependent {
			continue
		}
		// Cross-Tenant derived ownership needs a separate authorized collector. It
		// remains an observable uncovered blocker, never a false local completion.
		if origin.TenantID != job.TenantID {
			return nil, ErrNextcloudContentGCUncovered
		}
		var current knowledgeConfigurationInputOrigin
		current, err := configurationOriginalBodyCurrentOrigin(tx, origin.TenantID, origin.KnowledgeBaseID)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// GC-cleared configurations may have advanced their native controls. A
			// retained head still binds that old generation; never infer supersession
			// from a missing current provenance fact alone.
			var n int64
			if e := bodyInternal(
				tx,
			).Model(&originalBodyFieldHead{}).
				Where(("source_table='knowledge_bases' AND tenant_id=? AND row_key=? AND admission" +
					"_id=?"), origin.TenantID, origin.KnowledgeBaseID, origin.OriginID).
				Count(&n).
				Error; e != nil {
				return nil, e
			}
			if n == 0 {
				continue
			}
			current = origin
			err = nil
		}
		if err != nil {
			return nil, err
		}
		if current.OriginID != origin.OriginID {
			continue
		}
		var manifest knowledgeConfigurationOriginalBodyManifest
		if bodyInternal(tx).Where("origin_id=?", origin.OriginID).Take(&manifest).Error != nil {
			return nil, ErrOriginalBodyUnavailable
		}
		if err := verifyConfigurationOriginalBodyInventoryInTx(
			tx,
			origin.TenantID,
			origin.KnowledgeBaseID,
		); err != nil {
			return nil, err
		}
		seen[configurationOriginalBodyDependentKB{origin.TenantID, origin.KnowledgeBaseID}] = true
	}
	result := []configurationOriginalBodyDependentKB{}
	for target := range seen {
		result = append(result, target)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].TenantID == result[j].TenantID {
			return result[i].KnowledgeBaseID < result[j].KnowledgeBaseID
		}
		return result[i].TenantID < result[j].TenantID
	})
	return result, nil
}

func configurationOriginalBodyDependentTemplateInputs(tx *gorm.DB, job nextcloudGCJob) ([]string, error) {
	if !tx.Migrator().HasTable("knowledge_enrichment_template_inputs") {
		return nil, nil
	}
	var rows []struct {
		ID            string
		TenantID      uint64
		SourceLineage string
	}
	// Parents is the vault body and is deliberately absent from this selector.
	if err := bodyInternal(
		tx,
	).Table("knowledge_enrichment_template_inputs").
		Select("id,tenant_id,source_lineage").
		Order("id").
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	ids := []string{}
	for _, row := range rows {
		var lineage types.SourceLineage
		if json.Unmarshal([]byte(row.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil {
			return nil, ErrOriginalBodyUnavailable
		}
		for _, source := range lineage.Sources {
			if source.TenantID == job.TenantID && source.KnowledgeBaseID == job.KnowledgeBaseID &&
				source.KnowledgeID == job.KnowledgeID &&
				source.DataSourceID == job.DataSourceID &&
				source.ExternalID == job.ExternalID {
				if row.TenantID != job.TenantID {
					return nil, ErrNextcloudContentGCUncovered
				}
				ids = append(ids, row.ID)
				break
			}
		}
	}
	return ids, nil
}
