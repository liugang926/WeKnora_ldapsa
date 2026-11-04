package repository

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// DingTalkRefetchState is a read-only planning hint. It never proves that a
// document remains selected: recovery must still consume a new connector fetch.
func (s *KnowledgeInputProvenanceStore) DingTalkRefetchState(ctx context.Context) (bootstrap bool, tombstones []string, err error) {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok {
		return false, nil, types.ErrSourceLineageUnknown
	}
	var current []string
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := checkDingTalkRun(tx, f); e != nil {
			return e
		}
		var rows []dingTalkPublication
		if e := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ?", f.Tenant, f.KB, f.Source).Find(&rows).Error; e != nil {
			return e
		}
		bootstrap = len(rows) == 0
		for _, row := range rows {
			if row.Tombstone {
				tombstones = append(tombstones, row.ExternalID)
			} else {
				current = append(current, row.ExternalID)
			}
		}
		return nil
	})
	if err == nil {
		for _, id := range current {
			changed, e := s.DingTalkConfigurationChanged(ctx, id)
			if e != nil {
				return false, nil, e
			}
			if changed {
				bootstrap = true
				break
			}
		}
	}
	return
}

// DingTalkConfigurationChanged plans a fresh fetch for changed controls or stale
// local output. It grants no repair authority: fresh publication independently
// verifies source ownership and the newly fetched artifact.
func (s *KnowledgeInputProvenanceStore) DingTalkConfigurationChanged(ctx context.Context, external string) (changed bool, err error) {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok {
		return false, types.ErrSourceLineageUnknown
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := checkDingTalkRun(tx, f); e != nil {
			return e
		}
		var p dingTalkPublication
		e := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ?", f.Tenant, f.KB, f.Source, external).Take(&p).Error
		if errors.Is(e, gorm.ErrRecordNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		if p.Tombstone {
			return nil
		}
		if _, e := loadDingTalkPublishedOriginal(tx, p); e != nil {
			// A damaged local identity requires refetch for this document, not
			// failure of the whole inventory. Publication will still reject it.
			if errors.Is(e, ErrKnowledgeInputChanged) || errors.Is(e, gorm.ErrRecordNotFound) {
				changed = true
				return nil
			}
			return e
		}
		var k types.Knowledge
		if e := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", p.KnowledgeID, f.Tenant, f.KB).Take(&k).Error; e != nil {
			return e
		}
		var material knowledgeInputMaterialReceipt
		if e := tx.Where("row_kind = 'knowledge' AND row_id = ?", k.ID).Take(&material).Error; e != nil {
			// Missing current local derivation is a refetch hint, not source
			// failure. The per-item publisher still validates fresh authority.
			if errors.Is(e, gorm.ErrRecordNotFound) {
				changed = true
				return nil
			}
			return e
		}
		var a knowledgeInputAdmission
		if e := tx.Where("admission_id = ? AND knowledge_id = ? AND tenant_id = ? AND knowledge_base_id = ?", material.AdmissionID, k.ID, f.Tenant, f.KB).Take(&a).Error; e != nil {
			// Missing current local derivation is a refetch hint, not source
			// failure. The per-item publisher still validates fresh authority.
			if errors.Is(e, gorm.ErrRecordNotFound) {
				changed = true
				return nil
			}
			return e
		}
		local, e := isDingTalkLocalReparseAdmission(tx, &a)
		if e != nil {
			return e
		}
		if a.OriginKind != dingTalkOriginalKind && !local {
			return ErrKnowledgeInputChanged
		}
		root, e := knowledgeRootDigest(&k)
		if e != nil {
			return e
		}
		if root != a.RootDigest {
			changed = true
			return nil
		}
		if e := verifyKnowledgeInputMaterial(tx, &a, &k, nil); e != nil {
			if errors.Is(e, ErrKnowledgeInputChanged) {
				changed = true
				return nil
			}
			return e
		}
		kb, e := loadKnowledgeBuildConfigurationInTx(tx, f.Tenant, f.KB)
		if e != nil {
			return e
		}
		revision, e := knowledgeConfigurationControlRevision(tx, kb.ID)
		if e != nil {
			return e
		}
		digest, e := knowledgeConfigDigest(kb, &k)
		if e != nil {
			return e
		}
		changed = revision != a.ConfigRevision || digest != a.ConfigDigest
		return nil
	})
	return
}

// DingTalkRetirementRequired distinguishes an old failed cursor entry which
// never published anything from legacy live knowledge which cannot be adopted.
func (s *KnowledgeInputProvenanceStore) DingTalkRetirementRequired(ctx context.Context, external string) (required bool, err error) {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok {
		return false, types.ErrSourceLineageUnknown
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := checkDingTalkRun(tx, f); err != nil {
			return err
		}
		var p dingTalkPublication
		err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND data_source_id = ? AND external_id = ?", f.Tenant, f.KB, f.Source, external).Take(&p).Error
		if err == nil {
			required = !p.Tombstone
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var count int64
		if err := tx.Model(&types.Knowledge{}).Where("tenant_id = ? AND knowledge_base_id = ? AND metadata->>'datasource_id' = ? AND metadata->>'external_id' = ?", f.Tenant, f.KB, f.Source, external).Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return ErrDingTalkPublicationConflict
		}
		return nil
	})
	return
}

// IsDingTalkSourceTask identifies a denied/deleted source without authorizing
// access to it. Historical claims preserve its type after physical removal.
// Query errors are returned: absence of evidence must not authorize a blind
// generic sync-log update.
func (s *KnowledgeInputProvenanceStore) IsDingTalkSourceTask(ctx context.Context, tenant uint64, source string) (bool, error) {
	var ds types.DataSource
	err := s.db.WithContext(ctx).Unscoped().Where("id = ? AND tenant_id = ?", source, tenant).Take(&ds).Error
	if err == nil {
		return ds.Type == types.ConnectorTypeDingTalk, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}
	var count int64
	err = s.db.WithContext(ctx).Model(&dingTalkRunClaim{}).Where("data_source_id = ? AND tenant_id = ?", source, tenant).Count(&count).Error
	return count != 0, err
}
