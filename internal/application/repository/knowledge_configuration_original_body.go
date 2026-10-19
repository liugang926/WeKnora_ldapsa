package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// These exact SQL representations comprise knowledgeConfigDigest's actual
// JSON controls and templates. IDs, storage credentials, names and computed
// generated profiles have independent metadata/material protocols.
var knowledgeConfigurationOriginalBodyColumns = []string{"chunking_config", "image_processing_config", "vlm_config", "asr_config", "extract_config", "faq_config", "question_generation_config", "auto_tag_config", "profile_config", "wiki_config", "indexing_strategy", "enrichment_templates"}

type knowledgeConfigurationOriginalBodyManifest struct {
	OriginID                                                               string `gorm:"primaryKey"`
	TenantID                                                               uint64
	KnowledgeBaseID                                                        string
	ConfigRevision                                                         uint64
	ConfigDigest, HeadsJSON, EmptyColumnsJSON, ParentsJSON, ManifestSHA256 string
}

func (knowledgeConfigurationOriginalBodyManifest) TableName() string {
	return "knowledge_configuration_original_body_manifests"
}

type KnowledgeConfigurationOriginalBodyReadPlan struct {
	TenantID                  uint64
	KnowledgeBaseID, OriginID string
	ConfigRevision            uint64
	ConfigDigest              string
	manifest                  knowledgeConfigurationOriginalBodyManifest
	heads                     []originalBodyFieldHead
	parents                   []originalBodyParentRef
	bodies                    []originalBodyPayload
	leaseMu                   sync.Mutex
	leaseOwners               map[string]string
}

func configurationOriginalBodyCurrentOrigin(tx *gorm.DB, tenant uint64, kb string) (knowledgeConfigurationInputOrigin, error) {
	var fact knowledgeConfigurationInputOrigin
	if !tx.Migrator().HasTable("knowledge_config_input_origins") || !tx.Migrator().HasTable("knowledge_configuration_control_versions") {
		return fact, gorm.ErrRecordNotFound
	}
	err := bodyInternal(tx).Table("knowledge_config_input_origins o").Select("o.*").Joins("JOIN knowledge_configuration_control_versions v ON v.knowledge_base_id=o.knowledge_base_id AND v.revision=o.config_revision").Where("o.tenant_id=? AND o.knowledge_base_id=?", tenant, kb).Take(&fact).Error
	return fact, err
}
func configurationOriginalBodyManifestDigest(m knowledgeConfigurationOriginalBodyManifest) (string, error) {
	m.ManifestSHA256 = ""
	return KnowledgeInputDigest(m)
}
func configurationOriginalBodyHeads(tx *gorm.DB, tenant uint64, kb string) ([]originalBodyFieldHead, error) {
	var heads []originalBodyFieldHead
	err := bodyInternal(tx).Where("source_table='knowledge_bases' AND row_key=? AND tenant_id=?", kb, tenant).Order("column_name").Find(&heads).Error
	return heads, err
}
func configurationOriginalBodyCatalogComplete(m knowledgeConfigurationOriginalBodyManifest, heads []originalBodyFieldHead, fact knowledgeConfigurationInputOrigin) bool {
	var empty []string
	if json.Unmarshal([]byte(m.EmptyColumnsJSON), &empty) != nil {
		return false
	}
	allowed := map[string]bool{}
	for _, column := range knowledgeConfigurationOriginalBodyColumns {
		allowed[column] = true
	}
	seen := map[string]bool{}
	for _, column := range empty {
		if !allowed[column] || seen[column] {
			return false
		}
		seen[column] = true
	}
	scope := originalBodyScope{Tenant: fact.TenantID, KB: fact.KnowledgeBaseID, OwnerKind: "knowledge_bases", OwnerID: fact.KnowledgeBaseID}
	for _, head := range heads {
		if !allowed[head.ColumnName] || seen[head.ColumnName] || head.SourceTable != "knowledge_bases" || head.RowKey != fact.KnowledgeBaseID || head.TenantID != fact.TenantID || head.KnowledgeBaseID != fact.KnowledgeBaseID || head.KnowledgeID != "" || head.OwnerKind != scope.OwnerKind || head.OwnerID != scope.OwnerID || head.AdmissionID != fact.OriginID || head.SourceLineage != fact.SourceLineage || head.SourceIdentitySHA256 != bodyIdentity("knowledge_bases", head.RowKey, head.ColumnName, scope) {
			return false
		}
		ref, ok := bodyReference(head.OriginalBodyRef)
		if !ok || ref.SlotID != head.SlotID || ref.BodyID != head.BodyID {
			return false
		}
		seen[head.ColumnName] = true
	}
	return len(seen) == len(allowed)
}

// Called after the actual producer persisted its immutable origin, or by the
// explicit offline storage inventory after verifying those same original
// bytes against the previously persisted origin. Never creates an input origin.
func bindKnowledgeConfigurationOriginalBodiesInTx(tx *gorm.DB, kb *types.KnowledgeBase, fact knowledgeConfigurationInputOrigin) error {
	if !originalBodySchemaPresent(tx) {
		return nil
	}
	if !tx.Migrator().HasTable(&knowledgeConfigurationOriginalBodyManifest{}) {
		return ErrOriginalBodyUnavailable
	}
	digest, err := knowledgeConfigurationOriginDigest(kb)
	if err != nil || digest != fact.InputDigest || kb.ID != fact.KnowledgeBaseID || kb.TenantID != fact.TenantID {
		return ErrOriginalBodyUnavailable
	}
	d, ok := bodyDescriptor("knowledge_bases")
	if !ok {
		return ErrOriginalBodyUnavailable
	}
	raw, err := bodySourceRow(tx, d, kb.ID)
	if err != nil {
		return err
	}
	scope := originalBodyScope{Tenant: kb.TenantID, KB: kb.ID, OwnerKind: d.Table, OwnerID: kb.ID, AdmissionID: fact.OriginID, Lineage: fact.SourceLineage}
	store := NewOriginalBodyRetentionStore(tx)
	empty := []string{}
	for _, column := range d.Columns {
		value, err := bodyFieldValue(raw, column)
		if err != nil {
			return err
		}
		if value == "" || value == "null" {
			empty = append(empty, column)
			// The real producer has removed this representation. Retain its immutable
			// revision and deadline, while removing only the obsolete current pointer.
			var prior originalBodyFieldHead
			if err := bodyInternal(tx).Where("source_table=? AND row_key=? AND column_name=?", d.Table, kb.ID, column).Take(&prior).Error; err == nil {
				now, err := nextcloudLeaseNowMS(bodyInternal(tx))
				if err != nil {
					return err
				}
				if err := bodyInternal(tx).Model(&originalBodyPayload{}).Where("id=? AND retired_at_ms IS NULL", prior.BodyID).Updates(map[string]any{"retired_at_ms": now, "body_deadline_ms": now + 3600000, "metadata_deadline_ms": now + 90*86400000}).Error; err != nil {
					return err
				}
				if err := bodyInternal(tx).Where("slot_id=?", prior.SlotID).Delete(&originalBodyFieldHead{}).Error; err != nil {
					return err
				}
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			continue
		}
		if err := store.captureField(tx, d, kb.ID, column, value, scope); err != nil {
			return err
		}
	}
	heads, err := configurationOriginalBodyHeads(tx, kb.TenantID, kb.ID)
	if err != nil {
		return err
	}
	for _, head := range heads {
		if head.AdmissionID != fact.OriginID || head.SourceLineage != fact.SourceLineage {
			return ErrOriginalBodyUnavailable
		}
	}
	sort.Strings(empty)
	encoded, err := json.Marshal(heads)
	if err != nil {
		return err
	}
	empties, err := json.Marshal(empty)
	if err != nil {
		return err
	}
	parents, err := configurationOriginalBodyParents(tx, heads)
	if err != nil {
		return err
	}
	if err := configurationOriginalBodyParentCoverage(tx, parents, heads, fact, true); err != nil {
		return err
	}
	parentJSON, err := json.Marshal(parents)
	if err != nil {
		return err
	}
	manifest := knowledgeConfigurationOriginalBodyManifest{OriginID: fact.OriginID, TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, ConfigRevision: fact.ConfigRevision, ConfigDigest: fact.InputDigest, HeadsJSON: string(encoded), EmptyColumnsJSON: string(empties), ParentsJSON: string(parentJSON)}
	manifest.ManifestSHA256, err = configurationOriginalBodyManifestDigest(manifest)
	if err != nil {
		return err
	}
	return bodyInternal(tx).Create(&manifest).Error
}

// Explicit DB-owner forward inventory, never a request-side fallback. An old
// unknown configuration cannot acquire lineage or a new origin here. Missing
// schema, digest mismatch, external purge/rollback or missing parent receipts
// refuses the whole transaction and leaves it an observable blocker.
func (s *KnowledgeInputProvenanceStore) InventoryKnownKnowledgeConfigurationOriginalBodies(ctx context.Context, tenant uint64, kbID string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if !originalBodySchemaPresent(tx) || !tx.Migrator().HasTable(&knowledgeConfigurationOriginalBodyManifest{}) {
			return ErrOriginalBodyUnavailable
		}
		fact, err := configurationOriginalBodyCurrentOrigin(tx, tenant, kbID)
		if err != nil {
			return ErrOriginalBodyUnavailable
		}
		var actual types.KnowledgeBase
		if err := bodyInternal(tx).Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", kbID, tenant).Take(&actual).Error; err != nil {
			return err
		}
		actualDigest, err := knowledgeConfigurationOriginDigest(&actual)
		if err != nil || actualDigest != fact.InputDigest {
			return ErrOriginalBodyUnavailable
		}
		var prior knowledgeConfigurationOriginalBodyManifest
		err = bodyInternal(tx).Where("origin_id=?", fact.OriginID).Take(&prior).Error
		if err == nil {
			digest, e := configurationOriginalBodyManifestDigest(prior)
			if e != nil || digest != prior.ManifestSHA256 || prior.ConfigDigest != fact.InputDigest || prior.ConfigRevision != fact.ConfigRevision {
				return ErrOriginalBodyUnavailable
			}
			var bound []originalBodyFieldHead
			if json.Unmarshal([]byte(prior.HeadsJSON), &bound) != nil || !configurationOriginalBodyCatalogComplete(prior, bound, fact) {
				return ErrOriginalBodyUnavailable
			}
			heads, e := configurationOriginalBodyHeads(tx, tenant, kbID)
			if e != nil || !sameOriginalBodyHeads(bound, heads) {
				return ErrOriginalBodyUnavailable
			}
			if _, e := configurationOriginalBodySavedParents(tx, prior, heads, fact, true); e != nil {
				return e
			}
			return CheckOriginalBodyAvailableInTx(ctx, tx, "knowledge_bases", kbID)
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		return bindKnowledgeConfigurationOriginalBodiesInTx(tx, &actual, fact)
	})
}

func recordConfigurationOriginalBodyParents(tx *gorm.DB, bodyID string, scope originalBodyScope) error {
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(scope.Lineage), &lineage) != nil || lineage.RequireComplete() != nil {
		return ErrOriginalBodyUnavailable
	}
	for _, source := range lineage.Sources {
		var receipt knowledgeInputMaterialReceipt
		if bodyInternal(tx).Where("row_kind='knowledge' AND row_id=?", source.KnowledgeID).Take(&receipt).Error != nil {
			return ErrOriginalBodyUnavailable
		}
		var admission knowledgeInputAdmission
		if bodyInternal(tx).Where("admission_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", receipt.AdmissionID, source.TenantID, source.KnowledgeBaseID, source.KnowledgeID).Take(&admission).Error != nil {
			return ErrOriginalBodyUnavailable
		}
		var admitted types.SourceLineage
		if json.Unmarshal([]byte(admission.SourceLineage), &admitted) != nil || admitted.RequireComplete() != nil {
			return ErrOriginalBodyUnavailable
		}
		exact := false
		for _, original := range admitted.Sources {
			if original == source {
				exact = true
				break
			}
		}
		if !exact {
			return ErrOriginalBodyUnavailable
		}
		parent := originalBodyParentRef{BodyID: bodyID, TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, AdmissionID: admission.AdmissionID}
		if err := bodyInternal(tx).Clauses(clause.OnConflict{DoNothing: true}).Create(&parent).Error; err != nil {
			return err
		}
	}
	return nil
}

// Metadata-only plan: no knowledge_bases config or template body is selected.
func (s *KnowledgeInputProvenanceStore) KnowledgeConfigurationBodyReadPlan(ctx context.Context, tenant uint64, kb string) (*KnowledgeConfigurationOriginalBodyReadPlan, error) {
	var result *KnowledgeConfigurationOriginalBodyReadPlan
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		fact, err := configurationOriginalBodyCurrentOrigin(tx, tenant, kb)
		if err != nil {
			return ErrOriginalBodyUnavailable
		}
		if !tx.Migrator().HasTable(&knowledgeConfigurationOriginalBodyManifest{}) {
			return ErrOriginalBodyUnavailable
		}
		var m knowledgeConfigurationOriginalBodyManifest
		if err := bodyInternal(tx).Where("origin_id=? AND tenant_id=? AND knowledge_base_id=?", fact.OriginID, tenant, kb).Take(&m).Error; err != nil {
			return ErrOriginalBodyUnavailable
		}
		digest, err := configurationOriginalBodyManifestDigest(m)
		if err != nil || digest != m.ManifestSHA256 || m.ConfigRevision != fact.ConfigRevision || m.ConfigDigest != fact.InputDigest {
			return ErrOriginalBodyUnavailable
		}
		var saved []originalBodyFieldHead
		if json.Unmarshal([]byte(m.HeadsJSON), &saved) != nil || !configurationOriginalBodyCatalogComplete(m, saved, fact) {
			return ErrOriginalBodyUnavailable
		}
		actual, err := configurationOriginalBodyHeads(tx, tenant, kb)
		if err != nil || !sameOriginalBodyHeads(saved, actual) {
			return ErrOriginalBodyUnavailable
		}
		parents, err := configurationOriginalBodySavedParents(tx, m, actual, fact, true)
		if err != nil {
			return err
		}
		plan := &KnowledgeConfigurationOriginalBodyReadPlan{TenantID: tenant, KnowledgeBaseID: kb, OriginID: fact.OriginID, ConfigRevision: fact.ConfigRevision, ConfigDigest: fact.InputDigest, manifest: m, heads: saved, parents: parents}
		seen := map[string]bool{}
		for _, head := range saved {
			if head.AdmissionID != fact.OriginID || head.SourceLineage != fact.SourceLineage {
				return ErrOriginalBodyUnavailable
			}
			if seen[head.BodyID] {
				continue
			}
			seen[head.BodyID] = true
			var body originalBodyPayload
			if err := bodyInternal(tx).Select("id,tenant_id,body_sha256,origin_domain_sha256,body_bytes,state,purge_epoch,storage_kind").Where("id=?", head.BodyID).Take(&body).Error; err != nil || body.State != "live" || body.StorageKind != "inline" || body.TenantID != tenant || body.BodyBytes <= 0 || body.OriginDomainSHA256 != bodySHA([]byte(fmt.Sprintf("%d/%s/%s/%s", tenant, kb, "", "admission/"+fact.OriginID))) {
				return ErrOriginalBodyUnavailable
			}
			ref, ok := bodyReference(head.OriginalBodyRef)
			if !ok || ref.SHA256 != body.BodySHA256 {
				return ErrOriginalBodyUnavailable
			}
			plan.bodies = append(plan.bodies, body)
		}
		result = plan
		return ctx.Err()
	})
	return result, err
}

func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeConfigurationBodyReadLeasesInTx(ctx context.Context, tx *gorm.DB, plan *KnowledgeConfigurationOriginalBodyReadPlan, ids []string) error {
	if plan == nil {
		return ErrOriginalBodyUnavailable
	}
	owners := map[string]string{}
	plan.leaseMu.Lock()
	for _, id := range ids {
		owners[id] = plan.leaseOwners[id]
	}
	plan.leaseMu.Unlock()
	return s.recheckKnowledgeConfigurationBodyReadLeasesInTx(ctx, tx, plan, ids, owners)
}
func (s *KnowledgeInputProvenanceStore) recheckKnowledgeConfigurationBodyReadLeasesInTx(ctx context.Context, tx *gorm.DB, plan *KnowledgeConfigurationOriginalBodyReadPlan, ids []string, owners map[string]string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if plan == nil || len(ids) != len(plan.bodies) {
		return ErrOriginalBodyUnavailable
	}
	fact, err := configurationOriginalBodyCurrentOrigin(tx, plan.TenantID, plan.KnowledgeBaseID)
	if err != nil || fact.OriginID != plan.OriginID || fact.ConfigRevision != plan.ConfigRevision || fact.InputDigest != plan.ConfigDigest {
		return ErrOriginalBodyUnavailable
	}
	var m knowledgeConfigurationOriginalBodyManifest
	if bodyInternal(tx).Where("origin_id=?", plan.OriginID).Take(&m).Error != nil || m != plan.manifest {
		return ErrOriginalBodyUnavailable
	}
	heads, err := configurationOriginalBodyHeads(tx, plan.TenantID, plan.KnowledgeBaseID)
	if err != nil || !sameOriginalBodyHeads(heads, plan.heads) {
		return ErrOriginalBodyUnavailable
	}
	parents, err := configurationOriginalBodySavedParents(tx, m, heads, fact, true)
	if err != nil {
		return err
	}
	a, _ := json.Marshal(parents)
	b, _ := json.Marshal(plan.parents)
	if string(a) != string(b) {
		return ErrOriginalBodyUnavailable
	}
	now, err := nextcloudLeaseNowMS(bodyInternal(tx))
	if err != nil {
		return err
	}
	for i, original := range plan.bodies {
		var body originalBodyPayload
		if bodyInternal(tx).Select("id,tenant_id,body_sha256,origin_domain_sha256,body_bytes,state,purge_epoch,storage_kind").Where("id=?", original.ID).Take(&body).Error != nil || body.ID != original.ID || body.TenantID != original.TenantID || body.BodySHA256 != original.BodySHA256 || body.OriginDomainSHA256 != original.OriginDomainSHA256 || body.BodyBytes != original.BodyBytes || body.State != "live" || body.PurgeEpoch != original.PurgeEpoch || body.StorageKind != original.StorageKind {
			return ErrOriginalBodyUnavailable
		}
		var lease originalBodyLease
		if bodyInternal(tx).Where("lease_id=? AND body_id=? AND kind='read' AND released_at_ms IS NULL AND expires_at_ms>?", ids[i], original.ID, now).Take(&lease).Error != nil || owners[ids[i]] == "" || lease.OwnerID != owners[ids[i]] || lease.Epoch != original.PurgeEpoch {
			return ErrOriginalBodyUnavailable
		}
	}
	return CheckOriginalBodyAvailableInTx(ctx, tx, "knowledge_bases", plan.KnowledgeBaseID)
}
func (s *KnowledgeInputProvenanceStore) AcquireKnowledgeConfigurationBodyReadLeases(ctx context.Context, plan *KnowledgeConfigurationOriginalBodyReadPlan, owner string, ttl time.Duration) ([]string, error) {
	if plan == nil || owner == "" || ttl <= 0 || ttl > 30*time.Minute {
		return nil, ErrOriginalBodyUnavailable
	}
	ids := []string{}
	owners := map[string]string{}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		store := NewOriginalBodyRetentionStore(tx)
		for _, body := range plan.bodies {
			id, err := store.acquireLeaseInTx(bodyInternal(tx), body.ID, "read", owner, ttl)
			if err != nil {
				return err
			}
			ids = append(ids, id)
			owners[id] = owner
		}
		return s.recheckKnowledgeConfigurationBodyReadLeasesInTx(ctx, tx, plan, ids, owners)
	})
	if err != nil {
		return nil, err
	}
	plan.leaseMu.Lock()
	if plan.leaseOwners == nil {
		plan.leaseOwners = map[string]string{}
	}
	for id, owner := range owners {
		plan.leaseOwners[id] = owner
	}
	plan.leaseMu.Unlock()
	return ids, nil
}
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeConfigurationBodyReadLeases(ctx context.Context, plan *KnowledgeConfigurationOriginalBodyReadPlan, ids []string, ttl time.Duration) error {
	if ttl <= 0 || ttl > 30*time.Minute {
		return ErrOriginalBodyUnavailable
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.RecheckKnowledgeConfigurationBodyReadLeasesInTx(ctx, tx, plan, ids); err != nil {
			return err
		}
		now, err := nextcloudLeaseNowMS(bodyInternal(tx))
		if err != nil {
			return err
		}
		for _, id := range ids {
			q := bodyInternal(tx).Model(&originalBodyLease{}).Where("lease_id=? AND released_at_ms IS NULL AND expires_at_ms>?", id, now).Update("expires_at_ms", now+ttl.Milliseconds())
			if q.Error != nil {
				return q.Error
			}
			if q.RowsAffected != 1 {
				return ErrOriginalBodyUnavailable
			}
		}
		return ctx.Err()
	})
}

// GC inventory may consume and verify existing inline fields, but cannot fill
// a missing committed configuration head from current bytes. A closed body
// retains its old immutable manifest even when clearing advances native config
// controls; only exact retained original heads can identify that generation.
func verifyConfigurationOriginalBodyInventoryInTx(tx *gorm.DB, tenant uint64, kb string) error {
	if !tx.Migrator().HasTable(&knowledgeConfigurationOriginalBodyManifest{}) {
		return ErrOriginalBodyUnavailable
	}
	heads, err := configurationOriginalBodyHeads(tx, tenant, kb)
	if err != nil {
		return err
	}
	fact, err := configurationOriginalBodyCurrentOrigin(tx, tenant, kb)
	if errors.Is(err, gorm.ErrRecordNotFound) && len(heads) > 0 && heads[0].AdmissionID != "" {
		err = bodyInternal(tx).Where("origin_id=? AND tenant_id=? AND knowledge_base_id=?", heads[0].AdmissionID, tenant, kb).Take(&fact).Error
	}
	if err != nil {
		return ErrOriginalBodyUnavailable
	}
	var m knowledgeConfigurationOriginalBodyManifest
	if bodyInternal(tx).Where("origin_id=? AND tenant_id=? AND knowledge_base_id=?", fact.OriginID, tenant, kb).Take(&m).Error != nil {
		return ErrOriginalBodyUnavailable
	}
	digest, err := configurationOriginalBodyManifestDigest(m)
	if err != nil || digest != m.ManifestSHA256 || m.ConfigRevision != fact.ConfigRevision || m.ConfigDigest != fact.InputDigest {
		return ErrOriginalBodyUnavailable
	}
	var bound []originalBodyFieldHead
	if json.Unmarshal([]byte(m.HeadsJSON), &bound) != nil || !configurationOriginalBodyCatalogComplete(m, bound, fact) || !sameOriginalBodyHeads(bound, heads) {
		return ErrOriginalBodyUnavailable
	}
	_, err = configurationOriginalBodySavedParents(tx, m, heads, fact, false)
	return err
}

func configurationOriginalBodyParents(tx *gorm.DB, heads []originalBodyFieldHead) ([]originalBodyParentRef, error) {
	bodies := []string{}
	seen := map[string]bool{}
	for _, head := range heads {
		if !seen[head.BodyID] {
			seen[head.BodyID] = true
			bodies = append(bodies, head.BodyID)
		}
	}
	parents := []originalBodyParentRef{}
	if len(bodies) > 0 {
		if err := bodyInternal(tx).Where("body_id IN ?", bodies).Order("body_id,tenant_id,knowledge_base_id,knowledge_id,admission_id").Find(&parents).Error; err != nil {
			return nil, err
		}
	}
	return parents, nil
}

func configurationOriginalBodyParentCoverage(tx *gorm.DB, parents []originalBodyParentRef, heads []originalBodyFieldHead, fact knowledgeConfigurationInputOrigin, requireCurrent bool) error {
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(fact.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil {
		return ErrOriginalBodyUnavailable
	}
	bodies := map[string]bool{}
	for _, head := range heads {
		bodies[head.BodyID] = true
	}
	if len(parents) != len(bodies)*len(lineage.Sources) {
		return ErrOriginalBodyUnavailable
	}
	seen := map[originalBodyParentRef]bool{}
	for _, parent := range parents {
		if !bodies[parent.BodyID] || parent.AdmissionID == "" || seen[parent] {
			return ErrOriginalBodyUnavailable
		}
		seen[parent] = true
		var admission knowledgeInputAdmission
		if bodyInternal(tx).Where("admission_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", parent.AdmissionID, parent.TenantID, parent.KnowledgeBaseID, parent.KnowledgeID).Take(&admission).Error != nil {
			return ErrOriginalBodyUnavailable
		}
		var admitted types.SourceLineage
		if json.Unmarshal([]byte(admission.SourceLineage), &admitted) != nil || admitted.RequireComplete() != nil {
			return ErrOriginalBodyUnavailable
		}
		exact := false
		for _, source := range lineage.Sources {
			if source.TenantID != parent.TenantID || source.KnowledgeBaseID != parent.KnowledgeBaseID || source.KnowledgeID != parent.KnowledgeID {
				continue
			}
			for _, original := range admitted.Sources {
				if original == source {
					exact = true
					break
				}
			}
		}
		if !exact {
			return ErrOriginalBodyUnavailable
		}
		if requireCurrent {
			var receipt knowledgeInputMaterialReceipt
			if bodyInternal(tx).Where("row_kind='knowledge' AND row_id=? AND admission_id=?", parent.KnowledgeID, parent.AdmissionID).Take(&receipt).Error != nil {
				return ErrOriginalBodyUnavailable
			}
		}
	}
	// Validate every body/source pair, so equal counts cannot conceal a missing
	// dependency behind a duplicate admission for another source.
	for body := range bodies {
		for _, source := range lineage.Sources {
			matches := 0
			for _, parent := range parents {
				if parent.BodyID == body && parent.TenantID == source.TenantID && parent.KnowledgeBaseID == source.KnowledgeBaseID && parent.KnowledgeID == source.KnowledgeID {
					matches++
				}
			}
			if matches != 1 {
				return ErrOriginalBodyUnavailable
			}
		}
	}
	return nil
}

func configurationOriginalBodySavedParents(tx *gorm.DB, m knowledgeConfigurationOriginalBodyManifest, heads []originalBodyFieldHead, fact knowledgeConfigurationInputOrigin, requireCurrent bool) ([]originalBodyParentRef, error) {
	var saved []originalBodyParentRef
	if json.Unmarshal([]byte(m.ParentsJSON), &saved) != nil || saved == nil {
		return nil, ErrOriginalBodyUnavailable
	}
	actual, err := configurationOriginalBodyParents(tx, heads)
	if err != nil {
		return nil, err
	}
	a, err := json.Marshal(actual)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(saved)
	if err != nil || string(a) != string(b) {
		return nil, ErrOriginalBodyUnavailable
	}
	if err := configurationOriginalBodyParentCoverage(tx, actual, heads, fact, requireCurrent); err != nil {
		return nil, err
	}
	return actual, nil
}
