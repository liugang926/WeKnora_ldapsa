package repository

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
	"time"
)

// bodyOwnerDeadline derives closure from actual rows/heads. Unknown legacy
// ownership is retained; a caller cannot pass ready/closed booleans or dates.
func (s *OriginalBodyRetentionStore) bodyKnowledgeDeadline(tx *gorm.DB, tenant uint64, kb, id, admission string, now int64) (int64, bool, error) {
	var k types.Knowledge
	e := bodyInternal(tx).Unscoped().Where("id=? AND tenant_id=? AND knowledge_base_id=?", id, tenant, kb).Take(&k).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return now + 3600000, false, nil
	}
	if e != nil {
		return 0, false, e
	}
	if k.Channel == types.ConnectorTypeNextcloud {
		meta := k.GetMetadata()
		if meta["datasource_id"] == "" || meta["external_id"] == "" {
			return 0, true, nil
		}
		var version nextcloudSourceVersion
		if e := nextcloudVersionQuery(bodyInternal(tx), tenant, kb, meta["datasource_id"], meta["external_id"]).Take(&version).Error; e != nil {
			return 0, true, e
		}
		if version.State != "tombstone" && version.CandidateKnowledgeID == id {
			return 0, true, nil
		}
		var fence nextcloudContentFence
		if e := bodyInternal(tx).Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", tenant, kb, id).Take(&fence).Error; e != nil {
			return 0, true, e
		}
		if fence.State != "retired" || fence.RetiredAtMS == nil {
			return 0, true, nil
		}
		if version.State == "tombstone" {
			return *fence.RetiredAtMS, false, nil
		}
		if k.PendingSubtasksCount > 0 {
			return 0, true, nil
		}
		window := int64(3600000)
		if k.ParseStatus == types.ParseStatusFailed || k.ParseStatus == types.ParseStatusCancelled {
			window = 24 * 3600000
		}
		return *fence.RetiredAtMS + window, false, nil
	}
	if k.DeletedAt.Valid {
		return k.DeletedAt.Time.UnixMilli() + 3600000, false, nil
	}
	if admission != "" && tx.Migrator().HasTable("knowledge_input_material_receipts") {
		var receipt knowledgeInputMaterialReceipt
		if e := bodyInternal(tx).Where("row_kind='knowledge' AND row_id=?", id).Take(&receipt).Error; e == nil && receipt.AdmissionID != admission {
			return now + 3600000, false, nil
		}
	}
	return 0, true, nil
}
func (s *OriginalBodyRetentionStore) fieldCurrent(tx *gorm.DB, head originalBodyFieldHead, now int64) (bool, int64, error) {
	var currentHead originalBodyFieldHead
	if e := bodyInternal(tx).Where("slot_id=?", head.SlotID).Take(&currentHead).Error; e != nil {
		return true, 0, e
	}
	if currentHead.BodyID != head.BodyID {
		return false, 0, nil
	}
	d, ok := bodyDescriptor(head.SourceTable)
	if !ok || d.Mode == "unsupported" {
		return true, 0, nil
	}
	row, e := bodySourceRow(tx, d, head.RowKey)
	if errors.Is(e, gorm.ErrRecordNotFound) {
		return false, now + 3600000, nil
	}
	if e != nil {
		return true, 0, e
	}
	scope, e := bodyScopeFromRow(tx, d, head.RowKey, row)
	if e != nil {
		return true, 0, e
	}
	if bodyIdentity(d.Table, head.RowKey, head.ColumnName, scope) != head.SourceIdentitySHA256 {
		return true, 0, ErrOriginalBodyUnavailable
	}
	if scope.Knowledge != "" {
		deadline, current, e := s.bodyKnowledgeDeadline(tx, scope.Tenant, scope.KB, scope.Knowledge, head.AdmissionID, now)
		return current, deadline, e
	}
	switch d.Table {
	case "faq_input_sessions":
		var refs int64
		if e := bodyInternal(tx).Table("faq_entry_material_receipts r").Joins("JOIN faq_entry_input_admissions a ON a.id=r.admission_id").Where("a.session_id=?", head.RowKey).Count(&refs).Error; e != nil {
			return true, 0, e
		}
		if refs > 0 {
			return true, 0, nil
		}
		return false, now + 3600000, nil
	case "faq_import_output_admissions":
		var refs int64
		if e := bodyInternal(tx).Table("faq_import_output_heads").Where("admission_id=?", head.RowKey).Count(&refs).Error; e != nil {
			return true, 0, e
		}
		if refs > 0 {
			return true, 0, nil
		}
		return false, now + 3600000, nil
	case "knowledge_display_input_origins":
		var version struct{ Revision int64 }
		if e := bodyInternal(tx).Table("knowledge_display_versions").Where("kind=? AND row_id=?", row["kind"], row["row_id"]).Take(&version).Error; e != nil {
			return true, 0, e
		}
		if version.Revision == int64(bodyUint(row["revision"])) {
			return true, 0, nil
		}
		return false, now + 3600000, nil
	case "messages", "message_generation_origins":
		message := row
		if d.Table == "message_generation_origins" {
			message, e = bodySourceRow(tx, originalBodyFieldDescriptor{Table: "messages", Key: "id"}, bodyText(row["message_id"]))
			if e != nil {
				return true, 0, e
			}
		}
		if message["deleted_at"] != nil && bodyText(message["deleted_at"]) != "" {
			return false, now + 3600000, nil
		}
		return true, 0, nil
	case "sessions":
		if row["deleted_at"] != nil && bodyText(row["deleted_at"]) != "" {
			return false, now + 3600000, nil
		}
		return true, 0, nil
	case "knowledge_enrichment_template_inputs":
		// Each actual captured parent is checked below; no current configuration
		// or absent result rows can prove this original was unused.
		var parents int64
		if e := bodyInternal(tx).Model(&originalBodyParentRef{}).Where("body_id=?", head.BodyID).Count(&parents).Error; e != nil {
			return true, 0, e
		}
		if parents == 0 {
			return true, 0, nil
		}
		return false, now + 3600000, nil
	default:
		return true, 0, nil
	}
}
func (s *OriginalBodyRetentionStore) bodyEligibility(tx *gorm.DB, body originalBodyPayload, heads []originalBodyFieldHead, now int64) (int64, error) {
	deadline := int64(0)
	for _, head := range heads {
		current, d, e := s.fieldCurrent(tx, head, now)
		if e != nil {
			return 0, e
		}
		if current {
			return 0, ErrOriginalBodyRetained
		}
		if d > deadline {
			deadline = d
		}
	}
	var parents []originalBodyParentRef
	if e := bodyInternal(tx).Where("body_id=?", body.ID).Find(&parents).Error; e != nil {
		return 0, e
	}
	// Provenance inputs supply lock/lease scopes, not incoming ownership. A
	// superseded child snapshot must not be kept merely because its source is
	// still current. Actual incoming field heads above protect shared payloads.
	if len(heads) > 0 {
		for _, parent := range parents {
			d, current, e := s.bodyKnowledgeDeadline(tx, parent.TenantID, parent.KnowledgeBaseID, parent.KnowledgeID, parent.AdmissionID, now)
			if e != nil {
				return 0, e
			}
			if !current && d > deadline {
				deadline = d
			}
		}
	}
	// A prior real supersession/closure deadline is never moved later merely by
	// repeated scans. Current refs and active leases still take precedence.
	if body.BodyDeadlineMS != nil && *body.BodyDeadlineMS > deadline {
		deadline = *body.BodyDeadlineMS
	}
	if deadline == 0 {
		if len(heads) == 0 && body.RetiredAtMS == nil {
			return 0, ErrOriginalBodyRetained
		}
		deadline = now + 3600000
	}
	return deadline, nil
}
func (s *OriginalBodyRetentionStore) lockBodyScopes(tx *gorm.DB, heads []originalBodyFieldHead, parents []originalBodyParentRef) error {
	type scope struct {
		Tenant uint64
		KB     string
	}
	keys := map[scope]bool{}
	for _, h := range heads {
		if h.KnowledgeBaseID != "" {
			keys[scope{h.TenantID, h.KnowledgeBaseID}] = true
		}
	}
	for _, p := range parents {
		if p.KnowledgeBaseID != "" {
			keys[scope{p.TenantID, p.KnowledgeBaseID}] = true
		}
	}
	scopes := []scope{}
	for key := range keys {
		scopes = append(scopes, key)
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].Tenant == scopes[j].Tenant {
			return scopes[i].KB < scopes[j].KB
		}
		return scopes[i].Tenant < scopes[j].Tenant
	})
	for _, key := range scopes {
		var kb types.KnowledgeBase
		if e := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", key.Tenant, key.KB).Take(&kb).Error; e != nil {
			return e
		}
		if tx.Migrator().HasTable("nextcloud_content_fences") {
			now, e := nextcloudLeaseNowMS(tx)
			if e != nil {
				return e
			}
			if _, e := nextcloudLockKBFence(bodyInternal(tx), key.Tenant, key.KB, now); e != nil {
				return e
			}
			var active int64
			if e := bodyInternal(tx).Table("nextcloud_content_leases").Where("tenant_id=? AND knowledge_base_id=? AND released_at_ms IS NULL AND expires_at_ms>?", key.Tenant, key.KB, now).Count(&active).Error; e != nil {
				return e
			}
			if active != 0 {
				return ErrOriginalBodyRetained
			}
		}
	}
	return nil
}

// PurgeDue uses DB time; advancing a scheduler argument cannot shorten any
// body/recovery deadline. External append and exact DB ACK occur under fences.
func (s *OriginalBodyRetentionStore) PurgeDue(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if s.journal == nil {
		return 0, ErrOriginalBodyUnavailable
	}
	var ids []string
	if e := bodyInternal(s.db.WithContext(ctx)).Model(&originalBodyPayload{}).Where("state='live'").Order("created_at_ms,id").Limit(limit).Pluck("id", &ids).Error; e != nil {
		return 0, e
	}
	done := 0
	for _, id := range ids {
		collected := false
		e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if e := s.checkJournalInTx(tx); e != nil {
				return e
			}
			var heads []originalBodyFieldHead
			if e := bodyInternal(tx).Table("original_body_field_revisions").Where("body_id=?", id).Order("source_table,row_key,column_name,revision").Find(&heads).Error; e != nil {
				return e
			}
			var parents []originalBodyParentRef
			if e := bodyInternal(tx).Where("body_id=?", id).Order("tenant_id,knowledge_base_id,knowledge_id,admission_id").Find(&parents).Error; e != nil {
				return e
			}
			if e := s.lockBodyScopes(tx, heads, parents); e != nil {
				return e
			}
			var body originalBodyPayload
			if e := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", id).Take(&body).Error; e != nil {
				return e
			}
			if body.State != "live" {
				return ErrOriginalBodyUnavailable
			}
			if body.StorageKind == "unsupported" {
				return ErrOriginalBodyRetained
			}
			now, e := nextcloudLeaseNowMS(tx)
			if e != nil {
				return e
			}
			var active int64
			if e := bodyInternal(tx).Model(&originalBodyLease{}).Where("body_id=? AND released_at_ms IS NULL AND expires_at_ms>?", id, now).Count(&active).Error; e != nil {
				return e
			}
			if active != 0 {
				return ErrOriginalBodyRetained
			}
			deadline, e := s.bodyEligibility(tx, body, heads, now)
			if e != nil {
				return e
			}
			if body.BodyDeadlineMS == nil {
				if e := bodyInternal(tx).Model(&body).Updates(map[string]any{"retired_at_ms": now, "body_deadline_ms": deadline, "metadata_deadline_ms": now + 90*86400000}).Error; e != nil {
					return e
				}
			}
			if deadline > now {
				return nil
			}
			token := uuid.NewString()
			epoch := body.PurgeEpoch + 1
			cas := bodyInternal(tx).Model(&body).Where("state='live' AND purge_epoch=?", body.PurgeEpoch).Updates(map[string]any{"state": "pending", "purge_token": token, "purge_epoch": epoch})
			if cas.Error != nil {
				return cas.Error
			}
			if cas.RowsAffected != 1 {
				return ErrOriginalBodyUnavailable
			}
			fields := []string{}
			for _, h := range heads {
				fields = append(fields, h.SourceIdentitySHA256)
			}
			sort.Strings(fields)
			scopeJSON, _ := json.Marshal(struct {
				Tenant  uint64
				BodyID  string
				Fields  []string
				Parents []originalBodyParentRef
			}{body.TenantID, id, fields, parents})
			scopeSHA := bodySHA(scopeJSON)
			metaDeadline := now + 90*86400000
			if body.MetadataDeadlineMS != nil {
				metaDeadline = *body.MetadataDeadlineMS
			}
			event, e := s.journal.append(originalBodyJournalEvent{BodyID: id, BodySHA256: body.BodySHA256, ScopeSHA256: scopeSHA, TenantID: body.TenantID, Fields: fields, BeforeBytes: body.BodyBytes, BodyDeadlineMS: deadline, MetadataDeadlineMS: metaDeadline, RecordedAtMS: now, PurgeEpoch: epoch})
			if e != nil {
				return e
			}
			cleared := int64(0)
			if body.StorageKind == "inline" {
				for _, head := range heads {
					var currentHead originalBodyFieldHead
					if e := bodyInternal(tx).Where("slot_id=?", head.SlotID).Take(&currentHead).Error; e != nil {
						return e
					}
					if currentHead.BodyID != id {
						continue
					}
					d, _ := bodyDescriptor(head.SourceTable)
					row, e := bodySourceRow(tx, d, head.RowKey)
					if errors.Is(e, gorm.ErrRecordNotFound) {
						continue
					}
					if e != nil {
						return e
					}
					raw := bodyText(row[head.ColumnName])
					if bodySHA([]byte(raw)) != body.BodySHA256 {
						return ErrOriginalBodyUnavailable
					}
					clearedValue := ""
					if isJSON, e := originalBodyColumnIsJSON(tx, d.Table, head.ColumnName); e != nil {
						return e
					} else if isJSON {
						clearedValue = "null"
					}
					if e := originalBodyRewrite(tx, d, head.RowKey, head.ColumnName, id, raw, clearedValue); e != nil {
						return e
					}
					cleared++
				}
			}
			ack := bodyInternal(tx).Model(&body).Where("state='pending' AND purge_token=? AND purge_epoch=?", token, epoch).Updates(map[string]any{"payload": nil, "state": "purged"})
			if ack.Error != nil {
				return ack.Error
			}
			if ack.RowsAffected != 1 {
				return ErrOriginalBodyUnavailable
			}
			receipt := originalBodyPurgeReceipt{BodyID: id, BodySHA256: body.BodySHA256, ScopeSHA256: scopeSHA, BeforeBytes: body.BodyBytes, AfterBytes: 0, ClearedFields: cleared, JournalSequence: event.Sequence, JournalHeadSHA256: event.HeadSHA256, PurgeToken: token, PurgeEpoch: epoch, CompletedAtMS: now}
			if e := bodyInternal(tx).Create(&receipt).Error; e != nil {
				return e
			}
			if e := bodyInternal(tx).Model(&originalBodyJournalHead{}).Where("id=1").Updates(map[string]any{"sequence": event.Sequence, "head_sha256": event.HeadSHA256}).Error; e != nil {
				return e
			}
			collected = true
			return nil
		})
		if errors.Is(e, ErrOriginalBodyRetained) {
			continue
		}
		if e != nil {
			return done, e
		}
		if collected {
			done++
		}
	}
	return done, nil
}

var _ = time.Hour
