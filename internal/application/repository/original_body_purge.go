package repository

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
	"strings"
	"time"
)

// bodyOwnerDeadline derives closure from actual rows/heads. Unknown legacy
// ownership is retained; a caller cannot pass ready/closed booleans or dates.
func (s *OriginalBodyRetentionStore) bodyKnowledgeDeadline(tx *gorm.DB, tenant uint64, kb, id, admission string, now int64) (int64, bool, error) {
	var k types.Knowledge
	e := bodyInternal(tx).Unscoped().Where("id=? AND tenant_id=? AND knowledge_base_id=?", id, tenant, kb).Take(&k).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		if tx.Migrator().HasTable("nextcloud_gc_jobs") {
			var job nextcloudGCJob
			if e := bodyInternal(tx).Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", tenant, kb, id).Take(&job).Error; e == nil {
				return job.NotBefore.UnixMilli(), false, nil
			} else if !errors.Is(e, gorm.ErrRecordNotFound) {
				return 0, true, e
			}
		}
		return -1, false, nil
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
		withdrawn := false
		if tx.Migrator().HasTable(&NextcloudIndexedWithdrawal{}) {
			var n int64
			if e := bodyInternal(tx).Model(&NextcloudIndexedWithdrawal{}).Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state=?", tenant, kb, meta["datasource_id"], "withdrawn_inventory_incomplete").Count(&n).Error; e != nil {
				return 0, true, e
			}
			withdrawn = n > 0
		}
		if !withdrawn && version.State != "tombstone" && tx.Migrator().HasTable("data_sources") {
			var source types.DataSource
			if e := bodyInternal(tx).Where("id=? AND tenant_id=? AND knowledge_base_id=?", meta["datasource_id"], tenant, kb).Take(&source).Error; e != nil {
				return 0, true, e
			}
			if source.Status != types.DataSourceStatusActive {
				return 0, true, nil
			}
		}
		if !withdrawn && version.State != "tombstone" && version.CandidateKnowledgeID == id {
			return 0, true, nil
		}
		var fence nextcloudContentFence
		if e := bodyInternal(tx).Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", tenant, kb, id).Take(&fence).Error; e != nil {
			return 0, true, e
		}
		if fence.State != "retired" || fence.RetiredAtMS == nil {
			return 0, true, nil
		}
		if k.PendingSubtasksCount > 0 {
			return 0, true, nil
		}
		if version.State == "tombstone" || withdrawn {
			return *fence.RetiredAtMS, false, nil
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
			return -1, false, nil
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
		return false, -1, nil
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
	case "steer_authored_origins":
		var control struct {
			Consumed, Discarded, Deleted bool
			UserMessageID                string
			CreatedAtMS                  int64
		}
		if e := bodyInternal(tx).Table("steer_control_admissions a").Select("a.consumed,a.discarded,a.deleted,a.user_message_id,a.created_at_ms").Joins("JOIN steer_control_heads h ON h.head_id=a.id AND h.origin_id=a.origin_id").Where("a.origin_id=?", head.RowKey).Take(&control).Error; e != nil {
			return true, 0, e
		}
		if control.Discarded || control.Deleted {
			return false, control.CreatedAtMS + 3600000, nil
		}
		if control.Consumed && control.UserMessageID != "" {
			var user struct{ DeletedAt gorm.DeletedAt }
			e := bodyInternal(tx).Unscoped().Table("messages").Select("deleted_at").Where("id=?", control.UserMessageID).Take(&user).Error
			if errors.Is(e, gorm.ErrRecordNotFound) {
				return false, -1, nil
			}
			if e != nil {
				return true, 0, e
			}
			if user.DeletedAt.Valid {
				return false, user.DeletedAt.Time.UnixMilli() + 3600000, nil
			}
		}
		var session struct{ DeletedAt gorm.DeletedAt }
		if e := bodyInternal(tx).Unscoped().Table("sessions").Select("deleted_at").Where("id=?", bodyText(row["session_id"])).Take(&session).Error; e != nil {
			return true, 0, e
		}
		if session.DeletedAt.Valid {
			return false, session.DeletedAt.Time.UnixMilli() + 3600000, nil
		}
		// A queued/consumed original is hot only for the explicit seven-day
		// authored-input window; later reads require a new actual human input.
		return false, int64(bodyUint(row["created_at_ms"])) + 7*86400000, nil
	case "datasource_name_inputs":
		source, e := bodySourceRow(tx, originalBodyFieldDescriptor{Table: "data_sources", Key: "id"}, bodyText(row["datasource_id"]))
		if errors.Is(e, gorm.ErrRecordNotFound) {
			return false, -1, nil
		}
		if e != nil {
			return true, 0, e
		}
		if bodyUint(source["tenant_id"]) != head.TenantID || bodyText(source["knowledge_base_id"]) != head.KnowledgeBaseID {
			return true, 0, ErrOriginalBodyUnavailable
		}
		if tx.Migrator().HasTable(&NextcloudIndexedWithdrawal{}) {
			var withdrawal NextcloudIndexedWithdrawal
			e := bodyInternal(tx).Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state=?", head.TenantID, head.KnowledgeBaseID, row["datasource_id"], "withdrawn_inventory_incomplete").Take(&withdrawal).Error
			if e == nil {
				return false, withdrawal.CreatedAt.UnixMilli(), nil
			}
			if !errors.Is(e, gorm.ErrRecordNotFound) {
				return true, 0, e
			}
		}
		if bodyText(source["status"]) != types.DataSourceStatusActive {
			return true, 0, nil
		}
		var version struct{ Revision uint64 }
		if e := bodyInternal(tx).Table("datasource_name_versions").Where("datasource_id=?", row["datasource_id"]).Take(&version).Error; e != nil {
			return true, 0, e
		}
		if version.Revision == bodyUint(row["revision"]) {
			return true, 0, nil
		}
		return false, -1, nil
	case "faq_input_sessions":
		var refs int64
		if e := bodyInternal(tx).Table("faq_entry_material_receipts r").Joins("JOIN faq_entry_input_admissions a ON a.id=r.admission_id").Where("a.session_id=?", head.RowKey).Count(&refs).Error; e != nil {
			return true, 0, e
		}
		if refs > 0 {
			return true, 0, nil
		}
		return false, -1, nil
	case "faq_import_output_admissions":
		var refs int64
		if e := bodyInternal(tx).Table("faq_import_output_heads").Where("admission_id=?", head.RowKey).Count(&refs).Error; e != nil {
			return true, 0, e
		}
		if refs > 0 {
			return true, 0, nil
		}
		return false, -1, nil
	case "knowledge_display_input_origins":
		var version struct{ Revision int64 }
		if e := bodyInternal(tx).Table("knowledge_display_versions").Where("kind=? AND row_id=?", row["kind"], row["row_id"]).Take(&version).Error; e != nil {
			return true, 0, e
		}
		if version.Revision == int64(bodyUint(row["revision"])) {
			return true, 0, nil
		}
		return false, -1, nil
	case "message_generated_field_admissions", "message_fork_parent_origins":
		return s.parentCopyCurrent(tx, head, row, now)
	case "messages", "message_generation_origins":
		messageID := head.RowKey
		if d.Table == "message_generation_origins" {
			messageID = bodyText(row["message_id"])
		}
		var message struct {
			ID, SessionID string
			DeletedAt     gorm.DeletedAt
		}
		e := bodyInternal(tx).Unscoped().Table("messages").Select("id,session_id,deleted_at").Where("id=?", messageID).Take(&message).Error
		if errors.Is(e, gorm.ErrRecordNotFound) {
			return false, -1, nil
		}
		if e != nil {
			return true, 0, e
		}
		if message.DeletedAt.Valid {
			return false, message.DeletedAt.Time.UnixMilli() + 3600000, nil
		}
		var session struct {
			ID        string
			DeletedAt gorm.DeletedAt
		}
		e = bodyInternal(tx).Unscoped().Table("sessions").Select("id,deleted_at").Where("id=?", message.SessionID).Take(&session).Error
		if errors.Is(e, gorm.ErrRecordNotFound) {
			return false, -1, nil
		}
		if e != nil {
			return true, 0, e
		}
		if session.DeletedAt.Valid {
			return false, session.DeletedAt.Time.UnixMilli() + 3600000, nil
		}
		if tx.Migrator().HasTable("steer_consumption_origins") {
			var origins []struct{ CreatedAtMS int64 }
			if e := bodyInternal(tx).Table("steer_consumption_origins p").Select("o.created_at_ms").Joins("JOIN steer_authored_origins o ON o.id=p.origin_id").Where("p.user_message_id=?", messageID).Scan(&origins).Error; e != nil {
				return true, 0, e
			}
			if len(origins) > 0 {
				deadline := origins[0].CreatedAtMS + 7*86400000
				for _, origin := range origins {
					if origin.CreatedAtMS+7*86400000 < deadline {
						deadline = origin.CreatedAtMS + 7*86400000
					}
				}
				return false, deadline, nil
			}
		}
		return true, 0, nil
	case "sessions":
		if row["deleted_at"] != nil && bodyText(row["deleted_at"]) != "" {
			return false, -1, nil
		}
		return true, 0, nil
	case "knowledge_enrichment_template_inputs":
		// A completed template still used by the current KB protects its captured
		// input. Missing result evidence never proves that a build terminated.
		if !tx.Migrator().HasTable("knowledge_enrichment_template_results") {
			return true, 0, nil
		}
		var results []struct{ OutputDigest string }
		if e := bodyInternal(tx).Table("knowledge_enrichment_template_results").Where("input_id=?", head.RowKey).Find(&results).Error; e != nil {
			return true, 0, e
		}
		if len(results) == 0 {
			return true, 0, nil
		}
		var kb types.KnowledgeBase
		if e := bodyInternal(tx).Where("id=? AND tenant_id=?", head.KnowledgeBaseID, head.TenantID).Take(&kb).Error; e != nil {
			return true, 0, e
		}
		if kb.EnrichmentTemplates != nil {
			current := kb.EnrichmentTemplates.Content(bodyText(row["purpose"]))
			digest, e := KnowledgeInputDigest(current)
			if e != nil {
				return true, 0, e
			}
			for _, result := range results {
				if result.OutputDigest == digest {
					return true, 0, nil
				}
			}
		}
		return false, -1, nil
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
	for _, head := range heads {
		if head.SourceTable != "messages" && head.SourceTable != "message_generation_origins" && !parentCopyTable(head.SourceTable) {
			continue
		}
		var resolved []originalBodyLeaseScope
		if e := bodyInternal(tx).Where("body_id=?", head.BodyID).Find(&resolved).Error; e != nil {
			return e
		}
		if !bodyLeaseScopesComplete(resolved, head.TenantID) {
			return ErrOriginalBodyRetained
		}
		if parentCopyTable(head.SourceTable) {
			if _, err := parentCopyScopeEvidence(tx, head, resolved); err != nil { return err }
		}
		for _, scopeRef := range resolved {
			if scopeRef.KnowledgeBaseID != "" {
				keys[scope{scopeRef.TenantID, scopeRef.KnowledgeBaseID}] = true
			}
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
			now, e := nextcloudLeaseNowMS(bodyInternal(tx))
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
	if e := bodyInternal(s.db.WithContext(ctx)).Model(&originalBodyPayload{}).Where("state='live'").Order("last_checked_at_ms,created_at_ms,id").Limit(limit).Pluck("id", &ids).Error; e != nil {
		return 0, e
	}
	return s.purgeBodyIDs(ctx, ids, nil)
}

// PurgeSelectedJob selects only body owners/parent tuples of one persisted job.
// The same SQL transaction repeats admin, exact scope, source and lease gates.
func (s *OriginalBodyRetentionStore) PurgeSelectedJob(ctx context.Context, tenant uint64, jobID string, limit int) (int, error) {
	if !originalBodySchemaPresent(s.db) {
		return 0, nil
	}
	if tenant == 0 || jobID == "" {
		return 0, ErrNextcloudGCJobUnavailable
	}
	var job nextcloudGCJob
	if e := bodyInternal(s.db.WithContext(ctx)).Where("id=? AND tenant_id=?", jobID, tenant).Take(&job).Error; e != nil {
		return 0, ErrNextcloudGCJobUnavailable
	}
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if e := s.inventorySelectedJob(ctx, job); e != nil {
		recordErr := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := gcValidateManualAdmin(ctx, bodyInternal(tx), job); err != nil {
				return err
			}
			now, err := nextcloudLeaseNowMS(bodyInternal(tx))
			if err != nil {
				return err
			}
			return bodyInternal(tx).Model(&nextcloudGCJob{}).Where("id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", job.ID, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Updates(map[string]any{"state": "blocked", "completed_at": nil, "last_error_code": "original_body_retained_or_unverified", "next_attempt_at": time.UnixMilli(now).Add(nextcloudGCRetryWindow)}).Error
		})
		return 0, errors.Join(e, recordErr)
	}
	var ids []string
	q := `SELECT p.id FROM original_body_payloads p WHERE p.state='live' AND p.tenant_id=? AND (EXISTS(SELECT 1 FROM original_body_field_revisions r WHERE r.body_id=p.id AND r.tenant_id=? AND r.knowledge_base_id=? AND r.knowledge_id=?) OR EXISTS(SELECT 1 FROM original_body_parent_refs r WHERE r.body_id=p.id AND r.tenant_id=? AND r.knowledge_base_id=? AND r.knowledge_id=?)) ORDER BY p.last_checked_at_ms,p.created_at_ms,p.id LIMIT ?`
	if s.db.Migrator().HasTable("datasource_name_inputs") {
		q = strings.Replace(q, ")) ORDER BY p.last_checked_at_ms,p.created_at_ms,p.id LIMIT ?", ") OR EXISTS(SELECT 1 FROM original_body_field_revisions r JOIN datasource_name_inputs n ON r.source_table='datasource_name_inputs' AND n.id=r.row_key WHERE r.body_id=p.id AND n.tenant_id=? AND n.knowledge_base_id=? AND n.datasource_id=?)) ORDER BY p.last_checked_at_ms,p.created_at_ms,p.id LIMIT ?", 1)
	}
	args := []any{tenant, tenant, job.KnowledgeBaseID, job.KnowledgeID, tenant, job.KnowledgeBaseID, job.KnowledgeID}
	if s.db.Migrator().HasTable("datasource_name_inputs") {
		args = append(args, tenant, job.KnowledgeBaseID, job.DataSourceID)
	}
	args = append(args, limit)
	if e := bodyInternal(s.db.WithContext(ctx)).Raw(q, args...).Scan(&ids).Error; e != nil {
		return 0, e
	}
	if len(ids) == 0 {
		return 0, nil
	}
	e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := gcValidateManualAdmin(ctx, bodyInternal(tx), job); e != nil {
			return e
		}
		now, e := nextcloudLeaseNowMS(bodyInternal(tx))
		if e != nil {
			return e
		}
		for _, id := range ids {
			var body originalBodyPayload
			if e := bodyInternal(tx).Where("id=?", id).Take(&body).Error; e != nil {
				return e
			}
			item := originalBodyGCItem{JobID: job.ID, BodyID: id, TenantID: job.TenantID, KnowledgeBaseID: job.KnowledgeBaseID, KnowledgeID: job.KnowledgeID, State: "pending", EstimatedBytes: body.BodyBytes, CreatedAtMS: now}
			if e := bodyInternal(tx).Clauses(clause.OnConflict{DoNothing: true}).Create(&item).Error; e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		return 0, e
	}
	n, purgeErr := s.purgeBodyIDs(ctx, ids, &job)
	recordErr := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := gcValidateManualAdmin(ctx, bodyInternal(tx), job); e != nil {
			return e
		}
		if e := bodyInternal(tx).Exec("UPDATE original_body_gc_items SET state='collected' WHERE job_id=? AND EXISTS(SELECT 1 FROM original_body_payloads p WHERE p.id=body_id AND p.state='purged' AND p.payload IS NULL)", job.ID).Error; e != nil {
			return e
		}
		var pending int64
		if e := bodyInternal(tx).Model(&originalBodyGCItem{}).Where("job_id=? AND state<>'collected'", job.ID).Count(&pending).Error; e != nil {
			return e
		}
		if pending > 0 {
			return bodyInternal(tx).Model(&nextcloudGCJob{}).Where("id=?", job.ID).Updates(map[string]any{"state": "blocked", "completed_at": nil, "last_error_code": "original_body_retained_or_unverified"}).Error
		}
		return nil
	})
	return n, errors.Join(purgeErr, recordErr)
}
func (s *OriginalBodyRetentionStore) purgeBodyIDs(ctx context.Context, ids []string, selected *nextcloudGCJob) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if s.journal == nil {
		return 0, ErrOriginalBodyUnavailable
	}
	done := 0
	for _, id := range ids {
		collected := false
		e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var journalHead originalBodyJournalHead
			if e := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=1").Take(&journalHead).Error; e != nil {
				return e
			}
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
			if selected != nil {
				var job nextcloudGCJob
				if e := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", selected.ID, selected.TenantID).Take(&job).Error; e != nil {
					return e
				}
				if job.KnowledgeBaseID != selected.KnowledgeBaseID || job.KnowledgeID != selected.KnowledgeID || job.DataSourceID != selected.DataSourceID || job.ExternalID != selected.ExternalID {
					return ErrOriginalBodyUnavailable
				}
				if e := gcValidateManualAdmin(ctx, bodyInternal(tx), job); e != nil {
					return e
				}
				now, e := nextcloudLeaseNowMS(bodyInternal(tx))
				if e != nil {
					return e
				}
				if job.NotBefore.UnixMilli() > now {
					return ErrOriginalBodyRetained
				}
				if _, e := gcCheckRetiredContent(bodyInternal(tx), job, now); e != nil {
					return e
				}
				var version nextcloudSourceVersion
				if e := nextcloudVersionQuery(bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}), job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID).Take(&version).Error; e != nil {
					return e
				}
				if version.State != "tombstone" && version.CandidateKnowledgeID == job.KnowledgeID {
					return ErrOriginalBodyRetained
				}
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
			now, e := nextcloudLeaseNowMS(bodyInternal(tx))
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
			scopeSHA, e := originalBodyScopeDigest(tx, body, fields, parents)
			if e != nil {
				return e
			}
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
					raw, e := bodyFieldValue(row, head.ColumnName)
					if e != nil {
						return e
					}
					if bodySHA([]byte(raw)) != body.BodySHA256 {
						return ErrOriginalBodyUnavailable
					}
					clearedValue := ""
					if isJSON, e := originalBodyColumnIsJSON(tx, d.Table, head.ColumnName); e != nil {
						return e
					} else if isJSON && !strings.Contains(head.ColumnName, ".") {
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
		if e == nil || errors.Is(e, ErrOriginalBodyRetained) {
			checked, clockErr := nextcloudLeaseNowMS(bodyInternal(s.db.WithContext(ctx)))
			if clockErr != nil {
				return done, clockErr
			}
			if markErr := bodyInternal(s.db.WithContext(ctx)).Model(&originalBodyPayload{}).Where("id=?", id).Update("last_checked_at_ms", checked).Error; markErr != nil {
				return done, markErr
			}
		}
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
