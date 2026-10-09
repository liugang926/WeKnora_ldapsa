package repository

import (
	"context"
	"encoding/json"
	"errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
	"strings"
)

// ReconcileExternalPurge replays verified external decisions after a failed
// DB ACK or database restore. It never clears an actual current owner, active
// lease, changed tuple or unexpired recovery artifact to make the pins match.
func (s *OriginalBodyRetentionStore) ReconcileExternalPurge(ctx context.Context) (int, error) {
	if s.journal == nil {
		return 0, ErrOriginalBodyUnavailable
	}
	events, pin, e := s.journal.read()
	if e != nil {
		return 0, e
	}
	count := 0
	e = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var head originalBodyJournalHead
		if e := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=1").Take(&head).Error; e != nil {
			return e
		}
		if head.JournalID != pin.JournalID || head.Sequence > pin.Sequence {
			return ErrOriginalBodyUnavailable
		}
		for _, event := range events {
			var body originalBodyPayload
			if e := bodyInternal(tx).Where("id=?", event.BodyID).Take(&body).Error; e != nil {
				return e
			}
			if body.BodySHA256 != event.BodySHA256 || body.TenantID != event.TenantID {
				return ErrOriginalBodyUnavailable
			}
			var heads []originalBodyFieldHead
			if e := bodyInternal(tx).Table("original_body_field_revisions").Where("body_id=?", body.ID).Order("source_table,row_key,column_name,revision").Find(&heads).Error; e != nil {
				return e
			}
			var parents []originalBodyParentRef
			if e := bodyInternal(tx).Where("body_id=?", body.ID).Order("tenant_id,knowledge_base_id,knowledge_id,admission_id").Find(&parents).Error; e != nil {
				return e
			}
			fields := []string{}
			for _, h := range heads {
				fields = append(fields, h.SourceIdentitySHA256)
			}
			sort.Strings(fields)
			scope, _ := json.Marshal(struct {
				Tenant  uint64
				BodyID  string
				Fields  []string
				Parents []originalBodyParentRef
			}{body.TenantID, body.ID, fields, parents})
			if bodySHA(scope) != event.ScopeSHA256 {
				return ErrOriginalBodyUnavailable
			}
			if body.State == "purged" {
				if body.Payload != nil {
					return ErrOriginalBodyUnavailable
				}
				var receipt originalBodyPurgeReceipt
				if e := bodyInternal(tx).Where("body_id=?", body.ID).Take(&receipt).Error; e != nil {
					return e
				}
				if receipt.JournalSequence != event.Sequence || receipt.JournalHeadSHA256 != event.HeadSHA256 {
					return ErrOriginalBodyUnavailable
				}
				// A prior DB receipt does not authenticate restored physical
				// bytes. Validate every still-owned field before idempotence.
				for _, field := range heads {
					var current originalBodyFieldHead
					if e := bodyInternal(tx).Where("slot_id=?", field.SlotID).Take(&current).Error; e != nil {
						return e
					}
					if current.BodyID != body.ID {
						continue
					}
					d, ok := bodyDescriptor(field.SourceTable)
					if !ok {
						return ErrOriginalBodyUnavailable
					}
					row, e := bodySourceRow(tx, d, field.RowKey)
					if errors.Is(e, gorm.ErrRecordNotFound) {
						continue
					}
					if e != nil {
						return e
					}
					raw, e := bodyFieldValue(row, field.ColumnName)
					if e != nil {
						return e
					}
					if d.Mode == "vault" {
						if raw != field.OriginalBodyRef {
							return ErrOriginalBodyUnavailable
						}
					} else if raw != "" && raw != "null" {
						return ErrOriginalBodyUnavailable
					}
				}
				continue
			}
			if e := s.lockBodyScopes(tx, heads, parents); e != nil {
				return e
			}
			now, e := nextcloudLeaseNowMS(bodyInternal(tx))
			if e != nil {
				return e
			}
			if event.BodyDeadlineMS > now {
				return ErrOriginalBodyRetained
			}
			var active int64
			if e := bodyInternal(tx).Model(&originalBodyLease{}).Where("body_id=? AND released_at_ms IS NULL AND expires_at_ms>?", body.ID, now).Count(&active).Error; e != nil {
				return e
			}
			if active != 0 {
				return ErrOriginalBodyRetained
			}
			for _, h := range heads {
				current, _, e := s.fieldCurrent(tx, h, now)
				if e != nil {
					return e
				}
				if current {
					return ErrOriginalBodyRetained
				}
			}
			if body.StorageKind == "vault" {
				if body.Payload == nil || bodySHA([]byte(*body.Payload)) != event.BodySHA256 {
					return ErrOriginalBodyUnavailable
				}
			}
			cleared := int64(0)
			if body.StorageKind == "inline" {
				for _, h := range heads {
					var current originalBodyFieldHead
					if e := bodyInternal(tx).Where("slot_id=?", h.SlotID).Take(&current).Error; e != nil {
						return e
					}
					if current.BodyID != body.ID {
						continue
					}
					d, ok := bodyDescriptor(h.SourceTable)
					if !ok || d.Mode == "unsupported" {
						return ErrOriginalBodyRetained
					}
					row, e := bodySourceRow(tx, d, h.RowKey)
					if errors.Is(e, gorm.ErrRecordNotFound) {
						continue
					}
					if e != nil {
						return e
					}
					raw, e := bodyFieldValue(row, h.ColumnName)
					if e != nil {
						return e
					}
					if bodySHA([]byte(raw)) != body.BodySHA256 {
						return ErrOriginalBodyUnavailable
					}
					empty := ""
					if jsonCol, e := originalBodyColumnIsJSON(tx, d.Table, h.ColumnName); e != nil {
						return e
					} else if jsonCol && !strings.Contains(h.ColumnName, ".") {
						empty = "null"
					}
					if e := originalBodyRewrite(tx, d, h.RowKey, h.ColumnName, body.ID, raw, empty); e != nil {
						return e
					}
					cleared++
				}
			}
			if e := bodyInternal(tx).Model(&body).Updates(map[string]any{"state": "purged", "payload": nil, "purge_epoch": event.PurgeEpoch, "body_deadline_ms": event.BodyDeadlineMS, "metadata_deadline_ms": event.MetadataDeadlineMS}).Error; e != nil {
				return e
			}
			receipt := originalBodyPurgeReceipt{BodyID: body.ID, BodySHA256: event.BodySHA256, ScopeSHA256: event.ScopeSHA256, BeforeBytes: event.BeforeBytes, AfterBytes: 0, ClearedFields: cleared, JournalSequence: event.Sequence, JournalHeadSHA256: event.HeadSHA256, PurgeToken: "external-replay", PurgeEpoch: event.PurgeEpoch, CompletedAtMS: now}
			if e := bodyInternal(tx).Create(&receipt).Error; e != nil {
				return e
			}
			count++
		}
		return bodyInternal(tx).Model(&head).Updates(map[string]any{"sequence": pin.Sequence, "head_sha256": pin.HeadSHA256}).Error
	})
	return count, e
}
