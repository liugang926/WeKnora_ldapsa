package repository

import (
	"context"
	"encoding/json"
	"errors"
	"gorm.io/gorm"
	"strings"
)

// gcOriginalBodyAuditMetadata preserves stable metadata fingerprints only
// after independently checking each known body's live bytes or purge receipt.
// Existing untracked rows enter the body protocol under their actual SQL scope;
// this does not create or authenticate an input provenance origin.
func gcOriginalBodyAuditMetadata(tx *gorm.DB, table, key string, row map[string]any) ([]byte, error) {
	if !originalBodySchemaPresent(tx) {
		return json.Marshal(row)
	}
	d, known := bodyDescriptor(table)
	if !known {
		return json.Marshal(row)
	}
	store := NewOriginalBodyRetentionStore(tx)
	scope, e := bodyScopeFromRow(tx, d, key, row)
	if e != nil {
		return nil, e
	}
	metadata := map[string]any{}
	for k, v := range row {
		metadata[k] = v
	}
	for _, field := range d.Columns {
		raw, e := bodyFieldValue(row, field)
		if e != nil {
			return nil, e
		}
		var head originalBodyFieldHead
		e = bodyInternal(tx).Where("source_table=? AND row_key=? AND column_name=?", table, key, field).Take(&head).Error
		if errors.Is(e, gorm.ErrRecordNotFound) {
			if raw == "" || raw == "null" {
				continue
			}
			if e = store.captureField(tx, d, key, field, raw, scope); e != nil {
				return nil, e
			}
			if e = bodyInternal(tx).Where("source_table=? AND row_key=? AND column_name=?", table, key, field).Take(&head).Error; e != nil {
				return nil, e
			}
		} else if e != nil {
			return nil, e
		}
		// Capture may have moved a legacy vault column into its reference. Reload
		// the exact physical row, bypassing consumer body resolution.
		actual, e := bodySourceRow(tx, d, key)
		if e != nil {
			return nil, e
		}
		raw, e = bodyFieldValue(actual, field)
		if e != nil {
			return nil, e
		}
		if e = store.verifyFieldStorageInTx(tx, d, head, raw, scope); e != nil {
			return nil, e
		}
		if strings.Contains(field, ".") {
			base := bodyFieldColumn(field)
			full := bodyText(metadata[base])
			if raw != "null" {
				cleared, e := bodyNestedReplace(full, field, raw, "")
				if e != nil {
					return nil, e
				}
				metadata[base] = json.RawMessage(cleared)
			}
		} else {
			delete(metadata, field)
		}
	}
	return json.Marshal(metadata)
}

func (s *OriginalBodyRetentionStore) verifyFieldStorageInTx(tx *gorm.DB, d originalBodyFieldDescriptor, head originalBodyFieldHead, raw string, scope originalBodyScope) error {
	if bodyIdentity(d.Table, head.RowKey, head.ColumnName, scope) != head.SourceIdentitySHA256 {
		return ErrOriginalBodyUnavailable
	}
	if e := s.checkJournalInTx(tx); e != nil {
		return e
	}
	var body originalBodyPayload
	if e := bodyInternal(tx).Where("id=?", head.BodyID).Take(&body).Error; e != nil {
		return e
	}
	if body.State == "live" {
		if d.Mode == "vault" {
			ref, ok := bodyReference(raw)
			if !ok || raw != head.OriginalBodyRef {
				return ErrOriginalBodyUnavailable
			}
			_, e := s.resolveInTx(tx, head, ref)
			return e
		}
		if bodySHA([]byte(raw)) != body.BodySHA256 {
			return ErrOriginalBodyUnavailable
		}
		return s.checkExternalBody(body.ID)
	}
	if body.State != "purged" || body.Payload != nil {
		return ErrOriginalBodyUnavailable
	}
	if d.Mode == "vault" {
		if raw != head.OriginalBodyRef {
			return ErrOriginalBodyUnavailable
		}
	} else if raw != "" && raw != "null" {
		return ErrOriginalBodyUnavailable
	}
	var receipt originalBodyPurgeReceipt
	if e := bodyInternal(tx).Where("body_id=?", body.ID).Take(&receipt).Error; e != nil {
		return e
	}
	if receipt.AfterBytes != 0 || receipt.BodySHA256 != body.BodySHA256 {
		return ErrOriginalBodyUnavailable
	}
	events, _, e := s.journal.read()
	if e != nil {
		return e
	}
	for _, event := range events {
		if event.BodyID == body.ID {
			if event.BodySHA256 != body.BodySHA256 || event.ScopeSHA256 != receipt.ScopeSHA256 || event.Sequence != receipt.JournalSequence || event.HeadSHA256 != receipt.JournalHeadSHA256 {
				return ErrOriginalBodyUnavailable
			}
			for _, identity := range event.Fields {
				if identity == head.SourceIdentitySHA256 {
					return nil
				}
			}
		}
	}
	return ErrOriginalBodyUnavailable
}

// InventorySelectedJob captures pre-protocol body columns in the same exact
// SQL inventory that local derived GC keeps as metadata. It verifies already
// purged references/empty leaves rather than admitting restored inline text.
func (s *OriginalBodyRetentionStore) inventorySelectedJob(ctx context.Context, job nextcloudGCJob) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := gcValidateManualAdmin(ctx, bodyInternal(tx), job); e != nil {
			return e
		}
		if _, e := gcAuditInventory(bodyInternal(tx), job); e != nil {
			return e
		}
		for _, table := range []string{"knowledges", "chunks"} {
			d, _ := bodyDescriptor(table)
			if !tx.Migrator().HasTable(table) {
				continue
			}
			query := bodyInternal(tx).Table(table).Where("tenant_id=? AND knowledge_base_id=?", job.TenantID, job.KnowledgeBaseID)
			if table == "knowledges" {
				query = query.Where("id=?", job.KnowledgeID)
			} else {
				query = query.Where("knowledge_id=?", job.KnowledgeID)
			}
			var rows []map[string]any
			if e := query.Find(&rows).Error; e != nil {
				return e
			}
			for _, row := range rows {
				if _, e := gcOriginalBodyAuditMetadata(tx, table, bodyText(row[d.Key]), row); e != nil {
					return e
				}
			}
		}
		return nil
	})
}
