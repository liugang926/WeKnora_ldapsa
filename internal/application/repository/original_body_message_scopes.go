package repository

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// A body gets a complete marker only at its actual authenticated producer.
// The empty KB marker means the original producer consumed no KB scope; no
// marker means unknown, not an empty set inferred from current metadata.
type originalBodyLeaseScope struct {
	BodyID          string `gorm:"primaryKey"`
	TenantID        uint64 `gorm:"primaryKey"`
	KnowledgeBaseID string `gorm:"primaryKey"`
}

func (originalBodyLeaseScope) TableName() string { return "original_body_lease_scopes" }

const originalBodyLeaseScopeDDL = `CREATE TABLE IF NOT EXISTS original_body_lease_scopes(body_id TEXT NOT NULL REFERENCES original_body_payloads(id),tenant_id BIGINT NOT NULL,knowledge_base_id TEXT NOT NULL,PRIMARY KEY(body_id,tenant_id,knowledge_base_id))`

// Explicit offline forward schema step for already-installed unpublished
// migration 152/71. It never backfills scope markers from current pointers.
func (s *OriginalBodyRetentionStore) UpgradeLeaseScopeSchema() error {
	if !originalBodySchemaPresent(s.db) {
		return ErrOriginalBodyUnavailable
	}
	return bodyInternal(s.db).Transaction(func(tx *gorm.DB) error {
		if e := tx.Exec(originalBodyLeaseScopeDDL).Error; e != nil {
			return e
		}
		if tx.Dialector.Name() == "postgres" {
			if e := tx.Exec(`CREATE OR REPLACE FUNCTION original_body_lease_scope_immutable() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'immutable original body lease scope'; END $$`).Error; e != nil {
				return e
			}
			if e := tx.Exec(`DROP TRIGGER IF EXISTS original_body_lease_scope_immutable ON original_body_lease_scopes`).Error; e != nil {
				return e
			}
			return tx.Exec(`CREATE TRIGGER original_body_lease_scope_immutable BEFORE UPDATE OR DELETE ON original_body_lease_scopes FOR EACH ROW EXECUTE FUNCTION original_body_lease_scope_immutable()`).Error
		}
		if tx.Dialector.Name() != "sqlite" {
			return ErrOriginalBodyUnavailable
		}
		for _, event := range []string{"UPDATE", "DELETE"} {
			if e := tx.Exec("CREATE TRIGGER IF NOT EXISTS original_body_lease_scope_immutable_" + strings.ToLower(event) + " BEFORE " + event + " ON original_body_lease_scopes BEGIN SELECT RAISE(ABORT,'immutable original body lease scope'); END").Error; e != nil {
				return e
			}
		}
		return nil
	})
}
func (s *OriginalBodyRetentionStore) recordMessageLeaseScopes(tx *gorm.DB, id string, d originalBodyFieldDescriptor, key, raw string, scope originalBodyScope) error {
	if d.Table != "messages" && d.Table != "message_generation_origins" {
		return nil
	}
	_, actualProducer := tx.Statement.Context.Value(messageProducerBoundaryKey{}).(messageProducerBoundary)
	generated, actualGenerated := tx.Statement.Context.Value(messageGeneratedFieldKey{}).(messageGeneratedFieldBoundary)
	actualGenerated = actualGenerated && d.Table == "messages" && generated.MessageID == key
	if !actualProducer && !actualGenerated {
		return nil
	}
	var execution types.MessageExecutionContext
	var lineage *types.SourceLineage
	if d.Table == "messages" {
		row, e := bodySourceRow(tx, d, key)
		if e != nil {
			return e
		}
		if e = json.Unmarshal([]byte(bodyText(row["execution_context"])), &execution); e != nil {
			return e
		}
		if text := bodyText(row["source_lineage"]); text != "" && text != "null" {
			if e = json.Unmarshal([]byte(text), &lineage); e != nil {
				return e
			}
		}
	} else {
		var envelope struct {
			Body      *types.Message
			Execution types.MessageExecutionContext
			Lineage   *types.SourceLineage
		}
		if e := json.Unmarshal([]byte(raw), &envelope); e != nil {
			return e
		}
		if envelope.Body == nil {
			return nil
		}
		execution = envelope.Execution
		lineage = envelope.Lineage
	}
	rows := []originalBodyLeaseScope{{BodyID: id, TenantID: scope.Tenant, KnowledgeBaseID: ""}}
	kbIDs := append(append([]string{}, execution.KnowledgeBaseIDs...), execution.AgentKnowledgeBaseIDs...)
	for _, tag := range execution.TagScopes {
		kbIDs = append(kbIDs, tag.KnowledgeBaseID)
	}
	for _, kb := range kbIDs {
		if kb == "" {
			continue
		}
		var actual struct {
			TenantID uint64
			ID       string
		}
		if e := bodyInternal(tx).Table("knowledge_bases").Select("tenant_id,id").Where("id=?", kb).Take(&actual).Error; e != nil {
			return e
		}
		if actual.TenantID == 0 {
			return ErrOriginalBodyUnavailable
		}
		rows = append(rows, originalBodyLeaseScope{id, actual.TenantID, actual.ID})
	}
	for _, knowledgeID := range execution.KnowledgeIDs {
		var knowledge struct {
			TenantID        uint64
			KnowledgeBaseID string
		}
		if e := bodyInternal(tx).Table("knowledges").Select("tenant_id,knowledge_base_id").Where("id=?", knowledgeID).Take(&knowledge).Error; e != nil {
			return e
		}
		if knowledge.TenantID == 0 || knowledge.KnowledgeBaseID == "" {
			return ErrOriginalBodyUnavailable
		}
		rows = append(rows, originalBodyLeaseScope{id, knowledge.TenantID, knowledge.KnowledgeBaseID})
	}
	if lineage != nil {
		for _, source := range lineage.Sources {
			rows = append(rows, originalBodyLeaseScope{id, source.TenantID, source.KnowledgeBaseID})
		}
	}
	if actualGenerated {
		for _, parent := range generated.Parents {
			for _, proof := range parent.Proofs {
				var original []originalBodyLeaseScope
				if e := bodyInternal(tx).Table("original_body_lease_scopes s").Select("s.*").Joins("JOIN original_body_field_heads h ON h.body_id=s.body_id").Where("h.source_table='message_generation_origins' AND h.row_key=?", proof.GenerationID).Find(&original).Error; e != nil {
					return e
				}
				if len(original) == 0 {
					return nil
				} // Do not certify incomplete legacy parents.
				for _, ref := range original {
					rows = append(rows, originalBodyLeaseScope{id, ref.TenantID, ref.KnowledgeBaseID})
				}
			}
		}
		for _, source := range generated.Sources.Sources {
			rows = append(rows, originalBodyLeaseScope{id, source.TenantID, source.KnowledgeBaseID})
		}
	}
	return bodyInternal(tx).Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error
}
func originalBodyScopeDigest(tx *gorm.DB, body originalBodyPayload, fields []string, parents []originalBodyParentRef) (string, error) {
	sort.Strings(fields)
	var scopes []originalBodyLeaseScope
	if e := bodyInternal(tx).Where("body_id=?", body.ID).Order("tenant_id,knowledge_base_id").Find(&scopes).Error; e != nil {
		return "", e
	}
	// Preserve the old authenticated format for old events without scope rows.
	if len(scopes) == 0 {
		data, e := json.Marshal(struct {
			Tenant  uint64
			BodyID  string
			Fields  []string
			Parents []originalBodyParentRef
		}{body.TenantID, body.ID, fields, parents})
		return bodySHA(data), e
	}
	data, e := json.Marshal(struct {
		Tenant      uint64
		BodyID      string
		Fields      []string
		Parents     []originalBodyParentRef
		LeaseScopes []originalBodyLeaseScope
	}{body.TenantID, body.ID, fields, parents, scopes})
	return bodySHA(data), e
}

// AcquireMessageBodyReadLeases performs metadata verification and exact body
// ID claims before the first original/material body read, on one caller-owned
// transaction. Parent plans are included without loading their message bytes.
func (s *KnowledgeInputProvenanceStore) AcquireMessageBodyReadLeases(ctx context.Context, plan *types.MessageMaterialReadPlan, owner string, ttl time.Duration, existing ...[]string) ([]string, error) {
	if !originalBodySchemaPresent(s.db) {
		return nil, nil
	}
	if plan == nil || owner == "" || ttl <= 0 || ttl > 30*time.Minute {
		return nil, ErrOriginalBodyUnavailable
	}
	ids := []string{}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := s.checkMessageMaterialPlanInTx(tx, plan.Request); e != nil {
			return e
		}
		retention := NewOriginalBodyRetentionStore(tx)
		if e := retention.checkJournalInTx(tx); e != nil {
			return e
		}
		seen := map[string]bool{}
		if len(existing) > 0 && len(existing[0]) > 0 {
			now, e := nextcloudLeaseNowMS(bodyInternal(tx))
			if e != nil {
				return e
			}
			var active []originalBodyLease
			if e := bodyInternal(tx).Select("body_id").Where("lease_id IN ? AND kind='read' AND released_at_ms IS NULL AND expires_at_ms>?", existing[0], now).Find(&active).Error; e != nil {
				return e
			}
			for _, lease := range active {
				seen[lease.BodyID] = true
			}
		}
		var claim func(*types.MessageMaterialReadPlan) error
		claim = func(current *types.MessageMaterialReadPlan) error {
			for _, parent := range current.ParentPlans {
				if e := claim(parent); e != nil {
					return e
				}
			}
			for _, control := range current.Controls {
				var heads []originalBodyFieldHead
				if e := bodyInternal(tx).Where("(source_table='messages' AND row_key=?) OR (source_table='message_generation_origins' AND row_key=?)", control.ID, control.GenerationID).Find(&heads).Error; e != nil {
					return e
				}
				for _, head := range heads {
					if seen[head.BodyID] {
						continue
					}
					seen[head.BodyID] = true
					id, e := retention.acquireLeaseInTx(tx, head.BodyID, "read", owner, ttl)
					if e != nil {
						return e
					}
					ids = append(ids, id)
				}
			}
			return nil
		}
		return claim(plan)
	})
	return ids, err
}
func (s *OriginalBodyRetentionStore) RenewLease(ctx context.Context, id string, ttl time.Duration) error {
	if id == "" || ttl <= 0 || ttl > 30*time.Minute {
		return ErrOriginalBodyUnavailable
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := s.checkJournalInTx(tx); e != nil {
			return e
		}
		now, e := nextcloudLeaseNowMS(bodyInternal(tx))
		if e != nil {
			return e
		}
		var lease originalBodyLease
		if e := bodyInternal(tx).Where("lease_id=? AND released_at_ms IS NULL AND expires_at_ms>?", id, now).Take(&lease).Error; e != nil {
			return e
		}
		var body originalBodyPayload
		if e := bodyInternal(tx).Select("id,state,purge_epoch").Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", lease.BodyID).Take(&body).Error; e != nil {
			return e
		}
		if body.State != "live" || body.PurgeEpoch != lease.Epoch {
			return ErrOriginalBodyUnavailable
		}
		if e := s.checkExternalBody(body.ID); e != nil {
			return e
		}
		result := bodyInternal(tx).Model(&originalBodyLease{}).Where("lease_id=? AND released_at_ms IS NULL AND expires_at_ms>?", id, now).Update("expires_at_ms", now+ttl.Milliseconds())
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrOriginalBodyUnavailable
		}
		return nil
	})
}
func (s *KnowledgeInputProvenanceStore) RenewMessageBodyReadLease(ctx context.Context, id string, ttl time.Duration) error {
	return NewOriginalBodyRetentionStore(s.db).RenewLease(ctx, id, ttl)
}
func (s *KnowledgeInputProvenanceStore) ReleaseMessageBodyReadLease(ctx context.Context, id string) error {
	return NewOriginalBodyRetentionStore(s.db).ReleaseLease(ctx, id)
}

func verifyBodyLeaseScopeSchema(db *gorm.DB) error {
	if !db.Migrator().HasTable(&originalBodyLeaseScope{}) {
		return ErrOriginalBodyUnavailable
	}
	for _, col := range []string{"body_id", "tenant_id", "knowledge_base_id"} {
		if !db.Migrator().HasColumn(&originalBodyLeaseScope{}, col) {
			return ErrOriginalBodyUnavailable
		}
	}
	var count int64
	if db.Dialector.Name() == "postgres" {
		if e := db.Raw("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname='original_body_lease_scopes' AND n.nspname=current_schema() AND t.tgname='original_body_lease_scope_immutable' AND t.tgenabled<>'D'").Scan(&count).Error; e != nil {
			return e
		}
		if count != 1 {
			return ErrOriginalBodyUnavailable
		}
	} else if db.Dialector.Name() == "sqlite" {
		if e := db.Raw("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND tbl_name='original_body_lease_scopes' AND name IN ('original_body_lease_scope_immutable_update','original_body_lease_scope_immutable_delete')").Scan(&count).Error; e != nil {
			return e
		}
		if count != 2 {
			return ErrOriginalBodyUnavailable
		}
	} else {
		return ErrOriginalBodyUnavailable
	}
	return nil
}
