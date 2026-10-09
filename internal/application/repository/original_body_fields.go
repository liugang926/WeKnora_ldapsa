package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type originalBodyFieldDescriptor struct {
	Table, Key, Mode string
	Columns          []string
}

var originalBodyFields = []originalBodyFieldDescriptor{
	{"knowledge_auto_tag_input_manifests", "manifest_id", "vault", []string{"actual_inputs"}},
	{"knowledge_display_input_origins", "origin_id", "vault", []string{"actual_input"}},
	{"knowledge_span_start_manifests", "run_token", "vault", []string{"original_input", "actual_inputs"}},
	{"knowledge_enrichment_template_inputs", "id", "vault", []string{"parents"}},
	{"faq_input_sessions", "id", "vault", []string{"input_body", "original_snapshot", "current_snapshot"}},
	{"faq_import_output_admissions", "id", "vault", []string{"body"}},
	{"message_generation_origins", "id", "vault", []string{"initial_input"}},
	// Current entity columns keep their SQL search/storage representation. Their
	// exact mirror receipts permit clearing only after actual lifecycle closure.
	{"knowledge_processing_spans", "id", "inline", []string{"input", "output"}},
	{"knowledges", "id", "inline", []string{"description", "profile", "custom_metadata"}},
	{"chunks", "id", "inline", []string{"content", "source_content", "context_header", "metadata"}},
	{"messages", "id", "inline", []string{"content", "rendered_content", "context_checkpoint", "agent_trace"}},
	{"sessions", "id", "inline", []string{"description"}},
	{"wiki_pages", "id", "unsupported", []string{"content", "summary", "page_metadata"}},
	{"wiki_page_revisions", "id", "unsupported", []string{"content", "summary"}},
}

func bodyDescriptor(table string) (originalBodyFieldDescriptor, bool) {
	for _, d := range originalBodyFields {
		if d.Table == table {
			return d, true
		}
	}
	return originalBodyFieldDescriptor{}, false
}
func bodyText(v any) string {
	switch value := v.(type) {
	case nil:
		return ""
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}
func bodyUint(v any) uint64 { n, _ := strconv.ParseUint(bodyText(v), 10, 64); return n }
func bodySourceRow(tx *gorm.DB, d originalBodyFieldDescriptor, key string) (map[string]any, error) {
	var rows []map[string]any
	e := bodyInternal(tx).Table(d.Table).Where(d.Key+"=?", key).Find(&rows).Error
	if e != nil {
		return nil, e
	}
	if len(rows) == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	if len(rows) != 1 {
		return nil, ErrOriginalBodyUnavailable
	}
	return rows[0], nil
}
func bodyScopeFromRow(tx *gorm.DB, d originalBodyFieldDescriptor, key string, row map[string]any) (originalBodyScope, error) {
	scope := originalBodyScope{Tenant: bodyUint(row["tenant_id"]), KB: bodyText(row["knowledge_base_id"]), Knowledge: bodyText(row["knowledge_id"]), OwnerKind: d.Table, OwnerID: key, AdmissionID: bodyText(row["admission_id"]), Lineage: bodyText(row["source_lineage"])}
	if d.Table == "knowledges" {
		scope.Knowledge = key
	}
	if d.Table == "messages" || d.Table == "message_generation_origins" {
		message := row
		if d.Table == "message_generation_origins" {
			var e error
			message, e = bodySourceRow(tx, originalBodyFieldDescriptor{Table: "messages", Key: "id"}, bodyText(row["message_id"]))
			if e != nil {
				return scope, e
			}
		}
		session, e := bodySourceRow(tx, originalBodyFieldDescriptor{Table: "sessions", Key: "id"}, bodyText(message["session_id"]))
		if e != nil {
			return scope, e
		}
		scope.Tenant = bodyUint(session["tenant_id"])
		scope.Knowledge = bodyText(message["knowledge_id"])
		scope.Lineage = bodyText(message["source_lineage"])
	}
	if scope.Knowledge != "" && (scope.Tenant == 0 || scope.KB == "") {
		knowledge, e := bodySourceRow(tx, originalBodyFieldDescriptor{Table: "knowledges", Key: "id"}, scope.Knowledge)
		if e != nil {
			return scope, e
		}
		if scope.Tenant == 0 {
			scope.Tenant = bodyUint(knowledge["tenant_id"])
		}
		if scope.KB == "" {
			scope.KB = bodyText(knowledge["knowledge_base_id"])
		}
	}
	if d.Table == "sessions" {
		scope.Tenant = bodyUint(row["tenant_id"])
	}
	if scope.Tenant == 0 {
		return scope, ErrOriginalBodyUnavailable
	}
	return scope, nil
}
func (s *OriginalBodyRetentionStore) captureField(tx *gorm.DB, d originalBodyFieldDescriptor, key, col, raw string, scope originalBodyScope) error {
	if e := s.checkJournalInTx(tx); e != nil {
		return e
	}
	if raw == "" || raw == "null" {
		return nil
	}
	if _, ok := bodyReference(raw); ok {
		var head originalBodyFieldHead
		if e := bodyInternal(tx).Where("source_table=? AND row_key=? AND column_name=?", d.Table, key, col).Take(&head).Error; e != nil {
			return ErrOriginalBodyUnavailable
		}
		if head.OriginalBodyRef != raw {
			return ErrOriginalBodyUnavailable
		}
		return CheckOriginalBodyAvailableInTx(tx.Statement.Context, tx, d.Table, key)
	}
	sha := bodySHA([]byte(raw))
	identity := bodyIdentity(d.Table, key, col, scope)
	var prior originalBodyFieldHead
	e := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("source_table=? AND row_key=? AND column_name=?", d.Table, key, col).Take(&prior).Error
	if e != nil && !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	now, e := nextcloudLeaseNowMS(tx)
	if e != nil {
		return e
	}
	if prior.SlotID != "" {
		if prior.SourceIdentitySHA256 != identity {
			return ErrOriginalBodyUnavailable
		}
		var old originalBodyPayload
		if e := bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", prior.BodyID).Take(&old).Error; e != nil {
			return e
		}
		if old.State != "live" {
			return ErrOriginalBodyUnavailable
		}
		if e := s.checkExternalBody(old.ID); e != nil {
			return e
		}
		if old.BodySHA256 == sha {
			return nil
		}
		// A genuine producer change retires the prior representation. Cached
		// originals are still checked by their own original producer before writing.
		if old.RetiredAtMS == nil {
			if e := bodyInternal(tx).Model(&old).Updates(map[string]any{"retired_at_ms": now, "body_deadline_ms": now + int64(3600000), "metadata_deadline_ms": now + int64(90*86400000)}).Error; e != nil {
				return e
			}
		}
	}
	domain := scope.OwnerKind + "/" + scope.OwnerID
	if scope.AdmissionID != "" {
		domain = "admission/" + scope.AdmissionID
	}
	domainSHA := bodySHA([]byte(fmt.Sprintf("%d/%s/%s/%s", scope.Tenant, scope.KB, scope.Knowledge, domain)))
	var body originalBodyPayload
	e = bodyInternal(tx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND origin_domain_sha256=? AND body_sha256=? AND storage_kind=? AND state='live'", scope.Tenant, domainSHA, sha, d.Mode).Order("created_at_ms,id").Take(&body).Error
	if errors.Is(e, gorm.ErrRecordNotFound) {
		id := uuid.NewString()
		var payload *string
		if d.Mode == "vault" {
			payload = &raw
		}
		body = originalBodyPayload{ID: id, TenantID: scope.Tenant, BodySHA256: sha, OriginDomainSHA256: domainSHA, Payload: payload, BodyBytes: int64(len([]byte(raw))), StorageKind: d.Mode, State: "live", CreatedAtMS: now}
		if e = bodyInternal(tx).Create(&body).Error; e != nil {
			return e
		}
	} else if e != nil {
		return e
	}
	id := body.ID
	slot := prior.SlotID
	if slot == "" {
		slot = uuid.NewString()
	}
	ref := bodyRefString(slot, id, sha)
	if d.Mode == "vault" && tx.Dialector.Name() == "postgres" {
		// JSON columns canonicalize object references in PostgreSQL; text fields
		// preserve exact producer bytes. Authority compares the actual stored form.
		if jsonCol, e := originalBodyColumnIsJSON(tx, d.Table, col); e != nil {
			return e
		} else if jsonCol {
			if e = bodyInternal(tx).Raw("SELECT CAST(? AS jsonb)::text", ref).Scan(&ref).Error; e != nil {
				return e
			}
		}
	}
	head := originalBodyFieldHead{SlotID: slot, SourceTable: d.Table, RowKey: key, ColumnName: col, TenantID: scope.Tenant, KnowledgeBaseID: scope.KB, KnowledgeID: scope.Knowledge, OwnerKind: scope.OwnerKind, OwnerID: scope.OwnerID, SourceIdentitySHA256: identity, BodyID: id, Revision: prior.Revision + 1, AdmissionID: scope.AdmissionID, SourceLineage: scope.Lineage, OriginalBodyRef: ref}
	if e := bodyInternal(tx).Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "source_table"}, {Name: "row_key"}, {Name: "column_name"}}, UpdateAll: true}).Create(&head).Error; e != nil {
		return e
	}
	if e := bodyInternal(tx).Table("original_body_field_revisions").Create(&head).Error; e != nil {
		return e
	}
	if e := s.recordBodyParents(tx, body.ID, raw); e != nil {
		return e
	}
	if d.Mode == "vault" {
		return originalBodyRewrite(tx, d, key, col, id, raw, ref)
	}
	return nil
}
func (s *OriginalBodyRetentionStore) recordBodyParents(tx *gorm.DB, id, raw string) error {
	var data any
	if json.Unmarshal([]byte(raw), &data) != nil {
		return nil
	}
	seen := map[string]bool{}
	var walk func(any) error
	walk = func(value any) error {
		switch v := value.(type) {
		case []any:
			for _, child := range v {
				if e := walk(child); e != nil {
					return e
				}
			}
		case map[string]any:
			admission, ok := v["admission_id"].(string)
			if ok && admission != "" && !seen[admission] && v["knowledge_id"] != nil {
				var origin knowledgeInputAdmission
				if e := bodyInternal(tx).Where("admission_id=?", admission).Take(&origin).Error; e != nil {
					return ErrOriginalBodyUnavailable
				}
				if bodyText(v["knowledge_id"]) != origin.KnowledgeID || bodyText(v["knowledge_base_id"]) != origin.KnowledgeBaseID {
					return ErrOriginalBodyUnavailable
				}
				seen[admission] = true
				if e := bodyInternal(tx).Clauses(clause.OnConflict{DoNothing: true}).Create(&originalBodyParentRef{BodyID: id, TenantID: origin.TenantID, KnowledgeBaseID: origin.KnowledgeBaseID, KnowledgeID: origin.KnowledgeID, AdmissionID: admission}).Error; e != nil {
					return e
				}
			}
			for _, child := range v {
				if e := walk(child); e != nil {
					return e
				}
			}
		}
		return nil
	}
	return walk(data)
}

// Inventory moves exact known SQL fields without granting a producer origin.
// Existing unknown/external copies remain independently observable; the scan
// never signs a legacy generation from a current configuration or empty rows.
func (s *OriginalBodyRetentionStore) Inventory(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	count := 0
	for _, d := range originalBodyFields {
		if !s.db.Migrator().HasTable(d.Table) {
			continue
		}
		var keys []string
		if e := bodyInternal(s.db.WithContext(ctx)).Table(d.Table).Select("CAST(" + d.Key + " AS TEXT)").Order(d.Key).Limit(limit).Scan(&keys).Error; e != nil {
			return count, e
		}
		for _, key := range keys {
			e := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				row, e := bodySourceRow(tx, d, key)
				if e != nil {
					return e
				}
				scope, e := bodyScopeFromRow(tx, d, key, row)
				if e != nil {
					return e
				}
				for _, col := range d.Columns {
					raw := bodyText(row[col])
					if raw == "" {
						continue
					}
					if e := s.captureField(tx, d, key, col, raw, scope); e != nil {
						return e
					}
				}
				return nil
			})
			if e != nil {
				return count, e
			}
			count++
		}
	}
	return count, nil
}

var originalBodyCallbackMu sync.Mutex

// Register real producer/read callbacks once. They are inactive before the
// migration and use the caller's transaction, including SQLite pool size one.
func InstallOriginalBodyRetentionCallbacks(db *gorm.DB) error {
	originalBodyCallbackMu.Lock()
	defer originalBodyCallbackMu.Unlock()
	if db.Callback().Query().Get("original_body:resolve") != nil {
		return nil
	}
	write := func(tx *gorm.DB) {
		if os.Getenv("WEKNORA_BODY_CALLBACK_DEBUG") == "1" {
			fmt.Fprintf(os.Stderr, "body callback table=%s type=%T internal=%v err=%v\n", tx.Statement.Table, tx.Statement.Dest, tx.Statement.Context.Value(originalBodyInternalKey{}), tx.Error)
		}
		if tx.Error != nil {
			return
		}
		if inside, _ := tx.Statement.Context.Value(originalBodyInternalKey{}).(bool); inside {
			return
		}
		d, ok := bodyDescriptor(tx.Statement.Table)
		if !ok {
			return
		}
		feature := originalBodySchemaPresent(tx)
		if os.Getenv("WEKNORA_BODY_CALLBACK_DEBUG") == "1" {
			fmt.Fprintf(os.Stderr, "body feature=%v table=%s schema=%v reflect=%v\n", feature, tx.Statement.Table, tx.Statement.Schema != nil, tx.Statement.ReflectValue.Kind())
		}
		if !feature {
			return
		}
		store := NewOriginalBodyRetentionStore(tx)
		if e := store.captureStatement(tx, d); e != nil {
			tx.AddError(e)
		}
	}
	read := func(tx *gorm.DB) {
		if tx.Error != nil {
			return
		}
		if inside, _ := tx.Statement.Context.Value(originalBodyInternalKey{}).(bool); inside {
			return
		}
		table := tx.Statement.Table
		if tx.Statement.Schema != nil {
			table = tx.Statement.Schema.Table
		}
		d, ok := bodyDescriptor(table)
		if !ok {
			return
		}
		if !originalBodySchemaPresent(tx) {
			return
		}
		store := NewOriginalBodyRetentionStore(tx)
		if e := store.resolveStatement(tx, d); e != nil {
			tx.AddError(e)
		}
	}
	if e := db.Callback().Create().After("gorm:create").Before("gorm:save_after_associations").Register("original_body:capture", write); e != nil {
		return e
	}
	if e := db.Callback().Update().After("gorm:update").Before("gorm:save_after_associations").Register("original_body:capture", write); e != nil {
		return e
	}
	return db.Callback().Query().After("gorm:query").Before("gorm:after_query").Register("original_body:resolve", read)
}
func bodyVisit(value reflect.Value, fn func(reflect.Value) error) error {
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		for n := 0; n < value.Len(); n++ {
			if e := bodyVisit(value.Index(n), fn); e != nil {
				return e
			}
		}
		return nil
	}
	return fn(value)
}
func bodyRecordKey(tx *gorm.DB, d originalBodyFieldDescriptor, v reflect.Value) string {
	if v.Kind() == reflect.Map {
		found := v.MapIndex(reflect.ValueOf(d.Key))
		if found.IsValid() {
			return bodyText(found.Interface())
		}
		return ""
	}
	if tx.Statement.Schema != nil {
		if field := tx.Statement.Schema.FieldsByDBName[d.Key]; field != nil {
			value, _ := field.ValueOf(tx.Statement.Context, v)
			return bodyText(value)
		}
	}
	return ""
}
func (s *OriginalBodyRetentionStore) captureStatement(tx *gorm.DB, d originalBodyFieldDescriptor) error {
	keys := map[string]bool{}
	if e := bodyVisit(tx.Statement.ReflectValue, func(v reflect.Value) error {
		if key := bodyRecordKey(tx, d, v); key != "" {
			keys[key] = true
		}
		return nil
	}); e != nil {
		return e
	}
	if len(keys) == 0 {
		if where, ok := tx.Statement.Clauses["WHERE"]; ok {
			var ids []string
			if e := bodyInternal(tx).Table(d.Table).Select("CAST(" + bodyQuoted(d.Key) + " AS TEXT)").Where(where.Expression).Limit(1001).Scan(&ids).Error; e != nil {
				return e
			}
			if len(ids) > 1000 {
				return ErrOriginalBodyUnavailable
			}
			for _, id := range ids {
				keys[id] = true
			}
		}
	}
	for key := range keys {
		row, e := bodySourceRow(tx, d, key)
		if e != nil {
			return e
		}
		scope, e := bodyScopeFromRow(tx, d, key, row)
		if e != nil {
			return e
		}
		for _, col := range d.Columns {
			raw := bodyText(row[col])
			if raw == "" {
				continue
			}
			if e := s.captureField(tx, d, key, col, raw, scope); e != nil {
				return e
			}
		}
	}
	return nil
}
func (s *OriginalBodyRetentionStore) resolveStatement(tx *gorm.DB, d originalBodyFieldDescriptor) error {
	return bodyVisit(tx.Statement.ReflectValue, func(v reflect.Value) error {
		key := bodyRecordKey(tx, d, v)
		if key == "" {
			return nil
		}
		var heads []originalBodyFieldHead
		if e := bodyInternal(tx).Where("source_table=? AND row_key=?", d.Table, key).Find(&heads).Error; e != nil {
			return e
		}
		for _, head := range heads {
			var raw string
			var field reflect.Value
			if v.Kind() == reflect.Map {
				value := v.MapIndex(reflect.ValueOf(head.ColumnName))
				if !value.IsValid() {
					continue
				}
				raw = bodyText(value.Interface())
			} else if tx.Statement.Schema != nil {
				f := tx.Statement.Schema.FieldsByDBName[head.ColumnName]
				if f == nil {
					continue
				}
				value, _ := f.ValueOf(tx.Statement.Context, v)
				switch body := value.(type) {
				case string:
					raw = body
				case []byte:
					raw = string(body)
				default:
					b, _ := json.Marshal(value)
					raw = string(b)
				}
				field = v.FieldByIndex(f.StructField.Index)
			}
			if ref, ok := bodyReference(raw); ok {
				body, e := s.resolveInTx(tx, head, ref)
				if e != nil {
					return e
				}
				if v.Kind() == reflect.Map {
					v.SetMapIndex(reflect.ValueOf(head.ColumnName), reflect.ValueOf(body))
					continue
				}
				if !field.CanSet() {
					return ErrOriginalBodyUnavailable
				}
				if field.Kind() == reflect.String {
					field.SetString(body)
				} else if field.Kind() == reflect.Slice && field.Type().Elem().Kind() == reflect.Uint8 {
					field.SetBytes([]byte(body))
				} else if e = json.Unmarshal([]byte(body), field.Addr().Interface()); e != nil {
					return e
				}
			} else {
				if e := CheckOriginalBodyAvailableInTx(tx.Statement.Context, tx, d.Table, key); e != nil {
					return e
				}
				if d.Mode == "vault" && raw != "" {
					return ErrOriginalBodyUnavailable
				} // inline restore of an original snapshot
			}
		}
		return nil
	})
}

var _ = strings.Contains

func originalBodySchemaPresent(tx *gorm.DB) bool {
	var count int64
	if tx.Dialector.Name() == "sqlite" {
		return bodyInternal(tx).Raw("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='original_body_payloads'").Scan(&count).Error == nil && count == 1
	}
	if tx.Dialector.Name() == "postgres" {
		return bodyInternal(tx).Raw("SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='original_body_payloads'").Scan(&count).Error == nil && count == 1
	}
	return false
}

// ShareOriginalBodyFieldInTx is an internal exact copy operation. The target
// producer has already persisted the same body bytes under its own authority;
// this only deduplicates storage and creates an additional incoming owner.
// It cannot introduce cached text, cross-tenant sharing, or a purged body ID.
func(s *OriginalBodyRetentionStore)ShareOriginalBodyFieldInTx(ctx context.Context,tx *gorm.DB,bodyID,table,key,col string)error{
 d,ok:=bodyDescriptor(table);if !ok||d.Mode!="vault"{return ErrOriginalBodyUnavailable}
 allowed:=false;for _,field:=range d.Columns{if field==col{allowed=true}};if !allowed{return ErrOriginalBodyUnavailable}
 if e:=s.checkJournalInTx(tx);e!=nil{return e};if e:=s.checkExternalBody(bodyID);e!=nil{return e}
 var source originalBodyPayload;if e:=bodyInternal(tx).Clauses(clause.Locking{Strength:"UPDATE"}).Where("id=?",bodyID).Take(&source).Error;e!=nil{return e}
 if source.State!="live"||source.Payload==nil||bodySHA([]byte(*source.Payload))!=source.BodySHA256{return ErrOriginalBodyUnavailable}
 row,e:=bodySourceRow(tx,d,key);if e!=nil{return e};scope,e:=bodyScopeFromRow(tx,d,key,row);if e!=nil{return e};if scope.Tenant!=source.TenantID{return ErrOriginalBodyUnavailable}
 var target originalBodyFieldHead;if e:=bodyInternal(tx).Where("source_table=? AND row_key=? AND column_name=?",table,key,col).Take(&target).Error;e!=nil{return e}
 ref,ok:=bodyReference(bodyText(row[col]));if !ok{return ErrOriginalBodyUnavailable}
 raw,e:=s.resolveInTx(tx,target,ref);if e!=nil{return e};if bodySHA([]byte(raw))!=source.BodySHA256{return ErrOriginalBodyUnavailable}
 target.BodyID=source.ID;target.Revision++;target.OriginalBodyRef=bodyRefString(target.SlotID,source.ID,source.BodySHA256)
 if e:=bodyInternal(tx).Model(&originalBodyFieldHead{}).Where("slot_id=?",target.SlotID).Updates(map[string]any{"body_id":target.BodyID,"revision":target.Revision,"original_body_ref":target.OriginalBodyRef}).Error;e!=nil{return e}
 if e:=bodyInternal(tx).Table("original_body_field_revisions").Create(&target).Error;e!=nil{return e}
 return originalBodyRewrite(tx,d,key,col,source.ID,bodyText(row[col]),target.OriginalBodyRef)
}
