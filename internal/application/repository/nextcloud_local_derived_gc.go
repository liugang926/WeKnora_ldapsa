package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
	"strconv"
	"time"
)

type nextcloudDerivedGCManifest struct {
	JobID                                                                string `gorm:"primaryKey"`
	PlanSHA256, InventoryJSON, InventorySHA256, State                    string
	CreatedAtMS                                                          int64
	CompletedAtMS                                                        *int64
	PhysicalItems, PhysicalCollected, ProtectedItems, RetainedAuditItems int
	AuditRetainUntilMS                                                   int64
}

func (nextcloudDerivedGCManifest) TableName() string { return "nextcloud_derived_gc_manifests" }

type nextcloudDerivedGCReceipt struct {
	JobID, ItemKind, ObjectRef  string `gorm:"primaryKey"`
	ScopeSHA256, BeforeSHA256   string
	DeletedRows, ConfirmedBytes int64
	ClaimToken                  string
	ClaimEpoch, CompletedAtMS   int64
}

func (nextcloudDerivedGCReceipt) TableName() string { return "nextcloud_derived_gc_receipts" }

type gcDerivedInventory struct {
	Chunks, Embeddings, Resources, GraphOperations []string
	Audit                                          []gcAuditEntry
	Backend                                        nextcloudBackendSelection
	EmbeddingDimensions                            map[string]int64
	ResourceIdentities                             map[string]string
}
type gcAuditEntry struct {
	Table, Key, ID, Digest string
	Bytes                  int64
}

func gcReadExactRows(tx *gorm.DB, table, where string, args ...any) ([]map[string]any, error) {
	var rows []map[string]any
	e := tx.Table(table).Where(where, args...).Find(&rows).Error
	return rows, e
}
func gcAuditInventory(tx *gorm.DB, job nextcloudGCJob) ([]gcAuditEntry, error) {
	result := []gcAuditEntry{}
	// Inventory includes fulltext-bearing rows, but AuditRetainUntilMS is only
	// a metadata/audit deadline. It does not authorize 90-day body retention;
	// the body lifecycle must apply PRD source-state-specific deadlines.
	tables := []struct {
		table, key, scope string
		args              []any
	}{
		{"knowledge_input_admissions", "admission_id", "knowledge_id=?", []any{job.KnowledgeID}},
		{"knowledge_input_material_receipts", "row_id", "admission_id IN (SELECT admission_id FROM knowledge_input_admissions WHERE knowledge_id=?)", []any{job.KnowledgeID}},
		{"knowledge_input_actor_selectors", "admission_id", "admission_id IN (SELECT admission_id FROM knowledge_input_admissions WHERE knowledge_id=?)", []any{job.KnowledgeID}},
		{"knowledge_config_input_origins", "origin_id", "tenant_id=? AND knowledge_base_id=?", []any{job.TenantID, job.KnowledgeBaseID}},
		{"knowledge_display_input_origins", "origin_id", "tenant_id=? AND knowledge_base_id=? AND (row_id=? OR kind='kb-name' OR row_id IN (SELECT tag_id FROM knowledge_tag_relations WHERE knowledge_id=?))", []any{job.TenantID, job.KnowledgeBaseID, job.KnowledgeID, job.KnowledgeID}},
		{"knowledge_span_body_receipts", "run_token", "knowledge_id=?", []any{job.KnowledgeID}},
		{"faq_entry_material_receipts", "chunk_id", "admission_id IN (SELECT id FROM faq_entry_input_admissions WHERE knowledge_id=?)", []any{job.KnowledgeID}},
		{"faq_input_actor_selectors", "admission_id", "admission_id IN (SELECT session_id FROM faq_entry_input_admissions WHERE knowledge_id=?)", []any{job.KnowledgeID}},
		{"faq_catalog_input_origins", "knowledge_id", "knowledge_id=?", []any{job.KnowledgeID}},
		{"knowledge_input_history_parents", "admission_id", "admission_id IN (SELECT admission_id FROM knowledge_input_admissions WHERE knowledge_id=?)", []any{job.KnowledgeID}},
		{"knowledge_producer_inputs", "id", "admission_id IN (SELECT admission_id FROM knowledge_input_admissions WHERE knowledge_id=?)", []any{job.KnowledgeID}},
		{"knowledge_span_start_manifests", "run_token", "knowledge_id=?", []any{job.KnowledgeID}},
		{"knowledge_processing_spans", "id", "knowledge_id=?", []any{job.KnowledgeID}},
		{"knowledge_auto_tag_input_manifests", "manifest_id", "tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", []any{job.TenantID, job.KnowledgeBaseID, job.KnowledgeID}},
		{"faq_entry_input_admissions", "id", "tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", []any{job.TenantID, job.KnowledgeBaseID, job.KnowledgeID}},
		{"faq_input_sessions", "id", "id IN (SELECT session_id FROM faq_entry_input_admissions WHERE knowledge_id=?)", []any{job.KnowledgeID}},
	}
	// Reverse source-dependent configuration bodies include cloned target KBs.
	// The exact immutable source identity is checked before body inventory; a
	// matching JSON substring or KB UUID is not sufficient proof.
	dependent, err := configurationOriginalBodyDependentKBs(tx, job)
	if err != nil {
		return nil, err
	}
	for _, target := range dependent {
		tables = append(tables, struct {
			table, key, scope string
			args              []any
		}{"knowledge_bases", "id", "tenant_id=? AND id=?", []any{target.TenantID, target.KnowledgeBaseID}})
	}
	inputs, err := configurationOriginalBodyDependentTemplateInputs(tx, job)
	if err != nil {
		return nil, err
	}
	for _, id := range inputs {
		tables = append(tables, struct {
			table, key, scope string
			args              []any
		}{"knowledge_enrichment_template_inputs", "id", "id=?", []any{id}})
	}
	for _, entry := range tables {
		if !tx.Migrator().HasTable(entry.table) {
			continue
		}
		rows, e := gcReadExactRows(tx, entry.table, entry.scope, entry.args...)
		if e != nil {
			return nil, e
		}
		for _, row := range rows {
			body, e := gcOriginalBodyAuditMetadata(tx, entry.table, bodyText(row[entry.key]), row)
			if e != nil {
				return nil, e
			}
			result = append(result, gcAuditEntry{entry.table, entry.key, fmt.Sprint(row[entry.key]), recoverySHA(body), int64(len(body))})
		}
	}
	return result, nil
}
func gcSealLocalManifest(tx *gorm.DB, job nextcloudGCJob, nowMS int64) (nextcloudDerivedGCManifest, error) {
	var existing nextcloudDerivedGCManifest
	if e := tx.Where("job_id=?", job.ID).Take(&existing).Error; e == nil {
		return existing, nil
	} else if !errors.Is(e, gorm.ErrRecordNotFound) {
		return existing, e
	}
	var plan nextcloudDerivedBackendPlan
	if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Take(&plan).Error; e != nil {
		return existing, ErrNextcloudContentGCUncovered
	}
	var backend nextcloudBackendSelection
	if json.Unmarshal([]byte(plan.BackendPlan), &backend) != nil || recoverySHA([]byte(plan.BackendPlan)) != plan.PlanSHA256 || plan.DataSourceID != job.DataSourceID || plan.ExternalID != job.ExternalID {
		return existing, ErrNextcloudContentLeaseDenied
	}
	if backend.Graph && backend.GraphBackend != NextcloudGraphBackend || backend.Wiki || backend.VectorStoreID != "" {
		return existing, fmt.Errorf("unverified external derived backend")
	}
	for _, engine := range backend.Engines {
		if engine != string(types.SQLiteRetrieverEngineType) && engine != string(types.PostgresRetrieverEngineType) {
			return existing, fmt.Errorf("unverified external derived backend")
		}
		if (engine == string(types.SQLiteRetrieverEngineType)) != (tx.Dialector.Name() == "sqlite") {
			return existing, fmt.Errorf("unverified separate derived database")
		}
	}
	inventory := gcDerivedInventory{Chunks: []string{}, Embeddings: []string{}, Resources: []string{}, Backend: backend, EmbeddingDimensions: map[string]int64{}, ResourceIdentities: map[string]string{}}
	if e := tx.Table("chunks").Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Order("id").Pluck("id", &inventory.Chunks).Error; e != nil {
		return existing, e
	}
	table := "embeddings"
	if tx.Dialector.Name() == "sqlite" {
		table = "lite_embeddings"
	}
	if len(backend.Engines) > 0 {
		if !tx.Migrator().HasTable(table) {
			return existing, ErrNextcloudContentGCUncovered
		}
		var ids []int64
		if e := tx.Table(table).Where("knowledge_base_id=? AND knowledge_id=?", job.KnowledgeBaseID, job.KnowledgeID).Order("id").Pluck("id", &ids).Error; e != nil {
			return existing, e
		}
		for _, id := range ids {
			key := strconv.FormatInt(id, 10)
			inventory.Embeddings = append(inventory.Embeddings, key)
			var row struct{ Dimension int64 }
			if e := tx.Table(table).Select("dimension").Where("id=?", id).Take(&row).Error; e != nil {
				return existing, e
			}
			inventory.EmbeddingDimensions[key] = row.Dimension
		}
	}
	// Every observed row must have an actual same-transaction writer witness.
	// The union also retains IDs removed by a covered retry, so native orphan
	// vector/FTS rows are verified rather than inferred from metadata absence.
	var artifacts []nextcloudDerivedArtifact
	if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Order("item_kind,object_ref").Find(&artifacts).Error; e != nil {
		return existing, e
	}
	chunkSet, embeddingSet, graphSet := map[string]bool{}, map[string]bool{}, map[string]bool{}
	expectedKind := "postgres_embedding"
	if tx.Dialector.Name() == "sqlite" {
		expectedKind = "sqlite_embedding"
	}
	for _, a := range artifacts {
		if a.PlanSHA256 != plan.PlanSHA256 {
			return existing, ErrNextcloudContentLeaseDenied
		}
		switch a.ItemKind {
		case "scoped_sql_graph":
			if !backend.Graph || backend.GraphBackend != NextcloudGraphBackend {
				return existing, ErrNextcloudContentGCUncovered
			}
			graphSet[a.ObjectRef] = true
		case "derived_chunk":
			chunkSet[a.ObjectRef] = true
		case expectedKind:
			if dim, ok := inventory.EmbeddingDimensions[a.ObjectRef]; ok && dim != a.Dimension {
				return existing, ErrNextcloudContentLeaseDenied
			}
			embeddingSet[a.ObjectRef] = true
			inventory.EmbeddingDimensions[a.ObjectRef] = a.Dimension
		default:
			return existing, ErrNextcloudContentGCUncovered
		}
	}
	if backend.Graph {
		var records []nextcloudGraphRecord
		if e := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Find(&records).Error; e != nil {
			return existing, e
		}
		for _, record := range records {
			if !graphSet[record.OperationID] || record.Backend != NextcloudGraphBackend || recoverySHA([]byte(record.BodyJSON)) != record.BodySHA256 {
				return existing, ErrNextcloudContentGCUncovered
			}
		}
	}
	for id := range graphSet {
		inventory.GraphOperations = append(inventory.GraphOperations, id)
	}
	sort.Strings(inventory.GraphOperations)
	for _, id := range inventory.Chunks {
		if !chunkSet[id] {
			return existing, ErrNextcloudContentGCUncovered
		}
	}
	for _, id := range inventory.Embeddings {
		if !embeddingSet[id] {
			return existing, ErrNextcloudContentGCUncovered
		}
	}
	inventory.Chunks = []string{}
	for id := range chunkSet {
		inventory.Chunks = append(inventory.Chunks, id)
	}
	sort.Strings(inventory.Chunks)
	inventory.Embeddings = []string{}
	for id := range embeddingSet {
		inventory.Embeddings = append(inventory.Embeddings, id)
	}
	sort.Strings(inventory.Embeddings)
	if e := tx.Model(&nextcloudGCItem{}).Where("job_id=? AND kind IN ('source_file','extracted_image')", job.ID).Order("object_ref").Pluck("object_ref", &inventory.Resources).Error; e != nil {
		return existing, e
	}
	resourceSet := map[string]bool{}
	for _, ref := range inventory.Resources {
		resourceSet[ref] = true
	}
	inventory.Resources = []string{}
	for ref := range resourceSet {
		inventory.Resources = append(inventory.Resources, ref)
	}
	sort.Strings(inventory.Resources)
	for _, ref := range inventory.Resources {
		handle, ok := types.ParseResourcePath(ref)
		if !ok {
			return existing, ErrNextcloudContentGCUncovered
		}
		var resource types.StoredResource
		if e := tx.Unscoped().Where("handle=? AND tenant_id=?", handle, job.TenantID).Take(&resource).Error; e != nil {
			return existing, e
		}
		if !scopedLocalGCPath(&resource) {
			return existing, ErrNextcloudContentGCUncovered
		}
		inventory.ResourceIdentities[ref] = gcDerivedResourceIdentity(resource)
	}
	audit, e := gcAuditInventory(tx, job)
	if e != nil {
		return existing, e
	}
	inventory.Audit = audit
	body, e := json.Marshal(inventory)
	if e != nil {
		return existing, e
	}
	existing = nextcloudDerivedGCManifest{JobID: job.ID, PlanSHA256: plan.PlanSHA256, InventoryJSON: string(body), InventorySHA256: recoverySHA(body), State: "sealed", CreatedAtMS: nowMS, PhysicalItems: len(inventory.Chunks) + len(inventory.Embeddings) + len(inventory.Resources) + len(inventory.GraphOperations), RetainedAuditItems: len(audit), AuditRetainUntilMS: job.CreatedAt.Add(90 * 24 * time.Hour).UnixMilli()}
	return existing, tx.Create(&existing).Error
}
func (s *NextcloudGCStore) runLocalDerived(ctx context.Context, id string, now time.Time) error {
	if !s.db.Migrator().HasTable(&nextcloudDerivedBackendPlan{}) {
		return nil
	} // Legacy remains its observable derived_index blocker.
	var identity nextcloudGCJob
	if e := s.db.WithContext(ctx).Where("id=?", id).Take(&identity).Error; e != nil {
		return e
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tx = bodyInternal(tx)
		nowMS, e := gcLockKBContent(tx, identity)
		if e != nil {
			return e
		}
		// Restored data is only reported: a collected manifest never authorizes
		// a second deletion. This also checks candidate/ETag restoration before
		// returning an idempotent success for physically absent SQL rows.
		var completed nextcloudDerivedGCManifest
		if e := tx.Where("job_id=? AND state='collected'", id).Take(&completed).Error; e == nil {
			var inventory gcDerivedInventory
			if recoverySHA([]byte(completed.InventoryJSON)) != completed.InventorySHA256 || json.Unmarshal([]byte(completed.InventoryJSON), &inventory) != nil {
				return ErrNextcloudContentLeaseDenied
			}
			var source nextcloudSourceVersion
			if e := nextcloudVersionQuery(tx, identity.TenantID, identity.KnowledgeBaseID, identity.DataSourceID, identity.ExternalID).Take(&source).Error; e != nil {
				return e
			}
			if source.State != "tombstone" && source.CandidateKnowledgeID == identity.KnowledgeID {
				return ErrNextcloudContentLeaseDenied
			}
			if e := s.verifyLocalDerivedGone(tx, identity, inventory); e != nil {
				return e
			}
			return s.verifyLocalResourceRefs(tx, identity, inventory)
		} else if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		fence, e := gcCheckRetiredContent(tx, identity, nowMS)
		if e != nil {
			return nil
		}
		var job nextcloudGCJob
		if e = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", id).Take(&job).Error; e != nil {
			return e
		}
		if e := gcValidateManualAdmin(ctx, tx, job); e != nil {
			return e
		}

		if gcContentScope(job) != gcContentScope(identity) || job.NotBefore.UnixMilli() > nowMS {
			return ErrNextcloudContentLeaseDenied
		}
		manifest, e := gcSealLocalManifest(tx, job, nowMS)
		if e != nil {
			return tx.Model(&job).Updates(map[string]any{"state": "blocked", "last_error_code": "derived_backend_inventory_unverified"}).Error
		}
		var inventory gcDerivedInventory
		if recoverySHA([]byte(manifest.InventoryJSON)) != manifest.InventorySHA256 || json.Unmarshal([]byte(manifest.InventoryJSON), &inventory) != nil {
			return ErrNextcloudContentLeaseDenied
		}
		if manifest.State == "collected" {
			return s.verifyLocalDerivedGone(tx, job, inventory)
		}
		token := uuid.NewString()
		claim, e := gcMarkContentClaim(tx, job, fence, token, nowMS)
		if e != nil {
			return nil
		}
		// The source/current/build checks are repeated after the generation fence.
		var source nextcloudSourceVersion
		if e = nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}), job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID).Take(&source).Error; e != nil {
			return e
		}
		if source.State != "tombstone" && source.CandidateKnowledgeID == job.KnowledgeID {
			return ErrNextcloudContentLeaseDenied
		}
		var knowledge types.Knowledge
		if e = tx.Unscoped().Where("id=? AND tenant_id=? AND knowledge_base_id=?", job.KnowledgeID, job.TenantID, job.KnowledgeBaseID).Take(&knowledge).Error; e != nil {
			return e
		}
		if knowledge.GetMetadata()["nextcloud_etag"] != "" || knowledge.PendingSubtasksCount > 0 || (knowledge.ParseStatus != types.ParseStatusCompleted && knowledge.ParseStatus != types.ParseStatusFailed && knowledge.ParseStatus != types.ParseStatusCancelled) {
			return ErrNextcloudContentLeaseDenied
		}
		for _, embeddingID := range inventory.Embeddings {
			if e = s.deleteLocalEmbeddingInTx(tx, claim, job, embeddingID, inventory.EmbeddingDimensions[embeddingID], nowMS); e != nil {
				return e
			}
		}
		for _, operationID := range inventory.GraphOperations {
			if e = s.deleteScopedGraphInTx(tx, claim, job, operationID, nowMS); e != nil {
				return e
			}
		}
		for _, chunkID := range inventory.Chunks {
			if e = s.deleteLocalChunkInTx(tx, claim, job, chunkID, nowMS); e != nil {
				return e
			}
		}
		if e = gcReturnContentRetired(tx, claim, nowMS); e != nil {
			return e
		}
		// A later actual object phase completes resources. This phase never changes
		// its physical-byte receipts or decides that unknown external output is gone.
		return s.finishLocalManifestInTx(tx, job, manifest, inventory, nowMS)
	})
}
func (s *NextcloudGCStore) derivedReceipt(tx *gorm.DB, claim NextcloudContentGCClaim, job nextcloudGCJob, kind, id string, rows []map[string]any, count int64, nowMS int64) error {
	body, e := json.Marshal(rows)
	if e != nil {
		return e
	}
	scope, e := json.Marshal(claim.Scope)
	if e != nil {
		return e
	}
	receipt := nextcloudDerivedGCReceipt{JobID: job.ID, ItemKind: kind, ObjectRef: id, ScopeSHA256: recoverySHA(scope), BeforeSHA256: recoverySHA(body), DeletedRows: count, ClaimToken: claim.Token, ClaimEpoch: claim.Epoch, CompletedAtMS: nowMS}
	if e = tx.Create(&receipt).Error; e != nil {
		return e
	}
	return tx.Model(&nextcloudGCItem{}).Where("job_id=? AND kind=? AND object_ref=?", job.ID, kind, id).Updates(map[string]any{"state": "collected", "updated_at": time.UnixMilli(nowMS)}).Error
}
func (s *NextcloudGCStore) deleteLocalEmbeddingInTx(tx *gorm.DB, claim NextcloudContentGCClaim, job nextcloudGCJob, id string, dimension, nowMS int64) error {
	number, e := strconv.ParseInt(id, 10, 64)
	if e != nil || number <= 0 {
		return ErrNextcloudContentLeaseDenied
	}
	table, kind := "embeddings", "postgres_embedding"
	if tx.Dialector.Name() == "sqlite" {
		table, kind = "lite_embeddings", "sqlite_embedding"
	}
	var receipt nextcloudDerivedGCReceipt
	if e = tx.Where("job_id=? AND item_kind=? AND object_ref=?", job.ID, kind, id).Take(&receipt).Error; e == nil {
		return s.verifyEmbeddingPhysicalGone(tx, table, number, dimension)
	} else if !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	rows, e := gcReadExactRows(tx, table, "id=?", number)
	if e != nil {
		return e
	}
	if len(rows) > 1 {
		return ErrNextcloudContentLeaseDenied
	}
	if len(rows) == 1 && (fmt.Sprint(rows[0]["knowledge_base_id"]) != job.KnowledgeBaseID || fmt.Sprint(rows[0]["knowledge_id"]) != job.KnowledgeID) {
		return ErrNextcloudContentLeaseDenied
	}
	deletedCount := int64(0)
	if table == "lite_embeddings" {
		dim := dimension
		if dim < 0 || dim > 65536 {
			return ErrNextcloudContentLeaseDenied
		}
		result := tx.Exec("DELETE FROM lite_embeddings_fts WHERE rowid=?", number)
		if result.Error != nil {
			return result.Error
		}
		deletedCount += result.RowsAffected
		if dim > 0 {
			result = tx.Exec(fmt.Sprintf("DELETE FROM vec_embeddings_%d WHERE rowid=?", dim), number)
			if result.Error != nil {
				return result.Error
			}
			deletedCount += result.RowsAffected
		}
	}
	deleted := tx.Table(table).Where("id=? AND knowledge_base_id=? AND knowledge_id=?", number, job.KnowledgeBaseID, job.KnowledgeID).Delete(map[string]any{})
	if deleted.Error != nil {
		return deleted.Error
	}
	if deleted.RowsAffected != int64(len(rows)) {
		return ErrNextcloudContentLeaseDenied
	}
	deletedCount += deleted.RowsAffected
	if e = s.verifyEmbeddingPhysicalGone(tx, table, number, dimension); e != nil {
		return e
	}
	return s.derivedReceipt(tx, claim, job, kind, id, rows, deletedCount, nowMS)
}
func (s *NextcloudGCStore) verifyEmbeddingPhysicalGone(tx *gorm.DB, table string, id, dimension int64) error {
	if e := s.verifyEmbeddingGone(tx, table, id); e != nil {
		return e
	}
	if table == "lite_embeddings" {
		var n int64
		if e := tx.Table("lite_embeddings_fts").Where("rowid=?", id).Count(&n).Error; e != nil {
			return e
		}
		if n != 0 {
			return ErrNextcloudContentLeaseDenied
		}
		if dimension > 0 {
			if dimension > 65536 {
				return ErrNextcloudContentLeaseDenied
			}
			if e := tx.Table(fmt.Sprintf("vec_embeddings_%d", dimension)).Where("rowid=?", id).Count(&n).Error; e != nil {
				return e
			}
			if n != 0 {
				return ErrNextcloudContentLeaseDenied
			}
		}
	}
	return nil
}
func (s *NextcloudGCStore) verifyEmbeddingGone(tx *gorm.DB, table string, id int64) error {
	var n int64
	if e := tx.Table(table).Where("id=?", id).Count(&n).Error; e != nil {
		return e
	}
	if n != 0 {
		return ErrNextcloudContentLeaseDenied
	}
	return nil
}
func (s *NextcloudGCStore) deleteLocalChunkInTx(tx *gorm.DB, claim NextcloudContentGCClaim, job nextcloudGCJob, id string, nowMS int64) error {
	var receipt nextcloudDerivedGCReceipt
	if e := tx.Where("job_id=? AND item_kind='derived_chunk' AND object_ref=?", job.ID, id).Take(&receipt).Error; e == nil {
		var n int64
		if e = tx.Table("chunks").Where("id=?", id).Count(&n).Error; e != nil {
			return e
		}
		if n != 0 {
			return ErrNextcloudContentLeaseDenied
		}
		if tx.Migrator().HasTable("chunk_revisions") {
			if e = tx.Table("chunk_revisions").Where("chunk_id=?", id).Count(&n).Error; e != nil {
				return e
			}
			if n != 0 {
				return ErrNextcloudContentLeaseDenied
			}
		}
		return nil
	} else if !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	rows, e := gcReadExactRows(tx, "chunks", "id=?", id)
	if e != nil {
		return e
	}
	if len(rows) > 1 || (len(rows) == 1 && (fmt.Sprint(rows[0]["tenant_id"]) != strconv.FormatUint(job.TenantID, 10) || fmt.Sprint(rows[0]["knowledge_base_id"]) != job.KnowledgeBaseID || fmt.Sprint(rows[0]["knowledge_id"]) != job.KnowledgeID)) {
		return ErrNextcloudContentLeaseDenied
	}
	var foreignParents int64
	if e := tx.Table("chunks").Where("(parent_chunk_id=? OR pre_chunk_id=? OR next_chunk_id=?) AND (tenant_id<>? OR knowledge_base_id<>? OR knowledge_id<>?)", id, id, id, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Count(&foreignParents).Error; e != nil {
		return e
	}
	if foreignParents != 0 {
		return ErrNextcloudContentLeaseDenied
	}
	if len(rows) == 1 {
		var images []types.ImageInfo
		if raw := fmt.Sprint(rows[0]["image_info"]); raw != "" && raw != "<nil>" && raw != "[]" {
			if json.Unmarshal([]byte(raw), &images) != nil {
				return ErrNextcloudContentLeaseDenied
			}
			for _, image := range images {
				if image.URL != "" {
					var n int64
					if e := tx.Model(&nextcloudGCItem{}).Where("job_id=? AND kind='extracted_image' AND object_ref=?", job.ID, image.URL).Count(&n).Error; e != nil {
						return e
					}
					if n != 1 {
						return ErrNextcloudContentLeaseDenied
					}
				}
			}
		}
	}
	deletedCount := int64(len(rows))
	if tx.Migrator().HasTable("chunk_revisions") {
		var revisions int64
		if e := tx.Table("chunk_revisions").Where("chunk_id=?", id).Count(&revisions).Error; e != nil {
			return e
		}
		deletedCount += revisions
		var foreign int64
		if e = tx.Table("chunk_revisions").Where("chunk_id=? AND (tenant_id<>? OR knowledge_base_id<>? OR knowledge_id<>?)", id, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Count(&foreign).Error; e != nil {
			return e
		}
		if foreign != 0 {
			return ErrNextcloudContentLeaseDenied
		}
		if e = tx.Table("chunk_revisions").Where("chunk_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", id, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Delete(map[string]any{}).Error; e != nil {
			return e
		}
	}
	if e = tx.Unscoped().Where("id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", id, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Delete(&types.Chunk{}).Error; e != nil {
		return e
	}
	return s.derivedReceipt(tx, claim, job, "derived_chunk", id, rows, deletedCount, nowMS)
}
func gcDerivedResourceIdentity(r types.StoredResource) string {
	body, _ := json.Marshal([]any{r.ID, r.Handle, r.TenantID, r.StorageBackendID, r.Provider, r.PhysicalPath, r.LocationHash, r.ContentHash, r.Size, r.SourceProvenance})
	return recoverySHA(body)
}
func (s *NextcloudGCStore) verifyLocalDerivedGone(tx *gorm.DB, job nextcloudGCJob, inventory gcDerivedInventory) error {
	var n int64
	if len(inventory.GraphOperations) > 0 {
		if e := tx.Table("nextcloud_graph_records").Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Count(&n).Error; e != nil {
			return e
		}
		if n != 0 {
			return ErrNextcloudContentLeaseDenied
		}
	}
	if e := tx.Table("chunks").Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Count(&n).Error; e != nil {
		return e
	}
	if n != 0 {
		return ErrNextcloudContentLeaseDenied
	}
	table := "embeddings"
	if tx.Dialector.Name() == "sqlite" {
		table = "lite_embeddings"
	}
	if len(inventory.Backend.Engines) > 0 {
		if e := tx.Table(table).Where("knowledge_base_id=? AND knowledge_id=?", job.KnowledgeBaseID, job.KnowledgeID).Count(&n).Error; e != nil {
			return e
		}
		if n != 0 {
			return ErrNextcloudContentLeaseDenied
		}
	}
	for _, id := range inventory.Embeddings {
		number, e := strconv.ParseInt(id, 10, 64)
		if e != nil {
			return e
		}
		if e = s.verifyEmbeddingPhysicalGone(tx, table, number, inventory.EmbeddingDimensions[id]); e != nil {
			return e
		}
	}
	return nil
}
func (s *NextcloudGCStore) verifyLocalResourceRefs(tx *gorm.DB, job nextcloudGCJob, inventory gcDerivedInventory) error {
	for _, ref := range inventory.Resources {
		handle, ok := types.ParseResourcePath(ref)
		if !ok {
			return ErrNextcloudContentLeaseDenied
		}
		var resource types.StoredResource
		if e := tx.Unscoped().Where("handle=? AND tenant_id=?", handle, job.TenantID).Take(&resource).Error; e != nil {
			return e
		}
		if inventory.ResourceIdentities[ref] == "" || gcDerivedResourceIdentity(resource) != inventory.ResourceIdentities[ref] {
			return ErrNextcloudContentLeaseDenied
		}
		var own int64
		if e := tx.Model(&types.ResourceBinding{}).Where("resource_id=? AND owner_type=? AND owner_id=?", resource.ID, types.ResourceOwnerKnowledge, job.KnowledgeID).Count(&own).Error; e != nil {
			return e
		}
		if own != 0 {
			return ErrNextcloudContentLeaseDenied
		}
		if resource.State != types.ResourceStateDeleted {
			var refs int64
			if e := tx.Model(&types.ResourceBinding{}).Where("resource_id=?", resource.ID).Count(&refs).Error; e != nil {
				return e
			}
			if resource.State != types.ResourceStateActive || refs == 0 {
				return ErrNextcloudContentLeaseDenied
			}
		}
	}
	return nil
}
func (s *NextcloudGCStore) finishLocalManifestInTx(tx *gorm.DB, job nextcloudGCJob, manifest nextcloudDerivedGCManifest, inventory gcDerivedInventory, nowMS int64) error {
	if e := s.verifyLocalDerivedGone(tx, job, inventory); e != nil {
		return e
	}
	var resourcesPending int64
	if e := tx.Model(&nextcloudGCItem{}).Where("job_id=? AND kind IN ('source_file','extracted_image') AND state<>'collected'", job.ID).Count(&resourcesPending).Error; e != nil {
		return e
	}
	if resourcesPending != 0 {
		return nil
	}
	if e := s.verifyLocalResourceRefs(tx, job, inventory); e != nil {
		return e
	}
	var receipts int64
	if e := tx.Model(&nextcloudDerivedGCReceipt{}).Where("job_id=?", job.ID).Count(&receipts).Error; e != nil {
		return e
	}
	if receipts != int64(len(inventory.Chunks)+len(inventory.Embeddings)+len(inventory.GraphOperations)) {
		return ErrNextcloudContentLeaseDenied
	}
	if e := tx.Model(&nextcloudGCItem{}).Where("job_id=? AND kind IN ('derived_index','knowledge')", job.ID).Update("state", "collected").Error; e != nil {
		return e
	}
	physicalCollected := len(inventory.Chunks) + len(inventory.Embeddings) + len(inventory.GraphOperations)
	protected := 0
	for _, ref := range inventory.Resources {
		handle, ok := types.ParseResourcePath(ref)
		if !ok {
			return ErrNextcloudContentLeaseDenied
		}
		var resource types.StoredResource
		if e := tx.Unscoped().Where("handle=? AND tenant_id=?", handle, job.TenantID).Take(&resource).Error; e != nil {
			return e
		}
		if !scopedLocalGCPath(&resource) {
			return ErrNextcloudContentLeaseDenied
		}
		switch resource.State {
		case types.ResourceStateDeleted:
			physicalCollected++
		case types.ResourceStateActive:
			var refs int64
			if e := tx.Model(&types.ResourceBinding{}).Where("resource_id=?", resource.ID).Count(&refs).Error; e != nil {
				return e
			}
			if refs == 0 {
				return ErrNextcloudContentLeaseDenied
			}
			protected++
		default:
			return ErrNextcloudContentLeaseDenied
		}
	}
	if physicalCollected+protected != manifest.PhysicalItems {
		return ErrNextcloudContentLeaseDenied
	}
	if e := tx.Model(&manifest).Updates(map[string]any{"state": "collected", "completed_at_ms": nowMS, "physical_collected": physicalCollected, "protected_items": protected}).Error; e != nil {
		return e
	}
	return tx.Model(&job).Updates(map[string]any{"state": "collected", "completed_at": time.UnixMilli(nowMS), "last_error_code": "", "updated_at": time.UnixMilli(nowMS)}).Error
}

// Graph material uses the existing immutable inventory and generation claim;
// this is an additional exact provider item, not an independent delete worker.
func (s *NextcloudGCStore) deleteScopedGraphInTx(tx *gorm.DB, claim NextcloudContentGCClaim, job nextcloudGCJob, id string, nowMS int64) error {
	if claim.Scope != gcContentScope(job) || job.NotBefore.UnixMilli() > nowMS {
		return ErrNextcloudContentLeaseDenied
	}
	if err := NewNextcloudContentLeaseStore(s.db).ValidateGCClaimInTx(tx.Statement.Context, tx, claim); err != nil {
		return err
	}
	var current nextcloudSourceVersion
	if err := nextcloudVersionQuery(tx, job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID).Take(&current).Error; err != nil {
		return err
	}
	if current.State != "tombstone" && current.CandidateKnowledgeID == job.KnowledgeID {
		return ErrNextcloudContentLeaseDenied
	}
	rows, e := gcReadExactRows(tx, "nextcloud_graph_records", "operation_id=?", id)
	if e != nil {
		return e
	}
	if len(rows) > 1 {
		return ErrNextcloudContentLeaseDenied
	}
	if len(rows) == 1 && (fmt.Sprint(rows[0]["tenant_id"]) != strconv.FormatUint(job.TenantID, 10) || fmt.Sprint(rows[0]["knowledge_base_id"]) != job.KnowledgeBaseID || fmt.Sprint(rows[0]["knowledge_id"]) != job.KnowledgeID || fmt.Sprint(rows[0]["backend"]) != NextcloudGraphBackend) {
		return ErrNextcloudContentLeaseDenied
	}
	var receipt nextcloudDerivedGCReceipt
	if e := tx.Where("job_id=? AND item_kind='scoped_sql_graph' AND object_ref=?", job.ID, id).Take(&receipt).Error; e == nil {
		if len(rows) != 0 {
			return ErrNextcloudContentLeaseDenied
		}
		return nil
	} else if !errors.Is(e, gorm.ErrRecordNotFound) {
		return e
	}
	deleted := tx.Table("nextcloud_graph_records").Where("operation_id=? AND tenant_id=? AND knowledge_base_id=? AND knowledge_id=?", id, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Delete(map[string]any{})
	if deleted.Error != nil {
		return deleted.Error
	}
	if deleted.RowsAffected != int64(len(rows)) {
		return ErrNextcloudContentLeaseDenied
	}
	return s.derivedReceipt(tx, claim, job, "scoped_sql_graph", id, rows, deleted.RowsAffected, nowMS)
}
