package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// A manifest is written exactly once, at Begin. Terminal writes cannot invent
// an origin for an unknown Begin. The receipt binds every exposed byte to this
// original execution and the SQL revision, including a body's A -> B -> A.
type spanStartManifest struct {
	RunToken      string `gorm:"primaryKey"`
	KnowledgeID   string
	Attempt       int
	SpanID        string
	ParentSpanID  string
	Name          string
	Kind          string
	OriginalInput string
	ActualInputs  string
	InputsDigest  string
}

func (spanStartManifest) TableName() string { return "knowledge_span_start_manifests" }

type spanBodyReceipt struct {
	KnowledgeID string `gorm:"primaryKey"`
	Attempt     int    `gorm:"primaryKey"`
	SpanID      string `gorm:"primaryKey"`
	RunToken    string
	RowRevision uint64
	BodyDigest  string
}

func (spanBodyReceipt) TableName() string { return "knowledge_span_body_receipts" }

type spanMaterialInput struct {
	Knowledge      *types.Knowledge
	KnowledgeBase  *types.KnowledgeBase
	Chunks         []*types.Chunk
	ChunkRevisions []uint64
	Original       *types.KnowledgeBuildInputProof
}

func spanBodyDigest(row *types.KnowledgeProcessingSpan) (string, error) {
	copy := *row
	copy.ContentProvenance = ""
	return KnowledgeInputDigest(struct {
		Row         types.KnowledgeProcessingSpan
		ErrorDetail string
	}{copy, row.ErrorDetail})
}
func spanRevision(tx *gorm.DB, id int64) (uint64, error) {
	var v struct{ Revision uint64 }
	err := tx.Table("knowledge_span_row_versions").Where("span_row_id = ?", id).Take(&v).Error
	if err == nil && v.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return v.Revision, err
}

func (r *knowledgeSpanRepository) attachSpanWriteTokens(ctx context.Context, rows []types.KnowledgeProcessingSpan) error {
	if len(rows) == 0 || !r.db.Migrator().HasTable(&spanBodyReceipt{}) {
		return nil
	}
	for i := range rows {
		var receipt spanBodyReceipt
		err := r.db.WithContext(ctx).Where("knowledge_id = ? AND attempt = ? AND span_id = ?", rows[i].KnowledgeID, rows[i].Attempt, rows[i].SpanID).Take(&receipt).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		revision, err := spanRevision(r.db.WithContext(ctx), rows[i].ID)
		if err != nil {
			return err
		}
		digest, err := spanBodyDigest(&rows[i])
		if err != nil {
			return err
		}
		if revision == receipt.RowRevision && digest == receipt.BodyDigest {
			rows[i].WriteToken = receipt.RunToken
		}
	}
	return nil
}

func captureSpanMaterials(tx *gorm.DB, store *KnowledgeInputProvenanceStore, original *types.KnowledgeBuildInputProof) (*spanMaterialInput, error) {
	if original == nil || original.Projection != "" || original.AdmissionID == "" || original.SourceLineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var k types.Knowledge
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", original.KnowledgeID, original.TenantID, original.KnowledgeBaseID).Take(&k).Error; err != nil {
		return nil, err
	}
	a, kb, _, err := store.admissionInTx(tx, &k, original)
	if err != nil {
		return nil, err
	}
	if a.AdmissionID != original.AdmissionID {
		return nil, ErrKnowledgeInputChanged
	}
	var chunks []*types.Chunk
	if err := tx.Where("knowledge_id = ? AND tenant_id = ? AND knowledge_base_id = ?", k.ID, k.TenantID, k.KnowledgeBaseID).Order("id ASC").Limit(4097).Find(&chunks).Error; err != nil {
		return nil, err
	}
	if len(chunks) > 4096 {
		return nil, types.ErrSourceLineageLimit
	}
	revisions := make([]uint64, len(chunks))
	for i, chunk := range chunks {
		if err := verifyKnowledgeInputMaterial(tx, a, &k, chunk); err != nil {
			return nil, err
		}
		revisions[i], err = inputRowRevision(tx, "chunk", chunk.ID)
		if err != nil {
			return nil, err
		}
	}
	return &spanMaterialInput{Knowledge: &k, KnowledgeBase: kb, Chunks: chunks, ChunkRevisions: revisions, Original: cloneKnowledgeInputProof(original)}, nil
}

// Recheck the immutable original input against its original admission and
// actual controls. Generated knowledge titles may legitimately change within
// that admission: their original bytes stay certified by the Begin snapshot.
// Referenced chunk bodies/metadata keep exact original revision checks.
func verifySpanMaterials(tx *gorm.DB, store *KnowledgeInputProvenanceStore, manifest *spanStartManifest) (*types.SourceLineage, error) {
	var saved spanMaterialInput
	if err := json.Unmarshal([]byte(manifest.ActualInputs), &saved); err != nil {
		return nil, err
	}
	digest, err := KnowledgeInputDigest(&saved)
	if err != nil {
		return nil, err
	}
	if digest != manifest.InputsDigest || saved.Knowledge == nil || saved.KnowledgeBase == nil || saved.Original == nil || saved.Original.Projection != "" || len(saved.Chunks) != len(saved.ChunkRevisions) {
		return nil, ErrKnowledgeInputChanged
	}
	var original types.KnowledgeBuildInputProof
	if err := json.Unmarshal([]byte(manifest.OriginalInput), &original); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(&original, saved.Original) || original.KnowledgeID != manifest.KnowledgeID || original.AdmissionID == "" || original.SourceLineage.RequireComplete() != nil {
		return nil, ErrKnowledgeInputChanged
	}
	var current types.Knowledge
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", original.KnowledgeID, original.TenantID, original.KnowledgeBaseID).Take(&current).Error; err != nil {
		return nil, err
	}
	a, _, lineage, err := store.admissionInTx(tx, &current, &original)
	if err != nil {
		return nil, err
	}
	if a.AdmissionID != original.AdmissionID || mustKnowledgeRootDigest(saved.Knowledge) != a.RootDigest {
		return nil, ErrKnowledgeInputChanged
	}
	config, err := knowledgeConfigDigest(saved.KnowledgeBase, saved.Knowledge)
	if err != nil || config != a.ConfigDigest {
		return nil, ErrKnowledgeInputChanged
	}
	for i, chunk := range saved.Chunks {
		if chunk == nil || chunk.ID == "" || chunk.KnowledgeID != current.ID || chunk.KnowledgeBaseID != current.KnowledgeBaseID || chunk.TenantID != current.TenantID {
			return nil, ErrKnowledgeInputChanged
		}
		var actual types.Chunk
		if err := tx.Where("id = ? AND knowledge_id = ? AND tenant_id = ? AND knowledge_base_id = ?", chunk.ID, current.ID, current.TenantID, current.KnowledgeBaseID).Take(&actual).Error; err != nil {
			return nil, err
		}
		if err := verifyKnowledgeInputMaterial(tx, a, &current, &actual); err != nil {
			return nil, err
		}
		revision, err := inputRowRevision(tx, "chunk", chunk.ID)
		if err != nil {
			return nil, err
		}
		before, err := knowledgeChunkDigest(chunk)
		if err != nil {
			return nil, err
		}
		after, err := knowledgeChunkDigest(&actual)
		if err != nil {
			return nil, err
		}
		if before != after || revision != saved.ChunkRevisions[i] {
			return nil, ErrKnowledgeInputChanged
		}
	}
	return types.UnionSourceLineage(lineage)
}

func (r *knowledgeSpanRepository) upsertWithOriginalSpan(ctx context.Context, row *types.KnowledgeProcessingSpan) error {
	if row == nil || row.KnowledgeID == "" || row.SpanID == "" {
		return errors.New("span identity required")
	}
	if ctx == nil {
		return types.ErrSourceLineageUnknown
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	original := KnowledgeBuildInputProofFromContext(ctx)
	available := r.db.Migrator().HasTable(&spanStartManifest{})
	if !available {
		if original != nil {
			return types.ErrSourceLineageUnknown
		}
		return r.upsertSpanRow(ctx, row)
	}
	if original != nil {
		if err := CheckKnowledgeBuildInputs(ctx); err != nil {
			return err
		}
	}
	guard, _ := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	store, _ := guard.reader.(*KnowledgeInputProvenanceStore)
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var existing types.KnowledgeProcessingSpan
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("knowledge_id = ? AND attempt = ? AND span_id = ?", row.KnowledgeID, row.Attempt, row.SpanID).Take(&existing).Error
		found := err == nil
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var receipt spanBodyReceipt
		if found {
			err = tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?", row.KnowledgeID, row.Attempt, row.SpanID).Take(&receipt).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		begin := row.Status == types.SpanStatusRunning && row.FinishedAt == nil && row.WriteToken == ""
		if !begin && row.WriteToken != "" && receipt.RunToken == "" {
			return ErrKnowledgeInputChanged
		}
		var manifest spanStartManifest
		if begin && original != nil && store != nil && original.Projection == "" {
			if original.KnowledgeID != row.KnowledgeID || row.Attempt <= 0 || row.StartedAt == nil {
				return ErrKnowledgeInputChanged
			}
			if found && (existing.ParentSpanID != row.ParentSpanID || existing.Name != row.Name || existing.Kind != row.Kind) {
				return ErrKnowledgeInputChanged
			}
			if row.Kind != types.SpanKindRoot {
				var parent types.KnowledgeProcessingSpan
				if row.ParentSpanID == "" || tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?", row.KnowledgeID, row.Attempt, row.ParentSpanID).Take(&parent).Error != nil {
					return types.ErrSourceLineageUnknown
				}
				if parent.Status == types.SpanStatusCancelled || parent.Status == types.SpanStatusFailed {
					return ErrKnowledgeInputChanged
				}
				var parentReceipt spanBodyReceipt
				err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?", parent.KnowledgeID, parent.Attempt, parent.SpanID).Take(&parentReceipt).Error
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if err == nil {
					var parentManifest spanStartManifest
					if err := tx.Where("run_token = ?", parentReceipt.RunToken).Take(&parentManifest).Error; err != nil {
						return err
					}
					var parentOriginal types.KnowledgeBuildInputProof
					if err := json.Unmarshal([]byte(parentManifest.OriginalInput), &parentOriginal); err != nil {
						return err
					}
					if parentOriginal.AdmissionID != original.AdmissionID {
						return ErrKnowledgeInputChanged
					}
				}
			}
			actual, err := captureSpanMaterials(tx, store, original)
			if err != nil {
				return err
			}
			if row.Kind == types.SpanKindRoot && !found {
				var roots int64
				if err := tx.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ? AND attempt = ? AND kind = ?", row.KnowledgeID, row.Attempt, types.SpanKindRoot).Count(&roots).Error; err != nil {
					return err
				}
				if roots != 0 {
					return ErrKnowledgeInputChanged
				}
			}
			data, err := json.Marshal(actual)
			if err != nil {
				return err
			}
			proof, err := json.Marshal(original)
			if err != nil {
				return err
			}
			digest, err := KnowledgeInputDigest(actual)
			if err != nil {
				return err
			}
			manifest = spanStartManifest{RunToken: uuid.NewString(), KnowledgeID: row.KnowledgeID, Attempt: row.Attempt, SpanID: row.SpanID, ParentSpanID: row.ParentSpanID, Name: row.Name, Kind: row.Kind, OriginalInput: string(proof), ActualInputs: string(data), InputsDigest: digest}
			if err := tx.Create(&manifest).Error; err != nil {
				return err
			}
			row.WriteToken = manifest.RunToken
		} else if receipt.RunToken != "" {
			if original == nil || store == nil || row.WriteToken == "" || row.WriteToken != receipt.RunToken || original.Projection != "" {
				return ErrKnowledgeInputChanged
			}
			if err := tx.Where("run_token = ?", receipt.RunToken).Take(&manifest).Error; err != nil {
				return err
			}
			if manifest.KnowledgeID != row.KnowledgeID || manifest.Attempt != row.Attempt || manifest.SpanID != row.SpanID || manifest.ParentSpanID != row.ParentSpanID || manifest.Name != row.Name || manifest.Kind != row.Kind {
				return ErrKnowledgeInputChanged
			}
			var admitted types.KnowledgeBuildInputProof
			if err := json.Unmarshal([]byte(manifest.OriginalInput), &admitted); err != nil {
				return err
			}
			if original.AdmissionID != admitted.AdmissionID || original.InputDigest != admitted.InputDigest || original.ConfigDigest != admitted.ConfigDigest || original.BuildGeneration != admitted.BuildGeneration || original.TenantID != admitted.TenantID || original.KnowledgeBaseID != admitted.KnowledgeBaseID || !reflect.DeepEqual(original.SourceLineage, admitted.SourceLineage) {
				return ErrKnowledgeInputChanged
			}
			digest, err := spanBodyDigest(&existing)
			if err != nil {
				return err
			}
			revision, err := spanRevision(tx, existing.ID)
			if err != nil {
				return err
			}
			if digest != receipt.BodyDigest || revision != receipt.RowRevision || existing.Status != types.SpanStatusRunning {
				return ErrKnowledgeInputChanged
			}
			// PostgreSQL timestamps retain microseconds. Compare the persisted
			// resolution; the repository seals the actual stored timestamp below.
			if row.StartedAt == nil || existing.StartedAt == nil || row.StartedAt.UnixMicro() != existing.StartedAt.UnixMicro() {
				return ErrKnowledgeInputChanged
			}
			for _, pair := range [][2]types.JSONMap{{row.Input, existing.Input}, {row.Metadata, existing.Metadata}} {
				if pair[0] == nil {
					continue
				}
				before, err := KnowledgeInputDigest(pair[0])
				if err != nil {
					return err
				}
				after, err := KnowledgeInputDigest(pair[1])
				if err != nil {
					return err
				}
				if before != after {
					return ErrKnowledgeInputChanged
				}
			}
			if _, err := verifySpanMaterials(tx, store, &manifest); err != nil {
				return err
			}
		} else {
			// No original Begin: preserve the operational status, but never mint
			// content provenance even if End happens to carry a current proof.
			row.WriteToken = ""
		}
		if begin && found {
			// A retry is a new Start. Nil input/output must clear old values;
			// Upsert's ordinary sparse terminal updates intentionally preserve
			// omitted content and must not accidentally seal the previous run.
			if err := tx.Model(&types.KnowledgeProcessingSpan{}).Where("id = ?", existing.ID).Updates(map[string]any{"input": nil, "output": nil, "metadata": nil, "error_code": "", "error_message": "", "error_detail": ""}).Error; err != nil {
				return err
			}
		}
		if err := (&knowledgeSpanRepository{db: tx}).upsertSpanRow(ctx, row); err != nil {
			return err
		}
		if manifest.RunToken == "" {
			return nil
		}
		var actual types.KnowledgeProcessingSpan
		if err := tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?", row.KnowledgeID, row.Attempt, row.SpanID).Take(&actual).Error; err != nil {
			return err
		}
		digest, err := spanBodyDigest(&actual)
		if err != nil {
			return err
		}
		revision, err := spanRevision(tx, actual.ID)
		if err != nil {
			return err
		}
		bound := spanBodyReceipt{KnowledgeID: row.KnowledgeID, Attempt: row.Attempt, SpanID: row.SpanID, RunToken: manifest.RunToken, RowRevision: revision, BodyDigest: digest}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "knowledge_id"}, {Name: "attempt"}, {Name: "span_id"}}, DoUpdates: clause.AssignmentColumns([]string{"run_token", "row_revision", "body_digest"})}).Create(&bound).Error
	})
}

func spanStatusOnly(row types.KnowledgeProcessingSpan) types.KnowledgeProcessingSpan {
	kind := row.Kind
	switch kind {
	case types.SpanKindRoot, types.SpanKindStage, types.SpanKindSubSpan, types.SpanKindGeneration:
	default:
		kind = types.SpanKindSubSpan
	}
	status := row.Status
	switch status {
	case types.SpanStatusPending, types.SpanStatusRunning, types.SpanStatusDone, types.SpanStatusFailed, types.SpanStatusSkipped, types.SpanStatusCancelled:
	default:
		status = "unknown"
	}
	name := "processing"
	if row.Kind == types.SpanKindRoot {
		name = "knowledge_processing"
	}
	if row.Kind == types.SpanKindStage {
		for _, stage := range types.AllStages {
			if row.Name == stage {
				name = stage
			}
		}
	}
	return types.KnowledgeProcessingSpan{KnowledgeID: row.KnowledgeID, Attempt: row.Attempt, SpanID: row.SpanID, ParentSpanID: row.ParentSpanID, Kind: kind, Name: name, Status: status, StartedAt: row.StartedAt, FinishedAt: row.FinishedAt, DurationMs: row.DurationMs, ContentProvenance: "unknown"}
}

func (s *KnowledgeInputProvenanceStore) CaptureSpanOutputInputs(ctx context.Context, request types.SpanOutputReadRequest) (*types.SpanOutputInputSnapshot, error) {
	if ctx == nil || s == nil || s.db == nil || request.TenantID == 0 || request.KnowledgeBaseID == "" || request.KnowledgeID == "" || request.Attempt < 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch s.db.Dialector.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return nil, ErrKnowledgeInputChanged
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return nil, ErrKnowledgeInputChanged
	}
	var result *types.SpanOutputInputSnapshot
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", request.KnowledgeID, request.TenantID, request.KnowledgeBaseID).Take(&current).Error; err != nil {
			return err
		}
		var latest int
		if err := tx.Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ?", request.KnowledgeID).Select("COALESCE(MAX(attempt),0)").Row().Scan(&latest); err != nil {
			return err
		}
		attempt := request.Attempt
		if attempt == 0 {
			attempt = latest
		}
		var rows []types.KnowledgeProcessingSpan
		if attempt > 0 {
			if err := tx.Where("knowledge_id = ? AND attempt = ?", current.ID, attempt).Order("id ASC").Limit(4097).Find(&rows).Error; err != nil {
				return err
			}
		}
		if len(rows) > 4096 {
			return types.ErrSourceLineageLimit
		}
		lineage, _ := types.NewCompleteSourceLineage()
		controlled, err := ragKnowledgeIsControlled(tx, &current)
		if err != nil {
			return err
		}
		if controlled {
			_, identity, err := (&knowledgeRepository{db: tx}).resolveNextcloudSourceLineageInTx(tx, current.TenantID, current.KnowledgeBaseID, current.ID)
			if err != nil {
				return err
			}
			lineage, err = types.NewCompleteSourceLineage(identity)
			if err != nil {
				return err
			}
		}
		byID := make(map[string]types.KnowledgeProcessingSpan, len(rows))
		roots := 0
		for _, row := range rows {
			byID[row.SpanID] = row
			if row.Kind == types.SpanKindRoot {
				roots++
			}
		}
		material := []string{fmt.Sprint(latest), fmt.Sprint(attempt)}
		projected := make([]types.KnowledgeProcessingSpan, 0, len(rows))
		for _, row := range rows {
			digest, err := spanBodyDigest(&row)
			if err != nil {
				return err
			}
			revision, err := spanRevision(tx, row.ID)
			if err != nil {
				return err
			}
			material = append(material, fmt.Sprintf("%d:%d:%s", row.ID, revision, digest))
			var receipt spanBodyReceipt
			err = tx.Where("knowledge_id = ? AND attempt = ? AND span_id = ?", row.KnowledgeID, row.Attempt, row.SpanID).Take(&receipt).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				projected = append(projected, spanStatusOnly(row))
				continue
			}
			if err != nil {
				return err
			}
			if receipt.BodyDigest != digest || receipt.RowRevision != revision {
				return ErrKnowledgeInputChanged
			}
			if roots != 1 {
				return ErrKnowledgeInputChanged
			}
			cursor := row
			seen := map[string]bool{}
			for {
				if seen[cursor.SpanID] || len(seen) > 128 {
					return ErrKnowledgeInputChanged
				}
				seen[cursor.SpanID] = true
				if cursor.Kind == types.SpanKindRoot {
					if cursor.ParentSpanID != "" {
						return ErrKnowledgeInputChanged
					}
					break
				}
				parent, ok := byID[cursor.ParentSpanID]
				if !ok {
					return ErrKnowledgeInputChanged
				}
				cursor = parent
			}
			var manifest spanStartManifest
			if err := tx.Where("run_token = ?", receipt.RunToken).Take(&manifest).Error; err != nil {
				return err
			}
			if manifest.KnowledgeID != row.KnowledgeID || manifest.Attempt != row.Attempt || manifest.SpanID != row.SpanID || manifest.Name != row.Name || manifest.Kind != row.Kind || manifest.ParentSpanID != row.ParentSpanID {
				return ErrKnowledgeInputChanged
			}
			originalLineage, err := verifySpanMaterials(tx, s, &manifest)
			if err != nil {
				return err
			}
			lineage, err = types.UnionSourceLineage(lineage, originalLineage)
			if err != nil {
				return err
			}
			row.ContentProvenance = "complete"
			row.WriteToken = ""
			projected = append(projected, row)
			material = append(material, manifest.RunToken, manifest.InputsDigest, receipt.BodyDigest)
		}
		digest, err := KnowledgeInputDigest(material)
		if err != nil {
			return err
		}
		parseStatus := current.ParseStatus
		switch parseStatus {
		case types.ParseStatusPending, types.ParseStatusProcessing, types.ParseStatusFinalizing, types.ParseStatusCompleted, types.ParseStatusFailed, types.ParseStatusDeleting, types.ParseStatusCancelled:
		default:
			parseStatus = "unknown"
		}
		result = &types.SpanOutputInputSnapshot{TenantID: request.TenantID, KnowledgeBaseID: request.KnowledgeBaseID, KnowledgeID: request.KnowledgeID, Attempt: attempt, LatestAttempt: latest, CurrentParseStatus: parseStatus, Rows: projected, SourceLineage: lineage, Receipt: digest}
		return nil
	}, options)
	return result, err
}

func (s *KnowledgeInputProvenanceStore) RecheckSpanOutputInputs(ctx context.Context, saved *types.SpanOutputInputSnapshot) error {
	if saved == nil || saved.Receipt == "" || saved.SourceLineage.RequireComplete() != nil {
		return types.ErrSourceLineageUnknown
	}
	current, err := s.CaptureSpanOutputInputs(ctx, types.SpanOutputReadRequest{TenantID: saved.TenantID, KnowledgeBaseID: saved.KnowledgeBaseID, KnowledgeID: saved.KnowledgeID, Attempt: saved.Attempt})
	if err != nil {
		return err
	}
	before, err := KnowledgeInputDigest(saved)
	if err != nil {
		return err
	}
	after, err := KnowledgeInputDigest(current)
	if err != nil {
		return err
	}
	if saved.Receipt != current.Receipt || before != after {
		return ErrKnowledgeInputChanged
	}
	return nil
}

var _ interfaces.SpanOutputInputReader = (*KnowledgeInputProvenanceStore)(nil)
