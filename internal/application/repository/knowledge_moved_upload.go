package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// A reparse changes KB configuration, but the file must still be the exact
// privately admitted upload. Keep the first upload's admission through later
// moves so that another checkpoint cannot bless a changed file or its controls.
const movedUploadOrigin = "knowledge:moved-upload-origin"

type movedUploadReceipt struct {
	OriginalAdmission string `json:"original_admission"`
	Task              string `json:"task"`
}

// MovedUploadInput authorizes a retained, originally human-uploaded file at its
// move checkpoint. Only input bytes with the immutable upload digest are accepted.
func (s *KnowledgeInputProvenanceStore) MovedUploadInput(
	ctx context.Context, k *types.Knowledge, source, target *types.KnowledgeBase, input []byte, prepareOnly bool,
) (*types.KnowledgeBuildInputProof, error) {
	if k == nil || source == nil || target == nil {
		return nil, ErrKnowledgeInputChanged
	}
	if err := access.RequireKBTransfer(ctx, source, target, access.KBTransferMove); err != nil {
		return nil, err
	}
	var proof *types.KnowledgeBuildInputProof
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current types.Knowledge
		kb := target
		phase := "done"
		if prepareOnly {
			kb, phase = source, "moving"
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id=? AND tenant_id=? AND knowledge_base_id=?", k.ID, kb.TenantID, kb.ID).
			Take(&current).Error; err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		var state struct {
			Task      string `json:"task_id"`
			Source    string `json:"source_kb"`
			Target    string `json:"target_kb"`
			ID        string `json:"source_id"`
			Operation string `json:"operation"`
			Mode      string `json:"mode"`
			Phase     string `json:"phase"`
		}
		if current.Type != "file" || current.FilePath == "" || current.FilePath != k.FilePath ||
			json.Unmarshal(current.Metadata, &fields) != nil ||
			json.Unmarshal(fields[types.KnowledgeTransferMetadataKey], &state) != nil ||
			state.Task == "" || state.Task != access.TransferTaskID(ctx) || state.ID != current.ID ||
			state.Source != source.ID || state.Target != target.ID || state.Mode != "reparse" ||
			state.Operation != string(access.KBTransferMove) || state.Phase != phase {
			return ErrKnowledgeInputChanged
		}
		if err := CheckOriginalBodyAvailableInTx(ctx, tx, "knowledges", current.ID); err != nil {
			return err
		}
		var active knowledgeInputAdmission
		err := tx.Table("knowledge_input_admissions AS a").Select("a.*").
			Joins("JOIN knowledge_input_material_receipts r ON r.admission_id=a.admission_id "+
				"AND r.row_id=a.knowledge_id").
			Where("r.row_kind='knowledge' AND a.tenant_id=? AND a.knowledge_id=?", current.TenantID, current.ID).
			Take(&active).Error
		if err != nil {
			return err
		}
		var lineage types.SourceLineage
		digest, err := KnowledgeInputDigest(input)
		if err != nil || active.InputDigest != digest ||
			json.Unmarshal([]byte(active.SourceLineage), &lineage) != nil ||
			lineage.RequireComplete() != nil {
			return ErrKnowledgeInputChanged
		}
		if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
			return err
		}
		original, previousTask, err := movedUploadAnchor(tx, &current, &active)
		if err != nil {
			return err
		}
		if err := checkMovedUploadRoot(&current, fields, original); err != nil {
			return err
		}
		if prepareOnly {
			return nil
		}
		boundKB, err := loadKnowledgeBuildConfigurationInTx(tx, current.TenantID, current.KnowledgeBaseID)
		if err != nil {
			return err
		}
		// A previous enqueue may have succeeded even when its acknowledgement was lost.
		if previousTask == state.Task {
			latest, e := loadKnowledgeOriginalInput(tx, current.TenantID, current.KnowledgeBaseID, current.ID)
			if e != nil {
				return e
			}
			_, _, _, e = s.admissionInTx(tx, &current, latest)
			proof = latest
			return e
		}
		proof, err = s.admitInTx(ctx, tx, &current, boundKB, input, knowledgeInputOrigin{&lineage, "derived"})
		if err != nil {
			return err
		}
		anchor, err := json.Marshal(movedUploadReceipt{OriginalAdmission: original.AdmissionID, Task: state.Task})
		if err != nil {
			return err
		}
		return tx.Create(&types.TaskPendingOp{
			TenantID: current.TenantID, TaskType: movedUploadOrigin,
			Scope: "knowledge", ScopeID: current.ID, DedupKey: proof.AdmissionID, Op: "admitted",
			Payload: anchor, EnqueuedAt: time.Now().UTC(),
		}).Error
	})
	return proof, err
}

// Only an immutable human admission or the recorded result of an earlier
// verified move may authorize this file's bytes.
func movedUploadAnchor(
	tx *gorm.DB, current *types.Knowledge, active *knowledgeInputAdmission,
) (*knowledgeInputAdmission, string, error) {
	var previousMove movedUploadReceipt
	original := *active
	if active.OriginKind != "human" {
		var link types.TaskPendingOp
		if active.OriginKind != "derived" {
			return nil, "", types.ErrSourceLineageUnknown
		}
		if err := tx.Where("tenant_id=? AND task_type=? AND scope=? AND scope_id=? AND dedup_key=? AND op=?",
			current.TenantID, movedUploadOrigin, "knowledge", current.ID, active.AdmissionID, "admitted").
			Take(&link).Error; err != nil {
			return nil, "", err
		}
		if json.Unmarshal(link.Payload, &previousMove) != nil ||
			previousMove.OriginalAdmission == "" || previousMove.Task == "" {
			return nil, "", ErrKnowledgeInputChanged
		}
		original = knowledgeInputAdmission{}
		if err := tx.Where("admission_id=? AND tenant_id=? AND knowledge_id=?",
			previousMove.OriginalAdmission, current.TenantID, current.ID).
			Where("origin_kind='human' AND input_digest=?", active.InputDigest).Take(&original).Error; err != nil {
			return nil, "", err
		}
	}
	return &original, previousMove.Task, nil
}

// Compare all original input fields, allowing only the KB relocation and its
// server-owned checkpoint. Generated display fields are outside the root digest.
func checkMovedUploadRoot(
	current *types.Knowledge, fields map[string]json.RawMessage, original *knowledgeInputAdmission,
) error {
	delete(fields, types.KnowledgeTransferMetadataKey)
	initial := *current
	initial.KnowledgeBaseID = original.KnowledgeBaseID
	metadata, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	initial.Metadata = metadata
	root, err := knowledgeRootDigest(&initial)
	if err != nil {
		return err
	}
	if root != original.RootDigest && len(fields) == 0 {
		// An upload with SQL NULL metadata acquires a JSON object on its first checkpoint.
		initial.Metadata = nil
		root, err = knowledgeRootDigest(&initial)
	}
	if err != nil || root != original.RootDigest {
		return ErrKnowledgeInputChanged
	}
	return nil
}
