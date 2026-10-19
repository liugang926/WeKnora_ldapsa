package repository

import (
	"context"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

const (
	historyKnowledgeCandidateMaximumIDs   = 16384
	historyKnowledgeCandidateMaximumPlans = 500
)

func ownedHistoryKnowledgeControlQuery(tx *gorm.DB, tenant uint64, kbID, owner string, sessionIDs []string) *gorm.DB {
	q := tx.Table("knowledge_input_material_receipts r").
		Joins("JOIN knowledge_input_admissions a ON a.admission_id=r.admission_id AND a.knowledge_id=r.row_id").
		Joins("JOIN knowledge_input_history_parents h ON h.admission_id=a.admission_id AND h.tenant_id=a.tenant_id").
		Joins("JOIN knowledges k ON k.id=a.knowledge_id AND k.knowledge_base_id=a.knowledge_base_id AND k.tenant_id=a.tenant_id AND k.deleted_at IS NULL").
		Joins("JOIN knowledge_bases kb ON kb.id=k.knowledge_base_id AND kb.tenant_id=k.tenant_id AND kb.deleted_at IS NULL").
		Joins("JOIN messages m ON m.id=h.assistant_message_id AND m.session_id=h.session_id AND m.request_id=h.request_id AND m.knowledge_id=k.id AND m.deleted_at IS NULL AND m.role='assistant' AND m.is_completed=?", true).
		Joins("JOIN messages u ON u.id=h.user_message_id AND u.session_id=h.session_id AND u.request_id=h.request_id AND u.deleted_at IS NULL AND u.role='user' AND u.is_completed=?", true).
		Joins("JOIN sessions s ON s.id=h.session_id AND s.tenant_id=a.tenant_id AND s.deleted_at IS NULL").
		Joins("JOIN message_session_owner_versions so ON so.session_id=s.id AND so.revision>0").
		Joins("JOIN message_material_receipts mr ON mr.message_id=m.id").
		Joins("JOIN message_material_admissions ma ON ma.id=mr.admission_id AND ma.message_id=m.id AND ma.session_id=s.id AND ma.tenant_id=s.tenant_id AND ma.owner_id=s.user_id AND ma.owner_revision=so.revision AND ma.generation_id<>''").
		Joins("JOIN message_material_versions mv ON mv.message_id=m.id AND mv.revision=ma.revision AND mv.revision>0").
		Joins("JOIN message_generation_origins mo ON mo.id=ma.generation_id AND mo.message_id=m.id AND mo.session_id=s.id AND mo.tenant_id=s.tenant_id AND mo.owner_id=s.user_id AND mo.owner_revision=so.revision AND mo.request_id=h.request_id AND mo.role='assistant'").
		Joins("JOIN knowledge_input_message_versions mh ON mh.message_id=m.id AND mh.revision=h.assistant_revision AND mh.revision>0").
		Joins("JOIN message_material_receipts ur ON ur.message_id=u.id").
		Joins("JOIN message_material_admissions ua ON ua.id=ur.admission_id AND ua.message_id=u.id AND ua.session_id=s.id AND ua.tenant_id=s.tenant_id AND ua.owner_id=s.user_id AND ua.owner_revision=so.revision AND ua.generation_id<>''").
		Joins("JOIN message_material_versions uv ON uv.message_id=u.id AND uv.revision=ua.revision AND uv.revision=h.user_revision AND uv.revision>0").
		Joins("JOIN message_generation_origins uo ON uo.id=ua.generation_id AND uo.message_id=u.id AND uo.session_id=s.id AND uo.tenant_id=s.tenant_id AND uo.owner_id=s.user_id AND uo.owner_revision=so.revision AND uo.request_id=h.request_id AND uo.role='user'").
		Where("r.row_kind='knowledge' AND a.origin_kind='history' AND a.tenant_id=? AND a.knowledge_base_id=? AND s.user_id=?", tenant, kbID, owner)
	if len(sessionIDs) > 0 {
		q = q.Where("s.id IN ?", sessionIDs)
	}
	return q
}

func validateHistoryKnowledgeControlSelector(
	ctx context.Context,
	tenant uint64,
	kbID, owner string,
	sessionIDs []string,
) error {
	current, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 || current != tenant || kbID == "" || len(kbID) > 128 || owner == "" ||
		types.SessionOwnerIDFromContext(ctx) != owner ||
		len(sessionIDs) > 100 {
		return types.ErrSourceLineageUnknown
	}
	for _, id := range sessionIDs {
		if id == "" || len(id) > 128 {
			return types.ErrSourceLineageUnknown
		}
	}
	return nil
}

// OwnedHistoryKnowledgeIDs supplies the nonempty server-owned whitelist for
// candidate-only retrieval. Current pointers alone never select an arbitrary
// knowledge: the current receipt must link its immutable history admission to
// that exact committed assistant and the current owned session. Both original
// message admissions must still match the immutable owner and monotonic body
// controls before native TopK. Assistant history revisions include its legal
// child-pointer write; user revisions cover raw authored fields only, so
// unconsumed generated user fields do not invalidate an otherwise valid child.
func (s *KnowledgeInputProvenanceStore) OwnedHistoryKnowledgeIDs(
	ctx context.Context,
	tenant uint64,
	kbID, owner string,
	sessionIDs []string,
) ([]string, error) {
	if err := validateHistoryKnowledgeControlSelector(ctx, tenant, kbID, owner, sessionIDs); err != nil {
		return nil, err
	}
	var out []string
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var rows []struct{ KnowledgeID string }
		if err := ownedHistoryKnowledgeControlQuery(
			tx,
			tenant,
			kbID,
			owner,
			sessionIDs,
		).Select("a.knowledge_id").
			Order("a.knowledge_id").
			Limit(historyKnowledgeCandidateMaximumIDs + 1).
			Scan(&rows).
			Error; err != nil {
			return err
		}
		if len(rows) > historyKnowledgeCandidateMaximumIDs {
			return types.ErrVectorCandidateRetrievalUnavailable
		}
		for _, row := range rows {
			out = append(out, row.KnowledgeID)
		}
		return nil
	})
	return out, err
}

// PeekOwnedHistoryKnowledgeParentPlans reads producer metadata only. It never
// selects message/content, original checkpoint, title or child material JSON.
// Actual current owner/source/body-lease admission is performed subsequently
// by beginMessageMaterialRead; these plans cannot substitute for that guard.
func (s *KnowledgeInputProvenanceStore) PeekOwnedHistoryKnowledgeParentPlans(
	ctx context.Context,
	tenant uint64,
	kbID, owner string,
	knowledgeIDs, sessionIDs []string,
) ([]*types.HistoryKnowledgeParentPlan, error) {
	if err := validateHistoryKnowledgeControlSelector(ctx, tenant, kbID, owner, sessionIDs); err != nil {
		return nil, err
	}
	if len(knowledgeIDs) == 0 || len(knowledgeIDs) > historyKnowledgeCandidateMaximumPlans {
		return nil, types.ErrSourceLineageUnknown
	}
	seen := map[string]bool{}
	for _, id := range knowledgeIDs {
		if id == "" || len(id) > 128 || seen[id] {
			return nil, types.ErrSourceLineageUnknown
		}
		seen[id] = true
	}
	ids := append([]string{}, knowledgeIDs...)
	sort.Strings(ids)
	var out []*types.HistoryKnowledgeParentPlan
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var rows []knowledgeInputAdmission
		if err := ownedHistoryKnowledgeControlQuery(
			tx,
			tenant,
			kbID,
			owner,
			sessionIDs,
		).Where("a.knowledge_id IN ?", ids).
			Select("a.admission_id,a.tenant_id,a.knowledge_base_id,a.knowledge_id,a.input_digest,a.build_generation").
			Order("a.knowledge_id").
			Scan(&rows).
			Error; err != nil {
			return err
		}
		for _, row := range rows {
			proof := &types.KnowledgeBuildInputProof{
				AdmissionID:     row.AdmissionID,
				TenantID:        row.TenantID,
				KnowledgeBaseID: row.KnowledgeBaseID,
				KnowledgeID:     row.KnowledgeID,
				InputDigest:     row.InputDigest,
				BuildGeneration: row.BuildGeneration,
			}
			plan, err := s.peekHistoryMaterialPlanForProofInTx(tx, proof)
			if err != nil {
				return err
			}
			if plan == nil {
				return ErrKnowledgeInputChanged
			}
			out = append(
				out,
				&types.HistoryKnowledgeParentPlan{
					KnowledgeID:     row.KnowledgeID,
					AdmissionID:     row.AdmissionID,
					InputDigest:     row.InputDigest,
					BuildGeneration: row.BuildGeneration,
					MessagePlan:     plan,
				},
			)
		}
		return nil
	})
	return out, err
}

var _ interfaces.HistoryKnowledgeCandidateControlReader = (*KnowledgeInputProvenanceStore)(nil)
