package repository

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type faqImportReadSetPlanKey struct{}

func WithFAQImportOutputReadSetPlan(ctx context.Context, plan *types.FAQImportOutputReadSetPlan) (context.Context, error) {
	if plan == nil || plan.MetadataDigest == "" || plan.SourceLineage.RequireComplete() != nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return ctx, err
	}
	var actual types.FAQImportOutputReadSetPlan
	if err = json.Unmarshal(encoded, &actual); err != nil {
		return ctx, err
	}
	ctx, err = WithMessageMaterialReadPlans(ctx, actual.MessagePlans)
	if err != nil {
		return ctx, err
	}
	for _, entry := range actual.Plans {
		ctx, err = WithMessageMaterialReadPlans(ctx, entry.MessagePlans)
		if err != nil {
			return ctx, err
		}
	}
	return context.WithValue(ctx, faqImportReadSetPlanKey{}, &actual), nil
}

func (s *KnowledgeInputProvenanceStore) PeekFAQImportOutputReadSetPlan(ctx context.Context, scopes []types.KnowledgeSearchScope, ids []string) (*types.FAQImportOutputReadSetPlan, error) {
	var plan *types.FAQImportOutputReadSetPlan
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		plan, err = s.peekFAQImportOutputReadSetPlanInTx(tx, scopes, ids)
		return err
	})
	return plan, err
}

// This is an existence/control probe, not a LastResult JSON/body read. Every
// actual current producer is pinned, including results later excluded by the
// ordinary collection selection. Unknown legacy result rows fail closed.
func (s *KnowledgeInputProvenanceStore) peekFAQImportOutputReadSetPlanInTx(tx *gorm.DB, scopes []types.KnowledgeSearchScope, ids []string) (*types.FAQImportOutputReadSetPlan, error) {
	if len(scopes) == 0 && len(ids) == 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	scopes = append([]types.KnowledgeSearchScope{}, scopes...)
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].TenantID != scopes[j].TenantID {
			return scopes[i].TenantID < scopes[j].TenantID
		}
		return scopes[i].KBID < scopes[j].KBID
	})
	ids = append([]string{}, ids...)
	sort.Strings(ids)
	q := tx.Table("knowledges k").Select("k.id,k.tenant_id,k.knowledge_base_id").Where("k.type=? AND k.deleted_at IS NULL AND k.last_faq_import_result IS NOT NULL AND CAST(k.last_faq_import_result AS TEXT) NOT IN ('','null')", types.KnowledgeTypeFAQ)
	if len(ids) > 0 {
		q = q.Where("k.id IN ?", ids)
	} else {
		conditions := ""
		args := []any{}
		for _, scope := range scopes {
			if scope.TenantID == 0 || scope.KBID == "" {
				return nil, types.ErrSourceLineageUnknown
			}
			if conditions != "" {
				conditions += " OR "
			}
			conditions += "(k.tenant_id=? AND k.knowledge_base_id=?)"
			args = append(args, scope.TenantID, scope.KBID)
		}
		q = q.Where(conditions, args...)
	}
	var rows []struct {
		ID, KnowledgeBaseID string
		TenantID, Revision  uint64
	}
	if err := q.Order("k.id").Scan(&rows).Error; err != nil {
		return nil, err
	}
	plan := &types.FAQImportOutputReadSetPlan{KnowledgeScopes: scopes, KnowledgeIDs: ids, Plans: []*types.FAQImportOutputReadPlan{}, Scopes: append([]types.KnowledgeSearchScope{}, scopes...)}
	lineages := []*types.SourceLineage{}
	for i := range rows {
		row := rows[i]
		revision, err := faqImportResultRevision(tx, row.ID)
		if err != nil {
			return nil, err
		}
		rows[i].Revision = revision
		current, err := s.peekFAQImportOutputReadPlanInTx(tx, types.FAQImportOutputReadRequest{TenantID: row.TenantID, KnowledgeBaseID: row.KnowledgeBaseID, KnowledgeID: row.ID, Kind: "result"})
		if err != nil {
			return nil, err
		}
		plan.Plans = append(plan.Plans, current)
		plan.Scopes = append(plan.Scopes, current.Scopes...)
		lineages = append(lineages, current.SourceLineage)
	}
	// Also probe the immutable saved-history parents of actual ordinary rows.
	hq := tx.Table("knowledge_input_material_receipts r").Select("a.admission_id,a.tenant_id,a.knowledge_base_id,a.knowledge_id,a.input_digest,a.build_generation").Joins("JOIN knowledge_input_admissions a ON a.admission_id=r.admission_id AND a.knowledge_id=r.row_id").Joins("JOIN knowledges k ON k.id=a.knowledge_id AND k.deleted_at IS NULL").Where("r.row_kind='knowledge' AND a.origin_kind='history'")
	if len(ids) > 0 {
		hq = hq.Where("k.id IN ?", ids)
	} else {
		conditions := ""
		args := []any{}
		for _, scope := range scopes {
			if conditions != "" {
				conditions += " OR "
			}
			conditions += "(k.tenant_id=? AND k.knowledge_base_id=?)"
			args = append(args, scope.TenantID, scope.KBID)
		}
		hq = hq.Where(conditions, args...)
	}
	var histories []knowledgeInputAdmission
	if err := hq.Order("a.knowledge_id").Scan(&histories).Error; err != nil {
		return nil, err
	}
	for _, entry := range histories {
		proof := &types.KnowledgeBuildInputProof{AdmissionID: entry.AdmissionID, TenantID: entry.TenantID, KnowledgeBaseID: entry.KnowledgeBaseID, KnowledgeID: entry.KnowledgeID, InputDigest: entry.InputDigest, BuildGeneration: entry.BuildGeneration}
		historyPlan, err := s.peekHistoryMaterialPlanForProofInTx(tx, proof)
		if err != nil {
			return nil, err
		}
		if historyPlan == nil {
			return nil, ErrKnowledgeInputChanged
		}
		plan.MessagePlans = append(plan.MessagePlans, historyPlan)
		plan.Scopes = append(plan.Scopes, types.KnowledgeSearchScope{TenantID: proof.TenantID, KBID: proof.KnowledgeBaseID})
		plan.Scopes = append(plan.Scopes, historyPlan.Scopes...)
		lineages = append(lineages, historyPlan.SourceLineage)
	}
	// This code probe consumed zero FAQ facts. It does not certify any
	// document body; those retain their independent original-source guards.
	if len(lineages) == 0 {
		empty, _ := types.NewCompleteSourceLineage()
		lineages = append(lineages, empty)
	}
	complete, err := types.UnionSourceLineage(lineages...)
	if err != nil {
		return nil, err
	}
	plan.SourceLineage = complete
	sort.Slice(plan.Scopes, func(i, j int) bool {
		if plan.Scopes[i].TenantID != plan.Scopes[j].TenantID {
			return plan.Scopes[i].TenantID < plan.Scopes[j].TenantID
		}
		return plan.Scopes[i].KBID < plan.Scopes[j].KBID
	})
	unique := []types.KnowledgeSearchScope{}
	for _, scope := range plan.Scopes {
		if len(unique) == 0 || unique[len(unique)-1] != scope {
			unique = append(unique, scope)
		}
	}
	plan.Scopes = unique
	plan.MetadataDigest, err = KnowledgeInputDigest(struct {
		Rows any
		Plan *types.FAQImportOutputReadSetPlan
	}{rows, plan})
	return plan, err
}

func (s *KnowledgeInputProvenanceStore) checkFAQImportReadSetPlanInTx(tx *gorm.DB) error {
	expected, ok := tx.Statement.Context.Value(faqImportReadSetPlanKey{}).(*types.FAQImportOutputReadSetPlan)
	if !ok {
		return nil
	}
	actual, err := s.peekFAQImportOutputReadSetPlanInTx(tx, expected.KnowledgeScopes, expected.KnowledgeIDs)
	if err != nil {
		return err
	}
	if !faqInputEqual(expected, actual) {
		return ErrKnowledgeInputChanged
	}
	if len(actual.Plans) == 0 && len(actual.MessagePlans) == 0 && len(actual.Scopes) == 0 {
		return nil
	}
	return validateFAQReadLeaseCoverageInTx(tx, actual.Scopes, actual.SourceLineage)
}

func (s *KnowledgeInputProvenanceStore) checkFAQImportOutputReadPlanInTx(tx *gorm.DB, req types.FAQImportOutputReadRequest) error {
	if err := s.checkFAQImportReadSetPlanInTx(tx); err != nil {
		return err
	}
	if set, ok := tx.Statement.Context.Value(faqImportReadSetPlanKey{}).(*types.FAQImportOutputReadSetPlan); ok {
		for _, plan := range set.Plans {
			if plan.Request.Kind == req.Kind && plan.Request.TenantID == req.TenantID && plan.Request.KnowledgeID == req.KnowledgeID {
				actual, err := s.peekFAQImportOutputReadPlanInTx(tx, req)
				if err != nil {
					return err
				}
				if !faqInputEqual(plan, actual) {
					return ErrKnowledgeInputChanged
				}
				return validateFAQReadLeaseCoverageInTx(tx, actual.Scopes, actual.SourceLineage)
			}
		}
		return ErrKnowledgeInputChanged
	}
	if expected, ok := tx.Statement.Context.Value(faqImportOutputReadPlanKey{}).(*types.FAQImportOutputReadPlan); ok {
		actual, err := s.peekFAQImportOutputReadPlanInTx(tx, req)
		if err != nil {
			return err
		}
		if !faqInputEqual(expected, actual) {
			return ErrKnowledgeInputChanged
		}
		return validateFAQReadLeaseCoverageInTx(tx, actual.Scopes, actual.SourceLineage)
	}
	return nil
}
