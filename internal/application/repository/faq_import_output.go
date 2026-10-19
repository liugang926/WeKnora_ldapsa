package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type faqImportOutputAdmission struct {
	ID string `gorm:"primaryKey"`

	OutputKey, SessionID                                                                        string
	TenantID                                                                                    uint64
	KnowledgeBaseID, KnowledgeID, TaskID, Kind, BodyDigest, Body, SourceLineage, OriginalInputs string
	MaterialRevision                                                                            uint64
}

// PeekFAQImportProgressScope reads faqimport progress scope.
// Only original receipt identities are inspected here. Callers must acquire
// the selected KB read lease and still perform complete input/source checks.
func (s *KnowledgeInputProvenanceStore) PeekFAQImportProgressScope(
	ctx context.Context,
	tenant uint64,
	task string,
) (types.KnowledgeSearchScope, error) {
	key, err := faqImportOutputKey(types.FAQImportOutputReadRequest{TenantID: tenant, TaskID: task, Kind: "progress"})
	if err != nil {
		return types.KnowledgeSearchScope{}, err
	}
	var row struct {
		TenantID        uint64
		KnowledgeBaseID string
	}
	err = s.db.WithContext(ctx).
		Table("faq_import_output_heads h").
		Select("a.tenant_id,a.knowledge_base_id").
		Joins("JOIN faq_import_output_admissions a ON a.id=h.admission_id AND a.output_key=h.output_key").
		Where("h.output_key=? AND a.tenant_id=? AND a.task_id=? AND a.kind='progress'", key, tenant, task).
		Take(&row).
		Error
	if err != nil || row.TenantID != tenant || row.KnowledgeBaseID == "" {
		if err == nil {
			err = ErrKnowledgeInputChanged
		}
		return types.KnowledgeSearchScope{}, err
	}
	return types.KnowledgeSearchScope{TenantID: row.TenantID, KBID: row.KnowledgeBaseID}, nil
}

// PeekFAQImportOutputReadPlan reads faqimport output read plan.
func (s *KnowledgeInputProvenanceStore) PeekFAQImportOutputReadPlan(
	ctx context.Context,
	request types.FAQImportOutputReadRequest,
) (*types.FAQImportOutputReadPlan, error) {
	var plan *types.FAQImportOutputReadPlan
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		plan, err = s.peekFAQImportOutputReadPlanInTx(tx, request)
		return err
	})
	return plan, err
}

func (s *KnowledgeInputProvenanceStore) peekFAQImportOutputReadPlanInTx(
	tx *gorm.DB,
	request types.FAQImportOutputReadRequest,
) (*types.FAQImportOutputReadPlan, error) {
	key, err := faqImportOutputKey(request)
	if err != nil {
		return nil, err
	}
	var row struct {
		ID, SessionID, KnowledgeBaseID, KnowledgeID, TaskID, Kind, SourceLineage, OriginalInputs string
		TenantID, Revision                                                                       uint64
	}
	err = tx.Table("faq_import_output_heads h").
		Select(("a.id,a.session_id,a.tenant_id,a.knowledge_base_id,a.knowledge_id,a.task_id" +
			",a.kind,a.source_lineage,a.original_inputs,h.revision")).
		Joins("JOIN faq_import_output_admissions a ON a.id=h.admission_id AND a.output_key=h.output_key").
		Where("h.output_key=? AND a.tenant_id=? AND a.kind=?", key, request.TenantID, request.Kind).
		Take(&row).
		Error
	if err != nil {
		return nil, err
	}
	if row.Revision == 0 || request.KnowledgeBaseID != "" && request.KnowledgeBaseID != row.KnowledgeBaseID {
		return nil, ErrKnowledgeInputChanged
	}
	var lineage types.SourceLineage
	if err := json.Unmarshal([]byte(row.SourceLineage), &lineage); err != nil {
		return nil, err
	}
	if err := lineage.RequireComplete(); err != nil {
		return nil, err
	}
	var originals []*types.KnowledgeBuildInputProof
	if err := json.Unmarshal([]byte(row.OriginalInputs), &originals); err != nil {
		return nil, err
	}
	scopes := map[string]types.KnowledgeSearchScope{}
	add := func(tenant uint64, kb string) error {
		if tenant == 0 || kb == "" {
			return types.ErrSourceLineageUnknown
		}
		scopes[fmt.Sprintf("%d:%s", tenant, kb)] = types.KnowledgeSearchScope{TenantID: tenant, KBID: kb}
		return nil
	}
	if err := add(row.TenantID, row.KnowledgeBaseID); err != nil {
		return nil, err
	}
	for _, source := range lineage.Sources {
		if err := add(source.TenantID, source.KnowledgeBaseID); err != nil {
			return nil, err
		}
	}
	lineages := []*types.SourceLineage{&lineage}
	messagePlans := []*types.MessageMaterialReadPlan{}
	for _, original := range originals {
		historyPlan, err := s.peekHistoryMaterialPlanForProofInTx(tx, original)
		if err != nil {
			return nil, err
		}
		if historyPlan != nil {
			messagePlans = append(messagePlans, historyPlan)
			for _, scope := range historyPlan.Scopes {
				if err := add(scope.TenantID, scope.KBID); err != nil {
					return nil, err
				}
			}
			lineages = append(lineages, historyPlan.SourceLineage)
		}
		if original == nil || original.BodyDigest == "" || original.SourceLineage.RequireComplete() != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		if err := add(original.TenantID, original.KnowledgeBaseID); err != nil {
			return nil, err
		}
		lineages = append(lineages, original.SourceLineage)
		for _, source := range original.SourceLineage.Sources {
			if err := add(source.TenantID, source.KnowledgeBaseID); err != nil {
				return nil, err
			}
		}
	}
	digest, err := KnowledgeInputDigest(row)
	if err != nil {
		return nil, err
	}
	actualRequest := request
	actualRequest.KnowledgeBaseID = row.KnowledgeBaseID
	actualRequest.KnowledgeID = row.KnowledgeID
	actualRequest.TaskID = row.TaskID
	complete, err := types.UnionSourceLineage(lineages...)
	if err != nil {
		return nil, err
	}
	plan := &types.FAQImportOutputReadPlan{
		Request:        actualRequest,
		AdmissionID:    row.ID,
		HeadRevision:   row.Revision,
		MetadataDigest: digest,
		SourceLineage:  complete,
		MessagePlans:   messagePlans,
	}
	keys := make([]string, 0, len(scopes))
	for key := range scopes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		plan.Scopes = append(plan.Scopes, scopes[key])
	}
	return plan, nil
}
func (faqImportOutputAdmission) TableName() string { return "faq_import_output_admissions" }

type faqImportOutputReadPlanKey struct{}

// WithFAQImportOutputReadPlan binds faqimport output read plan.
func WithFAQImportOutputReadPlan(ctx context.Context, plan *types.FAQImportOutputReadPlan) (context.Context, error) {
	if plan == nil || plan.AdmissionID == "" || plan.MetadataDigest == "" || len(plan.Scopes) == 0 {
		return ctx, types.ErrSourceLineageUnknown
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return ctx, err
	}
	var copied types.FAQImportOutputReadPlan
	if err := json.Unmarshal(encoded, &copied); err != nil {
		return ctx, err
	}
	ctx, err = WithMessageMaterialReadPlans(ctx, copied.MessagePlans)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, faqImportOutputReadPlanKey{}, &copied), nil
}

type faqImportOutputHead struct {
	OutputKey   string `gorm:"primaryKey"`
	AdmissionID string
	Revision    uint64
}

func (faqImportOutputHead) TableName() string { return "faq_import_output_heads" }
func faqImportOutputKey(req types.FAQImportOutputReadRequest) (string, error) {
	if req.TenantID == 0 {
		return "", types.ErrSourceLineageUnknown
	}
	switch req.Kind {
	case "progress":
		if req.TaskID != "" {
			return fmt.Sprintf("progress:%d:%s", req.TenantID, req.TaskID), nil
		}
	case "result":
		if req.KnowledgeID != "" {
			return fmt.Sprintf("result:%d:%s", req.TenantID, req.KnowledgeID), nil
		}
	}
	return "", types.ErrSourceLineageUnknown
}

func faqImportResultRevision(tx *gorm.DB, knowledgeID string) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table("faq_import_result_versions").Where("knowledge_id=?", knowledgeID).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

func sealFAQImportOutputInTx(
	tx *gorm.DB,
	session *faqInputSessionRow,
	kind, knowledgeID, taskID string,
	value any,
	materialRevision uint64,
) error {
	key, err := faqImportOutputKey(
		types.FAQImportOutputReadRequest{
			TenantID:    session.TenantID,
			KnowledgeID: knowledgeID,
			TaskID:      taskID,
			Kind:        kind,
		},
	)
	if err != nil {
		return err
	}
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	digest, err := KnowledgeInputDigest(value)
	if err != nil {
		return err
	}
	admission := faqImportOutputAdmission{
		ID:               uuid.NewString(),
		OutputKey:        key,
		SessionID:        session.ID,
		TenantID:         session.TenantID,
		KnowledgeBaseID:  session.KnowledgeBaseID,
		KnowledgeID:      knowledgeID,
		TaskID:           taskID,
		Kind:             kind,
		BodyDigest:       digest,
		Body:             string(body),
		SourceLineage:    session.SourceLineage,
		OriginalInputs:   session.OriginalInputs,
		MaterialRevision: materialRevision,
	}
	if err := tx.Create(&admission).Error; err != nil {
		return err
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{
		Name: "output_key",
	}}, DoUpdates: clause.Assignments(map[string]any{
		"admission_id": admission.ID,
		"revision":     gorm.Expr("faq_import_output_heads.revision+1"),
	})}).
		Create(&faqImportOutputHead{OutputKey: key, AdmissionID: admission.ID, Revision: 1}).
		Error
}

// SealFAQImportProgress seals faqimport progress.
// Only the actual worker/request session may mint a progress output. Redis,
// current metadata and client-provided receipts are never original evidence.
func (s *KnowledgeInputProvenanceStore) SealFAQImportProgress(
	ctx context.Context,
	progress *types.FAQImportProgress,
) error {
	if progress == nil || progress.TaskID == "" {
		return types.ErrSourceLineageUnknown
	}
	if err := s.CheckFAQInput(ctx); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		session, snapshot, err := s.faqSessionInTx(ctx, tx)
		if err != nil {
			return err
		}
		if session.Operation != "import" || progress.KBID != session.KnowledgeBaseID || snapshot.Knowledge == nil ||
			progress.KnowledgeID != snapshot.Knowledge.ID {
			return ErrKnowledgeInputChanged
		}
		var operation types.FAQInputOperation
		if err := json.Unmarshal([]byte(session.InputBody), &operation); err != nil {
			return err
		}
		if operation.Batch == nil || progress.Total != len(operation.Batch.Entries) ||
			operation.Batch.DryRun != progress.DryRun ||
			operation.Batch.TaskID != "" && operation.Batch.TaskID != progress.TaskID {
			return ErrKnowledgeInputChanged
		}
		return sealFAQImportOutputInTx(tx, session, "progress", progress.KnowledgeID, progress.TaskID, progress, 0)
	}, knowledgeDisplayWriteOptions(s.db))
}

// PublishFAQImportResult publishes faqimport result.
// Result and its output receipt commit atomically. Counts are supplied only by
// the trusted worker over the already admitted complete candidate manifest.
func (s *KnowledgeInputProvenanceStore) PublishFAQImportResult(
	ctx context.Context,
	knowledgeID string,
	result *types.FAQImportResult,
	progress *types.FAQImportProgress,
) error {
	if result == nil || progress == nil {
		return types.ErrSourceLineageUnknown
	}
	if err := s.CheckFAQInput(ctx); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		session, snapshot, err := s.faqSessionInTx(ctx, tx)
		if err != nil {
			return err
		}
		if session.Operation != "import" || snapshot.Knowledge == nil || snapshot.Knowledge.ID != knowledgeID ||
			progress.KnowledgeID != knowledgeID ||
			result.TaskID != progress.TaskID {
			return ErrKnowledgeInputChanged
		}
		var operation types.FAQInputOperation
		if err := json.Unmarshal([]byte(session.InputBody), &operation); err != nil {
			return err
		}
		if operation.Batch == nil || result.TotalEntries != len(operation.Batch.Entries) ||
			result.ImportMode != operation.Batch.Mode ||
			result.SuccessCount != progress.SuccessCount ||
			result.FailedCount != progress.FailedCount ||
			result.PartialFailedCount != progress.PartialFailedCount ||
			result.MergedCount != progress.MergedCount ||
			result.AddedCount != progress.AddedCount ||
			result.SkippedCount != progress.SkippedCount ||
			result.FailedEntriesURL != progress.FailedEntriesURL ||
			result.DisplayStatus != "open" {
			return ErrKnowledgeInputChanged
		}
		var knowledge types.Knowledge
		if err := tx.Where(
			"id=? AND tenant_id=? AND knowledge_base_id=? AND type=?",
			knowledgeID,
			session.TenantID,
			session.KnowledgeBaseID,
			types.KnowledgeTypeFAQ,
		).
			Take(&knowledge).
			Error; err != nil {
			return err
		}
		if err := knowledge.SetLastFAQImportResult(result); err != nil {
			return err
		}
		if err := tx.Model(&knowledge).
			Update("last_faq_import_result", knowledge.LastFAQImportResult).
			Error; err != nil {
			return err
		}
		revision, err := faqImportResultRevision(tx, knowledgeID)
		if err != nil {
			return err
		}
		if err := sealFAQImportOutputInTx(
			tx,
			session,
			"result",
			knowledgeID,
			result.TaskID,
			result,
			revision,
		); err != nil {
			return err
		}
		return s.advanceFAQSessionInTx(tx, session)
	}, knowledgeDisplayWriteOptions(s.db))
}

func (s *KnowledgeInputProvenanceStore) captureFAQImportOutputInTx(
	tx *gorm.DB,
	req types.FAQImportOutputReadRequest,
) (*types.FAQImportOutputSnapshot, error) {
	if err := s.checkFAQImportOutputReadPlanInTx(tx, req); err != nil {
		return nil, err
	}

	key, err := faqImportOutputKey(req)
	if err != nil {
		return nil, err
	}
	var head faqImportOutputHead
	if err := tx.Where("output_key=?", key).Take(&head).Error; err != nil {
		return nil, err
	}
	var admission faqImportOutputAdmission
	if err := tx.Where(
		"id=? AND output_key=? AND tenant_id=? AND kind=?",
		head.AdmissionID,
		key,
		req.TenantID,
		req.Kind,
	).
		Take(&admission).
		Error; err != nil {
		return nil, err
	}
	if head.Revision == 0 || req.KnowledgeBaseID != "" && req.KnowledgeBaseID != admission.KnowledgeBaseID ||
		req.KnowledgeID != "" && req.KnowledgeID != admission.KnowledgeID {
		return nil, ErrKnowledgeInputChanged
	}
	var producer faqInputSessionRow
	if err := tx.Where(
		"id=? AND tenant_id=? AND knowledge_base_id=?",
		admission.SessionID,
		admission.TenantID,
		admission.KnowledgeBaseID,
	).
		Take(&producer).
		Error; err != nil {
		return nil, err
	}
	if producer.Operation != "import" || producer.SourceLineage != admission.SourceLineage ||
		producer.OriginalInputs != admission.OriginalInputs {
		return nil, ErrKnowledgeInputChanged
	}
	parents, err := s.readFAQOriginalKnowledgeInputsInTx(tx, &producer)
	if err != nil {
		return nil, err
	}
	var lineage types.SourceLineage
	if err := json.Unmarshal([]byte(admission.SourceLineage), &lineage); err != nil {
		return nil, err
	}
	if err := lineage.RequireComplete(); err != nil {
		return nil, err
	}
	if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
		return nil, err
	}
	actualReq := req
	actualReq.KnowledgeBaseID = admission.KnowledgeBaseID
	actualReq.KnowledgeID = admission.KnowledgeID
	actualReq.TaskID = admission.TaskID
	snapshot := &types.FAQImportOutputSnapshot{
		Request:                 actualReq,
		AdmissionID:             admission.ID,
		SessionID:               admission.SessionID,
		HeadRevision:            head.Revision,
		MaterialRevision:        admission.MaterialRevision,
		BodyDigest:              admission.BodyDigest,
		SourceLineage:           &lineage,
		OriginalKnowledgeInputs: parents,
	}
	var value any
	if req.Kind == "progress" {
		snapshot.Progress = &types.FAQImportProgress{}
		if err := json.Unmarshal([]byte(admission.Body), snapshot.Progress); err != nil {
			return nil, err
		}
		if snapshot.Progress.TaskID != admission.TaskID || snapshot.Progress.KBID != admission.KnowledgeBaseID ||
			snapshot.Progress.KnowledgeID != admission.KnowledgeID {
			return nil, ErrKnowledgeInputChanged
		}
		value = snapshot.Progress
	} else {
		snapshot.Result = &types.FAQImportResult{}
		if err := json.Unmarshal([]byte(admission.Body), snapshot.Result); err != nil {
			return nil, err
		}
		var knowledge types.Knowledge
		if err := tx.Where(
			"id=? AND tenant_id=? AND knowledge_base_id=? AND type=?",
			admission.KnowledgeID,
			admission.TenantID,
			admission.KnowledgeBaseID,
			types.KnowledgeTypeFAQ,
		).
			Take(&knowledge).
			Error; err != nil {
			return nil, err
		}
		revision, err := faqImportResultRevision(tx, knowledge.ID)
		if err != nil {
			return nil, err
		}
		if revision != admission.MaterialRevision {
			return nil, ErrKnowledgeInputChanged
		}
		current, err := knowledge.GetLastFAQImportResult()
		if err != nil {
			return nil, err
		}
		if !faqInputEqual(current, snapshot.Result) || snapshot.Result.TaskID != admission.TaskID {
			return nil, ErrKnowledgeInputChanged
		}
		value = snapshot.Result
	}
	digest, err := KnowledgeInputDigest(value)
	if err != nil {
		return nil, err
	}
	if digest != admission.BodyDigest {
		return nil, ErrKnowledgeInputChanged
	}
	return snapshot, nil
}

// CaptureFAQImportOutputInputs captures faqimport output inputs.
func (s *KnowledgeInputProvenanceStore) CaptureFAQImportOutputInputs(
	ctx context.Context,
	req types.FAQImportOutputReadRequest,
) (*types.FAQImportOutputSnapshot, error) {
	var snapshot *types.FAQImportOutputSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		snapshot, err = s.captureFAQImportOutputInTx(tx, req)
		return err
	})
	return snapshot, err
}

// RecheckFAQImportOutputInputs rechecks faqimport output inputs.
func (s *KnowledgeInputProvenanceStore) RecheckFAQImportOutputInputs(
	ctx context.Context,
	snapshot *types.FAQImportOutputSnapshot,
) error {
	if snapshot == nil {
		return types.ErrSourceLineageUnknown
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		actual, err := s.captureFAQImportOutputInTx(tx, snapshot.Request)
		if err != nil {
			return err
		}
		if !faqInputEqual(actual, snapshot) {
			return ErrKnowledgeInputChanged
		}
		return nil
	})
}

// UpdateFAQImportResultDisplayWithInput updates faqimport result display with input.
// A display-only edit preserves every retained counter/body's original
// producer. An unknown legacy result cannot be signed by this new request.
func (s *KnowledgeInputProvenanceStore) UpdateFAQImportResultDisplayWithInput(
	ctx context.Context,
	original *types.FAQImportOutputSnapshot,
	status string,
) error {
	if original == nil || original.Result == nil || status != "open" && status != "close" {
		return types.ErrSourceLineageUnknown
	}
	boundary, ok := ctx.Value(faqInputBoundaryKey{}).(faqInputBoundary)
	digest, err := KnowledgeInputDigest(types.FAQInputOperation{Kind: "display-status", DisplayStatus: status})
	if err != nil {
		return err
	}
	if !ok || boundary.Origin.kind != "human" || boundary.KB != original.Request.KnowledgeBaseID ||
		boundary.Digest != digest {
		return types.ErrSourceLineageUnknown
	}
	ctx, err = s.AuthorizeCurrentKnowledgeInputWrite(
		ctx,
		&types.KnowledgeBase{ID: original.Request.KnowledgeBaseID, TenantID: original.Request.TenantID},
	)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		actual, err := s.captureFAQImportOutputInTx(tx, original.Request)
		if err != nil {
			return err
		}
		if !faqInputEqual(actual, original) {
			return ErrKnowledgeInputChanged
		}
		var producer faqInputSessionRow
		if err := tx.Where("id=?", original.SessionID).Take(&producer).Error; err != nil {
			return err
		}
		result := *actual.Result
		result.DisplayStatus = status
		var knowledge types.Knowledge
		if err := tx.Where(
			"id=? AND tenant_id=? AND knowledge_base_id=?",
			actual.Request.KnowledgeID,
			actual.Request.TenantID,
			actual.Request.KnowledgeBaseID,
		).
			Take(&knowledge).
			Error; err != nil {
			return err
		}
		if err := knowledge.SetLastFAQImportResult(&result); err != nil {
			return err
		}
		if err := tx.Model(&knowledge).
			Update("last_faq_import_result", knowledge.LastFAQImportResult).
			Error; err != nil {
			return err
		}
		revision, err := faqImportResultRevision(tx, knowledge.ID)
		if err != nil {
			return err
		}
		if err := sealFAQImportOutputInTx(
			tx,
			&producer,
			"result",
			knowledge.ID,
			result.TaskID,
			&result,
			revision,
		); err != nil {
			return err
		}
		progress, err := s.captureFAQImportOutputInTx(
			tx,
			types.FAQImportOutputReadRequest{TenantID: producer.TenantID, TaskID: result.TaskID, Kind: "progress"},
		)
		if err != nil {
			return err
		}
		if progress.SessionID != producer.ID || progress.Progress == nil {
			return ErrKnowledgeInputChanged
		}
		updated := *progress.Progress
		updated.DisplayStatus = status
		return sealFAQImportOutputInTx(tx, &producer, "progress", knowledge.ID, result.TaskID, &updated, 0)
	}, knowledgeDisplayWriteOptions(s.db))
}

var _ interfaces.FAQImportOutputInputReader = (*KnowledgeInputProvenanceStore)(nil)
