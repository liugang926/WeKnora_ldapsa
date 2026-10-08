package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type faqInputReadPlanKey struct{}

func WithFAQInputReadPlan(ctx context.Context, plan *types.FAQInputReadPlan) (context.Context, error) {
	if plan == nil || plan.MetadataDigest == "" || len(plan.Scopes) == 0 {
		return ctx, types.ErrSourceLineageUnknown
	}
	body, err := json.Marshal(plan)
	if err != nil {
		return ctx, err
	}
	var actual types.FAQInputReadPlan
	if err = json.Unmarshal(body, &actual); err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, faqInputReadPlanKey{}, &actual), nil
}

func (s *KnowledgeInputProvenanceStore) PeekFAQInputReadPlan(ctx context.Context, request types.FAQInputReadRequest) (*types.FAQInputReadPlan, error) {
	var plan *types.FAQInputReadPlan
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		plan, err = s.peekFAQInputReadPlanInTx(tx, request)
		return err
	})
	return plan, err
}

func (s *KnowledgeInputProvenanceStore) checkFAQInputReadPlanInTx(tx *gorm.DB, request types.FAQInputReadRequest) error {
	expected, ok := tx.Statement.Context.Value(faqInputReadPlanKey{}).(*types.FAQInputReadPlan)
	if !ok {
		return nil
	}
	if expected == nil || expected.Request.TenantID != request.TenantID || expected.Request.KnowledgeBaseID != request.KnowledgeBaseID {
		return ErrKnowledgeInputChanged
	}
	if !expected.Request.AllEntries && !faqInputEqual(expected.Request, request) {
		return ErrKnowledgeInputChanged
	}
	actual, err := s.peekFAQInputReadPlanInTx(tx, expected.Request)
	if err != nil {
		return err
	}
	if !faqInputEqual(actual, expected) {
		return ErrKnowledgeInputChanged
	}
	return nil
}

// This probe selects row IDs, revisions and immutable source/parent envelopes.
// Entry text, FAQ metadata, tag names, prompt/config body and parent chunks are
// read only after the caller acquires all scopes and binds the returned plan.
func (s *KnowledgeInputProvenanceStore) peekFAQInputReadPlanInTx(tx *gorm.DB, request types.FAQInputReadRequest) (*types.FAQInputReadPlan, error) {
	if request.TenantID == 0 || request.KnowledgeBaseID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	scopes := map[string]types.KnowledgeSearchScope{}
	add := func(tenant uint64, kb string) error {
		if tenant == 0 || kb == "" {
			return types.ErrSourceLineageUnknown
		}
		scopes[fmt.Sprintf("%d:%s", tenant, kb)] = types.KnowledgeSearchScope{TenantID: tenant, KBID: kb}
		return nil
	}
	lineageScopes := func(raw string) error {
		var lineage types.SourceLineage
		if err := json.Unmarshal([]byte(raw), &lineage); err != nil {
			return err
		}
		if err := lineage.RequireComplete(); err != nil {
			return err
		}
		for _, source := range lineage.Sources {
			if err := add(source.TenantID, source.KnowledgeBaseID); err != nil {
				return err
			}
		}
		return nil
	}
	if err := add(request.TenantID, request.KnowledgeBaseID); err != nil {
		return nil, err
	}
	var config knowledgeConfigurationInputOrigin
	err := tx.Table("knowledge_config_input_origins o").Select("o.*").Joins("JOIN knowledge_configuration_control_versions v ON v.knowledge_base_id=o.knowledge_base_id AND v.revision=o.config_revision").Where("o.tenant_id=? AND o.knowledge_base_id=?", request.TenantID, request.KnowledgeBaseID).Take(&config).Error
	if err != nil {
		return nil, err
	}
	if err = lineageScopes(config.SourceLineage); err != nil {
		return nil, err
	}
	var scopeRevision uint64
	if request.AllEntries {
		var version struct{ Revision uint64 }
		if err := tx.Table("faq_input_scope_versions").Where("tenant_id=? AND knowledge_base_id=?", request.TenantID, request.KnowledgeBaseID).Take(&version).Error; err != nil {
			return nil, err
		}
		scopeRevision = version.Revision
	}
	var catalogs []struct {
		ID                        string
		Revision, ControlRevision uint64
	}
	if err := tx.Table("knowledges k").Select("k.id,v.revision,cv.revision AS control_revision").Joins("LEFT JOIN knowledge_input_row_versions v ON v.row_kind='knowledge' AND v.row_id=k.id").Joins("LEFT JOIN faq_catalog_control_versions cv ON cv.knowledge_id=k.id").Where("k.tenant_id=? AND k.knowledge_base_id=? AND k.type=? AND k.deleted_at IS NULL", request.TenantID, request.KnowledgeBaseID, types.KnowledgeTypeFAQ).Order("k.id").Scan(&catalogs).Error; err != nil {
		return nil, err
	}
	if len(catalogs) > 1 {
		return nil, ErrKnowledgeInputChanged
	}
	var entries []struct{ ID, KnowledgeID, TagID string }
	q := tx.Table("chunks").Select("id,knowledge_id,tag_id").Where("tenant_id=? AND knowledge_base_id=? AND chunk_type=? AND deleted_at IS NULL", request.TenantID, request.KnowledgeBaseID, types.ChunkTypeFAQ)
	if !request.AllEntries {
		q = q.Where("id IN ?", request.ChunkIDs)
	}
	if err := q.Order("id").Scan(&entries).Error; err != nil {
		return nil, err
	}
	if !request.AllEntries && len(entries) != len(request.ChunkIDs) {
		return nil, ErrKnowledgeInputChanged
	}
	var proofs []struct {
		ChunkID, AdmissionID, SessionID, KnowledgeID, SourceLineage, OriginalInputs, BodyDigest string
		Revision, CurrentRevision                                                               uint64
	}
	if len(entries) > 0 {
		ids := make([]string, 0, len(entries))
		for _, entry := range entries {
			ids = append(ids, entry.ID)
		}
		err := tx.Table("faq_entry_material_receipts m").Select("m.chunk_id,m.admission_id,m.revision,m.body_digest,a.session_id,a.knowledge_id,a.source_lineage,s.original_inputs,v.revision AS current_revision").Joins("JOIN faq_entry_input_admissions a ON a.id=m.admission_id AND a.chunk_id=m.chunk_id").Joins("JOIN faq_input_sessions s ON s.id=a.session_id").Joins("JOIN faq_input_row_versions v ON v.chunk_id=m.chunk_id").Where("m.chunk_id IN ? AND a.tenant_id=? AND a.knowledge_base_id=?", ids, request.TenantID, request.KnowledgeBaseID).Order("m.chunk_id").Scan(&proofs).Error
		if err != nil {
			return nil, err
		}
		if len(proofs) != len(entries) {
			return nil, types.ErrSourceLineageUnknown
		}
	}
	for i, proof := range proofs {
		if proof.KnowledgeID != entries[i].KnowledgeID || proof.Revision != proof.CurrentRevision {
			return nil, ErrKnowledgeInputChanged
		}
		if err := lineageScopes(proof.SourceLineage); err != nil {
			return nil, err
		}
		var parents []*types.KnowledgeBuildInputProof
		if err := json.Unmarshal([]byte(proof.OriginalInputs), &parents); err != nil {
			return nil, err
		}
		if len(parents) > 4096 {
			return nil, types.ErrSourceLineageLimit
		}
		for _, parent := range parents {
			if parent == nil || parent.BodyDigest == "" || parent.Projection != "" || parent.SourceLineage.RequireComplete() != nil {
				return nil, types.ErrSourceLineageUnknown
			}
			if err := add(parent.TenantID, parent.KnowledgeBaseID); err != nil {
				return nil, err
			}
			for _, source := range parent.SourceLineage.Sources {
				if err := add(source.TenantID, source.KnowledgeBaseID); err != nil {
					return nil, err
				}
			}
		}
	}
	var tags []struct {
		ID, Kind, OriginID, SourceLineage string
		Revision                          uint64
	}
	tagQuery := tx.Table("knowledge_tags t").Select("t.id,v.kind,o.origin_id,o.source_lineage,v.revision").Joins("JOIN knowledge_display_versions v ON v.kind IN ('tag-name','tag-color','tag-sort_order','tag-envelope') AND v.row_id=t.id").Joins("JOIN knowledge_display_input_origins o ON o.kind=v.kind AND o.row_id=t.id AND o.revision=v.revision").Where("t.tenant_id=? AND t.knowledge_base_id=?", request.TenantID, request.KnowledgeBaseID)
	if !request.AllEntries {
		ids := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.TagID != "" {
				ids = append(ids, entry.TagID)
			}
		}
		tagQuery = tagQuery.Where("t.id IN ?", ids)
	}
	if err := tagQuery.Order("t.id,v.kind").Scan(&tags).Error; err != nil {
		return nil, err
	}
	for _, tag := range tags {
		if err := lineageScopes(tag.SourceLineage); err != nil {
			return nil, err
		}
	}
	digest, err := KnowledgeInputDigest(struct {
		Request                         types.FAQInputReadRequest
		Config                          knowledgeConfigurationInputOrigin
		ScopeRevision                   uint64
		Catalogs, Entries, Proofs, Tags any
	}{request, config, scopeRevision, catalogs, entries, proofs, tags})
	if err != nil {
		return nil, err
	}
	plan := &types.FAQInputReadPlan{Request: request, MetadataDigest: digest}
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
