package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type messageMaterialPlanKey struct{}

func WithMessageMaterialReadPlan(ctx context.Context, plan *types.MessageMaterialReadPlan) (context.Context, error) {
	if plan == nil || plan.MetadataDigest == "" || plan.SourceLineage.RequireComplete() != nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return ctx, err
	}
	var actual types.MessageMaterialReadPlan
	if err = json.Unmarshal(encoded, &actual); err != nil {
		return ctx, err
	}
	plans, _ := ctx.Value(messageMaterialPlanKey{}).([]*types.MessageMaterialReadPlan)
	plans = append(append([]*types.MessageMaterialReadPlan{}, plans...), &actual)
	return context.WithValue(ctx, messageMaterialPlanKey{}, plans), nil
}

func (s *KnowledgeInputProvenanceStore) PeekMessageMaterialReadPlan(ctx context.Context, req types.MessageMaterialReadRequest) (*types.MessageMaterialReadPlan, error) {
	var result *types.MessageMaterialReadPlan
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		result, err = s.peekMessageMaterialReadPlanInTx(tx, req)
		return err
	})
	return result, err
}

func (s *KnowledgeInputProvenanceStore) CommittedMessageParentRequest(ctx context.Context, tenant uint64, sessionID, assistantID string) (types.MessageMaterialReadRequest, error) {
	req := types.MessageMaterialReadRequest{TenantID: tenant, SessionID: sessionID}
	var origin messageGenerationOrigin
	if err := s.db.WithContext(ctx).Where("message_id=? AND session_id=? AND tenant_id=? AND role='assistant'", assistantID, sessionID, tenant).Take(&origin).Error; err != nil {
		return req, err
	}
	var users []struct{ MessageID string }
	if err := s.db.WithContext(ctx).Table("message_generation_origins").Select("message_id").Where("session_id=? AND tenant_id=? AND request_id=? AND role='user'", sessionID, tenant, origin.RequestID).Limit(2).Scan(&users).Error; err != nil {
		return req, err
	}
	if len(users) != 1 {
		return req, ErrKnowledgeInputChanged
	}
	req.MessageIDs = []string{assistantID, users[0].MessageID}
	sort.Strings(req.MessageIDs)
	return req, nil
}

func (s *KnowledgeInputProvenanceStore) MessagePageMaterialRequest(ctx context.Context, tenant uint64, sessionID string, before time.Time, limit, page int, descending bool) (types.MessageMaterialReadRequest, error) {
	req := types.MessageMaterialReadRequest{TenantID: tenant, SessionID: sessionID, MessageIDs: []string{}}
	q := s.db.WithContext(ctx).Table("messages").Select("id").Where("session_id=? AND deleted_at IS NULL", sessionID)
	if !before.IsZero() {
		q = q.Where("created_at < ?", before)
	}
	if descending {
		q = q.Order("created_at DESC,id DESC")
	} else {
		q = q.Order("created_at ASC,id ASC")
	}
	if limit > 0 {
		q = q.Limit(limit)
	}
	if page > 1 {
		q = q.Offset((page - 1) * limit)
	}
	var rows []struct{ ID string }
	if err := q.Scan(&rows).Error; err != nil {
		return req, err
	}
	for _, row := range rows {
		req.MessageIDs = append(req.MessageIDs, row.ID)
	}
	sort.Strings(req.MessageIDs)
	return req, nil
}
func (s *KnowledgeInputProvenanceStore) peekMessageMaterialReadPlanInTx(tx *gorm.DB, req types.MessageMaterialReadRequest) (*types.MessageMaterialReadPlan, error) {
	req.MessageIDs = append([]string{}, req.MessageIDs...)
	sort.Strings(req.MessageIDs)
	if req.TenantID == 0 || req.SessionID == "" || !req.AllSession && len(req.MessageIDs) == 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	var session struct {
		ID, UserID string
		TenantID   uint64
		DeletedAt  gorm.DeletedAt
	}
	if err := tx.Table("sessions").Select("id,user_id,tenant_id,deleted_at").Where("id=? AND tenant_id=? AND deleted_at IS NULL", req.SessionID, req.TenantID).Take(&session).Error; err != nil {
		return nil, err
	}
	ownerRevision, err := messageVersion(tx, "message_session_owner_versions", "session_id", session.ID)
	if err != nil {
		return nil, err
	}
	membership, err := messageVersion(tx, "message_session_membership_versions", "session_id", session.ID)
	if err != nil {
		return nil, err
	}
	var ids []struct{ ID string }
	query := tx.Table("messages").Select("id").Where("session_id=? AND deleted_at IS NULL", req.SessionID)
	if !req.AllSession {
		query = query.Where("id IN ?", req.MessageIDs)
	}
	if err = query.Order("id").Scan(&ids).Error; err != nil {
		return nil, err
	}
	if !req.AllSession && len(ids) != len(req.MessageIDs) {
		return nil, ErrKnowledgeInputChanged
	}
	facts := []messageMaterialAdmission{}
	lineages := []*types.SourceLineage{}
	scopes := map[string]types.KnowledgeSearchScope{}
	add := func(tenant uint64, kb string) error {
		if tenant == 0 || kb == "" {
			return types.ErrSourceLineageUnknown
		}
		scopes[fmt.Sprintf("%d:%s", tenant, kb)] = types.KnowledgeSearchScope{TenantID: tenant, KBID: kb}
		return nil
	}
	for _, id := range ids {
		var fact messageMaterialAdmission
		if err := tx.Table("message_material_admissions a").Select("a.*").Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").Where("a.message_id=? AND a.session_id=? AND a.tenant_id=?", id.ID, req.SessionID, req.TenantID).Take(&fact).Error; err != nil {
			return nil, err
		}
		revision, err := messageVersion(tx, "message_material_versions", "message_id", id.ID)
		if err != nil {
			return nil, err
		}
		if revision != fact.Revision || fact.OwnerID != session.UserID || fact.OwnerRevision != ownerRevision {
			return nil, ErrKnowledgeInputChanged
		}
		var lineage types.SourceLineage
		if err := json.Unmarshal([]byte(fact.SourceLineage), &lineage); err != nil {
			return nil, err
		}
		if err := lineage.RequireComplete(); err != nil {
			return nil, err
		}
		lineages = append(lineages, &lineage)
		for _, source := range lineage.Sources {
			if err := add(source.TenantID, source.KnowledgeBaseID); err != nil {
				return nil, err
			}
		}
		var execution types.MessageExecutionContext
		if err := json.Unmarshal([]byte(fact.ExecutionContext), &execution); err != nil {
			return nil, err
		}
		kbIDs := append(append([]string{}, execution.KnowledgeBaseIDs...), execution.AgentKnowledgeBaseIDs...)
		for _, tag := range execution.TagScopes {
			kbIDs = append(kbIDs, tag.KnowledgeBaseID)
		}
		for _, knowledgeID := range execution.KnowledgeIDs {
			var row struct{ KnowledgeBaseID string }
			if err := tx.Table("knowledges").Select("knowledge_base_id").Where("id=? AND deleted_at IS NULL", knowledgeID).Take(&row).Error; err != nil {
				return nil, err
			}
			kbIDs = append(kbIDs, row.KnowledgeBaseID)
		}
		for _, kbID := range kbIDs {
			var kb struct{ TenantID uint64 }
			if err := tx.Table("knowledge_bases").Select("tenant_id").Where("id=? AND deleted_at IS NULL", kbID).Take(&kb).Error; err != nil {
				return nil, err
			}
			if err := add(kb.TenantID, kbID); err != nil {
				return nil, err
			}
		}
		facts = append(facts, fact)
	}
	if len(lineages) == 0 {
		empty, _ := types.NewCompleteSourceLineage()
		lineages = append(lineages, empty)
	}
	lineage, err := types.UnionSourceLineage(lineages...)
	if err != nil {
		return nil, err
	}
	digest, err := KnowledgeInputDigest(struct {
		Request           types.MessageMaterialReadRequest
		Session           any
		Owner, Membership uint64
		Facts             []messageMaterialAdmission
	}{req, session, ownerRevision, membership, facts})
	if err != nil {
		return nil, err
	}
	result := &types.MessageMaterialReadPlan{Request: req, MetadataDigest: digest, SourceLineage: lineage}
	for _, scope := range scopes {
		result.Scopes = append(result.Scopes, scope)
	}
	sort.Slice(result.Scopes, func(i, j int) bool {
		if result.Scopes[i].TenantID != result.Scopes[j].TenantID {
			return result.Scopes[i].TenantID < result.Scopes[j].TenantID
		}
		return result.Scopes[i].KBID < result.Scopes[j].KBID
	})
	return result, nil
}
func (s *KnowledgeInputProvenanceStore) checkMessageMaterialPlanInTx(tx *gorm.DB, req types.MessageMaterialReadRequest) error {
	req.MessageIDs = append([]string{}, req.MessageIDs...)
	sort.Strings(req.MessageIDs)
	expected := findMessageMaterialReadPlan(tx.Statement.Context, req)
	if expected == nil {
		return ErrNextcloudContentGCUncovered
	}
	if expected.Request.SessionID != req.SessionID || expected.Request.TenantID != req.TenantID {
		return ErrKnowledgeInputChanged
	}
	current, err := s.peekMessageMaterialReadPlanInTx(tx, expected.Request)
	if err != nil {
		return err
	}
	if !faqInputEqual(current, expected) {
		return ErrKnowledgeInputChanged
	}
	if len(current.Scopes) == 0 {
		return nil
	}
	return validateFAQReadLeaseCoverageInTx(tx, current.Scopes, current.SourceLineage)
}

// SQL-local authority runs on the caller's existing transaction. No global
// message service/source HTTP/second database connection is called here.
func (s *KnowledgeInputProvenanceStore) verifyMessageMaterialInTx(tx *gorm.DB, message *types.Message) (*types.MessageMaterialProof, error) {
	fresh, session, ownerRevision, err := s.currentMessageOwnerInTx(tx, message.SessionID)
	if err != nil {
		return nil, err
	}
	var fact messageMaterialAdmission
	if err := tx.Table("message_material_admissions a").Select("a.*").Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").Where("a.message_id=? AND a.session_id=? AND a.tenant_id=?", message.ID, session.ID, session.TenantID).Take(&fact).Error; err != nil {
		return nil, err
	}
	if err := s.checkMessageOriginalBodyAvailability(tx, fact.GenerationID); err != nil {
		return nil, err
	}
	revision, err := messageVersion(tx, "message_material_versions", "message_id", message.ID)
	if err != nil {
		return nil, err
	}
	digest, err := messageMaterialDigest(message)
	if err != nil {
		return nil, err
	}
	if !message.IsCompleted || fact.BodyDigest != digest || fact.Revision != revision || fact.OwnerID != session.UserID || fact.OwnerRevision != ownerRevision {
		return nil, ErrKnowledgeInputChanged
	}
	var lineage types.SourceLineage
	if err := json.Unmarshal([]byte(fact.SourceLineage), &lineage); err != nil {
		return nil, err
	}
	if err := lineage.RequireComplete(); err != nil {
		return nil, err
	}
	if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
		return nil, err
	}
	authority, err := s.messageAuthorityInTx(tx)
	if err != nil {
		return nil, err
	}
	var execution types.MessageExecutionContext
	if err := json.Unmarshal([]byte(fact.ExecutionContext), &execution); err != nil {
		return nil, err
	}
	// Resolve current Agent existence/share using its original selector.
	if fact.AgentID != "" {
		var agent types.CustomAgent
		if err := tx.Where("id=? AND tenant_id=?", fact.AgentID, fact.AgentTenantID).Take(&agent).Error; err != nil {
			return nil, err
		}
		if fact.AgentTenantID != types.CallerFromContext(fresh).TenantID {
			if authority.messageActorAgents == nil {
				return nil, types.ErrSourceLineageUnknown
			}
			shared, err := authority.messageActorAgents.GetSharedAgentForTenant(fresh, types.CallerFromContext(fresh).TenantID, types.CallerFromContext(fresh).Role, fact.AgentID, fact.AgentTenantID)
			if err != nil {
				return nil, err
			}
			if shared == nil {
				return nil, access.ErrForbidden
			}
		}
		permission, err := authority.actorGroups.EffectivePermission(fresh, fact.AgentTenantID, types.GroupResourceTypeAgent, fact.AgentID, types.ResourceActionUse, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if !permission.Allowed {
			return nil, access.ErrForbidden
		}
	}
	kbIDs := append(append([]string{}, execution.KnowledgeBaseIDs...), execution.AgentKnowledgeBaseIDs...)
	for _, source := range lineage.Sources {
		kbIDs = append(kbIDs, source.KnowledgeBaseID)
	}
	for _, tag := range execution.TagScopes {
		kbIDs = append(kbIDs, tag.KnowledgeBaseID)
	}
	for _, id := range execution.KnowledgeIDs {
		var row struct{ KnowledgeBaseID string }
		if err := tx.Table("knowledges").Select("knowledge_base_id").Where("id=? AND deleted_at IS NULL", id).Take(&row).Error; err != nil {
			return nil, err
		}
		kbIDs = append(kbIDs, row.KnowledgeBaseID)
	}
	if types.CallerFromContext(fresh).TenantID != session.TenantID && len(kbIDs) == 0 {
		return nil, access.ErrForbidden
	}
	for _, id := range kbIDs {
		var kb types.KnowledgeBase
		if err := tx.Where("id=?", id).Take(&kb).Error; err != nil {
			return nil, err
		}
		caller := types.CallerFromContext(fresh)
		_, err := access.ResolveKB(fresh, access.KBRequest{Caller: caller}, &kb, types.OrgRoleViewer, authority.actorShares, authority.messageActorAgents)
		if err != nil && fact.AgentID != "" && authority.messageActorAgents != nil {
			agent, e := authority.messageActorAgents.GetSharedAgentForTenant(fresh, caller.TenantID, caller.Role, fact.AgentID, fact.AgentTenantID)
			if e == nil && agent != nil && types.SharedAgentIncludesKB(agent, &kb) {
				err = nil
			}
		}
		if err != nil {
			return nil, err
		}
		permission, err := authority.actorGroups.EffectivePermission(fresh, kb.TenantID, types.GroupResourceTypeKnowledgeBase, kb.ID, types.ResourceActionRead, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if !permission.Allowed {
			return nil, access.ErrForbidden
		}
	}
	return &types.MessageMaterialProof{AdmissionID: fact.ID, GenerationID: fact.GenerationID, MessageID: fact.MessageID, SessionID: fact.SessionID, TenantID: fact.TenantID, OwnerID: fact.OwnerID, OwnerRevision: fact.OwnerRevision, Revision: fact.Revision, BodyDigest: fact.BodyDigest, SourceLineage: &lineage, Execution: execution, AgentID: fact.AgentID, AgentTenantID: fact.AgentTenantID}, nil
}
func (s *KnowledgeInputProvenanceStore) captureMessageMaterialInTx(tx *gorm.DB, req types.MessageMaterialReadRequest) (*types.MessageMaterialSnapshot, error) {
	req.MessageIDs = append([]string{}, req.MessageIDs...)
	sort.Strings(req.MessageIDs)
	if err := s.checkMessageMaterialPlanInTx(tx, req); err != nil {
		return nil, err
	}
	// Resolve only receipt identities and availability before the first body read.
	var generations []struct{ GenerationID string }
	gq := tx.Table("message_material_admissions a").Select("a.generation_id").Joins("JOIN message_material_receipts r ON r.admission_id=a.id AND r.message_id=a.message_id").Where("a.session_id=? AND a.tenant_id=?", req.SessionID, req.TenantID)
	if !req.AllSession {
		gq = gq.Where("a.message_id IN ?", req.MessageIDs)
	}
	if err := gq.Scan(&generations).Error; err != nil {
		return nil, err
	}
	for _, generation := range generations {
		if err := s.checkMessageOriginalBodyAvailability(tx, generation.GenerationID); err != nil {
			return nil, err
		}
	}
	_, session, ownerRevision, err := s.currentMessageOwnerInTx(tx, req.SessionID)
	if err != nil {
		return nil, err
	}
	membership, err := messageVersion(tx, "message_session_membership_versions", "session_id", req.SessionID)
	if err != nil {
		return nil, err
	}
	q := tx.Where("session_id=?", req.SessionID)
	if !req.AllSession {
		q = q.Where("id IN ?", req.MessageIDs)
	}
	result := &types.MessageMaterialSnapshot{Request: req, SessionOwnerID: session.UserID, SessionOwnerRevision: ownerRevision, MembershipRevision: membership, Messages: []*types.Message{}, Proofs: []*types.MessageMaterialProof{}}
	var identities []struct{ ID, Role string }
	if err := q.Model(&types.Message{}).Select("id,role").Order("created_at,id").Scan(&identities).Error; err != nil {
		return nil, err
	}
	for _, identity := range identities {
		message, err := readMessageMaterialBodyInTx(tx, identity.ID, req.SessionID, identity.Role)
		if err != nil {
			return nil, err
		}
		result.Messages = append(result.Messages, message)
	}
	for _, message := range result.Messages {
		proof, err := s.verifyMessageMaterialInTx(tx, message)
		if err != nil {
			return nil, err
		}
		if message.Role == "user" {
			message.SourceLineage = proof.SourceLineage
		}
		result.Proofs = append(result.Proofs, proof)
	}
	expected := findMessageMaterialReadPlan(tx.Statement.Context, req)
	result.MetadataDigest = expected.MetadataDigest
	return result, nil
}
func (s *KnowledgeInputProvenanceStore) CaptureMessageMaterialInputs(ctx context.Context, req types.MessageMaterialReadRequest) (*types.MessageMaterialSnapshot, error) {
	var result *types.MessageMaterialSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		var err error
		result, err = s.captureMessageMaterialInTx(tx, req)
		return err
	})
	return result, err
}
func (s *KnowledgeInputProvenanceStore) RecheckMessageMaterialInputs(ctx context.Context, expected *types.MessageMaterialSnapshot) error {
	if expected == nil {
		return types.ErrSourceLineageUnknown
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		actual, err := s.captureMessageMaterialInTx(tx, expected.Request)
		if err != nil {
			return err
		} // KnowledgeID linkage and UpdatedAt are independent code bookkeeping.
		a, b := *actual, *expected
		a.Messages = nil
		b.Messages = nil
		if !faqInputEqual(&a, &b) {
			return ErrKnowledgeInputChanged
		}
		return nil
	})
}

func WithMessageMaterialReadPlans(ctx context.Context, plans []*types.MessageMaterialReadPlan) (context.Context, error) {
	var err error
	for _, plan := range plans {
		ctx, err = WithMessageMaterialReadPlan(ctx, plan)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}
func findMessageMaterialReadPlan(ctx context.Context, req types.MessageMaterialReadRequest) *types.MessageMaterialReadPlan {
	plans, _ := ctx.Value(messageMaterialPlanKey{}).([]*types.MessageMaterialReadPlan)
	for _, plan := range plans {
		if plan.Request.TenantID != req.TenantID || plan.Request.SessionID != req.SessionID {
			continue
		}
		covered := plan.Request.AllSession
		if !covered {
			ids := map[string]bool{}
			for _, id := range plan.Request.MessageIDs {
				ids[id] = true
			}
			covered = !req.AllSession
			for _, id := range req.MessageIDs {
				if !ids[id] {
					covered = false
					break
				}
			}
		}
		if covered {
			return plan
		}
	}
	return nil
}
func (s *KnowledgeInputProvenanceStore) peekHistoryMaterialPlanForProofInTx(tx *gorm.DB, proof *types.KnowledgeBuildInputProof) (*types.MessageMaterialReadPlan, error) {
	if proof == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var admission knowledgeInputAdmission
	if err := tx.Select("admission_id,tenant_id,knowledge_base_id,knowledge_id,origin_kind,input_digest,build_generation").Where("admission_id=? AND tenant_id=? AND knowledge_id=?", proof.AdmissionID, proof.TenantID, proof.KnowledgeID).Take(&admission).Error; err != nil {
		return nil, err
	}
	if admission.InputDigest != proof.InputDigest || admission.BuildGeneration != proof.BuildGeneration {
		return nil, ErrKnowledgeInputChanged
	}
	if admission.OriginKind != "history" {
		return nil, nil
	}
	var parent knowledgeInputHistoryParents
	if err := tx.Where("admission_id=? AND tenant_id=?", admission.AdmissionID, admission.TenantID).Take(&parent).Error; err != nil {
		return nil, err
	}
	return s.peekMessageMaterialReadPlanInTx(tx, types.MessageMaterialReadRequest{TenantID: parent.TenantID, SessionID: parent.SessionID, MessageIDs: []string{parent.AssistantMessageID, parent.UserMessageID}})
}
