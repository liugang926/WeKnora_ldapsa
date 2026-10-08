package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type knowledgeBuildActorSelector struct {
	AdmissionID    string `gorm:"primaryKey"`
	CallerTenantID uint64
	UserID         string
	PrincipalType  string
	PrincipalID    string
	APIKeyID       *uint64
}

type knowledgeBuildActorContextKey struct{}

func (s *KnowledgeInputProvenanceStore) recheckKnowledgeWorkerActor(ctx context.Context, proof *types.KnowledgeBuildInputProof) error {
	id, _ := ctx.Value(knowledgeBuildActorContextKey{}).(string)
	if id == "" {
		return nil
	}
	if proof == nil || id != proof.AdmissionID {
		return ErrKnowledgeInputChanged
	}
	fresh, err := s.RestoreKnowledgeBuildActor(ctx, proof)
	if err != nil {
		return err
	}
	if types.CallerFromContext(ctx) != types.CallerFromContext(fresh) {
		return ErrKnowledgeInputChanged
	}
	oldPrincipal, _ := types.PrincipalFromContext(ctx)
	newPrincipal, _ := types.PrincipalFromContext(fresh)
	if oldPrincipal != newPrincipal {
		return ErrKnowledgeInputChanged
	}
	oldKey, oldOK := types.TenantAPIKeyScopeFromContext(ctx)
	newKey, newOK := types.TenantAPIKeyScopeFromContext(fresh)
	if oldOK != newOK || !reflect.DeepEqual(oldKey, newKey) {
		return ErrKnowledgeInputChanged
	}
	return nil
}

func (knowledgeBuildActorSelector) TableName() string { return "knowledge_input_actor_selectors" }

func captureKnowledgeActorSelector(ctx context.Context, admissionID string) (knowledgeBuildActorSelector, error) {
	caller := types.CallerFromContext(ctx)
	principal, ok := types.PrincipalFromContext(ctx)
	selector := knowledgeBuildActorSelector{AdmissionID: admissionID, CallerTenantID: caller.TenantID, UserID: caller.UserID, PrincipalType: principal.Type, PrincipalID: principal.ID}
	if !ok || caller.TenantID == 0 {
		return selector, types.ErrSourceLineageUnknown
	}
	switch principal.Type {
	case types.PrincipalWebUser:
		if caller.UserID == "" || principal.ID != caller.UserID {
			return selector, ErrKnowledgeInputChanged
		}
	case types.PrincipalAPITenant, types.PrincipalAPIExternalUser:
		scope, ok := types.TenantAPIKeyScopeFromContext(ctx)
		if !ok || scope.KeyID == 0 || scope.IsPlatform() {
			return selector, ErrKnowledgeInputChanged
		}
		selector.UserID = ""
		key := scope.KeyID
		selector.APIKeyID = &key
	default:
		return selector, types.ErrSourceLineageUnknown
	}
	return selector, nil
}

func ConfigureKnowledgeInputActorGroupAccess(store *KnowledgeInputProvenanceStore, groups interfaces.GroupAccessService) {
	store.actorGroups = groups
}

func (s *KnowledgeInputProvenanceStore) KnowledgeBuildActorRequired(ctx context.Context, proof *types.KnowledgeBuildInputProof) (bool, error) {
	if proof == nil {
		return false, types.ErrSourceLineageUnknown
	}
	var admission knowledgeInputAdmission
	if err := s.db.WithContext(ctx).Where("admission_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", proof.AdmissionID, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID).Take(&admission).Error; err != nil {
		return false, err
	}
	return admission.OriginKind == "history", nil
}

// RestoreKnowledgeBuildActor loads the original immutable selector and current
// active account/key/membership. Persisted roles or owner strings never grant
// authority. KB/file/parent permissions are still freshly authorized by their
// mandatory scoped guards after this reconstruction.
func (s *KnowledgeInputProvenanceStore) RestoreKnowledgeBuildActor(ctx context.Context, proof *types.KnowledgeBuildInputProof) (context.Context, error) {
	if proof == nil || proof.AdmissionID == "" {
		return ctx, types.ErrSourceLineageUnknown
	}
	var actor knowledgeBuildActorSelector
	if err := s.db.WithContext(ctx).Where("admission_id = ?", proof.AdmissionID).Take(&actor).Error; err != nil {
		return ctx, err
	}
	var admission knowledgeInputAdmission
	if err := s.db.WithContext(ctx).Where("admission_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", proof.AdmissionID, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID).Take(&admission).Error; err != nil {
		return ctx, err
	}
	if admission.BuildGeneration != proof.BuildGeneration || admission.InputDigest != proof.InputDigest {
		return ctx, ErrKnowledgeInputChanged
	}
	principal := types.Principal{Type: actor.PrincipalType, ID: actor.PrincipalID}
	caller := types.Caller{TenantID: actor.CallerTenantID, UserID: actor.UserID, Role: types.TenantRoleViewer}
	// Drop any cached request/task grants. They are not reconstructed authority.
	ctx = context.WithValue(ctx, types.KBGrantsContextKey, nil)
	ctx = context.WithValue(ctx, types.TenantAPIKeyScopeContextKey, nil)
	ctx = context.WithValue(ctx, types.UserIDContextKey, "")
	ctx = context.WithValue(ctx, types.UserContextKey, nil)
	ctx = context.WithValue(ctx, types.SystemAdminContextKey, false)
	ctx = context.WithValue(ctx, types.TenantRoleContextKey, types.TenantRoleViewer)
	switch actor.PrincipalType {
	case types.PrincipalWebUser:
		if actor.UserID == "" || actor.PrincipalID != actor.UserID || actor.APIKeyID != nil {
			return ctx, ErrKnowledgeInputChanged
		}
		var user types.User
		if err := s.db.WithContext(ctx).Where("id = ? AND is_active = ?", actor.UserID, true).Take(&user).Error; err != nil {
			return ctx, err
		}
		if s.actorGroups != nil {
			effective, err := s.actorGroups.EffectiveTenantRole(ctx, user.ID, actor.CallerTenantID, time.Now().UTC())
			if err != nil {
				return ctx, err
			}
			if !effective.Member || !effective.Role.IsValid() {
				return ctx, ErrKnowledgeInputChanged
			}
			caller.Role = effective.Role
		} else {
			var member types.TenantMember
			if err := s.db.WithContext(ctx).Where("user_id = ? AND tenant_id = ? AND status = ?", user.ID, actor.CallerTenantID, types.TenantMemberStatusActive).Take(&member).Error; err != nil {
				return ctx, err
			}
			if !member.Role.IsValid() {
				return ctx, ErrKnowledgeInputChanged
			}
			caller.Role = member.Role
		}
		ctx = context.WithValue(ctx, types.UserIDContextKey, user.ID)
	case types.PrincipalAPITenant, types.PrincipalAPIExternalUser:
		if actor.APIKeyID == nil || actor.UserID != "" {
			return ctx, ErrKnowledgeInputChanged
		}
		var key types.TenantAPIKey
		if err := s.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND revoked_at IS NULL", *actor.APIKeyID, actor.CallerTenantID).Take(&key).Error; err != nil {
			return ctx, err
		}
		if key.IsPlatform() || key.ExpiresAt != nil && !key.ExpiresAt.After(time.Now().UTC()) {
			return ctx, ErrKnowledgeInputChanged
		}
		if actor.PrincipalType == types.PrincipalAPITenant && actor.PrincipalID != fmt.Sprintf("%d", actor.CallerTenantID) {
			return ctx, ErrKnowledgeInputChanged
		}
		if actor.PrincipalType == types.PrincipalAPIExternalUser && !strings.HasPrefix(actor.PrincipalID, fmt.Sprintf("%d:", actor.CallerTenantID)) {
			return ctx, ErrKnowledgeInputChanged
		}
		scope := types.TenantAPIKeyScope{KeyID: key.ID, Name: key.Name, ScopeType: key.ScopeType, FullAccess: key.FullAccess, KnowledgeBaseIDs: key.KnowledgeBaseIDs, Capabilities: key.Capabilities}
		ctx = types.WithTenantAPIKeyScope(ctx, scope)
		// Match the authenticated machine boundary: FullAccess is a current DB
		// credential capability, rather than an invented workspace-owner user.
		if key.FullAccess {
			caller.Role = types.TenantRoleOwner
		}
	default:
		return ctx, types.ErrSourceLineageUnknown
	}
	ctx = types.WithCaller(ctx, caller)
	ctx = types.WithPrincipal(ctx, principal)
	ctx = context.WithValue(ctx, types.TenantRoleContextKey, caller.Role)
	ctx = types.WithExecutionTenant(ctx, proof.TenantID)
	ctx = context.WithValue(ctx, knowledgeBuildActorContextKey{}, proof.AdmissionID)
	return ctx, nil
}

var _ interface {
	RestoreKnowledgeBuildActor(context.Context, *types.KnowledgeBuildInputProof) (context.Context, error)
} = (*KnowledgeInputProvenanceStore)(nil)
