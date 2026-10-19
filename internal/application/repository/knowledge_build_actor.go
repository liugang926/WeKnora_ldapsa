package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
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
type knowledgeBuildWritePurposeContextKey struct{}

func (s *KnowledgeInputProvenanceStore) recheckKnowledgeWorkerActor(ctx context.Context, proof *types.KnowledgeBuildInputProof) error {
	if marker, ok := ctx.Value(knowledgeCurrentProducerWriteKey{}).(knowledgeCurrentProducerWrite); ok {
		if proof == nil || marker.tenant != proof.TenantID || marker.kb != proof.KnowledgeBaseID {
			return ErrKnowledgeInputChanged
		}
		fresh, err := s.refreshKnowledgeCurrentProducerWrite(ctx, marker)
		if err != nil {
			return err
		}
		a, _ := types.PrincipalFromContext(ctx)
		b, _ := types.PrincipalFromContext(fresh)
		if a != b || types.CallerFromContext(ctx) != types.CallerFromContext(fresh) {
			return ErrKnowledgeInputChanged
		}
		return nil
	}

	if producer, ok := ctx.Value(knowledgeProducerActorContextKey{}).(*types.KnowledgeProducerInputProof); ok {
		fresh, err := s.RestoreKnowledgeProducerActor(ctx, proof, producer)
		if err != nil {
			return err
		}
		a, _ := types.PrincipalFromContext(ctx)
		b, _ := types.PrincipalFromContext(fresh)
		if a != b || types.CallerFromContext(ctx) != types.CallerFromContext(fresh) {
			return ErrKnowledgeInputChanged
		}
		return nil
	}
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
	if ctx == nil {
		return knowledgeBuildActorSelector{}, types.ErrSourceLineageUnknown
	}
	caller, capturedCaller := ctx.Value(types.CallerContextKey).(types.Caller)
	principal, ok := ctx.Value(types.PrincipalContextKey).(types.Principal)
	selector := knowledgeBuildActorSelector{AdmissionID: admissionID, CallerTenantID: caller.TenantID, UserID: caller.UserID, PrincipalType: principal.Type, PrincipalID: principal.ID}
	if !capturedCaller || !ok || !principal.Valid() || principal != principal.Normalize() || caller.TenantID == 0 {
		return selector, types.ErrSourceLineageUnknown
	}
	switch principal.Type {
	case types.PrincipalWebUser:
		_, machine := types.TenantAPIKeyScopeFromContext(ctx)
		if caller.UserID == "" || principal.ID != caller.UserID || types.IsSyntheticUserID(caller.UserID) || machine {
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

func ConfigureKnowledgeInputActorShares(store *KnowledgeInputProvenanceStore, shares access.KBShareLookup) {
	store.actorShares = shares
}

// RestoreKnowledgeBuildActorIfRequired is the trusted worker boundary. It
// reloads the immutable actor selector for actor-bound original admissions;
// ordinary and Nextcloud admissions keep their existing execution context.
func RestoreKnowledgeBuildActorIfRequired(ctx context.Context, reader interfaces.KnowledgeBuildInputReader, proof *types.KnowledgeBuildInputProof) (context.Context, error) {
	if reader == nil || proof == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	probe, ok := reader.(interface {
		KnowledgeBuildActorRequired(context.Context, *types.KnowledgeBuildInputProof) (bool, error)
	})
	if !ok {
		return ctx, types.ErrSourceLineageUnknown
	}
	required, err := probe.KnowledgeBuildActorRequired(ctx, proof)
	if err != nil || !required {
		return ctx, err
	}
	restorer, ok := reader.(interface {
		RestoreKnowledgeBuildActor(context.Context, *types.KnowledgeBuildInputProof) (context.Context, error)
	})
	if !ok {
		return ctx, types.ErrSourceLineageUnknown
	}
	return restorer.RestoreKnowledgeBuildActor(ctx, proof)
}

func (s *KnowledgeInputProvenanceStore) KnowledgeBuildActorRequired(ctx context.Context, proof *types.KnowledgeBuildInputProof) (bool, error) {
	if proof == nil {
		return false, types.ErrSourceLineageUnknown
	}
	var admission knowledgeInputAdmission
	if err := s.db.WithContext(ctx).Where("admission_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", proof.AdmissionID, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID).Take(&admission).Error; err != nil {
		return false, err
	}
	var count int64
	if err := s.db.WithContext(ctx).Model(&knowledgeBuildActorSelector{}).Where("admission_id = ?", proof.AdmissionID).Count(&count).Error; err != nil {
		return false, err
	}
	return admission.OriginKind == "history" || count == 1, nil
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
	fresh, err := s.restoreCurrentKnowledgeActor(ctx, actor, proof.TenantID)
	if err != nil {
		return ctx, err
	}
	var kb types.KnowledgeBase
	if err := s.db.WithContext(fresh).Where("id = ? AND tenant_id = ?", proof.KnowledgeBaseID, proof.TenantID).Take(&kb).Error; err != nil {
		return ctx, err
	}
	fresh, err = s.authorizeKnowledgeActorWrite(fresh, &kb)
	if err != nil {
		return ctx, err
	}
	fresh = context.WithValue(fresh, knowledgeBuildWritePurposeContextKey{}, cloneKnowledgeInputProof(proof))
	return context.WithValue(fresh, knowledgeBuildActorContextKey{}, proof.AdmissionID), nil
}

// AuthorizeCurrentKnowledgeInputWrite checks a current authenticated request's
// exact actor against a server-loaded KB. It never restores another admission's
// actor. Bind this store and its group/share lookups to the writer transaction
// when the authorized producer publishes a receipt in that transaction.
func (s *KnowledgeInputProvenanceStore) AuthorizeCurrentKnowledgeInputWrite(ctx context.Context, kb *types.KnowledgeBase) (context.Context, error) {
	if kb == nil || kb.ID == "" || kb.TenantID == 0 {
		return ctx, ErrKnowledgeInputChanged
	}
	actor, err := captureKnowledgeActorSelector(ctx, "")
	if err != nil {
		return ctx, err
	}
	fresh, err := s.restoreCurrentKnowledgeActor(ctx, actor, kb.TenantID)
	if err != nil {
		return ctx, err
	}
	var actual types.KnowledgeBase
	if err := s.db.WithContext(fresh).Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).Take(&actual).Error; err != nil {
		return ctx, err
	}
	return s.authorizeKnowledgeActorWrite(fresh, &actual)
}

// authorizeOriginalKnowledgeProducerWrite is confined to derived producer
// commits. The exact private Nextcloud lease and persisted original admission
// authorize only this worker's generation; public readers never call it.
func (s *KnowledgeInputProvenanceStore) authorizeOriginalKnowledgeProducerWrite(ctx context.Context, kb *types.KnowledgeBase, original *types.KnowledgeBuildInputProof) (context.Context, error) {
	if kb == nil || original == nil || original.TenantID != kb.TenantID || original.KnowledgeBaseID != kb.ID {
		return ctx, ErrKnowledgeInputChanged
	}
	if !NextcloudBuildLeasePresent(ctx, original.TenantID, original.KnowledgeBaseID, original.KnowledgeID) {
		return s.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	}
	bound := KnowledgeBuildInputProofFromContext(ctx)
	if bound == nil || bound.AdmissionID != original.AdmissionID || bound.InputDigest != original.InputDigest || bound.BuildGeneration != original.BuildGeneration || bound.KnowledgeID != original.KnowledgeID || bound.TenantID != original.TenantID || bound.KnowledgeBaseID != original.KnowledgeBaseID {
		return ctx, ErrKnowledgeInputChanged
	}
	if err := ValidateNextcloudBuildWrite(ctx, s.db, original.TenantID, original.KnowledgeBaseID, original.KnowledgeID); err != nil {
		return ctx, err
	}
	var k types.Knowledge
	if err := s.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", original.KnowledgeID, original.TenantID, original.KnowledgeBaseID).Take(&k).Error; err != nil {
		return ctx, err
	}
	admission, _, _, err := s.admissionInTx(s.db.WithContext(ctx), &k, original)
	if err != nil {
		return ctx, err
	}
	if admission.OriginKind != "nextcloud" {
		return ctx, ErrKnowledgeInputChanged
	}
	if err := s.recheckKnowledgeWorkerActor(ctx, original); err != nil {
		return ctx, err
	}
	return ctx, nil
}

func (s *KnowledgeInputProvenanceStore) authorizeKnowledgeActorWrite(ctx context.Context, kb *types.KnowledgeBase) (context.Context, error) {
	caller := types.CallerFromContext(ctx)
	if scope, ok := types.TenantAPIKeyScopeFromContext(ctx); ok {
		if requirement, bound := types.APIKeyOperationRequirementFromContext(ctx); bound && !requirement.Allows(scope) {
			return ctx, access.ErrForbidden
		}
		if !scope.FullAccess && !scope.HasCapability(types.APIKeyCapabilityIngest) {
			return ctx, access.ErrForbidden
		}
	} else if !caller.Role.HasPermission(types.TenantRoleContributor) {
		return ctx, access.ErrForbidden
	}
	var shares access.KBShareLookup
	if caller.UserID != "" {
		shares = s.actorShares
	}
	grant, err := access.ResolveKB(ctx, access.KBRequest{Caller: caller}, kb, types.OrgRoleEditor, shares, nil)
	if err != nil {
		return ctx, err
	}
	if s.actorGroups == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	permission, err := s.actorGroups.EffectivePermission(ctx, kb.TenantID, types.GroupResourceTypeKnowledgeBase, kb.ID, types.ResourceActionEdit, time.Now().UTC())
	if err != nil {
		return ctx, err
	}
	if !permission.Allowed {
		return ctx, access.ErrForbidden
	}
	fresh := grant.Context(ctx)
	if err := access.RequireKBWrite(fresh, kb); err != nil {
		return ctx, err
	}
	if _, machine := types.TenantAPIKeyScopeFromContext(fresh); machine {
		if _, routeBound := types.APIKeyOperationRequirementFromContext(fresh); !routeBound {
			// This current, exact KB write has already been authorized. Its
			// worker may inspect the original admitted inputs to complete the
			// write. An HTTP request keeps its exact route capability policy.
			fresh = types.WithAPIKeyOperationRequirement(fresh, types.APIKeyOperationRequirement{RequireFullAccess: true, Capabilities: []types.APIKeyCapability{types.APIKeyCapabilityIngest}})
		}
	}
	return fresh, nil
}

func (s *KnowledgeInputProvenanceStore) restoreCurrentKnowledgeActor(ctx context.Context, actor knowledgeBuildActorSelector, executionTenant uint64) (context.Context, error) {
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
		if actor.PrincipalType == types.PrincipalAPIExternalUser {
			prefix := fmt.Sprintf("%d:", actor.CallerTenantID)
			if !strings.HasPrefix(actor.PrincipalID, prefix) || strings.TrimSpace(strings.TrimPrefix(actor.PrincipalID, prefix)) == "" {
				return ctx, ErrKnowledgeInputChanged
			}
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
	ctx = types.WithExecutionTenant(ctx, executionTenant)
	return ctx, nil
}

var _ interface {
	RestoreKnowledgeBuildActor(context.Context, *types.KnowledgeBuildInputProof) (context.Context, error)
} = (*KnowledgeInputProvenanceStore)(nil)
