package service

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func historyActorIntegrationFixture(t *testing.T, kind string) (*knowledgeService, *repository.KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.Message, *types.TenantAPIKey, *types.User) {
	t.Helper()
	service, store, db, _, _ := knowledgeInputServiceFixture(t)
	require.NoError(t, db.Exec("PRAGMA journal_mode=WAL").Error)
	require.NoError(t, db.AutoMigrate(&types.User{}, &types.TenantMember{}, &types.TenantAPIKey{}))
	ddl, err := os.ReadFile("../../../migrations/sqlite/000028_directory_group_access.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(ddl)).Error)
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
	repository.ConfigureKnowledgeCurrentReadActor(service.repo, store)
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
	guard := access.NewNextcloudSourceLineageGuard(service.repo.(access.NextcloudLineageSnapshotLookup), kbs, nil, nil, groups, nil)
	verifier := &messageService{sourceLineageGuard: guard, kbService: kbs, knowService: service}
	repository.ConfigureKnowledgeInputHistoryVerifier(store, verifier)
	var ctx context.Context
	var user *types.User
	var key *types.TenantAPIKey
	if kind == "web" {
		user = &types.User{ID: "actual-user", TenantID: 7, Username: "actor", Email: "actor@example.test", PasswordHash: "unused-fixture", IsActive: true}
		require.NoError(t, db.Create(user).Error)
		require.NoError(t, db.Create(&types.TenantMember{UserID: user.ID, TenantID: 7, Role: types.TenantRoleContributor, Status: types.TenantMemberStatusActive}).Error)
		ctx = types.WithCaller(context.Background(), types.Caller{TenantID: 7, UserID: user.ID, Role: types.TenantRoleContributor})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: user.ID})
	} else {
		tenant := uint64(7)
		key = &types.TenantAPIKey{TenantID: &tenant, ScopeType: types.APIKeyScopeTenant, Name: "real scoped key", KeyHash: "key-fixture", Capabilities: types.StringArray{string(types.APIKeyCapabilityChat), string(types.APIKeyCapabilityIngest)}, KnowledgeBaseIDs: types.StringArray{"kb"}}
		require.NoError(t, db.Create(key).Error)
		ctx = types.WithCaller(context.Background(), types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
		ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: key.ID, ScopeType: key.ScopeType, KnowledgeBaseIDs: key.KnowledgeBaseIDs, Capabilities: key.Capabilities})
	}
	ctx = types.WithExecutionTenant(ctx, 7)
	session := &types.Session{TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, db.Create(session).Error)
	lineage, _ := types.NewCompleteSourceLineage()
	userMessage := &types.Message{SessionID: session.ID, RequestID: "actual-request", Role: "user", Content: "actual plain user question", IsCompleted: true, SourceLineage: lineage, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}}
	answer := &types.Message{SessionID: session.ID, RequestID: "actual-request", Role: "assistant", Content: "actual committed answer from plain text", IsCompleted: true, SourceLineage: lineage, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}}
	require.NoError(t, db.Create(userMessage).Error)
	require.NoError(t, db.Create(answer).Error)
	return service, store, db, ctx, answer, key, user
}

func TestKnowledgeHistoryActorActualAsynqRestoresCurrentPrincipal(t *testing.T) {
	for _, kind := range []string{"web", "api"} {
		t.Run(kind, func(t *testing.T) {
			service, store, db, ctx, answer, _, _ := historyActorIntegrationFixture(t, kind)
			input, err := store.CreateCommittedHistoryKnowledgeInput(ctx, types.CommittedHistoryKnowledgeInputRequest{TenantID: 7, KnowledgeBaseID: "kb", SessionID: answer.SessionID, AssistantMessageID: answer.ID})
			require.NoError(t, err)
			data, err := json.Marshal(types.DocumentProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: input.Knowledge.ID, Passages: input.Passages, OriginalInput: input.Proof})
			require.NoError(t, err)
			// A real task entry receives a new background context without JWT/role.
			require.NoError(t, service.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, data)))
			var count int64
			require.NoError(t, db.Model(&types.Chunk{}).Where("knowledge_id = ?", input.Knowledge.ID).Count(&count).Error)
			require.Positive(t, count)
			restored, err := store.RestoreKnowledgeBuildActor(context.Background(), input.Proof)
			require.NoError(t, err)
			require.Equal(t, types.CallerFromContext(ctx), types.CallerFromContext(restored))
			require.False(t, types.IsSystemAdminFromContext(restored))
		})
	}
}

func TestKnowledgeHistoryActorCurrentRevocationBeforeActualWorkerStopsAllWrites(t *testing.T) {
	for _, mode := range []string{"disabled", "membership", "role-viewer", "key-revoked", "key-expired", "key-scope", "key-tenant", "key-capability", "key-group-restricted"} {
		t.Run(mode, func(t *testing.T) {
			kind := "web"
			if mode[:3] == "key" {
				kind = "api"
			}
			service, store, db, ctx, answer, key, user := historyActorIntegrationFixture(t, kind)
			input, err := store.CreateCommittedHistoryKnowledgeInput(ctx, types.CommittedHistoryKnowledgeInputRequest{TenantID: 7, KnowledgeBaseID: "kb", SessionID: answer.SessionID, AssistantMessageID: answer.ID})
			require.NoError(t, err)
			switch mode {
			case "disabled":
				require.NoError(t, db.Model(user).Update("is_active", false).Error)
			case "membership":
				require.NoError(t, db.Model(&types.TenantMember{}).Where("user_id = ?", user.ID).Update("status", types.TenantMemberStatusSuspended).Error)
			case "role-viewer":
				require.NoError(t, db.Model(&types.TenantMember{}).Where("user_id = ?", user.ID).Update("role", types.TenantRoleViewer).Error)
			case "key-revoked":
				require.NoError(t, db.Model(key).Update("revoked_at", time.Now()).Error)
			case "key-expired":
				require.NoError(t, db.Model(key).Update("expires_at", time.Now().Add(-time.Hour)).Error)
			case "key-scope":
				require.NoError(t, db.Model(key).Update("knowledge_base_ids", types.StringArray{"other-kb"}).Error)
			case "key-tenant":
				require.NoError(t, db.Model(key).Update("tenant_id", uint64(8)).Error)
			case "key-capability":
				require.NoError(t, db.Model(key).Update("capabilities", types.StringArray{string(types.APIKeyCapabilityChat)}).Error)
			case "key-group-restricted":
				require.NoError(t, db.Create(&types.ResourceAccessPolicy{ID: "restricted-policy", TenantID: 7, ResourceType: types.GroupResourceTypeKnowledgeBase, ResourceID: "kb", Mode: types.ResourceAccessRestricted}).Error)
			}
			data, err := json.Marshal(types.DocumentProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: input.Knowledge.ID, Passages: input.Passages, OriginalInput: input.Proof})
			require.NoError(t, err)
			require.Error(t, service.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, data)))
			var count int64
			require.NoError(t, db.Model(&types.Chunk{}).Where("knowledge_id = ?", input.Knowledge.ID).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func seedHistoryActorGroup(t *testing.T, db *gorm.DB, user *types.User) {
	t.Helper()
	now := time.Now().UTC()
	require.NoError(t, db.Create(&types.Directory{ID: "actor-directory", Name: "fixture", Protocol: "ldap", Enabled: true, TLSMode: "ldaps", BaseDN: "dc=example,dc=test", UserBaseDN: "dc=example,dc=test", GroupBaseDN: "dc=example,dc=test", UserFilter: "(objectClass=user)", GroupFilter: "(objectClass=group)", ServiceAccountDN: "cn=fixture", PasswordCiphertext: "fixture", LastSuccessfulSyncAt: &now, StaleAfterSeconds: 900, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryIdentity{ID: "actor-identity", DirectoryID: "actor-directory", ObjectGUID: "user-guid", DN: "cn=actor", UserID: &user.ID, Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryGroup{ID: "actor-group", DirectoryID: "actor-directory", ObjectGUID: "group-guid", DN: "cn=group", DisplayName: "group", Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryGroupMembership{DirectoryID: "actor-directory", GroupID: "actor-group", IdentityID: "actor-identity", Source: types.DirectoryMembershipDirect, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.TenantGroupRoleGrant{ID: "actor-role", TenantID: 7, DirectoryGroupID: "actor-group", Role: types.TenantRoleContributor, Origin: types.GrantOriginManual}).Error)
	require.NoError(t, db.Create(&types.ResourceAccessPolicy{ID: "actor-policy", TenantID: 7, ResourceType: types.GroupResourceTypeKnowledgeBase, ResourceID: "kb", Mode: types.ResourceAccessRestricted}).Error)
	require.NoError(t, db.Create(&types.ResourceGroupGrant{ID: "actor-resource", TenantID: 7, ResourceType: types.GroupResourceTypeKnowledgeBase, ResourceID: "kb", DirectoryGroupID: "actor-group", Permission: types.ResourcePermissionEdit, Origin: types.GrantOriginManual}).Error)
}

func TestKnowledgeHistoryActorActualGroupOnlyMembershipAndRestrictedKB(t *testing.T) {
	for _, mode := range []string{"allowed", "group-membership-withdrawn", "resource-grant-withdrawn", "resource-read-only", "directory-disabled", "directory-stale"} {
		t.Run(mode, func(t *testing.T) {
			service, store, db, ctx, answer, _, user := historyActorIntegrationFixture(t, "web")
			seedHistoryActorGroup(t, db, user)
			require.NoError(t, db.Where("user_id = ?", user.ID).Delete(&types.TenantMember{}).Error)
			input, err := store.CreateCommittedHistoryKnowledgeInput(ctx, types.CommittedHistoryKnowledgeInputRequest{TenantID: 7, KnowledgeBaseID: "kb", SessionID: answer.SessionID, AssistantMessageID: answer.ID})
			require.NoError(t, err)
			switch mode {
			case "group-membership-withdrawn":
				require.NoError(t, db.Where("group_id = ?", "actor-group").Delete(&types.DirectoryGroupMembership{}).Error)
			case "resource-grant-withdrawn":
				require.NoError(t, db.Where("id = ?", "actor-resource").Delete(&types.ResourceGroupGrant{}).Error)
			case "resource-read-only":
				require.NoError(t, db.Model(&types.ResourceGroupGrant{}).Where("id = ?", "actor-resource").Update("permission", types.ResourcePermissionRead).Error)
			case "directory-disabled":
				require.NoError(t, db.Model(&types.Directory{}).Where("id = ?", "actor-directory").Update("enabled", false).Error)
			case "directory-stale":
				require.NoError(t, db.Model(&types.Directory{}).Where("id = ?", "actor-directory").Update("last_successful_sync_at", time.Now().Add(-time.Hour)).Error)
			}
			data, err := json.Marshal(types.DocumentProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: input.Knowledge.ID, Passages: input.Passages, OriginalInput: input.Proof})
			require.NoError(t, err)
			err = service.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, data))
			var count int64
			require.NoError(t, db.Model(&types.Chunk{}).Where("knowledge_id = ?", input.Knowledge.ID).Count(&count).Error)
			if mode == "allowed" {
				require.NoError(t, err)
				require.Positive(t, count)
			} else {
				require.Error(t, err)
				require.Zero(t, count)
			}
		})
	}
}

// This adapter keeps the real message batch verifier authoritative. The hook
// injects a committed revocation after its last parent callback.
type historyActorRevokingVerifier struct {
	interfaces.MessageSourceLineageVerifier
	batch interfaces.MessageSourceLineageBatchVerifier
	after func()
}

func (v historyActorRevokingVerifier) CheckMessagePublications(ctx context.Context, messages []*types.Message) error {
	if err := v.batch.CheckMessagePublications(ctx, messages); err != nil {
		return err
	}
	v.after()
	return nil
}

func TestKnowledgeHistoryActorRevocationDuringRealParentAuthorizationStopsWorker(t *testing.T) {
	for _, mode := range []string{"user-disabled", "key-revoked", "key-scope", "key-ingest", "group-edit"} {
		t.Run(mode, func(t *testing.T) {
			kind := "web"
			if mode != "user-disabled" && mode != "group-edit" {
				kind = "api"
			}
			service, store, db, ctx, answer, key, user := historyActorIntegrationFixture(t, kind)
			if mode == "group-edit" {
				seedHistoryActorGroup(t, db, user)
			}
			input, err := store.CreateCommittedHistoryKnowledgeInput(ctx, types.CommittedHistoryKnowledgeInputRequest{TenantID: 7, KnowledgeBaseID: "kb", SessionID: answer.SessionID, AssistantMessageID: answer.ID})
			require.NoError(t, err)
			groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
			kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
			guard := access.NewNextcloudSourceLineageGuard(service.repo.(access.NextcloudLineageSnapshotLookup), kbs, nil, nil, groups, nil)
			verifier := &messageService{sourceLineageGuard: guard, kbService: kbs, knowService: service}
			fired := false
			repository.ConfigureKnowledgeInputHistoryVerifier(store, historyActorRevokingVerifier{MessageSourceLineageVerifier: verifier, batch: verifier, after: func() {
				if fired {
					return
				}
				fired = true
				switch mode {
				case "user-disabled":
					require.NoError(t, db.Model(user).Update("is_active", false).Error)
				case "key-revoked":
					require.NoError(t, db.Model(key).Update("revoked_at", time.Now()).Error)
				case "key-scope":
					require.NoError(t, db.Model(key).Update("knowledge_base_ids", types.StringArray{"other-kb"}).Error)
				case "key-ingest":
					require.NoError(t, db.Model(key).Update("capabilities", types.StringArray{string(types.APIKeyCapabilityChat)}).Error)
				case "group-edit":
					require.NoError(t, db.Model(&types.ResourceGroupGrant{}).Where("id = ?", "actor-resource").Update("permission", types.ResourcePermissionRead).Error)
				}
			}})
			data, err := json.Marshal(types.DocumentProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: input.Knowledge.ID, Passages: input.Passages, OriginalInput: input.Proof})
			require.NoError(t, err)
			require.Error(t, service.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, data)))
			require.True(t, fired, "real parent batch authorization must have succeeded before revocation")
			var count int64
			require.NoError(t, db.Model(&types.Chunk{}).Where("knowledge_id = ?", input.Knowledge.ID).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func TestKnowledgeHistoryActorActualSharedWritePreservesOriginalCallerTenant(t *testing.T) {
	for _, mode := range []string{"allowed", "caller-membership-removed", "share-read-only", "org-member-removed", "proof-owner-swapped"} {
		t.Run(mode, func(t *testing.T) {
			service, store, db, ctx, answer, _, user := historyActorIntegrationFixture(t, "web")
			require.NoError(t, db.AutoMigrate(&types.Organization{}, &types.OrganizationTenantMember{}, &types.KnowledgeBaseShare{}))
			require.NoError(t, db.Create(&types.Organization{ID: "actor-org", Name: "actual sharing", OwnerID: user.ID, OwnerTenantID: 7}).Error)
			require.NoError(t, db.Create(&types.OrganizationTenantMember{ID: "actor-org-owner", OrganizationID: "actor-org", TenantID: 7, Role: types.OrgRoleAdmin}).Error)
			require.NoError(t, db.Create(&types.OrganizationTenantMember{ID: "actor-org-caller", OrganizationID: "actor-org", TenantID: 3, Role: types.OrgRoleEditor}).Error)
			require.NoError(t, db.Create(&types.KnowledgeBaseShare{ID: "actor-share", KnowledgeBaseID: "kb", OrganizationID: "actor-org", SourceTenantID: 7, SharedByUserID: user.ID, Permission: types.OrgRoleEditor}).Error)
			require.NoError(t, db.Where("user_id = ?", user.ID).Delete(&types.TenantMember{}).Error)
			require.NoError(t, db.Create(&types.TenantMember{UserID: user.ID, TenantID: 3, Role: types.TenantRoleContributor, Status: types.TenantMemberStatusActive}).Error)
			ctx = types.WithCaller(ctx, types.Caller{TenantID: 3, UserID: user.ID, Role: types.TenantRoleContributor})
			shares := NewKBShareService(repository.NewKBShareRepository(db), repository.NewOrganizationRepository(db), repository.NewKnowledgeBaseRepository(db), service.repo, nil, service.chunkRepo, nil)
			repository.ConfigureKnowledgeInputActorShares(store, shares)
			groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
			kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db), kbShareService: shares}
			guard := access.NewNextcloudSourceLineageGuard(service.repo.(access.NextcloudLineageSnapshotLookup), kbs, shares, nil, groups, nil)
			repository.ConfigureKnowledgeInputHistoryVerifier(store, &messageService{sourceLineageGuard: guard, kbService: kbs, knowService: service})
			input, err := store.CreateCommittedHistoryKnowledgeInput(ctx, types.CommittedHistoryKnowledgeInputRequest{TenantID: 7, KnowledgeBaseID: "kb", SessionID: answer.SessionID, AssistantMessageID: answer.ID})
			require.NoError(t, err)
			switch mode {
			case "caller-membership-removed":
				require.NoError(t, db.Where("user_id = ? AND tenant_id = 3", user.ID).Delete(&types.TenantMember{}).Error)
			case "share-read-only":
				require.NoError(t, db.Model(&types.KnowledgeBaseShare{}).Where("id = ?", "actor-share").Update("permission", types.OrgRoleViewer).Error)
			case "org-member-removed":
				require.NoError(t, db.Where("id = ?", "actor-org-caller").Delete(&types.OrganizationTenantMember{}).Error)
			case "proof-owner-swapped":
				input.Proof.TenantID = 3
			}
			data, err := json.Marshal(types.DocumentProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: input.Knowledge.ID, Passages: input.Passages, OriginalInput: input.Proof})
			require.NoError(t, err)
			err = service.ProcessDocument(context.Background(), asynq.NewTask(types.TypeDocumentProcess, data))
			var count int64
			require.NoError(t, db.Model(&types.Chunk{}).Where("knowledge_id = ?", input.Knowledge.ID).Count(&count).Error)
			if mode == "allowed" {
				require.NoError(t, err)
				require.Positive(t, count)
				restored, err := store.RestoreKnowledgeBuildActor(context.Background(), input.Proof)
				require.NoError(t, err)
				require.Equal(t, uint64(3), types.CallerFromContext(restored).TenantID)
				require.Equal(t, uint64(7), restored.Value(types.TenantIDContextKey))
				require.False(t, types.IsSystemAdminFromContext(restored))
			} else {
				require.Error(t, err)
				require.Zero(t, count)
			}
		})
	}
}
