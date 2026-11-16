package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/agent/operations"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestScopedTagAssignmentCommitsOnlyAuthorizedBatch(t *testing.T) {
	h, db, store, _, _, _ := collectionHTTPFixture(t)
	now := time.Now().UTC()
	user := "user"
	require.NoError(t, db.Model(&types.TenantMember{}).Where("tenant_id = ? AND user_id = ?", 7, user).
		Update("role", types.TenantRoleViewer).Error)
	for _, row := range []any{
		&types.Directory{ID: "directory", Enabled: true, LastSuccessfulSyncAt: &now},
		&types.DirectoryIdentity{
			ID: "identity", DirectoryID: "directory", UserID: &user, Status: types.DirectoryObjectActive,
		},
		&types.DirectoryGroup{ID: "editors", DirectoryID: "directory", Status: types.DirectoryObjectActive},
		&types.DirectoryGroupMembership{DirectoryID: "directory", IdentityID: "identity", GroupID: "editors"},
		&types.ResourceAccessPolicy{
			ID: "restricted", TenantID: 7, ResourceType: types.GroupResourceTypeKnowledgeBase,
			ResourceID: "kb", Mode: types.ResourceAccessRestricted,
		},
		&types.ResourceGroupGrant{
			ID: "edit", TenantID: 7, ResourceType: types.GroupResourceTypeKnowledgeBase, ResourceID: "kb",
			DirectoryGroupID: "editors", Permission: types.ResourcePermissionEdit,
		},
		&types.KnowledgeBase{ID: "other-kb", TenantID: 7, Name: "other"},
		&types.Knowledge{ID: "other-doc", TenantID: 7, KnowledgeBaseID: "other-kb", Title: "other"},
		&types.KnowledgeTag{ID: "label", TenantID: 7, KnowledgeBaseID: "kb", Name: "label"},
	} {
		require.NoError(t, db.Create(row).Error)
	}
	knowledge := repository.NewKnowledgeRepository(db)
	tags := repository.NewKnowledgeTagRepository(db)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		authority := repository.NewKnowledgeInputProvenanceStore(tx)
		groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(tx))
		repository.ConfigureKnowledgeInputActorGroupAccess(authority, groups)
		return authority
	}
	repository.ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
	var err error
	h.cfg = transferHandlerConfig()
	h.groupAccess = service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	h.kgService, err = service.NewKnowledgeService(h.cfg, knowledge, nil, h.kbService, nil, nil, nil, nil, tags, nil,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	op, visible := operations.Builtin().For(operations.Access{
		Role: types.TenantRoleViewer, EnforceRole: true, Level: types.AssistantAccessConfirm,
	}).Get("knowledge.set_tags")
	require.True(t, visible)
	r := positiveGroupRouter(types.TenantRoleViewer, 7)
	r.Handle(op.Method, op.Route, h.UpdateKnowledgeTagBatch)
	dispatch := operations.NewHandlerDispatcher()
	dispatch.Bind(r)
	call := func(ids, tagIDs []string) int {
		t.Helper()
		documents, err := json.Marshal(ids)
		require.NoError(t, err)
		labels, err := json.Marshal(tagIDs)
		require.NoError(t, err)
		request, err := op.Build(map[string]json.RawMessage{
			"knowledge_base_id": json.RawMessage(`"kb"`), "knowledge_ids": documents, "tag_ids": labels,
		})
		require.NoError(t, err)
		identity := operations.Identity{UserID: user, TenantID: 7}
		response, err := dispatch.Dispatch(context.Background(), identity, request)
		require.NoError(t, err)
		return response.Status
	}
	countLabels := func() int64 {
		t.Helper()
		var count int64
		require.NoError(t, db.Model(&types.KnowledgeTagRelation{}).Where("tag_id = ?", "label").Count(&count).Error)
		return count
	}

	ids := []string{"excluded", "match"}
	label := []string{"label"}
	require.Equal(t, http.StatusOK, call(ids, label))
	require.EqualValues(t, 2, countLabels())
	countOrigins := func() int64 {
		t.Helper()
		var count int64
		require.NoError(t, db.Table("knowledge_display_input_origins").
			Where("kind = 'relations' AND row_id IN ?", ids).Count(&count).Error)
		return count
	}
	require.EqualValues(t, 2, countOrigins())

	mixedStatus := call([]string{"match", "other-doc"}, []string{})
	require.Contains(t, []int{http.StatusForbidden, http.StatusNotFound}, mixedStatus)
	require.EqualValues(t, 2, countLabels())
	// Downgrade the grant after the first row changes, within the same SQL
	// transaction. The second authorization must reject and roll back both.
	require.NoError(t, db.Exec(`CREATE TRIGGER revoke_batch AFTER DELETE ON knowledge_tag_relations
		WHEN OLD.knowledge_id = 'excluded' BEGIN
		UPDATE resource_group_grants SET permission = 'read' WHERE id = 'edit';
		END`).Error)
	require.Equal(t, http.StatusForbidden, call(ids, []string{}))
	require.EqualValues(t, 2, countLabels())
	require.EqualValues(t, 2, countOrigins())
	require.NoError(t, db.Exec("DROP TRIGGER revoke_batch").Error)
	require.Equal(t, http.StatusOK, call(ids, []string{}))
	require.Zero(t, countLabels())

	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 7, UserID: user, Role: types.TenantRoleViewer})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: user})
	ctx = types.WithExecutionTenant(ctx, 7)
	_, err = factory(db).AuthorizeCurrentKnowledgeInputWrite(ctx, &types.KnowledgeBase{ID: "kb", TenantID: 7})
	require.ErrorIs(t, err, access.ErrForbidden)
	require.NoError(t, db.Delete(&types.ResourceGroupGrant{}, "id = ?", "edit").Error)
	require.Equal(t, http.StatusForbidden, call(ids, label))
	require.Zero(t, countLabels())
}
