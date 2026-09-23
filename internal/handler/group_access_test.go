package handler

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type groupAccessDirectoryStub struct {
	interfaces.DirectoryRepository
	directories []*types.Directory
	groups      map[string][]*types.DirectoryGroup
	memberships map[string][]*types.DirectoryGroupMembership
	identities  map[string]*types.DirectoryIdentity
}

func (s *groupAccessDirectoryStub) List(context.Context) ([]*types.Directory, error) {
	return s.directories, nil
}

func (s *groupAccessDirectoryStub) Get(_ context.Context, id string) (*types.Directory, error) {
	for _, directory := range s.directories {
		if directory.ID == id {
			return directory, nil
		}
	}
	return nil, errors.New("not found")
}

func (s *groupAccessDirectoryStub) ListGroups(
	_ context.Context,
	directoryID, _ string,
	offset, limit int,
) ([]*types.DirectoryGroup, error) {
	rows := s.groups[directoryID]
	if offset >= len(rows) {
		return nil, nil
	}
	end := offset + limit
	if end > len(rows) {
		end = len(rows)
	}
	return rows[offset:end], nil
}

func (s *groupAccessDirectoryStub) ListGroupMemberships(
	_ context.Context,
	groupID string,
) ([]*types.DirectoryGroupMembership, error) {
	return s.memberships[groupID], nil
}

func (s *groupAccessDirectoryStub) ListGroupEdges(
	context.Context,
	string,
) ([]*types.DirectoryGroupEdge, error) {
	return nil, nil
}

func (s *groupAccessDirectoryStub) GetIdentity(
	_ context.Context,
	id string,
) (*types.DirectoryIdentity, error) {
	return s.identities[id], nil
}

type groupAccessRepoStub struct {
	interfaces.GroupAccessRepository
	tenantGrants   []*types.TenantGroupRoleGrant
	resourceGrants []*types.ResourceGroupGrant
	policy         *types.ResourceAccessPolicy
}

func (s *groupAccessRepoStub) ListTenantGroupRoleGrants(
	context.Context,
	uint64,
) ([]*types.TenantGroupRoleGrant, error) {
	return s.tenantGrants, nil
}

func (s *groupAccessRepoStub) ListResourceGroupGrants(
	context.Context,
	uint64,
	types.ResourceType,
	string,
) ([]*types.ResourceGroupGrant, error) {
	return s.resourceGrants, nil
}

func (s *groupAccessRepoStub) GetResourceAccessPolicy(
	context.Context,
	uint64,
	types.ResourceType,
	string,
) (*types.ResourceAccessPolicy, error) {
	return s.policy, nil
}

type groupAccessServiceStub struct {
	interfaces.GroupAccessService
	deleted []*types.ResourceGroupGrant
	upserts []*types.ResourceGroupGrant
	policy  *types.ResourceAccessPolicy
}

func (s *groupAccessServiceStub) DeleteResourceGroupGrant(
	_ context.Context,
	tenantID uint64,
	resourceType types.ResourceType,
	resourceID, groupID string,
	permission types.ResourcePermission,
	origin types.GrantOrigin,
) error {
	s.deleted = append(s.deleted, &types.ResourceGroupGrant{
		TenantID: tenantID, ResourceType: resourceType, ResourceID: resourceID,
		DirectoryGroupID: groupID, Permission: permission, Origin: origin,
	})
	return nil
}

func (s *groupAccessServiceStub) UpsertResourceGroupGrant(
	_ context.Context,
	grant *types.ResourceGroupGrant,
) error {
	cloned := *grant
	s.upserts = append(s.upserts, &cloned)
	return nil
}

func (s *groupAccessServiceStub) SetResourceAccessPolicy(
	_ context.Context,
	policy *types.ResourceAccessPolicy,
) error {
	cloned := *policy
	s.policy = &cloned
	return nil
}

type groupAccessKBRepoStub struct {
	interfaces.KnowledgeBaseRepository
	kb  *types.KnowledgeBase
	err error
}

func (s *groupAccessKBRepoStub) GetKnowledgeBaseByIDAndTenant(
	_ context.Context,
	id string,
	tenantID uint64,
) (*types.KnowledgeBase, error) {
	if s.err != nil || s.kb == nil || s.kb.ID != id || s.kb.TenantID != tenantID {
		if s.err != nil {
			return nil, s.err
		}
		return nil, errors.New("not found")
	}
	return s.kb, nil
}

func groupAccessTestContext(
	method, path, body string,
	tenantID uint64,
) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, path, bytes.NewBufferString(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	requestCtx := types.WithExecutionTenant(ctx.Request.Context(), tenantID)
	requestCtx = types.WithCaller(
		requestCtx,
		types.Caller{TenantID: tenantID, UserID: "admin", Role: types.TenantRoleAdmin},
	)
	ctx.Request = ctx.Request.WithContext(requestCtx)
	return ctx, recorder
}

func TestGroupAccessRejectsOwnerRoleForDirectoryGroup(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &GroupAccessHandler{}
	ctx, _ := groupAccessTestContext(
		"POST",
		"/tenants/7/directory-groups",
		`{"directory_id":"d1","directory_group_id":"g1","role":"owner"}`,
		7,
	)
	ctx.Params = gin.Params{{Key: "id", Value: "7"}}

	handler.AddTenantDirectoryGroup(ctx)

	if len(ctx.Errors) != 1 {
		t.Fatalf("expected one validation error, got %d", len(ctx.Errors))
	}
	appErr, ok := ctx.Errors[0].Err.(*apperrors.AppError)
	if !ok || appErr.HTTPCode != 400 {
		t.Fatalf("expected HTTP 400 AppError, got %#v", ctx.Errors[0].Err)
	}
}

func TestGroupAccessRejectsTenantPathMismatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := &GroupAccessHandler{}
	ctx, _ := groupAccessTestContext("GET", "/tenants/8/directory-groups", "", 7)
	ctx.Params = gin.Params{{Key: "id", Value: "8"}}

	handler.ListTenantDirectoryGroups(ctx)

	if len(ctx.Errors) != 1 {
		t.Fatalf("expected one authorization error, got %d", len(ctx.Errors))
	}
	appErr, ok := ctx.Errors[0].Err.(*apperrors.AppError)
	if !ok || appErr.HTTPCode != 403 {
		t.Fatalf("expected HTTP 403 AppError, got %#v", ctx.Errors[0].Err)
	}
}

func TestGroupAccessResourceUpdatePreservesDirectoryOrigin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC()
	directoryRepo := &groupAccessDirectoryStub{
		directories: []*types.Directory{{ID: "d1", Enabled: true, LastSuccessfulSyncAt: &now}},
		groups: map[string][]*types.DirectoryGroup{"d1": {
			{
				ID:          "g-new",
				DirectoryID: "d1",
				DisplayName: "New",
				Status:      types.DirectoryObjectActive,
			},
			{
				ID:          "g-manual-old",
				DirectoryID: "d1",
				DisplayName: "Old",
				Status:      types.DirectoryObjectActive,
			},
			{
				ID:          "g-directory",
				DirectoryID: "d1",
				DisplayName: "Synced",
				Status:      types.DirectoryObjectActive,
			},
		}},
		memberships: map[string][]*types.DirectoryGroupMembership{},
	}
	groupRepo := &groupAccessRepoStub{
		resourceGrants: []*types.ResourceGroupGrant{
			{
				ID:               "manual",
				TenantID:         7,
				ResourceType:     types.GroupResourceTypeKnowledgeBase,
				ResourceID:       "kb1",
				DirectoryGroupID: "g-manual-old",
				Permission:       types.ResourcePermissionRead,
				Origin:           types.GrantOriginManual,
			},
			{
				ID:               "directory",
				TenantID:         7,
				ResourceType:     types.GroupResourceTypeKnowledgeBase,
				ResourceID:       "kb1",
				DirectoryGroupID: "g-directory",
				Permission:       types.ResourcePermissionRead,
				Origin:           types.GrantOriginDirectory,
			},
		},
	}
	access := &groupAccessServiceStub{}
	handler := NewGroupAccessHandler(
		directoryRepo,
		groupRepo,
		access,
		&groupAccessKBRepoStub{kb: &types.KnowledgeBase{ID: "kb1", TenantID: 7}},
		nil,
		nil,
	)
	ctx, recorder := groupAccessTestContext(
		"PUT",
		"/group-access/knowledge_base/kb1",
		`{"mode":"restricted","grants":[{"directory_id":"d1","directory_group_id":"g-new","permission":"read"}]}`,
		7,
	)
	ctx.Params = gin.Params{
		{Key: "resource_type", Value: "knowledge_base"},
		{Key: "resource_id", Value: "kb1"},
	}

	handler.UpdateResourceGroupAccess(ctx)

	if len(ctx.Errors) != 0 {
		t.Fatalf("unexpected handler errors: %v", ctx.Errors)
	}
	if recorder.Code != 200 {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if len(access.deleted) != 1 || access.deleted[0].DirectoryGroupID != "g-manual-old" ||
		access.deleted[0].Origin != types.GrantOriginManual {
		t.Fatalf("expected only the stale manual grant to be deleted, got %#v", access.deleted)
	}
	if len(access.upserts) != 1 || access.upserts[0].DirectoryGroupID != "g-new" ||
		access.upserts[0].Origin != types.GrantOriginManual {
		t.Fatalf("expected one new manual grant, got %#v", access.upserts)
	}
	if access.policy == nil || access.policy.Mode != types.ResourceAccessRestricted {
		t.Fatalf("expected restricted policy to be committed last, got %#v", access.policy)
	}
}
