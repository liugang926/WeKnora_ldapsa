package handler

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type positiveGroupRepo struct {
	interfaces.GroupAccessRepository
	mode       types.ResourceAccessMode
	permission types.ResourcePermission
	fresh      bool
}

type positiveKBService struct{ *stubKBService }

func (s positiveKBService) GetKnowledgeBaseByIDOnly(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	return s.get(ctx, id)
}

func (r *positiveGroupRepo) GetResourceAccessPolicy(
	context.Context, uint64, types.ResourceType, string,
) (*types.ResourceAccessPolicy, error) {
	return &types.ResourceAccessPolicy{Mode: r.mode}, nil
}

func (*positiveGroupRepo) GetDirectTenantRole(context.Context, string, uint64) (*types.TenantRole, error) {
	role := types.TenantRoleViewer
	return &role, nil
}

func (*positiveGroupRepo) ListGroupRoleMatches(context.Context, string, uint64) ([]types.GroupRoleMatch, error) {
	return nil, nil
}

func (r *positiveGroupRepo) ListResourceGroupMatches(
	context.Context, string, uint64, types.ResourceType, string,
) ([]types.ResourceGroupMatch, error) {
	now := time.Now().UTC()
	return []types.ResourceGroupMatch{{
		Permission: r.permission, DirectoryEnabled: r.fresh, LastSuccessfulSyncAt: &now,
	}}, nil
}

func positiveGroupRouter(role types.TenantRole, callerTenant uint64) *gin.Engine {
	r := gin.New()
	r.Use(middleware.ErrorHandler(), func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.UserIDContextKey, "user")
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
		ctx = types.WithCaller(ctx, types.Caller{TenantID: callerTenant, UserID: "user", Role: role})
		c.Request = c.Request.WithContext(types.WithExecutionTenant(ctx, callerTenant))
		c.Set(types.TenantIDContextKey.String(), callerTenant)
		c.Next()
	})
	return r
}

func TestRestrictedGroupEditReachesHTTPAndRealKnowledgeService(t *testing.T) {
	for _, tc := range []struct {
		name       string
		role       types.TenantRole
		permission types.ResourcePermission
		mode       types.ResourceAccessMode
		fresh      bool
		tenant     uint64
		status     int
	}{
		{
			"viewer edit", types.TenantRoleViewer, types.ResourcePermissionEdit,
			types.ResourceAccessRestricted, true, 7, 200,
		},
		{
			"contributor edit", types.TenantRoleContributor, types.ResourcePermissionEdit,
			types.ResourceAccessRestricted, true, 7, 200,
		},
		{
			"viewer read", types.TenantRoleViewer, types.ResourcePermissionRead,
			types.ResourceAccessRestricted, true, 7, 403,
		},
		{
			"stale edit", types.TenantRoleViewer, types.ResourcePermissionEdit,
			types.ResourceAccessRestricted, false, 7, 403,
		},
		{
			"cross space", types.TenantRoleViewer, types.ResourcePermissionEdit,
			types.ResourceAccessRestricted, true, 8, 403,
		},
		{
			"inherit viewer", types.TenantRoleViewer, types.ResourcePermissionEdit,
			types.ResourceAccessInherit, true, 7, 403,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "knowledge.db")), &gorm.Config{})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
			require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.KnowledgeTag{}, &types.KnowledgeTagRelation{}))
			require.NoError(t, db.Create(&types.Knowledge{
				ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Title: "before",
			}).Error)
			kb := positiveKBService{admissionKB("another-creator")}
			knowledgeRepo := repository.NewKnowledgeRepository(db)
			kg, err := service.NewKnowledgeService(transferHandlerConfig(), knowledgeRepo, nil, kb,
				nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
				nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			require.NoError(t, err)
			groups := service.NewGroupAccessService(&positiveGroupRepo{
				mode: tc.mode, permission: tc.permission, fresh: tc.fresh,
			})
			h := &KnowledgeHandler{cfg: transferHandlerConfig(), kbService: kb, kgService: kg, groupAccess: groups}
			r := positiveGroupRouter(tc.role, tc.tenant)
			resolver := middleware.KBIDFromKnowledgeIDParam("id", kg)
			r.PUT("/knowledge/:id",
				middleware.RequireOwnershipOrRoleWithGroupEdit(types.TenantRoleAdmin,
					func(*gin.Context) (string, error) { return "another-creator", nil }, h.cfg,
					types.GroupResourceTypeKnowledgeBase, resolver, groups),
				middleware.RequireKBAccess(resolver, types.OrgRoleEditor, kb, nil, nil, h.cfg, groups),
				h.UpdateKnowledge)
			r.POST("/knowledge/folder", middleware.RequireRole(types.TenantRoleViewer, h.cfg), h.MoveKnowledgeToFolder)
			r.GET("/knowledge/:id/download",
				middleware.RequireRoleWithGroupEdit(types.TenantRoleContributor, h.cfg,
					types.GroupResourceTypeKnowledgeBase, resolver, groups),
				middleware.RequireKBAccess(resolver, types.OrgRoleEditor, kb, nil, nil, h.cfg, groups),
				func(c *gin.Context) {
					require.NoError(t, access.RequireKBWrite(c.Request.Context(),
						&types.KnowledgeBase{ID: "kb", TenantID: 7}))
					require.Equal(t, tc.role, types.CallerFromContext(c.Request.Context()).Role)
					c.Status(http.StatusOK)
				})
			w := mutationRequest(r, http.MethodPut, "/knowledge/doc", `{"title":"after"}`)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			stored, err := knowledgeRepo.GetKnowledgeByID(context.Background(), 7, "doc")
			require.NoError(t, err)
			if tc.status == 200 {
				require.Equal(t, "after", stored.Title)
			} else {
				require.Equal(t, "before", stored.Title)
			}
			w = mutationRequest(r, http.MethodPost, "/knowledge/folder",
				`{"kb_id":"kb","knowledge_ids":["doc"],"folder_path":"new-folder"}`)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			stored, err = knowledgeRepo.GetKnowledgeByID(context.Background(), 7, "doc")
			require.NoError(t, err)
			if tc.status == 200 {
				require.Equal(t, "new-folder", stored.FolderPath)
			} else {
				require.Empty(t, stored.FolderPath)
			}
			w = mutationRequest(r, http.MethodGet, "/knowledge/doc/download", "")
			require.Equal(t, tc.status, w.Code, w.Body.String())
		})
	}
}

func TestRestrictedViewerGroupUseAndEditReachRealAgentService(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "agent.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(&types.CustomAgent{}))
	require.NoError(t, db.Create(&types.CustomAgent{
		ID: "agent", TenantID: 7, CreatedBy: "another-creator", Name: "before",
	}).Error)
	repo := repository.NewCustomAgentRepository(db)
	agents := service.NewCustomAgentService(repo, nil, nil, nil, nil, nil, nil, nil)
	groupRepo := &positiveGroupRepo{
		mode: types.ResourceAccessRestricted, permission: types.ResourcePermissionUse, fresh: true,
	}
	groups := service.NewGroupAccessService(groupRepo)
	h := &CustomAgentHandler{service: agents, groupAccess: groups}
	r := positiveGroupRouter(types.TenantRoleViewer, 7)
	resolver := func(c *gin.Context) (string, error) { return c.Param("id"), nil }
	r.GET("/agents/:id", middleware.RequireGroupResourceAccess(
		types.GroupResourceTypeAgent, types.ResourceActionUse, resolver, groups), h.GetAgent)
	r.PUT("/agents/:id", middleware.RequireOwnershipOrRoleWithGroupEdit(types.TenantRoleAdmin,
		func(*gin.Context) (string, error) { return "another-creator", nil }, transferHandlerConfig(),
		types.GroupResourceTypeAgent, resolver, groups),
		middleware.RequireGroupResourceAccess(types.GroupResourceTypeAgent, types.ResourceActionEdit, resolver, groups),
		h.UpdateAgent)
	r.DELETE("/agents/:id", middleware.RequireOwnershipOrRole(types.TenantRoleAdmin,
		func(*gin.Context) (string, error) { return "another-creator", nil }, transferHandlerConfig()), h.DeleteAgent)
	w := mutationRequest(r, http.MethodGet, "/agents/agent", "")
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"group_access_permission":"use"`)
	w = mutationRequest(r, http.MethodPut, "/agents/agent", `{"name":"after","config":{}}`)
	require.Equal(t, 403, w.Code, w.Body.String())
	groupRepo.permission = types.ResourcePermissionEdit
	w = mutationRequest(r, http.MethodPut, "/agents/agent", `{"name":"after","config":{}}`)
	require.Equal(t, 200, w.Code, w.Body.String())
	stored, err := repo.GetAgentByID(context.Background(), "agent", 7)
	require.NoError(t, err)
	require.Equal(t, "after", stored.Name)
	w = mutationRequest(r, http.MethodDelete, "/agents/agent", "")
	require.Equal(t, 403, w.Code, w.Body.String())
	_, err = repo.GetAgentByID(context.Background(), "agent", 7)
	require.NoError(t, err)
}

func TestGroupEditProjectionSeparatesReadEditAndManagement(t *testing.T) {
	groups := service.NewGroupAccessService(&positiveGroupRepo{
		mode: types.ResourceAccessRestricted, permission: types.ResourcePermissionEdit, fresh: true,
	})
	ctx := types.WithCaller(context.Background(), types.Caller{
		TenantID: 7, UserID: "user", Role: types.TenantRoleViewer,
	})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
	require.Equal(t, types.ResourcePermissionEdit,
		projectedGroupPermission(ctx, groups, 7, types.GroupResourceTypeKnowledgeBase, "kb"))
	permission, err := groups.EffectivePermission(ctx, 7,
		types.GroupResourceTypeKnowledgeBase, "kb", types.ResourceActionManage, time.Now().UTC())
	require.NoError(t, err)
	require.False(t, permission.Allowed)
}
