package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

func TestHTTPMatchedImageRequiresCurrentShareAndStorage(t *testing.T) {
	t.Setenv("APP_EXTERNAL_URL", "https://resources.example.test")
	_, db, store, _, _, _ := collectionHTTPFixture(t)
	require.NoError(t, db.AutoMigrate(
		&types.Tenant{}, &types.Organization{}, &types.OrganizationTenantMember{},
		&types.KnowledgeBaseShare{}, &types.StorageBackend{}, &types.StoredResource{},
		&types.ResourceBinding{}, &types.ResourceAccessGrant{},
	))
	for _, id := range []uint64{7, 9} {
		tenant := types.Tenant{ID: id, Name: "Workspace"}
		require.NoError(t, db.FirstOrCreate(&tenant, id).Error)
	}
	require.NoError(t, db.Create(&types.Organization{ID: "sign-org", Name: "example", OwnerTenantID: 9}).Error)
	for _, id := range []uint64{7, 9} {
		require.NoError(t, db.Create(&types.OrganizationTenantMember{
			ID: strconv.FormatUint(id, 10), OrganizationID: "sign-org", TenantID: id, Role: types.OrgRoleViewer,
		}).Error)
	}
	share := types.KnowledgeBaseShare{
		ID: "sign-share", KnowledgeBaseID: "shared-kb",
		OrganizationID: "sign-org", SourceTenantID: 9, Permission: types.OrgRoleViewer,
	}
	require.NoError(t, db.Create(&share).Error)
	ctx := collectionHumanInputContext(7)
	catalog := service.NewResourceCatalog(repository.NewResourceRepository(db))
	repo := repository.NewStorageBackendRepository(db)
	tenants := service.NewTenantService(repository.NewTenantRepository(db), repo, nil)
	storage := service.NewStorageBackendService(repo, db, catalog)
	backend := &types.StorageBackend{ID: "sign-local", TenantID: 9, Name: "local", Provider: "local"}
	require.NoError(t, repo.Create(ctx, backend))
	ref, err := catalog.Register(ctx, 9, "storage://sign-local/local://9/exports/clip.png",
		interfaces.ResourceRegistration{Kind: "image", OriginalName: "clip.png"})
	require.NoError(t, err)
	err = catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "shared-match", types.ResourceRelationExtractedImage)
	require.NoError(t, err)
	shares := service.NewKBShareService(repository.NewKBShareRepository(db), repository.NewOrganizationRepository(db),
		repository.NewKnowledgeBaseRepository(db), nil, nil, nil, nil)
	groups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	guard := access.NewNextcloudSourceLineageGuard(nil, collectionHTTPKB{db: db}, shares, nil, groups, nil, store)
	factory := storageurl.NewHTTPResourceResolver(catalog, tenants, guard)
	refs := types.References{{MatchedImages: []types.MatchedImage{{URL: ref}}}}
	h := &MessageHandler{ResourceURLResolver: factory, StorageResolver: storage, MessageService: &stubMessageService{
		getRecent: func(context.Context, string, int) ([]*types.Message, error) {
			return []*types.Message{{KnowledgeReferences: refs}}, nil
		},
	}}
	router := gin.New()
	router.GET("/messages/:session_id/load", h.LoadMessages)
	load := func() string {
		response := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/messages/s/load?resource_urls=public", nil)
		router.ServeHTTP(response, req.WithContext(ctx))
		require.Equal(t, http.StatusOK, response.Code)
		return response.Body.String()
	}
	require.Contains(t, load(), "https://resources.example.test/r/")
	w := storageurl.NewRequestRewriter(ctx, storageurl.ModePublic, nil, storage, factory)
	require.Contains(t, w.CopyReferences(ctx, refs)[0].MatchedImages[0].URL, "https://resources.example.test/r/")
	midSign := &revokeAtSigning{StorageBackendResolver: storage, revoke: func() {
		require.NoError(t, db.Delete(&share).Error)
	}}
	concurrent := storageurl.NewRequestRewriter(ctx, storageurl.ModePublic, nil, midSign, factory)
	require.Equal(t, ref, concurrent.CopyReferences(ctx, refs)[0].MatchedImages[0].URL)
	require.Equal(t, ref, w.CopyReferences(ctx, refs)[0].MatchedImages[0].URL)
	require.NotContains(t, load(), "/r/")
	require.NoError(t, db.Create(&types.KnowledgeBaseShare{
		ID: "fresh-grant", KnowledgeBaseID: "shared-kb",
		OrganizationID: "sign-org", SourceTenantID: 9, Permission: types.OrgRoleViewer,
	}).Error)
	require.NoError(t, db.Model(&types.StorageBackend{}).Where("id = ?", backend.ID).
		Update("status", types.StorageBackendStatusDisabled).Error)
	require.Equal(t, ref, w.CopyReferences(ctx, refs)[0].MatchedImages[0].URL)
}

// Wrap the real backend only to schedule revocation between issuing and
// releasing a URL. All permission, catalog and storage decisions still hit SQL.
type revokeAtSigning struct {
	interfaces.StorageBackendResolver
	revoke func()
}

func (r *revokeAtSigning) ResolveFileService(
	ctx context.Context, tenant *types.Tenant, backendID, provider, base string,
) (interfaces.FileService, string, error) {
	files, resolved, err := r.StorageBackendResolver.ResolveFileService(ctx, tenant, backendID, provider, base)
	return revokeAfterFileURL{FileService: files, revoke: r.revoke}, resolved, err
}

type revokeAfterFileURL struct {
	interfaces.FileService
	revoke func()
}

func (s revokeAfterFileURL) GetFileURL(ctx context.Context, ref string) (string, error) {
	url, err := s.FileService.GetFileURL(ctx, ref)
	s.revoke()
	return url, err
}
