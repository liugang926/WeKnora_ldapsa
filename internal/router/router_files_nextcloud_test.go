package router

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestPublicResourceGrantRejectsNextcloudAndDeletedOwnersBeforeStorage(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner *types.Knowledge
	}{
		{"Nextcloud", &types.Knowledge{Channel: types.ConnectorTypeNextcloud}},
		{"deleted", &types.Knowledge{DeletedAt: gorm.DeletedAt{Valid: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opened := false
			catalog := &stubResourceCatalog{
				resource: &types.StoredResource{
					ID: "resource-1", Handle: "AbCdEfGhIjKlMnOpQrStUv",
					TenantID: 7, PhysicalPath: "local://7/doc/source.pdf",
				},
				knowledgeOwners: func(context.Context, uint64, string) ([]*types.Knowledge, bool, error) {
					return []*types.Knowledge{tc.owner}, true, nil
				},
			}
			r := gin.New()
			serveResourceGrants(r, catalog,
				&stubTenantService{get: func(context.Context, uint64) (*types.Tenant, error) {
					t.Fatal("tenant must not be fetched")
					return nil, nil
				}},
				&stubFileService{getFile: func(context.Context, string) (io.ReadCloser, error) {
					opened = true
					return io.NopCloser(strings.NewReader("secret")), nil
				}}, nil, nil, access.NewNextcloudPublicationGuard(nil, nil))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/r/token", nil))
			if w.Code != http.StatusForbidden || opened || strings.Contains(w.Body.String(), "secret") {
				t.Fatalf("source file escaped: status=%d opened=%v body=%q", w.Code, opened, w.Body.String())
			}
		})
	}
}

func TestGuardedFileRouteRejectsUnrecognizedRawPath(t *testing.T) {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantInfoContextKey, &types.Tenant{ID: 7})
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	opened := false
	r.GET("/files", newFileServeHandlerWithGroupAccess(
		&stubFileService{getFile: func(context.Context, string) (io.ReadCloser, error) {
			opened = true
			return io.NopCloser(strings.NewReader("secret")), nil
		}}, nil,
		&stubResourceCatalog{knowledgeOwners: func(context.Context, uint64, string) ([]*types.Knowledge, bool, error) {
			return nil, false, nil
		}}, nil, access.NewNextcloudPublicationGuard(nil, nil),
	))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/files?file_path=local://7/doc/source.pdf", nil))
	if w.Code != http.StatusForbidden || opened {
		t.Fatalf("unrecognized raw file escaped: status=%d opened=%v", w.Code, opened)
	}
}

func TestGuardedFileRoutePreservesUnrelatedRegisteredResource(t *testing.T) {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantInfoContextKey, &types.Tenant{ID: 7})
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.GET("/files", newFileServeHandlerWithGroupAccess(
		&stubFileService{getFile: func(context.Context, string) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("ordinary")), nil
		}}, nil,
		&stubResourceCatalog{
			resource: &types.StoredResource{TenantID: 7, PhysicalPath: "local://7/exports/chart.png"},
			knowledgeOwners: func(context.Context, uint64, string) ([]*types.Knowledge, bool, error) {
				return []*types.Knowledge{{Channel: "web"}}, true, nil
			},
		}, nil, access.NewNextcloudPublicationGuard(nil, nil),
	))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
		"/files?file_path=local://7/exports/chart.png", nil))
	if w.Code != http.StatusOK || w.Body.String() != "ordinary" {
		t.Fatalf("ordinary registered resource failed: status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestPresignedFileRejectsNextcloudAfterSignatureCheck(t *testing.T) {
	t.Setenv("SYSTEM_SIGNING_KEY", "")
	t.Setenv("SYSTEM_AES_KEY", "weknora-test-aes-key-32bytes!!!")
	path := "local://7/doc/source.pdf"
	catalog := &stubResourceCatalog{
		resource: &types.StoredResource{ID: "resource-1", TenantID: 7, PhysicalPath: path},
		knowledgeOwners: func(context.Context, uint64, string) ([]*types.Knowledge, bool, error) {
			return []*types.Knowledge{{Channel: types.ConnectorTypeNextcloud}}, true, nil
		},
	}
	r := gin.New()
	r.GET("/api/v1/files/presigned", presignedFileHandler(
		&stubTenantService{get: func(context.Context, uint64) (*types.Tenant, error) {
			t.Fatal("tenant must not be fetched for Nextcloud presigned URL")
			return nil, nil
		}}, t.TempDir(), nil, catalog, nil, access.NewNextcloudPublicationGuard(nil, nil),
	))
	signed, err := secutils.SignFileURL("https://weknora.example.com", path, 7, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(signed)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/files/presigned?"+u.RawQuery, nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("presigned Nextcloud file escaped: status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestNextcloudResourceTombstoneRejectsFilesGrantAndPresignedAfterHardDelete(t *testing.T) {
	t.Setenv("SYSTEM_SIGNING_KEY", "")
	t.Setenv("SYSTEM_AES_KEY", "weknora-test-aes-key-32bytes!!!")
	path := "local://7/doc/source.pdf"
	opened := false
	catalog := &stubResourceCatalog{
		resource: &types.StoredResource{
			ID: "resource-1", Handle: "AbCdEfGhIjKlMnOpQrStUv", TenantID: 7,
			PhysicalPath: path, SourceProvenance: types.ResourceProvenanceNextcloud,
		},
		knowledgeOwners: func(context.Context, uint64, string) ([]*types.Knowledge, bool, error) {
			return nil, true, nil // knowledge and source binding were hard deleted
		},
	}
	files := &stubFileService{getFile: func(context.Context, string) (io.ReadCloser, error) {
		opened = true
		return io.NopCloser(strings.NewReader("secret")), nil
	}}
	guard := access.NewNextcloudPublicationGuard(nil, nil)
	for _, route := range []string{"files", "grant", "presigned"} {
		t.Run(route, func(t *testing.T) {
			r := gin.New()
			var request *http.Request
			switch route {
			case "files":
				r.Use(func(c *gin.Context) {
					ctx := context.WithValue(c.Request.Context(), types.TenantInfoContextKey, &types.Tenant{ID: 7})
					c.Request = c.Request.WithContext(ctx)
					c.Next()
				})
				r.GET("/files", newFileServeHandlerWithGroupAccess(files, nil, catalog, nil, guard))
				request = httptest.NewRequest(http.MethodGet, "/files?file_path="+url.QueryEscape(path), nil)
			case "grant":
				serveResourceGrants(r, catalog, &stubTenantService{
					get: func(context.Context, uint64) (*types.Tenant, error) {
						t.Fatal("tombstoned grant must not fetch tenant")
						return nil, nil
					},
				}, files, nil, nil, guard)
				request = httptest.NewRequest(http.MethodGet, "/r/token", nil)
			case "presigned":
				r.GET("/api/v1/files/presigned", presignedFileHandler(nil, t.TempDir(), nil, catalog, nil, guard))
				signed, err := secutils.SignFileURL("https://weknora.example.com", path, 7, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				u, err := url.Parse(signed)
				if err != nil {
					t.Fatal(err)
				}
				request = httptest.NewRequest(http.MethodGet, "/api/v1/files/presigned?"+u.RawQuery, nil)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, request)
			if w.Code != http.StatusForbidden || opened {
				t.Fatalf("tombstoned source escaped: status=%d opened=%v", w.Code, opened)
			}
		})
	}
}

func TestNextcloudResourceTombstoneRejectsOrdinarySurvivingOwner(t *testing.T) {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantInfoContextKey, &types.Tenant{ID: 7})
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	catalog := &stubResourceCatalog{
		resource: &types.StoredResource{TenantID: 7, SourceProvenance: types.ResourceProvenanceNextcloud},
		knowledgeOwners: func(context.Context, uint64, string) ([]*types.Knowledge, bool, error) {
			return []*types.Knowledge{{Channel: types.ChannelWeb}}, true, nil
		},
	}
	r.GET("/files", newFileServeHandlerWithGroupAccess(&stubFileService{}, nil, catalog, nil,
		access.NewNextcloudPublicationGuard(nil, nil)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/files?file_path=local://7/doc/source.pdf", nil))
	if w.Code != http.StatusForbidden {
		t.Fatalf("surviving ordinary owner revived Nextcloud object: status=%d", w.Code)
	}
}
