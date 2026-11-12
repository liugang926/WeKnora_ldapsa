package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
	"github.com/gin-gonic/gin"
)

func servePresignedPreviewForTest(
	t *testing.T, catalog interfaces.ResourceCatalog, filePath string,
) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Setenv("LOCAL_STORAGE_BASE_DIR", t.TempDir())
	engine := gin.New()
	servePresignedPreview(engine, nil, nil, catalog)

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/files/presigned-preview?file_path="+url.QueryEscape(filePath), nil)
	ctx := context.WithValue(req.Context(), types.TenantInfoContextKey, &types.Tenant{ID: 42})
	ctx = types.WithCaller(ctx, types.Caller{TenantID: 42, UserID: "admin", Role: types.TenantRoleAdmin})
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req.WithContext(ctx))
	return recorder
}

// A known handle of another tenant's resource must not be exchangeable for an
// anonymous /r/ grant (the stub catalog panics if a grant is minted).
func TestPresignedPreviewRejectsCrossTenantResource(t *testing.T) {
	catalog := &stubResourceCatalog{resource: &types.StoredResource{
		TenantID: 7, PhysicalPath: "local://7/exports/a.png",
	}}
	code := servePresignedPreviewForTest(t, catalog, "resource://AbCdEfGhIjKlMnOpQrStUv").Code
	if code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", code, http.StatusForbidden)
	}
}

func TestPresignedPreviewRejectsCrossTenantPath(t *testing.T) {
	for _, filePath := range []string{"local://7/knowledge/secret.pdf", "local://docs/example.txt"} {
		if code := servePresignedPreviewForTest(t, &stubResourceCatalog{}, filePath).Code; code != http.StatusForbidden {
			t.Fatalf("%s: status = %d, want %d", filePath, code, http.StatusForbidden)
		}
	}
}

// Without a catalog a resource reference cannot be attributed to a tenant, so
// it is refused rather than signed.
func TestPresignedPreviewRejectsUnresolvableResource(t *testing.T) {
	code := servePresignedPreviewForTest(t, nil, "resource://AbCdEfGhIjKlMnOpQrStUv").Code
	if code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", code, http.StatusForbidden)
	}
}

func TestPresignedPreviewAllowsOwnTenantPath(t *testing.T) {
	t.Setenv("APP_EXTERNAL_URL", "https://preview.example.test")
	t.Setenv("SYSTEM_SIGNING_KEY", "")
	t.Setenv("SYSTEM_AES_KEY", "weknora-test-aes-key-32bytes!!!")
	const filePath = "local://42/exports/a.png"
	catalog := &stubResourceCatalog{resource: &types.StoredResource{
		TenantID: 42, PhysicalPath: filePath, SourceProvenance: types.ResourceProvenanceOrdinary,
	}}
	response := servePresignedPreviewForTest(t, catalog, filePath)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var preview struct{ URL string }
	if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(preview.URL)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if u.Path != "/api/v1/files/presigned" || q.Get("file_path") != filePath || q.Get("tenant_id") != "42" ||
		!secutils.VerifyFileURLSig(filePath, 42, q.Get("expires"), q.Get("sig")) {
		t.Fatal("expected a signed preview for the registered tenant file")
	}
}

func TestPresignedPreviewRejectsUnregisteredOwnTenantPath(t *testing.T) {
	response := servePresignedPreviewForTest(t, &stubResourceCatalog{}, "local://42/exports/a.png")
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}
