package adminui

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/gin-gonic/gin"
)

func TestStaticNavigationWithWeKnoraAuth(t *testing.T) {
	const prefix = "/api/v1/extensions/nextcloud-webdav/"
	router := gin.New()
	router.Use(middleware.Auth(nil, nil, nil, nil, nil))
	reachedAPI := false
	api := func(c *gin.Context) {
		reachedAPI = true
		c.Status(http.StatusOK)
	}
	router.GET("/api/v1/datasource", api)
	Register(router)
	router.PUT("/api/v1/datasource/:id/credentials", api)

	for _, asset := range []struct{ route, file, mime string }{
		{"", "index.html", "text/html; charset=utf-8"},
		{"manager.js", "manager.js", "text/javascript; charset=utf-8"},
		{"manager.css", "manager.css", "text/css; charset=utf-8"},
	} {
		t.Run(asset.file, func(t *testing.T) {
			w := httptest.NewRecorder()
			router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, prefix+asset.route, nil))
			want, err := assets.ReadFile(asset.file)
			if err != nil {
				t.Fatal(err)
			}
			if w.Code != http.StatusOK || w.Header().Get("Content-Type") != asset.mime || !bytes.Equal(w.Body.Bytes(), want) {
				t.Fatalf("embedded file not served: status %d, type %q", w.Code, w.Header().Get("Content-Type"))
			}
			if w.Header().Get("Content-Security-Policy") == "" || w.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatal("missing static content headers")
			}
		})
	}
	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/datasource"},
		{http.MethodPut, "/api/v1/datasource/example/credentials"},
		{http.MethodGet, prefix + "unlisted.js"},
		{http.MethodPost, prefix},
	} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(req.method, req.path, nil))
		if w.Code != http.StatusUnauthorized || reachedAPI {
			t.Fatalf("unexpected anonymous access: %s %s status %d", req.method, req.path, w.Code)
		}
	}
}
