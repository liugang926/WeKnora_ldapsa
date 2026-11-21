package integration

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/gin-gonic/gin"
	"go.uber.org/dig"
)

// Match production ordering: main's overlay installs hooks first; the container
// later provides the built-in registry and engine. Both must keep their identity.
func TestWebDAVExtensionBoot(t *testing.T) {
	c := dig.New()
	if err := Install(c); err != nil {
		t.Fatal(err)
	}

	baseRegistry := datasource.NewConnectorRegistry()
	baseRouter := gin.New()
	baseRouter.GET("/existing", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	if err := c.Provide(func() *datasource.ConnectorRegistry { return baseRegistry }); err != nil {
		t.Fatal(err)
	}
	if err := c.Provide(func() *gin.Engine { return baseRouter }); err != nil {
		t.Fatal(err)
	}
	if err := c.Invoke(func(registry *datasource.ConnectorRegistry, router *gin.Engine) {
		if registry != baseRegistry || router != baseRouter {
			t.Fatal("extension replaced a shared dependency")
		}
		connector, err := registry.Get("nextcloud_webdav")
		if err != nil || connector.Type() != "nextcloud_webdav" {
			t.Fatalf("WebDAV source is unavailable: %v", err)
		}
		if _, ok := datasource.ConnectorMetadataRegistry[connector.Type()]; !ok {
			t.Fatal("WebDAV is absent from the data source catalog")
		}
		for route, want := range map[string]int{
			"/existing":                            http.StatusNoContent,
			"/api/v1/extensions/nextcloud-webdav/": http.StatusOK,
		} {
			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, route, nil))
			if response.Code != want {
				t.Errorf("%s status = %d; want %d", route, response.Code, want)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
}
