// Package integration installs the WebDAV extension before WeKnora resolves its
// existing connector service and HTTP router. It adds no worker or ACL of its own.
package integration

import (
	"fmt"

	"github.com/Tencent/WeKnora/extensions/nextcloudwebdav/adminui"
	webdav "github.com/Tencent/WeKnora/extensions/nextcloudwebdav/connector"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/gin-gonic/gin"
	"go.uber.org/dig"
)

// Install adds the WebDAV connector and its static configuration page to the server container.
func Install(c *dig.Container) error {
	if err := c.Decorate(registerConnector); err != nil {
		return fmt.Errorf("register Nextcloud WebDAV connector hook: %w", err)
	}
	return c.Decorate(func(router *gin.Engine) *gin.Engine {
		adminui.Register(router)
		return router
	})
}

func registerConnector(registry *datasource.ConnectorRegistry) (*datasource.ConnectorRegistry, error) {
	connector := webdav.NewConnector()
	if err := registry.Register(connector); err != nil {
		return nil, err
	}
	// The registry is constructed during boot, before the API serves requests.
	datasource.ConnectorMetadataRegistry[connector.Type()] = datasource.ConnectorMetadata{
		Type: connector.Type(), Name: "Nextcloud (WebDAV)",
		Description: "Import files from selected Nextcloud folders using WebDAV",
		Priority:    8, AuthType: "basic", Capabilities: []string{"incremental", "hierarchical"},
	}
	return registry, nil
}
