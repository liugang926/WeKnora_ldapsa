// Package adminui serves the standalone manager for the WebDAV data source.
package adminui

import (
	"embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed index.html manager.js manager.css
var assets embed.FS

// Register exposes only the static shell. Data and credentials are handled
// by WeKnora's authenticated, tenant-scoped data source APIs.
func Register(router *gin.Engine) {
	// Initial page navigation and its static subresources cannot supply the
	// bearer token used by the manager's subsequent authenticated API calls.
	// Keep this group private to the three embedded files below.
	shell := router.Group("/api/v1/extensions/nextcloud-webdav")
	shell.Handlers = nil
	for _, asset := range []struct{ path, file, mime string }{
		{"/", "index.html", "text/html; charset=utf-8"},
		{"manager.js", "manager.js", "text/javascript; charset=utf-8"},
		{"manager.css", "manager.css", "text/css; charset=utf-8"},
	} {
		content, err := assets.ReadFile(asset.file)
		if err != nil {
			panic(err)
		}
		mimeType := asset.mime
		shell.GET(asset.path, func(c *gin.Context) {
			c.Header("Cache-Control", "no-store")
			c.Header("Content-Security-Policy",
				"default-src 'none'; script-src 'self'; style-src 'self'; "+
					"connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
			c.Header("Referrer-Policy", "no-referrer")
			c.Header("X-Content-Type-Options", "nosniff")
			c.Data(http.StatusOK, mimeType, content)
		})
	}
}
