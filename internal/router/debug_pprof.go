// Package router wires HTTP routes and background task handlers.
package router

import (
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/gin-gonic/gin"
)

// registerDebugPprof mounts the standard net/http/pprof handlers for operator
// diagnostics. It is disabled unless WEKNORA_DEBUG_PPROF=true, keeps default
// deployments identical to upstream behavior, and additionally admits only
// loopback clients. The frontend reverse proxy does not forward /debug/, so
// the surface stays internal to the host/docker network.
func registerDebugPprof(r *gin.Engine) {
	if os.Getenv("WEKNORA_DEBUG_PPROF") != "true" {
		return
	}
	debug := r.Group("/debug", func(c *gin.Context) {
		if ip := c.ClientIP(); ip != "127.0.0.1" && ip != "::1" {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
	})
	pprofRoutes := debug.Group("/pprof")
	pprofRoutes.GET("/", gin.WrapF(pprof.Index))
	pprofRoutes.GET("/cmdline", gin.WrapF(pprof.Cmdline))
	pprofRoutes.GET("/profile", gin.WrapF(pprof.Profile))
	pprofRoutes.GET("/symbol", gin.WrapF(pprof.Symbol))
	pprofRoutes.POST("/symbol", gin.WrapF(pprof.Symbol))
	pprofRoutes.GET("/trace", gin.WrapF(pprof.Trace))
	for _, profile := range []string{"allocs", "block", "goroutine", "heap", "mutex", "threadcreate"} {
		pprofRoutes.GET("/"+profile, gin.WrapH(pprof.Handler(profile)))
	}
}
