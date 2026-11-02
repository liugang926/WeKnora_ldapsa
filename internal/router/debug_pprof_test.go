package router

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newPprofTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerDebugPprof(r)
	return r
}

func serve(r *gin.Engine, target, remoteAddr string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.RemoteAddr = remoteAddr
	r.ServeHTTP(w, req)
	return w
}

func TestDebugPprofDisabledByDefault(t *testing.T) {
	t.Setenv("WEKNORA_DEBUG_PPROF", "")
	w := serve(newPprofTestRouter(), "/debug/pprof/", "127.0.0.1:5000")
	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestDebugPprofEnabledServesLoopbackOnly(t *testing.T) {
	t.Setenv("WEKNORA_DEBUG_PPROF", "true")

	w := serve(newPprofTestRouter(), "/debug/pprof/", "127.0.0.1:5000")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "goroutine")

	w = serve(newPprofTestRouter(), "/debug/pprof/", "10.0.0.9:5000")
	require.Equal(t, http.StatusForbidden, w.Code)
}
