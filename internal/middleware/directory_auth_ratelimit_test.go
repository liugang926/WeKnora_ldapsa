package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/ratelimit"
	"github.com/gin-gonic/gin"
)

func TestDirectoryAuthRateLimitReturns429PerClientIP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(ErrorHandler())
	router.POST("/api/v1/auth/ldap/login",
		directoryAuthRateLimit(ratelimit.New(nil, "test:", time.Minute, "test"), 2),
		func(c *gin.Context) { c.Status(http.StatusNoContent) },
	)

	for attempt, want := range []int{http.StatusNoContent, http.StatusNoContent, http.StatusTooManyRequests} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/ldap/login", nil)
		req.RemoteAddr = "192.0.2.10:4321"
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != want {
			t.Fatalf("attempt %d status = %d, want %d; body=%s", attempt+1, resp.Code, want, resp.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/ldap/login", nil)
	req.RemoteAddr = "192.0.2.11:4321"
	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, req)
	if resp.Code != http.StatusNoContent {
		t.Fatalf("independent IP status = %d, want %d", resp.Code, http.StatusNoContent)
	}
}
