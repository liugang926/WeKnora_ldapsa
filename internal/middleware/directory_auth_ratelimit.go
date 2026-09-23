package middleware

import (
	"net/http"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/ratelimit"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const (
	directoryAuthRateLimitMax    = 15
	directoryAuthRateLimitWindow = time.Minute
)

// DirectoryAuthRateLimit limits the public LDAP password endpoint per client
// IP. Redis shares the budget across pods; the limiter safely falls back to an
// in-process window for Lite deployments and transient Redis failures.
func DirectoryAuthRateLimit(redisClient *redis.Client) gin.HandlerFunc {
	limiter := ratelimit.New(redisClient, "directory:auth:ratelimit:", directoryAuthRateLimitWindow, "")
	go limiter.StartCleanup(make(chan struct{}))
	return directoryAuthRateLimit(limiter, directoryAuthRateLimitMax)
}

func directoryAuthRateLimit(limiter *ratelimit.Limiter, maxAttempts int) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		if ip == "" {
			ip = "_unknown_"
		}
		if !limiter.Allow(c.Request.Context(), ip, maxAttempts) {
			_ = c.Error(&apperrors.AppError{
				Code:     apperrors.ErrTooManyRequests,
				Message:  "too many directory login attempts; please retry shortly",
				HTTPCode: http.StatusTooManyRequests,
			})
			c.Abort()
			return
		}
		c.Next()
	}
}
