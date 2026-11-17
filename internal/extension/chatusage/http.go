package chatusage

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// Register attaches only read endpoints. The caller supplies the standard
// system-admin group. Explicitly disallow API keys and delegated callers,
// including platform keys accepted by other system-admin capabilities.
func Register(group *gin.RouterGroup, db *gorm.DB) {
	group.Use(func(c *gin.Context) {
		ctx := c.Request.Context()
		principal, present := types.PrincipalFromContext(ctx)
		_, apiKey := types.TenantAPIKeyScopeFromContext(ctx)
		if !present || !types.IsSystemAdminFromContext(ctx) || apiKey || principal.ActingVia != "" ||
			principal.Type != types.PrincipalWebUser ||
			types.PrincipalAuthSourceFromContext(ctx) != types.AuthSourceWeb {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "System administrator login required"})
			return
		}
		c.Header("Cache-Control", "no-store")
	})
	group.GET("/users", func(c *gin.Context) {
		p, ok := readPage(c)
		if !ok {
			return
		}
		query := strings.TrimSpace(c.Query("q"))
		if len(query) > 512 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Search query is too long"})
			return
		}
		report, err := listPeople(c.Request.Context(), db, p, query)
		writeReport(c, report, err)
	})
	group.GET("/users/:user_id/sessions", func(c *gin.Context) {
		p, ok := readPage(c)
		if !ok {
			return
		}
		report, err := listSessions(c.Request.Context(), db, p, c.Param("user_id"))
		writeReport(c, report, err)
	})
}

func readPage(c *gin.Context) (page, bool) {
	p, pageErr := strconv.ParseInt(c.DefaultQuery("page", "1"), 10, 32)
	size, sizeErr := strconv.ParseInt(c.DefaultQuery("page_size", "20"), 10, 32)
	if pageErr != nil || p < 1 || sizeErr != nil || size < 1 || size > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid pagination"})
		return page{}, false
	}
	return page{Page: int(p), PageSize: int(size)}, true
}

func writeReport(c *gin.Context, report any, err error) {
	if err != nil {
		logger.Errorf(c.Request.Context(), "Failed to query global chat usage: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Unable to query chat usage"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": report})
}
