// Package adminusers adds directory-aware account lookup to the system admin UI.
package adminusers

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// Only project fields needed by user management.
type account struct {
	ID            string    `json:"id"`
	Username      string    `json:"username"`
	DisplayName   string    `json:"display_name"`
	Email         string    `json:"email"`
	IsActive      bool      `json:"is_active"`
	IsSystemAdmin bool      `json:"is_system_admin"`
	CreatedAt     time.Time `json:"created_at"`
}

const accounts = `SELECT * FROM (SELECT u.id,u.username,u.email,u.is_active,u.is_system_admin,u.created_at,
	COALESCE((SELECT TRIM(i.display_name) FROM directory_identities i WHERE i.user_id=u.id
		AND i.status='active' AND TRIM(i.display_name) <> ''
		ORDER BY i.last_seen_at DESC NULLS LAST, i.id LIMIT 1),
		(SELECT TRIM(d.display_name) FROM dingtalk_identities d WHERE d.user_id=u.id
		AND d.status='active' AND TRIM(d.display_name) <> '' ORDER BY d.id LIMIT 1),'') AS display_name
	FROM users u WHERE u.deleted_at IS NULL) AS account_list`

func lookup(db *gorm.DB, query string, offset, limit int) ([]account, bool, error) {
	sql := accounts
	var args []any
	if query != "" {
		sql += ` WHERE LOWER(username) LIKE LOWER(?) ESCAPE '!' OR LOWER(email) LIKE LOWER(?) ESCAPE '!'
			OR LOWER(display_name) LIKE LOWER(?) ESCAPE '!'`
		term := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(query) + "%"
		args = append(args, term, term, term)
	}
	args = append(args, limit+1, offset)
	rows := make([]account, 0)
	err := db.Raw(sql+" ORDER BY created_at DESC, id LIMIT ? OFFSET ?", args...).Scan(&rows).Error
	if len(rows) > limit {
		return rows[:limit], true, err
	}
	return rows, false, err
}

// Register adds a read route; mutations stay with the official admin handlers.
func Register(group *gin.RouterGroup, db *gorm.DB) {
	group.GET("/account-list", func(c *gin.Context) {
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
		offset, offsetErr := strconv.Atoi(c.DefaultQuery("offset", "0"))
		limit, limitErr := strconv.Atoi(c.DefaultQuery("limit", "20"))
		q := strings.TrimSpace(c.Query("q"))
		if offsetErr != nil || offset < 0 || limitErr != nil || limit < 1 || limit > 200 || len(q) > 512 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid pagination or search query"})
			return
		}
		rows, more, err := lookup(db.WithContext(ctx), q, offset, limit)
		if err != nil {
			logger.Errorf(ctx, "List admin accounts: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Unable to list accounts"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"users": rows, "offset": offset, "limit": limit, "has_more": more})
	})
}
