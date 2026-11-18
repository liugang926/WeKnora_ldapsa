package chatusage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/Tencent/WeKnora/internal/types"
)

func reportDB(t *testing.T) *gorm.DB {
	t.Helper()
	driver := sqlite.Open(":memory:")
	if dsn := os.Getenv("WEKNORA_CHAT_USAGE_TEST_POSTGRES_DSN"); dsn != "" {
		driver = postgres.New(postgres.Config{DSN: dsn, PreferSimpleProtocol: true})
	}
	db, err := gorm.Open(driver, &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	sqlDB.SetMaxOpenConns(1)
	fixture := `
	CREATE TEMP TABLE users(id TEXT PRIMARY KEY, username TEXT);
	CREATE TEMP TABLE directory_identities(id TEXT PRIMARY KEY, user_id TEXT, status TEXT,
		display_name TEXT, last_seen_at DATETIME);
	CREATE TEMP TABLE dingtalk_identities(id INTEGER PRIMARY KEY, user_id TEXT, status TEXT, display_name TEXT);
	CREATE TEMP TABLE tenants(id INTEGER PRIMARY KEY, name TEXT);
	CREATE TEMP TABLE sessions(id TEXT PRIMARY KEY,user_id TEXT,tenant_id INTEGER,owner_auth_source TEXT,
		kind TEXT,parent_session_id TEXT,created_at DATETIME,deleted_at DATETIME);
	CREATE TEMP TABLE messages(id TEXT PRIMARY KEY,session_id TEXT,role TEXT,created_at DATETIME,
		usage TEXT,deleted_at DATETIME);
	INSERT INTO users VALUES ('u1','A_test'),('u2','Another'),('unused','Unused');
	INSERT INTO tenants VALUES (10,'First'),(20,'Second');
	INSERT INTO sessions VALUES
		('s1','u1',10,'web','',NULL,'2026-01-01',NULL),
		('s2','u1',20,'web','assistant','','2026-02-01','2026-02-03'),
		('branch','u1',10,'web','','s1','2026-03-01',NULL),
		('no-new-turn','u1',10,'web','','s1','2026-03-01',NULL),
		('s3','u2',10,'web','','','2026-01-01',NULL),
		('empty','u1',10,'web','','','2026-01-01',NULL),
		('legacy','u1',10,'','','','2026-01-01',NULL),
		('spoof','u1',10,'api_direct_header','','','2026-01-01',NULL),
		('automation','u1',10,'web','automation','','2026-01-01',NULL),
		('child','u1',10,'web','agent_delegated','','2026-01-01',NULL);
	INSERT INTO messages(id,session_id,role,created_at,usage) VALUES
		('1','s1','user','2026-01-02',NULL),('2','s1','assistant','2026-01-02',
		'{"prompt_tokens":12,"completion_tokens":5,"total_tokens":17,"cache_read_tokens":10}'),
		('3','s1','user','2026-01-03',NULL),('4','s1','assistant','2026-01-03',NULL),
		('5','s2','user','2026-02-02',NULL),('6','s2','assistant','2026-02-02','{"total_tokens":9}'),
		('7','branch','user','2026-01-02',NULL),('8','branch','assistant','2026-01-02','{"total_tokens":17}'),
		('9','branch','user','2026-03-02',NULL),('10','branch','assistant','2026-03-02','{"completion_tokens":2}'),
		('11','no-new-turn','user','2026-01-02',NULL),
		('12','s3','user','2026-01-02',NULL),('13','s3','assistant','2026-01-02','{"context_token_scale":1.2}'),
		('18','s3','assistant','2026-01-02','{"total_tokens":0,"provider_reported":true}'),
		('19','s3','assistant','2026-01-02','{"total_tokens":"3","provider_reported":false}'),
		('20','s3','assistant','2026-01-02','not-json'),
		('21','s3','assistant','2026-01-02','{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}'),
		('14','legacy','user','2026-01-02',NULL),('15','spoof','user','2026-01-02',NULL),
		('16','automation','user','2026-01-02',NULL),('17','child','user','2026-01-02',NULL);
	UPDATE messages SET deleted_at='2026-02-03' WHERE session_id='s2';`
	if driver.Name() == "postgres" {
		// JSONB rejects malformed input on write. Use a valid non-object to
		// exercise the corresponding unreported row on PostgreSQL.
		fixture = strings.NewReplacer("DATETIME", "TIMESTAMPTZ", "usage TEXT", "usage JSONB",
			"'not-json'", "'[]'").Replace(fixture)
	}
	require.NoError(t, db.Exec(fixture).Error)
	return db
}

func TestGlobalHistory(t *testing.T) {
	db := reportDB(t)
	ctx := context.Background()
	p := page{Page: 1, PageSize: 20}
	users, err := listPeople(ctx, db, p, "_")
	require.NoError(t, err)
	require.EqualValues(t, 2, users.Summary.UserCount)
	require.EqualValues(t, 4, users.Summary.SessionCount)
	require.Equal(t, Tokens{12, 7, 28, 4, 5}, users.Summary.Tokens)
	require.EqualValues(t, 1, users.Total)
	require.Len(t, users.Items, 1)
	require.Equal(t, Counts{3, Tokens{12, 7, 28, 3, 1}}, users.Items[0].Counts)

	detail, err := listSessions(ctx, db, p, "u1")
	require.NoError(t, err)
	require.Len(t, detail.Items, 3)
	require.EqualValues(t, 3, detail.Total)
	require.Equal(t, "branch", detail.Items[0].SessionID)
	require.Equal(t, "2026-03-02", detail.Items[0].StartedAt.Format("2006-01-02"))
	require.Equal(t, "Second", detail.Items[1].WorkspaceName)
	require.EqualValues(t, 20, detail.Items[1].WorkspaceID)
	require.EqualValues(t, 9, detail.Items[1].TotalTokens)
	p.Page = 10
	detail, err = listSessions(ctx, db, p, "u1")
	require.NoError(t, err)
	require.Empty(t, detail.Items)
	require.EqualValues(t, 3, detail.Total)
	users, err = listPeople(ctx, db, p, "")
	require.NoError(t, err)
	require.Empty(t, users.Items)
	require.EqualValues(t, 2, users.Total)
}

func TestPeopleWithLinkedNames(t *testing.T) {
	db := reportDB(t)
	ctx := context.Background()
	p := page{Page: 1, PageSize: 20}
	require.NoError(t, db.Exec(`
		INSERT INTO directory_identities VALUES
			('1','u1','active','旧名','2026-01-01'),
			('2','u1','active',' 王明 ','2026-02-01'),
			('3','u1','disabled','停用','2026-03-01');
		INSERT INTO dingtalk_identities VALUES (1,'u1','active','钉钉旧名'),(2,'u2','active','王明');
	`).Error)
	report, err := listPeople(ctx, db, p, "王明")
	require.NoError(t, err)
	require.EqualValues(t, 2, report.Total)
	require.Len(t, report.Items, 2)
	require.Equal(t, "A_test", report.Items[0].Username)
	require.Equal(t, "王明", report.Items[0].DisplayName)
	require.EqualValues(t, 3, report.Items[0].SessionCount)
	require.Equal(t, "Another", report.Items[1].Username)
	require.Equal(t, "王明", report.Items[1].DisplayName)
	require.Equal(t, Tokens{12, 7, 28, 4, 5}, report.Summary.Tokens)

	require.NoError(t, db.Exec("UPDATE dingtalk_identities SET status='unlinked' WHERE user_id='u2'").Error)
	report, err = listPeople(ctx, db, p, "another")
	require.NoError(t, err)
	require.Len(t, report.Items, 1)
	require.Empty(t, report.Items[0].DisplayName)
}

func TestOnlySystemAdminWebLogin(t *testing.T) {
	db := reportDB(t)
	ctx := types.WithPrincipal(context.Background(), types.Principal{Type: types.PrincipalWebUser, ID: "u1"})
	web := types.WithPrincipalAuthSource(ctx, types.AuthSourceWeb)
	admin := context.WithValue(web, types.SystemAdminContextKey, true)
	delegated := types.WithPrincipal(admin, types.Principal{
		Type: types.PrincipalWebUser, ID: "u1", ActingVia: "automation:a",
	})
	platform := types.WithPrincipal(admin, types.Principal{Type: types.PrincipalAPIPlatform, ID: "a"})
	scoped := types.WithTenantAPIKeyScope(admin, types.TenantAPIKeyScope{FullAccess: true})
	engine := gin.New()
	Register(engine.Group("/usage"), db)
	for _, tt := range []struct {
		ctx  context.Context
		path string
		want int
	}{
		{web, "/usage/users", 403},
		{delegated, "/usage/users", 403},
		{platform, "/usage/users/u1/sessions", 403},
		{scoped, "/usage/users/u1/sessions", 403},
		{admin, "/usage/users?page_size=101", 400},
		{admin, "/usage/users/u1/sessions?page=0", 400},
		{admin, "/usage/users/u1/sessions", 200},
		{admin, "/usage/users", 200},
	} {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tt.path, nil).WithContext(tt.ctx))
		require.Equal(t, tt.want, w.Code, tt.path)
		if tt.want == http.StatusOK {
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			require.Contains(t, w.Body.String(), `"items":[`)
			require.NotContains(t, w.Body.String(), "content")
		}
	}
}
