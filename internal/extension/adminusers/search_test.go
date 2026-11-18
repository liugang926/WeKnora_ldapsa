package adminusers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestAccountLookupBeforePagination(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	pool, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	require.NoError(t, db.Exec(`
	CREATE TABLE users(id TEXT, username TEXT, email TEXT, is_active BOOLEAN, is_system_admin BOOLEAN,
		created_at DATETIME, deleted_at DATETIME);
	CREATE TABLE directory_identities(id TEXT,user_id TEXT,display_name TEXT,status TEXT,last_seen_at DATETIME);
	CREATE TABLE dingtalk_identities(id TEXT,user_id TEXT,display_name TEXT,status TEXT);
	INSERT INTO users VALUES ('a','new','new@example.org',TRUE,FALSE,'2026-11-01',NULL),
		('b','t001','b@example.org',TRUE,FALSE,'2025-01-01',NULL),
		('c','t002','c@example.org',TRUE,FALSE,'2024-01-01',NULL),
		('d','deleted','d@example.org',TRUE,FALSE,'2025-01-01','2026-01-01');
	INSERT INTO directory_identities VALUES ('1','b','同名','active','2025-01-01'),
		('2','b','旧名','disabled','2026-01-01'),('3','d','同名','active','2025-01-01');
	INSERT INTO dingtalk_identities VALUES ('4','c','同名','active'),('5','b','外部名','active');
	`).Error)
	first, more, err := lookup(db, "", 0, 1)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, "new", first[0].Username)
	second, more, err := lookup(db, "T001", 0, 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, "同名", second[0].DisplayName)
	for i, id := range []string{"b", "c"} {
		rows, more, err := lookup(db, "同名", i, 1)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, id, rows[0].ID)
		require.Equal(t, i == 0, more)
	}
	rows, _, err := lookup(db, "%_", 0, 20)
	require.NoError(t, err)
	require.Empty(t, rows)
}
