package repository

import (
	"context"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudSourceTombstoneLookupNeverCertifiesAbsence(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.Exec(`CREATE TABLE nextcloud_source_tombstones (
		tenant_id INTEGER NOT NULL, scope_type TEXT NOT NULL, scope_id TEXT NOT NULL,
		PRIMARY KEY(tenant_id, scope_type, scope_id))`).Error; err != nil {
		t.Fatal(err)
	}
	repo := &DataSourceRepository{db: db}
	ctx := context.Background()
	state, err := repo.LookupNextcloudSourcePresence(ctx, 7, "tenant", "7")
	if err != nil || state != NextcloudSourcePresenceUnknown {
		t.Fatal("missing evidence must be unknown", state, err)
	}
	if err := db.Exec(("INSERT INTO nextcloud_source_tombstones VALUES (7, 'ten" +
		"ant', '7'), (7, 'knowledge_base', 'kb')")).Error; err != nil {
		t.Fatal(err)
	}
	state, err = repo.LookupNextcloudSourcePresence(ctx, 7, "knowledge_base", "kb")
	if err != nil || state != NextcloudSourcePresenceEver {
		t.Fatal("retained positive evidence lost", state, err)
	}
	state, err = repo.LookupNextcloudSourcePresence(ctx, 8, "knowledge_base", "kb")
	if err != nil || state != NextcloudSourcePresenceUnknown {
		t.Fatal("cross-tenant evidence accepted", state, err)
	}
	for _, scope := range [][2]string{{"tenant", "8"}, {"unsupported", "kb"}, {"datasource", ""}} {
		state, err = repo.LookupNextcloudSourcePresence(ctx, 7, scope[0], scope[1])
		if err == nil || state != NextcloudSourcePresenceUnknown {
			t.Fatal("invalid lookup did not fail closed", state, err)
		}
	}
	if err := db.Exec("DROP TABLE nextcloud_source_tombstones").Error; err != nil {
		t.Fatal(err)
	}
	state, err = repo.LookupNextcloudSourcePresence(ctx, 7, "tenant", "7")
	if err == nil || state != NextcloudSourcePresenceUnknown {
		t.Fatal("storage failure was treated as absence", state, err)
	}
}
