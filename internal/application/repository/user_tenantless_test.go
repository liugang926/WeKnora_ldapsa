package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestUserRepositoryTenantlessCreateAndUpdateKeepNullTenantID(t *testing.T) {
	db, err := gorm.Open(
		sqlite.Open("file:user_tenantless?mode=memory&cache=shared"),
		&gorm.Config{},
	)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&types.Tenant{}, &types.User{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	repo := NewUserRepository(db)
	user := &types.User{
		ID:           "tenantless-user",
		Username:     "before-update",
		Email:        "tenantless@example.com",
		PasswordHash: "hashed",
		TenantID:     0,
		IsActive:     true,
	}
	if err := repo.CreateUser(context.Background(), user); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	assertNullTenantID(t, db, user.ID)

	user.Username = "after-update"
	if err := repo.UpdateUser(context.Background(), user); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	assertNullTenantID(t, db, user.ID)

	var stored types.User
	if err := db.First(&stored, "id = ?", user.ID).Error; err != nil {
		t.Fatalf("reload user: %v", err)
	}
	if stored.Username != "after-update" || stored.TenantID != 0 {
		t.Fatalf("stored user = %#v", stored)
	}
}

func TestUserRepositoryFindUserByEmailOrUsernameFoldIsExactAndIncludesInactive(t *testing.T) {
	db, err := gorm.Open(
		sqlite.Open("file:user_fold_lookup?mode=memory&cache=shared"),
		&gorm.Config{},
	)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&types.Tenant{}, &types.User{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := NewUserRepository(db)
	users := []*types.User{
		{
			ID:           "inactive",
			Username:     "Alice.Local",
			Email:        "Alice@Example.COM",
			PasswordHash: "hash",
			IsActive:     true,
		},
		{
			ID:           "wildcard",
			Username:     "percent%user",
			Email:        "underscore_user@example.com",
			PasswordHash: "hash",
			IsActive:     true,
		},
	}
	for _, user := range users {
		if err := repo.CreateUser(context.Background(), user); err != nil {
			t.Fatalf("create %s: %v", user.ID, err)
		}
	}
	if err := db.Model(&types.User{}).Where("id = ?", "inactive").Update("is_active", false).Error; err != nil {
		t.Fatalf("deactivate: %v", err)
	}

	got, err := repo.FindUserByEmailOrUsernameFold(
		context.Background(),
		"alice@example.com",
		"unused",
	)
	if err != nil || got == nil || got.ID != "inactive" {
		t.Fatalf("case-fold inactive email lookup = %#v, %v", got, err)
	}
	got, err = repo.FindUserByEmailOrUsernameFold(
		context.Background(),
		"unused@example.com",
		"ALICE.LOCAL",
	)
	if err != nil || got == nil || got.ID != "inactive" {
		t.Fatalf("case-fold username lookup = %#v, %v", got, err)
	}
	got, err = repo.FindUserByEmailOrUsernameFold(context.Background(), "lice@example.com", "lice")
	if err != nil || got != nil {
		t.Fatalf("substring must not match: %#v, %v", got, err)
	}

	if err := db.Delete(&types.User{}, "id = ?", "inactive").Error; err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	got, err = repo.FindUserByEmailOrUsernameFold(
		context.Background(),
		"alice@example.com",
		"alice.local",
	)
	if err != nil || got != nil {
		t.Fatalf("soft-deleted user must be excluded: %#v, %v", got, err)
	}
}

func TestUserRepositorySearchUsersIsSQLitePortableAndEscapesWildcards(t *testing.T) {
	dsn := "file:user_search_escape_" + time.Now().
		Format("150405.000000000") +
		"?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&types.Tenant{}, &types.User{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := NewUserRepository(db)
	for _, user := range []*types.User{
		{
			ID: "literal-percent", Username: "percent%user", Email: "percent@example.com",
			PasswordHash: "hash", IsActive: true,
		},
		{
			ID: "literal-underscore", Username: "underscore_user", Email: "under@example.com",
			PasswordHash: "hash", IsActive: true,
		},
		{ID: "plain", Username: "percentXuser", Email: "plain@example.com", PasswordHash: "hash", IsActive: true},
	} {
		if err := repo.CreateUser(context.Background(), user); err != nil {
			t.Fatalf("create %s: %v", user.ID, err)
		}
	}

	got, err := repo.SearchUsers(context.Background(), "%", 20)
	if err != nil {
		t.Fatalf("search literal percent: %v", err)
	}
	if len(got) != 1 || got[0].ID != "literal-percent" {
		t.Fatalf("literal percent results = %#v", got)
	}
	got, err = repo.SearchUsers(context.Background(), "_", 20)
	if err != nil {
		t.Fatalf("search literal underscore: %v", err)
	}
	if len(got) != 1 || got[0].ID != "literal-underscore" {
		t.Fatalf("literal underscore results = %#v", got)
	}
}

func assertNullTenantID(t *testing.T, db *gorm.DB, userID string) {
	t.Helper()
	var count int64
	if err := db.Table("users").Where("id = ? AND tenant_id IS NULL", userID).Count(&count).Error; err != nil {
		t.Fatalf("check tenant_id: %v", err)
	}
	if count != 1 {
		t.Fatalf("tenant_id for user %s is not NULL", userID)
	}
}
