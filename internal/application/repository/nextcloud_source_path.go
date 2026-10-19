package repository

import (
	"context"

	"gorm.io/gorm"
)

// Resolution changes the live probe path only. Immutable original input rows
// and graph receipts retain the path that actually influenced their build.
func nextcloudCurrentSourcePath(ctx context.Context, db *gorm.DB, tenant uint64, kb, ds, path string) (string, error) {
	if !db.Migrator().HasTable("nextcloud_root_migrations") {
		return path, nil
	}
	return NewNextcloudOnboardingStore(db).ResolveCurrentSourcePath(ctx, tenant, kb, ds, path)
}

func nextcloudRootEpochAuthorized(
	ctx context.Context,
	db *gorm.DB,
	tenant uint64,
	kb, ds, path string,
	oldEpoch, currentEpoch int64,
) (bool, error) {
	if !db.Migrator().HasTable("nextcloud_root_migrations") {
		return false, nil
	}
	return NewNextcloudOnboardingStore(
		db,
	).RootMigrationAuthorizesEpoch(ctx, tenant, kb, ds, path, oldEpoch, currentEpoch)
}
