package service_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	appservice "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupDirectoryAccessSQLite(
	t *testing.T,
) (*gorm.DB, interfaces.DirectoryService, interfaces.GroupAccessService) {
	t.Helper()
	db, err := gorm.Open(
		sqlite.Open(
			"file:"+t.Name()+"?mode=memory&cache=shared&_foreign_keys=on&_busy_timeout=5000",
		),
		&gorm.Config{},
	)
	require.NoError(t, err)
	// Match the application's SQLite pool configuration. SQLite permits only
	// one writer; a multi-connection shared-memory test can otherwise return
	// SQLITE_LOCKED before the repository's atomic conditional update decides
	// which concurrent link wins.
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	migrationPath := filepath.Join(
		"..",
		"..",
		"..",
		"migrations",
		"sqlite",
		"000028_directory_group_access.up.sql",
	)
	ddl, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(ddl)).Error)
	require.NoError(t, db.AutoMigrate(&types.TenantMember{}, &types.AuthToken{}))
	directoryRepo := apprepo.NewDirectoryRepository(db)
	groupRepo := apprepo.NewGroupAccessRepository(db)
	return db, appservice.NewDirectoryService(
			directoryRepo,
		), appservice.NewGroupAccessService(
			groupRepo,
		)
}

func TestDirectoryLinkIdentityConcurrentNeverOverwritesSQLite(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)
	var identity types.DirectoryIdentity
	require.NoError(
		t,
		db.Where("directory_id = ? AND object_guid = ?", directory.ID, "user-guid").
			First(&identity).
			Error,
	)

	services := []interfaces.DirectoryService{
		appservice.NewDirectoryService(apprepo.NewDirectoryRepository(db)),
		appservice.NewDirectoryService(apprepo.NewDirectoryRepository(db)),
	}
	users := []string{"local-user-a", "local-user-b"}
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range services {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = services[i].LinkIdentity(context.Background(), identity.ID, users[i])
		}(i)
	}
	close(start)
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, appservice.ErrDirectoryIdentityAlreadyLinked):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent link error: %v", err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)
	require.NoError(t, db.First(&identity, "id = ?", identity.ID).Error)
	require.NotNil(t, identity.UserID)
	require.Contains(t, users, *identity.UserID)

	// Repeating the winning link is idempotent; the losing user can never
	// replace it, including from a fresh service/repository instance.
	require.NoError(
		t,
		services[0].LinkIdentity(context.Background(), identity.ID, *identity.UserID),
	)
	loser := users[0]
	if loser == *identity.UserID {
		loser = users[1]
	}
	require.ErrorIs(
		t,
		services[1].LinkIdentity(context.Background(), identity.ID, loser),
		appservice.ErrDirectoryIdentityAlreadyLinked,
	)
}

func TestDirectoryLinkIdentityConcurrentUniqueUserConflictSQLite(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)
	second := types.DirectoryIdentity{
		ID: "identity-second", DirectoryID: directory.ID, ObjectGUID: "user-guid-second",
		ObjectSID: "S-1-5-21-1001", DN: "cn=Second,dc=example,dc=test",
		Status: types.DirectoryObjectActive, SnapshotVersion: 1, LastSeenAt: time.Now().UTC(),
	}
	require.NoError(t, db.Create(&second).Error)
	var first types.DirectoryIdentity
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&first).Error)

	services := []interfaces.DirectoryService{
		appservice.NewDirectoryService(apprepo.NewDirectoryRepository(db)),
		appservice.NewDirectoryService(apprepo.NewDirectoryRepository(db)),
	}
	ids := []string{first.ID, second.ID}
	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range services {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = services[i].LinkIdentity(context.Background(), ids[i], "shared-user")
		}(i)
	}
	close(start)
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, appservice.ErrDirectoryIdentityAlreadyLinked) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent link error: %v", err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, conflicts)
	var linked int64
	require.NoError(t, db.Model(&types.DirectoryIdentity{}).
		Where("directory_id = ? AND user_id = ?", directory.ID, "shared-user").Count(&linked).Error)
	require.Equal(t, int64(1), linked)
}

func createTestDirectory(t *testing.T, svc interfaces.DirectoryService) *types.Directory {
	t.Helper()
	directory, err := svc.Create(context.Background(), &types.Directory{
		Name: "corp-ad", Protocol: types.DirectoryProtocolAD, Enabled: true,
		ConfigSource: types.DirectoryConfigSourceUI, TLSMode: types.DirectoryTLSLDAPS,
		ServerURLs: types.StringArray{"ldaps://dc1.example.test:636"},
		BaseDN:     "dc=example,dc=test", UserFilter: "(objectClass=user)", GroupFilter: "(objectClass=group)",
		AllowedLoginFilter: "(memberOf=cn=allowed,dc=example,dc=test)",
		ServiceAccountDN:   "cn=reader,dc=example,dc=test", PasswordCiphertext: "ciphertext",
	})
	require.NoError(t, err)
	return directory
}

func testDirectorySnapshot(directoryID string) *types.DirectorySnapshot {
	return &types.DirectorySnapshot{
		DirectoryID: directoryID, Complete: true, PaginationComplete: true,
		Identities: []types.DirectoryIdentitySnapshot{{
			ObjectGUID: "user-guid", ObjectSID: "S-1-5-21-1000", DN: "cn=Alice,dc=example,dc=test",
			SAMAccountName: "alice", UPN: "alice@example.test", DisplayName: "Alice",
			Email: "alice@example.test", PrimaryGroupSID: "S-1-5-21-513", Enabled: true,
		}},
		Groups: []types.DirectoryGroupSnapshot{
			{
				ObjectGUID:     "leaf-guid",
				ObjectSID:      "S-1-5-21-2001",
				DN:             "cn=Leaf,dc=example,dc=test",
				SAMAccountName: "leaf",
				DisplayName:    "Leaf",
				Email:          "leaf@example.test",
			},
			{
				ObjectGUID:  "parent-guid",
				ObjectSID:   "S-1-5-21-2002",
				DN:          "cn=Parent,dc=example,dc=test",
				DisplayName: "Parent",
			},
			{
				ObjectGUID:  "primary-guid",
				ObjectSID:   "S-1-5-21-513",
				DN:          "cn=Domain Users,dc=example,dc=test",
				DisplayName: "Domain Users",
			},
		},
		GroupEdges: []types.DirectoryGroupEdgeSnapshot{
			{ParentGroupObjectGUID: "parent-guid", ChildGroupObjectGUID: "leaf-guid"},
		},
		Memberships: []types.DirectoryMembershipSnapshot{
			{GroupObjectGUID: "leaf-guid", UserObjectGUID: "user-guid"},
		},
		StartedAt: time.Now().UTC().Add(-time.Second),
	}
}

func TestDirectorySnapshotRenameAndOUMoveKeepStableRowsSQLite(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	first := testDirectorySnapshot(directory.ID)
	_, err := directorySvc.ApplySnapshot(context.Background(), first)
	require.NoError(t, err)

	var originalIdentity types.DirectoryIdentity
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&originalIdentity).Error)
	require.NoError(
		t,
		directorySvc.LinkIdentity(context.Background(), originalIdentity.ID, "user-1"),
	)
	var originalGroup types.DirectoryGroup
	require.NoError(t, db.Where("object_guid = ?", "leaf-guid").First(&originalGroup).Error)
	require.Equal(t, "leaf", originalGroup.SAMAccountName)
	require.Equal(t, "leaf@example.test", originalGroup.Email)

	second := testDirectorySnapshot(directory.ID)
	second.Identities[0].DN = "cn=Alice Renamed,ou=Moved,dc=example,dc=test"
	second.Identities[0].SAMAccountName = "alice.renamed"
	second.Identities[0].DisplayName = "Alice Renamed"
	second.Groups[0].DN = "cn=Leaf Renamed,ou=Moved,dc=example,dc=test"
	second.Groups[0].SAMAccountName = "leaf-renamed"
	second.Groups[0].DisplayName = "Leaf Renamed"
	second.Groups[0].Email = "leaf-renamed@example.test"
	_, err = directorySvc.ApplySnapshot(context.Background(), second)
	require.NoError(t, err)

	var identities []types.DirectoryIdentity
	require.NoError(t, db.Where("directory_id = ?", directory.ID).Find(&identities).Error)
	require.Len(
		t,
		identities,
		1,
		"same objectGUID must not create a second identity after rename/OU move",
	)
	require.Equal(t, originalIdentity.ID, identities[0].ID)
	require.Equal(t, "cn=Alice Renamed,ou=Moved,dc=example,dc=test", identities[0].DN)
	require.NotNil(t, identities[0].UserID, "explicit account link must survive attribute updates")
	require.Equal(t, "user-1", *identities[0].UserID)

	var movedGroup types.DirectoryGroup
	require.NoError(t, db.Where("object_guid = ?", "leaf-guid").First(&movedGroup).Error)
	require.Equal(t, originalGroup.ID, movedGroup.ID)
	require.Equal(t, "leaf-renamed", movedGroup.SAMAccountName)
	require.Equal(t, "leaf-renamed@example.test", movedGroup.Email)
}

func TestDirectorySnapshotMarksDisabledAndMissingIdentitiesWithoutEmptyFailureRevocationSQLite(
	t *testing.T,
) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)

	disabled := testDirectorySnapshot(directory.ID)
	disabled.Identities[0].Enabled = false
	_, err = directorySvc.ApplySnapshot(context.Background(), disabled)
	require.NoError(t, err)
	var identity types.DirectoryIdentity
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&identity).Error)
	require.Equal(t, types.DirectoryObjectDisabled, identity.Status)

	missing := testDirectorySnapshot(directory.ID)
	missing.Identities = nil
	missing.Memberships = nil
	_, err = directorySvc.ApplySnapshot(context.Background(), missing)
	require.NoError(t, err)
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&identity).Error)
	require.Equal(t, types.DirectoryObjectOutOfScope, identity.Status)

	incomplete := testDirectorySnapshot(directory.ID)
	incomplete.Complete = false
	_, err = directorySvc.ApplySnapshot(context.Background(), incomplete)
	require.ErrorIs(t, err, appservice.ErrIncompleteDirectorySync)
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&identity).Error)
	require.Equal(
		t,
		types.DirectoryObjectOutOfScope,
		identity.Status,
		"failed snapshot must preserve prior identity state",
	)
}

func TestDirectorySnapshotRevokesSuspendedIdentityTokensAtomicallySQLite(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)
	var identity types.DirectoryIdentity
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&identity).Error)
	require.NoError(
		t,
		directorySvc.LinkIdentity(context.Background(), identity.ID, "directory-user"),
	)
	require.NoError(t, db.Omit("tenant_id").Create(&types.User{
		ID: "directory-user", Username: "directory-user", Email: "directory-user@example.test",
		PasswordHash: "hash", IsActive: true,
	}).Error)
	token := &types.AuthToken{
		ID: "directory-token", UserID: "directory-user", Token: "secret-token", TokenType: "access_token",
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	require.NoError(t, db.Create(token).Error)

	// Force the token update to fail and prove identity status/version roll back
	// with it rather than leaving an active session beside committed suspension.
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_directory_token_revoke
		BEFORE UPDATE OF is_revoked ON auth_tokens
		WHEN NEW.is_revoked = 1
		BEGIN SELECT RAISE(ABORT, 'forced token revoke failure'); END`).Error)
	disabled := testDirectorySnapshot(directory.ID)
	disabled.Identities[0].Enabled = false
	_, err = directorySvc.ApplySnapshot(context.Background(), disabled)
	require.Error(t, err)
	require.NoError(t, db.First(&identity, "id = ?", identity.ID).Error)
	require.Equal(t, types.DirectoryObjectActive, identity.Status)
	require.Equal(t, uint64(1), identity.SnapshotVersion)
	require.NoError(t, db.First(token, "id = ?", token.ID).Error)
	require.False(t, token.IsRevoked)

	require.NoError(t, db.Exec("DROP TRIGGER reject_directory_token_revoke").Error)
	_, err = directorySvc.ApplySnapshot(context.Background(), disabled)
	require.NoError(t, err)
	require.NoError(t, db.First(&identity, "id = ?", identity.ID).Error)
	require.Equal(t, types.DirectoryObjectDisabled, identity.Status)
	require.NoError(t, db.First(token, "id = ?", token.ID).Error)
	require.True(t, token.IsRevoked)
}

func TestDirectorySecurityConfigChangeRevokesLinkedTokensSQLite(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)
	var identity types.DirectoryIdentity
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&identity).Error)
	require.NoError(
		t,
		directorySvc.LinkIdentity(context.Background(), identity.ID, "directory-user"),
	)
	require.NoError(t, db.Omit("tenant_id").Create(&types.User{
		ID: "directory-user", Username: "directory-user", Email: "directory-user@example.test",
		PasswordHash: "hash", IsActive: true,
	}).Error)
	token := &types.AuthToken{
		ID: "config-token", UserID: "directory-user", Token: "config-secret-token", TokenType: "access_token",
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	require.NoError(t, db.Create(token).Error)

	reloaded, err := directorySvc.Get(context.Background(), directory.ID)
	require.NoError(t, err)
	reloaded.AllowedLoginFilter = "(department=security)"
	_, err = directorySvc.Update(context.Background(), reloaded)
	require.NoError(t, err)
	require.NoError(t, db.First(token, "id = ?", token.ID).Error)
	require.True(t, token.IsRevoked)
}

func TestDirectorySecurityConfigChangeRequiresFreshSnapshotSQLite(t *testing.T) {
	_, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)

	reloaded, err := directorySvc.Get(context.Background(), directory.ID)
	require.NoError(t, err)
	require.NotNil(t, reloaded.LastSuccessfulSyncAt)
	require.NotNil(t, reloaded.LastSyncAttemptAt)
	reloaded.AllowedLoginFilter = "(memberOf=cn=new-scope,dc=example,dc=test)"
	_, err = directorySvc.Update(context.Background(), reloaded)
	require.NoError(t, err)

	changed, err := directorySvc.Get(context.Background(), directory.ID)
	require.NoError(t, err)
	require.Equal(t, directory.ConfigVersion+1, changed.ConfigVersion)
	require.NotNil(
		t,
		changed.LastSuccessfulSyncAt,
		"last complete snapshot must remain available during grace period",
	)
	require.Nil(
		t,
		changed.LastSyncAttemptAt,
		"scheduler must consider the changed configuration immediately due",
	)
	require.NotEmpty(
		t,
		changed.LastSyncError,
		"new LDAP logins must wait for a snapshot under the new scope",
	)
}

func TestDirectoryRejectsSnapshotCollectedUnderOlderConfigurationSQLite(t *testing.T) {
	_, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	require.Equal(t, uint64(1), directory.ConfigVersion)
	stale := testDirectorySnapshot(directory.ID)
	stale.ExpectedConfigVersion = directory.ConfigVersion

	directory.GroupFilter = "(&(objectClass=group)(securityEnabled=TRUE))"
	updated, err := directorySvc.Update(context.Background(), directory)
	require.NoError(t, err)
	require.Equal(t, uint64(2), updated.ConfigVersion)

	_, err = directorySvc.ApplySnapshot(context.Background(), stale)
	require.ErrorIs(t, err, apprepo.ErrDirectoryConfigVersionChanged)
	reloaded, err := directorySvc.Get(context.Background(), directory.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(0), reloaded.SnapshotVersion)
	require.NotEmpty(t, reloaded.LastSyncError)
}

func TestLateSyncFailureCannotOverwriteNewerSuccessSQLite(t *testing.T) {
	_, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	first, err := directorySvc.ApplySnapshot(
		context.Background(),
		testDirectorySnapshot(directory.ID),
	)
	require.NoError(t, err)
	failedStarted := time.Now().UTC().Add(-time.Minute)
	_, err = directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)

	failedRun, err := directorySvc.RecordSyncFailure(
		context.Background(), directory.ID, "timeout", "old attempt failed", failedStarted,
		first.SyncRun.SnapshotVersion, directory.ConfigVersion, types.DirectorySyncTriggerScheduled,
	)
	require.NoError(t, err)
	require.Equal(t, types.DirectorySyncFailed, failedRun.Status)
	reloaded, err := directorySvc.Get(context.Background(), directory.ID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), reloaded.SnapshotVersion)
	require.Empty(
		t,
		reloaded.LastSyncError,
		"late failure must not poison a newer successful snapshot",
	)
	runs, err := directorySvc.ListSyncRuns(context.Background(), directory.ID, 0, 10)
	require.NoError(t, err)
	foundFailure := false
	for _, run := range runs {
		if run.ID == failedRun.ID {
			foundFailure = true
		}
	}
	require.True(t, foundFailure, "suppressed health update must still retain the failed audit run")
}

func TestDirectorySnapshotNestedPrimaryAndFailureIsAtomicSQLite(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)

	result, err := directorySvc.ApplySnapshot(
		context.Background(),
		testDirectorySnapshot(directory.ID),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.SyncRun.SnapshotVersion)
	require.Equal(t, 3, result.SyncRun.MembershipCount)

	var memberships []types.DirectoryGroupMembership
	require.NoError(t, db.Order("depth ASC, source ASC").Find(&memberships).Error)
	require.Len(t, memberships, 3)
	sources := map[types.DirectoryMembershipSource]bool{}
	for _, membership := range memberships {
		sources[membership.Source] = true
	}
	require.True(t, sources[types.DirectoryMembershipDirect])
	require.True(t, sources[types.DirectoryMembershipNested])
	require.True(t, sources[types.DirectoryMembershipPrimary])

	cycle := testDirectorySnapshot(directory.ID)
	cycle.GroupEdges = append(
		cycle.GroupEdges,
		types.DirectoryGroupEdgeSnapshot{
			ParentGroupObjectGUID: "leaf-guid",
			ChildGroupObjectGUID:  "parent-guid",
		},
	)
	_, err = directorySvc.ApplySnapshot(context.Background(), cycle)
	require.ErrorIs(t, err, appservice.ErrDirectoryGroupCycle)

	reloaded, err := directorySvc.Get(context.Background(), directory.ID)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(1),
		reloaded.SnapshotVersion,
		"failed snapshots must not replace the last successful version",
	)
	var count int64
	require.NoError(t, db.Model(&types.DirectoryGroupMembership{}).Count(&count).Error)
	require.Equal(t, int64(3), count, "failed snapshots must not revoke memberships")
}

func TestDirectoryLoginSnapshotUsesOnlyCurrentActiveSnapshotSQLite(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)
	repo := apprepo.NewDirectoryRepository(db)

	view, err := repo.GetLoginSnapshot(context.Background(), directory.ID, "user-guid")
	require.NoError(t, err)
	require.NotNil(t, view)
	require.NotNil(t, view.Identity)
	require.Equal(t, uint64(1), view.Directory.SnapshotVersion)
	require.ElementsMatch(
		t,
		[]string{"leaf-guid", "parent-guid", "primary-guid"},
		view.EffectiveGroupObjectGUIDs,
	)

	var identity types.DirectoryIdentity
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&identity).Error)
	staleGroup := types.DirectoryGroup{
		ID: "stale-group-id", DirectoryID: directory.ID, ObjectGUID: "stale-group-guid",
		ObjectSID: "S-1-5-21-9999", DN: "cn=Stale,dc=example,dc=test", DisplayName: "Stale",
		Status: types.DirectoryObjectActive, SnapshotVersion: 0, LastSeenAt: time.Now().UTC(),
	}
	require.NoError(t, db.Create(&staleGroup).Error)
	require.NoError(t, db.Create(&types.DirectoryGroupMembership{
		DirectoryID: directory.ID, GroupID: staleGroup.ID, IdentityID: identity.ID,
		Source: types.DirectoryMembershipDirect, SnapshotVersion: 0, CreatedAt: time.Now().UTC(),
	}).Error)
	view, err = repo.GetLoginSnapshot(context.Background(), directory.ID, "user-guid")
	require.NoError(t, err)
	require.ElementsMatch(
		t,
		[]string{"leaf-guid", "parent-guid", "primary-guid"},
		view.EffectiveGroupObjectGUIDs,
		"stale membership rows must not enter the login view",
	)

	require.NoError(t, db.Model(&types.DirectoryIdentity{}).Where("id = ?", identity.ID).
		Update("status", types.DirectoryObjectOutOfScope).Error)
	view, err = repo.GetLoginSnapshot(context.Background(), directory.ID, "user-guid")
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Nil(t, view.Identity, "an inactive identity must not be returned as login-eligible")
}

func TestGroupRoleRestrictedAuthorizationAndMembershipListingSQLite(t *testing.T) {
	db, directorySvc, groupSvc := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	_, err := directorySvc.ApplySnapshot(context.Background(), testDirectorySnapshot(directory.ID))
	require.NoError(t, err)

	var identity types.DirectoryIdentity
	require.NoError(t, db.Where("object_guid = ?", "user-guid").First(&identity).Error)
	require.NoError(t, directorySvc.LinkIdentity(context.Background(), identity.ID, "user-1"))
	var leaf, parent types.DirectoryGroup
	require.NoError(t, db.Where("object_guid = ?", "leaf-guid").First(&leaf).Error)
	require.NoError(t, db.Where("object_guid = ?", "parent-guid").First(&parent).Error)

	require.NoError(t, db.Create(&types.TenantMember{
		UserID: "user-1", TenantID: 9, Role: types.TenantRoleViewer,
		Status: types.TenantMemberStatusActive, JoinedAt: time.Now().UTC(),
	}).Error)
	require.NoError(
		t,
		groupSvc.UpsertTenantGroupRoleGrant(context.Background(), &types.TenantGroupRoleGrant{
			TenantID: 9, DirectoryGroupID: parent.ID, Role: types.TenantRoleContributor,
		}),
	)
	// Tenant 5 has no tenant_members row: it exists solely through the group
	// grant and must still appear in login membership enumeration.
	require.NoError(
		t,
		groupSvc.UpsertTenantGroupRoleGrant(context.Background(), &types.TenantGroupRoleGrant{
			TenantID: 5, DirectoryGroupID: parent.ID, Role: types.TenantRoleViewer,
		}),
	)

	effective, err := groupSvc.EffectiveTenantRole(
		context.Background(),
		"user-1",
		9,
		time.Now().UTC(),
	)
	require.NoError(t, err)
	require.True(t, effective.Member)
	require.Equal(
		t,
		types.TenantRoleContributor,
		effective.Role,
		"highest direct/group role must win",
	)
	require.NotEmpty(t, effective.GroupMatches)

	roles, err := groupSvc.ListEffectiveTenantRoles(
		context.Background(),
		"user-1",
		time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Len(t, roles, 2)
	require.Equal(t, uint64(5), roles[0].TenantID)
	require.Equal(t, types.TenantRoleViewer, roles[0].Role)
	require.Equal(t, uint64(9), roles[1].TenantID)

	require.NoError(
		t,
		groupSvc.SetResourceAccessPolicy(context.Background(), &types.ResourceAccessPolicy{
			TenantID: 9, ResourceType: types.GroupResourceTypeKnowledgeBase,
			ResourceID: "kb-1", Mode: types.ResourceAccessRestricted,
		}),
	)
	require.NoError(
		t,
		groupSvc.UpsertResourceGroupGrant(context.Background(), &types.ResourceGroupGrant{
			TenantID: 9, ResourceType: types.GroupResourceTypeKnowledgeBase, ResourceID: "kb-1",
			DirectoryGroupID: leaf.ID, Permission: types.ResourcePermissionRead,
		}),
	)

	userCtx := types.WithCaller(
		context.Background(),
		types.Caller{TenantID: 9, UserID: "user-1", Role: types.TenantRoleContributor},
	)
	userCtx = types.WithPrincipal(
		userCtx,
		types.Principal{Type: types.PrincipalWebUser, ID: "user-1"},
	)
	require.NoError(
		t,
		groupSvc.Authorize(
			userCtx,
			9,
			types.GroupResourceTypeKnowledgeBase,
			"kb-1",
			types.ResourceActionRead,
		),
	)
	require.ErrorIs(
		t,
		groupSvc.Authorize(
			userCtx,
			9,
			types.GroupResourceTypeKnowledgeBase,
			"kb-1",
			types.ResourceActionEdit,
		),
		appservice.ErrResourceAccessDenied,
	)

	apiCtx := types.WithTenantAPIKeyScope(userCtx, types.TenantAPIKeyScope{FullAccess: true})
	require.ErrorIs(
		t,
		groupSvc.Authorize(
			apiCtx,
			9,
			types.GroupResourceTypeKnowledgeBase,
			"kb-1",
			types.ResourceActionRead,
		),
		appservice.ErrResourceAccessDenied,
	)

	version, err := apprepo.NewGroupAccessRepository(db).
		GetPermissionVersion(context.Background(), 9)
	require.NoError(t, err)
	require.GreaterOrEqual(t, version, uint64(3))
}

func TestDirectoryRunSyncLocksFetchAndApply(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := directorySvc.RunSync(
			context.Background(),
			directory.ID,
			func(context.Context) (*types.DirectorySnapshot, error) {
				close(started)
				<-release
				return testDirectorySnapshot(directory.ID), nil
			},
		)
		done <- err
	}()
	<-started
	// A separately constructed service has a separate process-local mutex;
	// the database lease must still reject the overlapping collection.
	otherProcess := appservice.NewDirectoryService(apprepo.NewDirectoryRepository(db))
	_, err := otherProcess.RunSync(
		context.Background(),
		directory.ID,
		func(context.Context) (*types.DirectorySnapshot, error) {
			return nil, errors.New("must not run")
		},
	)
	require.ErrorIs(t, err, appservice.ErrDirectorySyncInProgress)
	close(release)
	require.NoError(t, <-done)

	_, err = otherProcess.RunSync(
		context.Background(),
		directory.ID,
		func(context.Context) (*types.DirectorySnapshot, error) {
			return testDirectorySnapshot(directory.ID), nil
		},
	)
	require.NoError(t, err, "released lease must permit the next process")
}

func TestDirectoryRunSyncRecoversExpiredDatabaseLease(t *testing.T) {
	db, directorySvc, _ := setupDirectoryAccessSQLite(t)
	directory := createTestDirectory(t, directorySvc)
	repo := apprepo.NewDirectoryRepository(db)
	acquired, err := repo.TryAcquireSyncLease(
		context.Background(),
		directory.ID,
		"crashed-worker",
		time.Minute,
	)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, db.Model(&types.Directory{}).Where("id = ?", directory.ID).
		Update("sync_lease_expires_at", time.Now().UTC().Add(-time.Minute)).Error)

	recoveredProcess := appservice.NewDirectoryService(apprepo.NewDirectoryRepository(db))
	_, err = recoveredProcess.RunSync(
		context.Background(),
		directory.ID,
		func(context.Context) (*types.DirectorySnapshot, error) {
			return testDirectorySnapshot(directory.ID), nil
		},
	)
	require.NoError(t, err, "expired crash lease must be reclaimable")
}
