package middleware

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	service "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type schemaTestDirectoryRuntime struct {
	interfaces.DirectoryRuntimeService
}

func (schemaTestDirectoryRuntime) GetStatus(context.Context) (*types.DirectoryHealth, error) {
	return &types.DirectoryHealth{Enabled: false}, nil
}

func authSchemaRoleFixture(t *testing.T, current bool) (*gorm.DB, interfaces.GroupAccessService) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "role-schema.db")), &gorm.Config{DisableForeignKeyConstraintWhenMigrating: true})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.TenantMember{}, &types.Directory{}, &types.DirectoryIdentity{}, &types.DirectoryGroup{}, &types.DirectoryGroupMembership{}, &types.TenantGroupRoleGrant{}, &types.ResourceAccessPolicy{}))
	if current {
		require.NoError(t, db.AutoMigrate(&types.NextcloudOnboardingOperation{}))
		require.NoError(t, db.Exec("CREATE TABLE sessions(id TEXT PRIMARY KEY, nextcloud_scoped BOOLEAN NOT NULL DEFAULT FALSE)").Error)
		// The independent session marker remains after the source ledger is lost.
		require.NoError(t, db.Migrator().DropTable(&types.NextcloudOnboardingOperation{}))
	} else {
		require.NoError(t, db.Exec("CREATE TABLE sessions(id TEXT PRIMARY KEY)").Error)
	}
	return db, service.NewGroupAccessService(repo.NewGroupAccessRepository(db))
}

func TestResolveTenantRoleActualMissingOnboardingLedgerCannotPromoteOrFailOpen(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, enforced := range []bool{false, true} {
			for _, home := range []bool{false, true} {
				t.Run("disabled="+boolString(disabled)+"/rbac="+boolString(enforced)+"/home="+boolString(home), func(t *testing.T) {
					_, access := authSchemaRoleFixture(t, true)
					if disabled {
						service.ConfigureGroupAccessDirectoryRuntime(access, schemaTestDirectoryRuntime{})
					}
					user := &types.User{ID: "source-reader", TenantID: 8}
					if home {
						user.TenantID = 7
					}
					_, err := access.EffectiveTenantRole(context.Background(), user.ID, 7, time.Now())
					require.ErrorIs(t, err, repo.ErrNextcloudOnboardingDenied)
					members := newFakeMemberService()
					role, ok := resolveTenantRole(context.Background(), members, user, 7, false, cfgWithRBAC(enforced), access)
					require.False(t, ok)
					require.Empty(t, role)
					require.Empty(t, members.addCalls, "a missing source ledger must not create an orphan Owner")
					if disabled {
						_, err = access.EffectivePermission(context.Background(), 7, types.GroupResourceTypeKnowledgeBase, "source-kb", types.ResourceActionRead, time.Now())
						require.ErrorIs(t, err, repo.ErrNextcloudOnboardingDenied, "missing managed-KB inventory must not enable legacy inheritance")
					}
				})
			}
		}
	}
}

func TestResolveTenantRoleActualMissingOnboardingLedgerPreservesDirectMember(t *testing.T) {
	db, access := authSchemaRoleFixture(t, true)
	require.NoError(t, db.Create(&types.TenantMember{UserID: "ordinary-member", TenantID: 7, Role: types.TenantRoleContributor, Status: types.TenantMemberStatusActive}).Error)
	members := newFakeMemberService()
	role, ok := resolveTenantRole(context.Background(), members, &types.User{ID: "ordinary-member", TenantID: 7}, 7, false, cfgWithRBAC(false), access)
	require.True(t, ok)
	require.Equal(t, types.TenantRoleContributor, role)
	require.Empty(t, members.addCalls)
}

func TestResolveTenantRoleActualLegacySchemaKeepsRolloutBehavior(t *testing.T) {
	for _, home := range []bool{false, true} {
		t.Run("home="+boolString(home), func(t *testing.T) {
			_, access := authSchemaRoleFixture(t, false)
			user := &types.User{ID: "legacy-user", TenantID: 8}
			expected := types.TenantRoleAdmin
			if home {
				user.TenantID = 7
				expected = types.TenantRoleOwner
			}
			members := newFakeMemberService()
			role, ok := resolveTenantRole(context.Background(), members, user, 7, false, cfgWithRBAC(false), access)
			require.True(t, ok)
			require.Equal(t, expected, role)
			if home {
				require.Len(t, members.addCalls, 1)
			} else {
				require.Empty(t, members.addCalls)
			}
		})
	}
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
