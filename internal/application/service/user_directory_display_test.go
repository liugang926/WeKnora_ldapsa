package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type displayNameUserRepo struct {
	interfaces.UserRepository
	users map[string]*types.User
}

func (r *displayNameUserRepo) GetUsersByIDs(
	_ context.Context,
	ids []string,
) (map[string]*types.User, error) {
	result := make(map[string]*types.User, len(ids))
	for _, id := range ids {
		if user := r.users[id]; user != nil {
			userCopy := *user
			result[id] = &userCopy
		}
	}
	return result, nil
}

type displayNameDirectoryRepo struct {
	interfaces.DirectoryRepository
	identities []*types.DirectoryIdentity
}

func (r *displayNameDirectoryRepo) GetIdentitiesByUserIDs(
	_ context.Context,
	_ []string,
) ([]*types.DirectoryIdentity, error) {
	return r.identities, nil
}

func TestGetUsersByIDsProjectsActiveDirectoryDisplayName(t *testing.T) {
	userID := "user-1"
	svc := &userService{
		userRepo: &displayNameUserRepo{users: map[string]*types.User{
			userID: {ID: userID, Username: "zhangsan", Email: "zhangsan@example.com"},
		}},
		directoryRepo: &displayNameDirectoryRepo{identities: []*types.DirectoryIdentity{{
			ID:          "identity-1",
			DirectoryID: "ad-1",
			DisplayName: "  张三  ",
			UserID:      &userID,
			Status:      types.DirectoryObjectActive,
		}}},
	}

	users, err := svc.GetUsersByIDs(context.Background(), []string{userID})
	require.NoError(t, err)
	require.Equal(t, "张三", users[userID].DisplayName)
	require.Equal(t, "zhangsan", users[userID].Username)
}
