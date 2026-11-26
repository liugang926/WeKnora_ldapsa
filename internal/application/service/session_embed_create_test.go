package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAnonymousEmbedSessionCreation(t *testing.T) {
	svc, db := newTestSessionService(t)
	require.NoError(t, db.AutoMigrate(&types.User{}))
	svc.groupAccess = NewGroupAccessService(nil)
	ConfigureGroupAccessUserEligibility(svc.groupAccess, &userService{userRepo: repository.NewUserRepository(db)})
	ch := &types.EmbedChannel{ID: "channel", TenantID: 7, Enabled: true}
	base := testSessionScopeContext(ch.TenantID, "embed-channel")
	ctx := context.WithValue(base, types.EmbedChannelContextKey, ch)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalEmbedChannel, ID: "7:channel"})
	ctx = types.WithPrincipalAuthSource(ctx, types.AuthSourceEmbed)
	marker := EmbedSessionDescription(ch.ID)
	create := func(ctx context.Context, tenantID uint64, description string) (*types.Session, error) {
		return svc.CreateSession(ctx, &types.Session{TenantID: tenantID, Description: description})
	}

	session, err := create(ctx, ch.TenantID, marker)
	require.NoError(t, err)
	row, err := svc.GetSessionByID(ctx, ch.TenantID, session.ID)
	require.NoError(t, err)
	require.Equal(t, marker, row.Description)
	require.Equal(t, types.EmbedSessionPrincipal(ch.TenantID, ch.ID, row.ID).StorageID(), row.UserID)
	require.Equal(t, types.AuthSourceEmbed, row.OwnerAuthSource)

	_, err = create(ctx, 8, marker)
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	_, err = create(ctx, 7, EmbedSessionDescription("other"))
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	_, err = create(types.WithPrincipalAuthSource(ctx, types.AuthSourceWeb), 7, marker)
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	otherChannel := types.Principal{Type: types.PrincipalEmbedChannel, ID: "7:other"}
	_, err = create(types.WithPrincipal(ctx, otherChannel), 7, marker)
	require.ErrorIs(t, err, ErrResourceAccessDenied)
	_, err = create(base, 7, marker)
	require.ErrorContains(t, err, "load executing user")

	// A failed bind must not leave an unowned row accessible through legacy session reads.
	require.NoError(t, db.Exec(`CREATE TRIGGER reject_bind BEFORE UPDATE OF user_id ON sessions
		BEGIN SELECT RAISE(ABORT, 'binding failed'); END`).Error)
	_, err = create(ctx, 7, marker)
	require.ErrorContains(t, err, "binding failed")
	var count int64
	require.NoError(t, db.Model(&types.Session{}).Count(&count).Error)
	require.EqualValues(t, 1, count)
}
