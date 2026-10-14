package repository_test

import (
	"context"
	"testing"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNextcloudOnboardingOperationNotFoundIsOpaqueAndScoped(t *testing.T) {
	db, store, credential := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB(uuid.NewString())
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, credential, onboardingRequest(credential, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	_, err = store.Operation(ctx, credential, uuid.NewString())
	require.ErrorIs(t, err, repo.ErrNextcloudOnboardingNotFound)
	for _, field := range []string{"credential", "tenant", "instance"} {
		other := credential
		switch field {
		case "credential":
			other.ID = "different-credential"
		case "tenant":
			other.TenantID++
		case "instance":
			other.InstanceID = "different-instance"
		}
		_, err = store.Operation(ctx, other, op.OperationID)
		require.ErrorIs(t, err, repo.ErrNextcloudOnboardingNotFound, field)
	}
	current, err := store.Operation(ctx, credential, op.OperationID)
	require.NoError(t, err)
	require.Equal(t, op.OperationID, current.OperationID)
}
