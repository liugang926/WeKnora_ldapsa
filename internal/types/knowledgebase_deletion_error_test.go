package types

import (
	"errors"
	"fmt"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/stretchr/testify/require"
)

func TestNextcloudKnowledgeBaseDeletionBlockedErrorPreservesPublicContractAndCause(t *testing.T) {
	cause := errors.New("nextcloud source must be unpaired before deleting its knowledge base")
	err := NewNextcloudKnowledgeBaseDeletionBlockedError(cause)
	const legacy = "Nextcloud source must be unpaired before deleting its knowledge base"
	require.Equal(t, legacy, err.Error())
	require.Equal(t, legacy, err.PublicMessage())
	require.Equal(t, legacy, apperrors.PublicMessage(err))
	require.Same(t, cause, errors.Unwrap(err))
	require.ErrorIs(t, err, cause)
	require.Equal(t, "nextcloud source must be unpaired before deleting its knowledge base", cause.Error())
	wrapped := fmt.Errorf("deletion context: %w", err)
	require.ErrorIs(t, wrapped, err)
	require.ErrorIs(t, wrapped, cause)
	require.Equal(t, "deletion context: "+legacy, wrapped.Error())
	_, isApp := apperrors.IsAppError(err)
	require.False(t, isApp, "domain refusal must not introduce new HTTP status/code semantics")
}

func TestNextcloudKnowledgeBaseDeletionBlockedErrorIsNilSafe(t *testing.T) {
	var empty *NextcloudKnowledgeBaseDeletionBlockedError
	require.Empty(t, empty.Error())
	require.Empty(t, empty.PublicMessage())
	require.Nil(t, empty.Unwrap())
	withoutCause := NewNextcloudKnowledgeBaseDeletionBlockedError(nil)
	require.Equal(t, "Nextcloud source must be unpaired before deleting its knowledge base", withoutCause.Error())
	require.Nil(t, errors.Unwrap(withoutCause))
}
