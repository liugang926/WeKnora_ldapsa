package handler

import (
	"context"
	stderrors "errors"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
)

// checkKnowledgePublication is called after the ordinary KB grant and before
// any source-derived metadata, chunks, or file bytes leave an HTTP handler.
// A missing guard is safe for unrelated knowledge but fails closed for a
// Nextcloud document, as enforced by CheckKnowledge.
func checkKnowledgePublication(
	ctx context.Context, guard *access.NextcloudPublicationGuard, knowledge *types.Knowledge,
) error {
	err := guard.CheckKnowledge(ctx, knowledge)
	if err == nil {
		return nil
	}
	if stderrors.Is(err, access.ErrNextcloudPublicationUnavailable) {
		return errors.NewServiceUnavailableError("Cannot verify current Nextcloud file access")
	}
	return errors.NewForbiddenError("Current Nextcloud file access is required")
}
