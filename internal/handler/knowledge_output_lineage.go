package handler

import (
	"context"
	stderrors "errors"
	"io"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

func bindKnowledgeOutputFile(
	ctx context.Context,
	knowledge *types.Knowledge,
	file io.ReadCloser,
) (io.ReadCloser, error) {
	guard := service.KnowledgeOutputGuardFromContext(ctx)
	if guard == nil {
		return file, nil
	}
	snapshot, err := guard.BindFile(ctx, knowledge, file)
	if err != nil {
		return nil, knowledgeOutputHTTPError(err)
	}
	return snapshot, nil
}

// ConfigureKnowledgeHandlerOutputLineage attaches mandatory original material
// checks to the production composition. It must be invoked with the actual
// origin store and current lineage guard before these paths are exposed.
func ConfigureKnowledgeHandlerOutputLineage(h *KnowledgeHandler, authorizer *service.KnowledgeOutputAuthorizer) {
	if h != nil {
		h.outputAuthorizer = authorizer
	}
}

func knowledgeOutputHTTPError(err error) error {
	if err == nil {
		return nil
	}
	if stderrors.Is(err, access.ErrNextcloudPublicationDenied) ||
		stderrors.Is(err, repository.ErrKnowledgeInputChanged) ||
		stderrors.Is(err, types.ErrSourceLineageUnknown) ||
		stderrors.Is(err, types.ErrSourceLineageInvalid) ||
		stderrors.Is(err, types.ErrSourceLineageLimit) ||
		stderrors.Is(err, gorm.ErrRecordNotFound) {
		return errors.NewForbiddenError("Current source and original material access is required")
	}
	return errors.NewServiceUnavailableError("Cannot verify original knowledge material")
}
