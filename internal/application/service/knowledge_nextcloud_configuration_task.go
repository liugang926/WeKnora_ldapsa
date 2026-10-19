package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// Called before a fresh task reads configuration or restores a body manifest.
// An ordinary task preserves its context. Each NC consumer owns a distinct
// guard; finishing one task cannot close a caller's sibling configuration read.
func beginNextcloudTaskConfigurationRead(ctx context.Context, reader any) (context.Context, func(), error) {
	noop := func() {}
	if !repository.NextcloudBuildConfigurationReadRequired(ctx) {
		return ctx, noop, nil
	}
	beginner, ok := reader.(interface {
		BeginNextcloudBuildConfigurationRead(context.Context) (context.Context, func(), error)
	})
	if !ok {
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	current, closeRead, err := beginner.BeginNextcloudBuildConfigurationRead(ctx)
	if err != nil {
		if closeRead != nil {
			closeRead()
		}
		return ctx, noop, err
	}
	if current == nil || closeRead == nil {
		if closeRead != nil {
			closeRead()
		}
		return ctx, noop, types.ErrSourceLineageUnknown
	}
	return current, closeRead, nil
}
