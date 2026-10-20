package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// BeginImportOutput captures the original inputs used to display FAQ import progress.
func (a *FAQOutputAuthorizer) BeginImportOutput(
	ctx context.Context,
	request types.FAQImportOutputReadRequest,
) (*FAQOutputGuard, error) {
	if a == nil || a.store == nil || a.checker == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	actual, err := a.store.CaptureFAQImportOutputInputs(ctx, request)
	if err != nil {
		return nil, err
	}
	snapshot, err := cloneCollection(actual)
	if err != nil {
		return nil, err
	}
	guard := &FAQOutputGuard{authorizer: a, importOutput: snapshot}
	return guard, guard.Recheck(ctx)
}

// ProgressReadScope resolves the current source scope of FAQ import progress.
func (a *FAQOutputAuthorizer) ProgressReadScope(ctx context.Context, task string) (types.KnowledgeSearchScope, error) {
	if a == nil || a.store == nil {
		return types.KnowledgeSearchScope{}, access.ErrNextcloudPublicationUnavailable
	}
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 {
		return types.KnowledgeSearchScope{}, types.ErrSourceLineageUnknown
	}
	return a.store.PeekFAQImportProgressScope(ctx, tenant, task)
}

// ProgressReadPlan plans original input reads for FAQ import progress.
func (a *FAQOutputAuthorizer) ProgressReadPlan(
	ctx context.Context,
	task string,
) (*types.FAQImportOutputReadPlan, error) {
	if a == nil || a.store == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	return a.store.PeekFAQImportOutputReadPlan(
		ctx,
		types.FAQImportOutputReadRequest{TenantID: tenant, TaskID: task, Kind: "progress"},
	)
}

func (g *FAQOutputGuard) recheckImportOutput(ctx context.Context) error {
	return checkFAQImportOutputReceipt(ctx, g.authorizer.store, g.authorizer.checker, g.importOutput)
}

func checkFAQImportOutputReceipt(
	ctx context.Context,
	reader interfaces.FAQImportOutputInputReader,
	checker knowledgeOutputLineageChecker,
	snapshot *types.FAQImportOutputSnapshot,
) error {
	if snapshot == nil || snapshot.SourceLineage.RequireComplete() != nil {
		return types.ErrSourceLineageUnknown
	}
	for round := 0; round < 2; round++ {
		if err := checkFAQOriginalKnowledgeScopes(ctx, checker, snapshot.OriginalKnowledgeInputs); err != nil {
			return err
		}
		lineage, err := types.UnionSourceLineage(snapshot.SourceLineage)
		if err != nil {
			return err
		}
		if err := checker.Check(
			ctx,
			lineage,
			access.LineageReadScope{
				Targets: types.SearchTargets{
					&types.SearchTarget{
						Type:            types.SearchTargetTypeKnowledgeBase,
						TenantID:        snapshot.Request.TenantID,
						KnowledgeBaseID: snapshot.Request.KnowledgeBaseID,
					},
				},
			},
		); err != nil {
			return err
		}
		copied, err := cloneCollection(snapshot)
		if err != nil {
			return err
		}
		if err := reader.RecheckFAQImportOutputInputs(ctx, copied); err != nil {
			return err
		}
	}
	return nil
}

// ImportProgress returns progress projected from the retained original inputs.
func (g *FAQOutputGuard) ImportProgress() (*types.FAQImportProgress, error) {
	if g == nil || g.importOutput == nil || g.importOutput.Progress == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	return cloneCollection(g.importOutput.Progress)
}

func (s *knowledgeService) getAdmittedFAQImportProgress(
	ctx context.Context,
	taskID string,
) (*types.FAQImportProgress, error) {
	tenant, ok := types.TenantIDFromContext(ctx)
	if !ok || tenant == 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	authorizer := &FAQOutputAuthorizer{s.faqInputs, s.faqLineageChecker}
	guard, err := authorizer.BeginImportOutput(
		ctx,
		types.FAQImportOutputReadRequest{TenantID: tenant, TaskID: taskID, Kind: "progress"},
	)
	if err != nil {
		return nil, err
	}
	progress, err := guard.ImportProgress()
	if err != nil {
		return nil, err
	}
	// The worker/toggle producer seals enriched fields when it writes them.
	// A new metadata read cannot authenticate legacy fields or another job.
	return progress, guard.Bind(ctx, progress)
}

func applyFAQImportResultToProgress(progress *types.FAQImportProgress, result *types.FAQImportResult) {
	progress.SuccessCount = result.SuccessCount
	progress.FailedCount = result.FailedCount
	progress.PartialFailedCount = result.PartialFailedCount
	progress.SkippedCount = result.SkippedCount
	progress.MergedCount = result.MergedCount
	progress.AddedCount = result.AddedCount
	progress.ImportMode = result.ImportMode
	progress.ImportedAt = result.ImportedAt
	progress.DisplayStatus = result.DisplayStatus
	progress.ProcessingTime = result.ProcessingTime
	if result.FailedEntriesURL != "" {
		progress.FailedEntriesURL = result.FailedEntriesURL
	}
}
