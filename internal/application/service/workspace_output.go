package service

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

// Workspace permission is checked against the output's selected KB. The
// individual source proofs are retained as provenance, not as extra read ACLs.
type workspaceOutputChecker struct {
	guard *access.NextcloudSourceLineageGuard
}

func (c workspaceOutputChecker) Check(
	ctx context.Context, _ *types.SourceLineage, scope access.LineageReadScope,
) error {
	if len(scope.Targets) == 0 && scope.AgentID == "" {
		return nil
	}
	return c.guard.CheckWorkspaceRead(ctx, scope)
}
