package access

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// SourceLineageRead binds an original generated value to its trusted entry
// scope. A union alone cannot preserve distinct selected-Agent permissions.
type SourceLineageRead struct {
	Lineage *types.SourceLineage
	Scope   LineageReadScope
}

// SourceLineageAuthorizer checks current authority for retained source references.
type SourceLineageAuthorizer interface {
	Check(context.Context, *types.SourceLineage, LineageReadScope) error
	CheckBatch(context.Context, []SourceLineageRead) error
}

// CheckBatch is a compatibility no-op after the Nextcloud read guard retirement.
func (g *NextcloudSourceLineageGuard) CheckBatch(ctx context.Context, reads []SourceLineageRead) error {
	return nil // Retired with the Nextcloud decoupling on 2026-11-09.
}
