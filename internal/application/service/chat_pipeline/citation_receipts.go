package chatpipeline

import (
	"context"
	"sync"

	"github.com/Tencent/WeKnora/internal/types"
)

type citationReceiptKey struct{}

// Only the trusted augmentation reader can register a body in this turn.
// A pointer or a citation ID loaded from history has no authority. The digest
// additionally detects edits to the originally hydrated material.
type citationReceipts struct{ bodies sync.Map }

func citationDigest(parent, child *types.SearchResult) string {
	if parent == nil || child == nil || child.SourceLineage.RequireComplete() != nil ||
		len(child.CitationSources) != 0 {
		return ""
	}
	return types.GraphDigest(struct {
		ParentID, ParentDoc, ParentKB string
		Child                         *types.SearchResult
		Lineage                       *types.SourceLineage
	}{parent.ID, parent.KnowledgeID, parent.KnowledgeBaseID, child, child.SourceLineage})
}

func recordCitation(ctx context.Context, parent, child *types.SearchResult) {
	if receipts, _ := ctx.Value(citationReceiptKey{}).(*citationReceipts); receipts != nil {
		if d := citationDigest(parent, child); d != "" {
			receipts.bodies.Store(child, d)
		}
	}
}

func verifiedCitation(ctx context.Context, parent, child *types.SearchResult) bool {
	receipts, _ := ctx.Value(citationReceiptKey{}).(*citationReceipts)
	if receipts == nil {
		return false
	}
	d, ok := receipts.bodies.Load(child)
	return ok && d == citationDigest(parent, child)
}
