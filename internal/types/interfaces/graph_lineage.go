package interfaces

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

// KnowledgeGraphSnapshotReader accounts the actual graph records and source
// bodies under current caller scope and server-acquired leases. Names/chunk IDs
// are hints; model/client JSON cannot create private writer receipts.
type KnowledgeGraphSnapshotReader interface {
	ReadKnowledgeGraphSnapshot(context.Context, types.SearchTargets, types.NameSpace, []string) (*types.KnowledgeGraphInputSnapshot, error)
}
