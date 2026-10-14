package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// NativeVectorCandidateRetriever is an optional, positive backend capability.
// No caller may emulate this port by falling back to Retrieve, which loads
// original index text before ownership/source/retention admission.
type NativeVectorCandidateRetriever interface {
	RetrieveVectorCandidates(context.Context, types.VectorCandidateParams) ([]*types.VectorIndexCandidate, error)
}
