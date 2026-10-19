package interfaces

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
)

// MessageSourceLineageVerifier is mandatory at generated message read/live
// boundaries. Missing capabilities are unavailable, never a legacy exemption.
type MessageSourceLineageVerifier interface {
	CheckMessagePublication(context.Context, *types.Message) error
	CheckLiveMessagePublication(context.Context, *types.Message) error
}

// MessageSourceLineageBatchVerifier authorizes original committed parents as
// one boundary while retaining every parent's Agent/KB scope. Unlike a public
// raw-user reader, it includes and requires the user parent's complete proof.
// A loop over the single-message verifier is not an equivalent capability.
type MessageSourceLineageBatchVerifier interface {
	CheckMessagePublications(context.Context, []*types.Message) error
}
