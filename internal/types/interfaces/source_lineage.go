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
