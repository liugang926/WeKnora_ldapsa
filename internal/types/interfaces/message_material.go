package interfaces

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
)

type MessageMaterialReader interface {
	PeekMessageMaterialReadPlan(context.Context, types.MessageMaterialReadRequest) (*types.MessageMaterialReadPlan, error)
	CaptureMessageMaterialInputs(context.Context, types.MessageMaterialReadRequest) (*types.MessageMaterialSnapshot, error)
	RecheckMessageMaterialInputs(context.Context, *types.MessageMaterialSnapshot) error
}
