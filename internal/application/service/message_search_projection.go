package service

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

// The grouped Q&A strings are a new projection of the admitted message rows.
// Keep its exact body attached to the request boundary too: a later change to
// AnswerContent cannot evade the guards by leaving the original rows intact.
func holdHistoricalSearchProjection(ctx context.Context, result *types.MessageSearchResult) error {
	if result == nil {
		return access.ErrNextcloudPublicationDenied
	}
	if len(result.Items) == 0 {
		return nil
	}
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return access.ErrNextcloudPublicationUnavailable
	}
	body, err := historicalSearchProjectionDigest(result)
	if err != nil {
		return err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.Closed || slot.Failed != nil {
		return access.ErrNextcloudPublicationUnavailable
	}
	slot.SearchOutputs = append(slot.SearchOutputs, result)
	slot.SearchOutputBodies = append(slot.SearchOutputBodies, body)
	return nil
}

func historicalSearchProjectionDigest(result *types.MessageSearchResult) (string, error) {
	body, err := json.Marshal(result)
	return string(body), err
}
