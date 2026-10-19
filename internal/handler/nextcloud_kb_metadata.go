package handler

import (
	"context"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

func ConfigureKnowledgeBaseSourceMetadataGuard(
	h *KnowledgeBaseHandler,
	guard *access.NextcloudPublicationGuard,
	pairs *repository.NextcloudSourcePairingRepository,
) {
	h.sourceMetadataGuard = guard
	h.sourceMetadataPairs = pairs
}

func (h *KnowledgeBaseHandler) checkSourceKBMetadata(ctx context.Context, kb *types.KnowledgeBase) (bool, error) {
	if kb == nil {
		return false, access.ErrNextcloudPublicationDenied
	}
	if h.dataSourceRepo == nil {
		if kb.EverHadNextcloudSource {
			return true, access.ErrNextcloudPublicationUnavailable
		}
		return false, nil
	}
	sources, err := h.dataSourceRepo.FindByKnowledgeBase(ctx, kb.ID)
	if err != nil {
		return kb.EverHadNextcloudSource, access.ErrNextcloudPublicationUnavailable
	}
	sourceCount := 0
	for _, source := range sources {
		if source == nil || source.Type != types.ConnectorTypeNextcloud {
			continue
		}
		sourceCount++
		if source.TenantID != kb.TenantID || source.KnowledgeBaseID != kb.ID || h.sourceMetadataGuard == nil ||
			h.sourceMetadataPairs == nil {
			return true, access.ErrNextcloudPublicationDenied
		}
		pair, actual, err := h.sourceMetadataPairs.ActiveSourcePairingForDataSource(ctx, kb.TenantID, source.ID)
		if err != nil || actual == nil {
			return true, access.ErrNextcloudPublicationDenied
		}
		closed, err := h.sourceMetadataPairs.ControlsClosed(ctx, pair.InstanceID)
		if err != nil || closed {
			return true, access.ErrNextcloudPublicationDenied
		}
		if err := h.sourceMetadataGuard.CheckSourceRoot(ctx, actual, pair.InstanceID, pair.BindingID); err != nil {
			return true, err
		}
	}
	if sourceCount > 1 || kb.EverHadNextcloudSource && sourceCount == 0 {
		return true, access.ErrNextcloudPublicationDenied
	}
	return sourceCount > 0, nil
}

func sourceKBStatsUnverified(response interface{}) interface{} {
	// Return only whitelisted source metadata even when ordinary response
	// serialization falls back to the original KB struct.
	safe := map[string]interface{}{}
	if row, ok := response.(map[string]interface{}); ok {
		for _, key := range []string{"id", "tenant_id", "name", "type", "is_pinned", "pin_order"} {
			if value, exists := row[key]; exists {
				safe[key] = value
			}
		}
	} else if kb, ok := response.(*types.KnowledgeBase); ok && kb != nil {
		safe["id"] = kb.ID
		safe["tenant_id"] = kb.TenantID
		safe["name"] = kb.Name
		safe["type"] = kb.Type
	}
	safe["knowledge_count"] = nil
	safe["chunk_count"] = nil
	safe["processing_count"] = nil
	safe["is_processing"] = nil
	safe["source_statistics_state"] = "unverified"
	safe["nextcloud_managed"] = true
	safe["my_permission"] = "viewer"
	return safe
}

// ordinaryKBMetadataAllowed excludes managed source KBs from manual move and
// organization-share discovery. Those routes cannot carry the current Files
// authorization proof; source KBs are discovered through their scoped entry.
// This classification uses local inventory and makes no per-row network calls.
func (h *KnowledgeBaseHandler) ordinaryKBMetadataAllowed(ctx context.Context, kb *types.KnowledgeBase) bool {
	if kb == nil || kb.EverHadNextcloudSource {
		return false
	}
	if h.dataSourceRepo == nil {
		return true
	}
	sources, err := h.dataSourceRepo.FindByKnowledgeBase(ctx, kb.ID)
	if err != nil {
		return false
	}
	for _, source := range sources {
		if source != nil && source.Type == types.ConnectorTypeNextcloud {
			return false
		}
	}
	return true
}

func ConfigureOrganizationSourceMetadataGuard(h *OrganizationHandler, kb *KnowledgeBaseHandler) {
	if h != nil {
		h.sourceMetadata = kb
	}
}
