package access

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
)

// CheckAnonymousPublication checks whether an existing anonymous grant may
// cover this document. It only uses local provenance and ownership records.
func (g *NextcloudPublicationGuard) CheckAnonymousPublication(ctx context.Context, knowledge *types.Knowledge) error {
	if knowledge == nil || knowledge.DeletedAt.Valid || knowledge.Channel == types.ConnectorTypeNextcloud {
		return ErrNextcloudPublicationDenied
	}
	if len(knowledge.Metadata) == 0 {
		return nil
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(knowledge.Metadata, &metadata); err != nil || metadata == nil {
		return ErrNextcloudPublicationDenied
	}
	for _, key := range []string{"nextcloud_instance_id", "nextcloud_binding_id", "nextcloud_file_id"} {
		if _, marked := metadata[key]; marked {
			return ErrNextcloudPublicationDenied
		}
	}
	sourceID, valid := publicationMetadataString(metadata, "datasource_id")
	if !valid {
		return ErrNextcloudPublicationDenied
	}
	if sourceID == "" {
		return nil
	}
	if g == nil || g.dataSources == nil {
		return ErrNextcloudPublicationUnavailable
	}
	source, err := g.dataSources.FindByID(ctx, sourceID)
	if err != nil {
		return ErrNextcloudPublicationUnavailable
	}
	if source == nil || source.ID != sourceID || source.TenantID != knowledge.TenantID ||
		source.KnowledgeBaseID != knowledge.KnowledgeBaseID || source.Type == types.ConnectorTypeNextcloud {
		return ErrNextcloudPublicationDenied
	}
	return nil
}
