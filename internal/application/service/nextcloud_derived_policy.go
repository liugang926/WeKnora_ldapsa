package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	werrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// ErrNextcloudDerivedContent reports operations that cannot preserve source-content authorization.
// A copied or synthesized document has no source authorization link that can
// reliably revoke it. These operations remain unavailable for Nextcloud data.
var ErrNextcloudDerivedContent = werrors.NewProtocolError(
	errors.New(
		("nextcloud source content cannot be copied, shared, or s" +
			"ynthesized")), "Nextcloud source content cannot be copied, shared, or synthesized")

// ErrNextcloudSourceManagedMutation reports a mutation requiring a new source connector candidate.
var ErrNextcloudSourceManagedMutation = werrors.NewConflictError(
	"Nextcloud source-managed files must be rebuilt through a new connector candidate",
)

func rejectNextcloudSourceManagedMutation(k *types.Knowledge) error {
	managed, err := isNextcloudKnowledge(k)
	if err != nil {
		return err
	}
	if managed {
		return ErrNextcloudSourceManagedMutation
	}
	return nil
}

func isNextcloudKnowledge(k *types.Knowledge) (bool, error) {
	if k == nil {
		return false, nil
	}
	if k.Channel == types.ConnectorTypeNextcloud {
		return true, nil
	}
	if len(k.Metadata) == 0 {
		return false, nil
	}
	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(k.Metadata, &metadata); err != nil || metadata == nil {
		return false, werrors.NewProtocolError(
			fmt.Errorf("%w: malformed knowledge metadata", ErrNextcloudDerivedContent),
			fmt.Sprintf("%s: malformed knowledge metadata", werrors.PublicMessage(ErrNextcloudDerivedContent)),
		)
	}
	for _, key := range []string{"nextcloud_instance_id", "nextcloud_binding_id", "nextcloud_file_id"} {
		if _, marked := metadata[key]; marked {
			return true, nil
		}
	}
	return false, nil
}

func rejectNextcloudKnowledgeRows(rows []*types.Knowledge) error {
	for _, item := range rows {
		marked, err := isNextcloudKnowledge(item)
		if err != nil {
			return err
		}
		if marked {
			return ErrNextcloudDerivedContent
		}
	}
	return nil
}

// RejectNextcloudDerivedKB checks live source configuration as well as stored
// documents. The configuration check closes the gap while a newly configured
// source has not imported its first file. A failed lookup is never treated as
// proof that the KB is safe to share or synthesize.
func RejectNextcloudDerivedKB(
	ctx context.Context,
	kb *types.KnowledgeBase,
	knowledgeRepo interfaces.KnowledgeRepository,
	dataSourceRepo interfaces.DataSourceRepository,
) error {
	if kb == nil || kb.ID == "" || kb.TenantID == 0 || knowledgeRepo == nil || dataSourceRepo == nil {
		return werrors.NewProtocolError(
			fmt.Errorf("%w: source scope unavailable", ErrNextcloudDerivedContent),
			fmt.Sprintf("%s: source scope unavailable", werrors.PublicMessage(ErrNextcloudDerivedContent)),
		)
	}
	if kb.EverHadNextcloudSource {
		return ErrNextcloudDerivedContent
	}
	sources, err := dataSourceRepo.FindByKnowledgeBase(ctx, kb.ID)
	if err != nil {
		return fmt.Errorf("check knowledge-base sources: %w", err)
	}
	for _, source := range sources {
		if source == nil || source.KnowledgeBaseID != kb.ID || source.TenantID != kb.TenantID {
			return werrors.NewProtocolError(
				fmt.Errorf("%w: source scope mismatch", ErrNextcloudDerivedContent),
				fmt.Sprintf("%s: source scope mismatch", werrors.PublicMessage(ErrNextcloudDerivedContent)),
			)
		}
		if source.Type == types.ConnectorTypeNextcloud {
			return ErrNextcloudDerivedContent
		}
	}
	knowledge, err := knowledgeRepo.ListKnowledgeByKnowledgeBaseID(ctx, kb.TenantID, kb.ID)
	if err != nil {
		return fmt.Errorf("check knowledge-base documents: %w", err)
	}
	for _, item := range knowledge {
		if item == nil || item.TenantID != kb.TenantID || item.KnowledgeBaseID != kb.ID {
			return werrors.NewProtocolError(
				fmt.Errorf("%w: document scope mismatch", ErrNextcloudDerivedContent),
				fmt.Sprintf("%s: document scope mismatch", werrors.PublicMessage(ErrNextcloudDerivedContent)),
			)
		}
	}
	return rejectNextcloudKnowledgeRows(knowledge)
}
