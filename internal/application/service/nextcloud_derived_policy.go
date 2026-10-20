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

// ErrNextcloudDerivedContent rejects copied or synthesized Nextcloud source content.
// A copied or synthesized document has no source authorization link that can
// reliably revoke it. These operations remain unavailable for Nextcloud data.
var ErrNextcloudDerivedContent = werrors.NewProtocolError(
	errors.New("nextcloud source content cannot be copied, shared, or synthesized"),
	"Nextcloud source content cannot be copied, shared, or synthesized",
)

// Keep the existing public context at this exact layer while preserving the
// lower-case Go diagnostic and the derived-content sentinel's identity.
func nextcloudDerivedContentContext(message string) error {
	return werrors.NewProtocolError(
		fmt.Errorf("%w: %s", ErrNextcloudDerivedContent, message),
		fmt.Sprintf("%s: %s", werrors.PublicMessage(ErrNextcloudDerivedContent), message),
	)
}

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
		return false, nextcloudDerivedContentContext("malformed knowledge metadata")
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
		return nextcloudDerivedContentContext("source scope unavailable")
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
			return nextcloudDerivedContentContext("source scope mismatch")
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
			return nextcloudDerivedContentContext("document scope mismatch")
		}
	}
	return rejectNextcloudKnowledgeRows(knowledge)
}
