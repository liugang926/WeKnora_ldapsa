package access

import (
	"context"
	"errors"
	"testing"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
)

func TestAdditionalPublicationCallersKeepContextAndIdentity(t *testing.T) {
	// These are the exact pre-adapter caller messages from immutable aa666/ede0.
	for _, row := range []struct {
		name       string
		call       func() error
		public     string
		diagnostic string
	}{
		{
			name: "invalid_knowledge_metadata",
			call: func() error {
				guard := NewNextcloudPublicationGuard(nil, nil)
				return guard.CheckKnowledge(context.Background(), &types.Knowledge{
					Channel: types.ConnectorTypeNextcloud, Metadata: []byte("{"),
				})
			},
			public:     "Nextcloud publication access denied: invalid knowledge metadata",
			diagnostic: "nextcloud publication access denied: invalid knowledge metadata",
		},
		{
			name: "history_reference_without_document_identity",
			call: func() error {
				guard := &NextcloudHistoryGuard{}
				return guard.CheckMessage(context.Background(), &types.Message{
					Role: "assistant",
					KnowledgeReferences: []*types.SearchResult{
						{KnowledgeChannel: types.ConnectorTypeNextcloud},
					},
				})
			},
			public:     "Nextcloud publication access denied: source reference lacks document identity",
			diagnostic: "nextcloud publication access denied: source reference lacks document identity",
		},
	} {
		t.Run(row.name, func(t *testing.T) {
			err := row.call()
			if !errors.Is(err, ErrNextcloudPublicationDenied) {
				t.Fatalf("actual authorization caller lost the denied sentinel: %v", err)
			}
			if got := apperrors.PublicMessage(err); got != row.public {
				t.Fatalf("actual caller public context changed: %q", got)
			}
			if got := err.Error(); got != row.diagnostic {
				t.Fatalf("actual caller internal context changed: %q", got)
			}
		})
	}
}
