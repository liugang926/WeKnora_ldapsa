package access

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestRetiredPublicationCallersPreserveMalformedAndLegacyInputs(t *testing.T) {
	t.Run("invalid knowledge metadata", func(t *testing.T) {
		guard := NewNextcloudPublicationGuard(nil, nil)
		knowledge := &types.Knowledge{Channel: types.ConnectorTypeNextcloud, Metadata: []byte("{")}
		if err := guard.CheckKnowledge(context.Background(), knowledge); err != nil {
			t.Fatal(err)
		}
		if string(knowledge.Metadata) != "{" || knowledge.Channel != types.ConnectorTypeNextcloud {
			t.Fatal("retired publication caller changed malformed input")
		}
	})
	t.Run("history reference without document identity", func(t *testing.T) {
		guard := &NextcloudHistoryGuard{knowledge: retiredPanicKnowledge{}, sources: retiredPanicSources{}}
		message := &types.Message{Role: "assistant", KnowledgeReferences: []*types.SearchResult{
			{KnowledgeChannel: types.ConnectorTypeNextcloud},
		}}
		assertRetiredReadUnchanged(t, message, func() error {
			return guard.CheckMessage(context.Background(), message)
		})
	})
}
