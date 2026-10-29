package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAgentRawInputMaterialsExcludeQuestionOriginAndKnowledgeBases(t *testing.T) {
	require.False(t, hasAgentRawInputMaterials(&types.QARequest{
		KnowledgeBaseIDs: []string{"kb"},
		KnowledgeIDs:     []string{"doc"},
		QuestionOrigin:   &types.QuestionOrigin{KnowledgeBaseID: "kb", KnowledgeID: "doc"},
	}))
	require.True(t, hasAgentRawInputMaterials(&types.QARequest{
		Attachments: types.MessageAttachments{{Content: "attached text"}},
	}))
	require.True(t, hasAgentRawInputMaterials(&types.QARequest{
		ImageURLs: []string{"resource://image"},
	}))
}

func TestWeKnoraACLHistoryAdmissionDoesNotApplyNextcloudReceipts(t *testing.T) {
	admission := weknoraACLHistoryAdmission{}
	require.NoError(t, admission.CheckAgentHistoryReplay(context.Background()))
	require.NoError(t, admission.CheckMessageSourceLineage(context.Background(), &types.Message{
		Role:          "assistant",
		SourceLineage: types.UnknownSourceLineage(),
	}))
}

func TestWeKnoraSessionHistoryReadDoesNotRequireNextcloudReceipt(t *testing.T) {
	service := &messageService{}
	messages, err := service.filterHistoryMessages(context.Background(), []*types.Message{
		{
			ID:            "assistant-1",
			Role:          "assistant",
			Content:       "answer",
			SourceLineage: types.UnknownSourceLineage(),
		},
	})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, "answer", messages[0].Content)
}

func TestWeKnoraSessionHistoryReadDoesNotMintNextcloudLeases(t *testing.T) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	_, assistant := produceActualMessagePair(ctx, t, messages, session)
	rows, err := messages.GetRecentMessagesBySession(ctx, session.ID, 20)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, assistant.Content, rows[1].Content)
	var live int64
	require.NoError(
		t,
		db.Table("nextcloud_content_leases").
			Where("kind='read' AND released_at_ms IS NULL").
			Count(&live).
			Error,
	)
	require.Zero(t, live, "ordinary WeKnora history reads must not acquire Nextcloud leases")
}
