package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
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
	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
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

func TestWeKnoraReadAuthorityKeepsSelectedDocumentsInsideSearchTargets(t *testing.T) {
	t.Setenv("WEKNORA_NEXTCLOUD_READ_AUTHORITY", "weknora")
	messages, knowledge, store, db, ctx, _, _ := messageBodyIntegrationFixture(
		t,
		false,
		agentPromptDisplayFixtureSchema(t),
	)
	ctx = agentPromptActualRequestContext(ctx, t)
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
	knowledge.kbService = kbs
	knowledge.groupAccess = NewGroupAccessService(repository.NewGroupAccessRepository(db))
	knowledge.contentLeases = store.ContentLeaseStore()
	knowledge.knowledgeInputs = store
	kb := &types.KnowledgeBase{
		CreatorID:        types.CallerFromContext(ctx).UserID,
		ID:               "weknora-read-kb",
		TenantID:         7,
		Name:             "WeKnora read KB",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: "embed",
		SummaryModelID:   "summary",
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
	}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(
		repository.WithIndependentKnowledgeBaseNameInput(
			repository.WithIndependentKnowledgeInput(ctx),
			"",
			kb.Name,
		),
		kb,
	))
	doc := &types.Knowledge{
		ID:              "weknora-read-doc",
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
		Type:            types.KnowledgeTypeManual,
		Source:          "manual",
		Channel:         "web",
		Title:           "Selected document",
		FileName:        "selected.md",
		FileType:        "md",
		CustomMetadata:  types.JSON(`{}`),
		Metadata:        types.JSON(`{"manual":{"content":"selected","version":1}}`),
	}
	require.NoError(t, db.Create(doc).Error)
	service := &agentService{
		db:                   db,
		knowledgeService:     knowledge,
		knowledgeBaseService: kbs,
		messageService:       messages,
	}
	target := types.SearchTargets{&types.SearchTarget{
		Type:            types.SearchTargetTypeKnowledgeBase,
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
	}}
	guard, err := service.prepareAgentPromptInputs(ctx, &types.AgentConfig{
		KnowledgeBases: []string{kb.ID},
		KnowledgeIDs:   []string{doc.ID},
		SearchTargets:  target,
	})
	require.NoError(t, err)
	require.Len(t, guard.selectedDocs, 1)
	require.Empty(t, guard.selectedDocs[0].SourceLineage.Sources)
	require.NoError(t, guard.Close())

	_, err = service.prepareAgentPromptInputs(ctx, &types.AgentConfig{
		KnowledgeIDs: []string{doc.ID},
		SearchTargets: types.SearchTargets{&types.SearchTarget{
			Type:            types.SearchTargetTypeKnowledgeBase,
			TenantID:        7,
			KnowledgeBaseID: "outside-target",
		}},
	})
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
}
