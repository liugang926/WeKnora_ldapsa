package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type historyInputVerifier struct{ check func(*types.Message) error }

func (v historyInputVerifier) CheckMessagePublication(_ context.Context, m *types.Message) error {
	if v.check != nil {
		return v.check(m)
	}
	return m.SourceLineage.RequireComplete()
}

// Test stub batch retains both envelopes; production requires scope-preserving
// authorizer CheckBatch with one final directory/source/grant snapshot.
func (v historyInputVerifier) CheckMessagePublications(ctx context.Context, messages []*types.Message) error {
	for _, message := range messages {
		if err := v.CheckMessagePublication(ctx, message); err != nil {
			return err
		}
	}
	for _, message := range messages {
		if err := v.CheckMessagePublication(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (v historyInputVerifier) CheckLiveMessagePublication(ctx context.Context, m *types.Message) error {
	return v.CheckMessagePublication(ctx, m)
}

func createHistoryInputParents(
	t *testing.T,
	store *KnowledgeInputProvenanceStore,
	db *gorm.DB,
) (*types.Message, *types.Message) {
	t.Helper()
	ConfigureKnowledgeInputHistoryVerifier(store, historyInputVerifier{})
	session := &types.Session{TenantID: 7, UserID: "user"}
	require.NoError(t, db.Create(session).Error)
	lineage, _ := types.NewCompleteSourceLineage()
	user := &types.Message{
		ID:              "user-message",
		SessionID:       session.ID,
		RequestID:       "request",
		Role:            "user",
		Content:         "actual committed raw question",
		RenderedContent: "expanded model-only material",
		IsCompleted:     true,
		SourceLineage:   lineage,
	}
	assistant := &types.Message{
		ID:            "assistant-message",
		SessionID:     session.ID,
		RequestID:     "request",
		Role:          "assistant",
		Content:       "<think>private reasoning</think> actual committed answer",
		IsCompleted:   true,
		SourceLineage: lineage,
	}
	require.NoError(t, db.Create(user).Error)
	require.NoError(t, db.Create(assistant).Error)
	return user, assistant
}

func historyInputAdmissionContext() context.Context {
	ctx := types.WithCaller(
		context.Background(),
		types.Caller{TenantID: 7, UserID: "user", Role: types.TenantRoleViewer},
	)
	return types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
}

func historyInputRequest(assistant *types.Message) types.CommittedHistoryKnowledgeInputRequest {
	return types.CommittedHistoryKnowledgeInputRequest{
		TenantID:           7,
		KnowledgeBaseID:    "kb",
		SessionID:          assistant.SessionID,
		AssistantMessageID: assistant.ID,
	}
}

// These rows were written directly to SQL. Current JSON and a permissive
// global source verifier cannot mint original material receipts. Production
// positive/ABA/atomic-write acceptance lives in message_material_input_test.go.
func checkKnowledgeHistoryExecutionContextABA(t *testing.T, store *KnowledgeInputProvenanceStore, db *gorm.DB) {
	t.Helper()
	_, assistant := createHistoryInputParents(t, store, db)
	_, err := store.CreateCommittedHistoryKnowledgeInput(historyInputAdmissionContext(), historyInputRequest(assistant))
	require.ErrorIs(t, err, ErrNextcloudContentGCUncovered)
	original := assistant.ExecutionContext
	require.NoError(
		t,
		db.Model(assistant).
			Update("execution_context", types.MessageExecutionContext{KnowledgeBaseIDs: []string{"changed-scope"}}).
			Error,
	)
	require.NoError(t, db.Model(assistant).Update("execution_context", original).Error)
	_, err = store.CreateCommittedHistoryKnowledgeInput(historyInputAdmissionContext(), historyInputRequest(assistant))
	require.ErrorIs(t, err, ErrNextcloudContentGCUncovered)
	var count int64
	require.NoError(t, db.Table("knowledge_input_history_parents").Count(&count).Error)
	require.Zero(t, count)
}

func TestKnowledgeHistoryExecutionContextABA(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	checkKnowledgeHistoryExecutionContextABA(t, store, db)
}

func TestPostgresKnowledgeHistoryExecutionContextABA(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	checkKnowledgeHistoryExecutionContextABA(t, store, db)
}

func TestKnowledgeHistoryRawCommittedPairCannotBeSignedByCurrentJSON(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	_, assistant := createHistoryInputParents(t, store, db)
	result, err := store.CreateCommittedHistoryKnowledgeInput(
		historyInputAdmissionContext(),
		historyInputRequest(assistant),
	)
	require.ErrorIs(t, err, ErrNextcloudContentGCUncovered)
	require.Nil(t, result)
	var count int64
	require.NoError(t, db.Table("knowledge_input_admissions").Where("origin_kind='history'").Count(&count).Error)
	require.Zero(t, count)
}

func TestKnowledgeHistoryLegacyUnpairedAndFailedLinkNeverAdmitOrphans(t *testing.T) {
	for _, mutation := range []string{"legacy", "missing-user", "duplicate-user", "link-failure", "late-denial"} {
		t.Run(mutation, func(t *testing.T) {
			store, db := knowledgeInputFixture(t)
			user, assistant := createHistoryInputParents(t, store, db)
			switch mutation {
			case "legacy":
				require.NoError(t, db.Model(assistant).Update("source_lineage", nil).Error)
			case "missing-user":
				require.NoError(t, db.Unscoped().Delete(user).Error)
			case "duplicate-user":
				copied := *user
				copied.ID = "duplicate"
				require.NoError(t, db.Create(&copied).Error)
			case "link-failure":
				require.NoError(
					t,
					db.Exec(
						("CREATE TRIGGER deny_history_link BEFORE UPDATE OF knowledge_id ON messages" +
							" BEGIN SELECT RAISE(ABORT,'injected link failure'); END"),
					).Error,
				)
			case "late-denial":
				calls := 0
				ConfigureKnowledgeInputHistoryVerifier(store, historyInputVerifier{check: func(*types.Message) error {
					calls++
					if calls > 4 {
						return ErrKnowledgeInputChanged
					}
					return nil
				}})
			}
			result, err := store.CreateCommittedHistoryKnowledgeInput(
				historyInputAdmissionContext(),
				historyInputRequest(assistant),
			)
			require.Error(t, err)
			require.Nil(t, result)
			var count int64
			require.NoError(t, db.Model(&types.Knowledge{}).Where("channel = 'internal_history'").Count(&count).Error)
			require.Zero(t, count)
			require.NoError(
				t,
				db.Table("knowledge_input_admissions").Where("origin_kind = 'history'").Count(&count).Error,
			)
			require.Zero(t, count)
			require.NoError(t, db.Table("knowledge_input_history_parents").Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func TestPostgresKnowledgeHistoryRawPairCannotBeSignedByGlobalVerifier(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	_, assistant := createHistoryInputParents(t, store, db)
	result, err := store.CreateCommittedHistoryKnowledgeInput(
		historyInputAdmissionContext(),
		historyInputRequest(assistant),
	)
	require.ErrorIs(t, err, ErrNextcloudContentGCUncovered)
	require.Nil(t, result)
	var count int64
	require.NoError(t, db.Table("knowledge_input_history_parents").Count(&count).Error)
	require.Zero(t, count)
}
