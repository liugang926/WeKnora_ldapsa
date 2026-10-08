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
func createHistoryInputParents(t *testing.T, store *KnowledgeInputProvenanceStore, db *gorm.DB) (*types.Message, *types.Message) {
	t.Helper()
	ConfigureKnowledgeInputHistoryVerifier(store, historyInputVerifier{})
	session := &types.Session{TenantID: 7, UserID: "user"}
	require.NoError(t, db.Create(session).Error)
	lineage, _ := types.NewCompleteSourceLineage()
	user := &types.Message{ID: "user-message", SessionID: session.ID, RequestID: "request", Role: "user", Content: "actual committed raw question", RenderedContent: "expanded model-only material", IsCompleted: true, SourceLineage: lineage}
	assistant := &types.Message{ID: "assistant-message", SessionID: session.ID, RequestID: "request", Role: "assistant", Content: "<think>private reasoning</think> actual committed answer", IsCompleted: true, SourceLineage: lineage}
	require.NoError(t, db.Create(user).Error)
	require.NoError(t, db.Create(assistant).Error)
	return user, assistant
}
func historyInputAdmissionContext() context.Context {
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 7, UserID: "user", Role: types.TenantRoleViewer})
	return types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
}
func historyInputRequest(assistant *types.Message) types.CommittedHistoryKnowledgeInputRequest {
	return types.CommittedHistoryKnowledgeInputRequest{TenantID: 7, KnowledgeBaseID: "kb", SessionID: assistant.SessionID, AssistantMessageID: assistant.ID}
}

func TestKnowledgeHistoryActualCommittedPairAtomicLinkAndHydration(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	user, assistant := createHistoryInputParents(t, store, db)
	result, err := store.CreateCommittedHistoryKnowledgeInput(historyInputAdmissionContext(), historyInputRequest(assistant))
	require.NoError(t, err)
	require.Contains(t, result.Passages[0], user.Content)
	require.Contains(t, result.Passages[0], "actual committed answer")
	require.NotContains(t, result.Passages[0], "private reasoning")
	require.NotContains(t, result.Passages[0], user.RenderedContent)
	var linked types.Message
	require.NoError(t, db.Where("id = ?", assistant.ID).Take(&linked).Error)
	require.Equal(t, result.Knowledge.ID, linked.KnowledgeID)
	ctx := WithKnowledgeBuildInputProof(context.Background(), store, result.Proof)
	c := &types.Chunk{ID: "history-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: result.Knowledge.ID, Content: result.Passages[0], ChunkType: "text", IsEnabled: true, IndexStatus: "ready"}
	require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{c}))
	result.Knowledge.ParseStatus = types.ParseStatusCompleted
	result.Knowledge.EnableStatus = "enabled"
	require.NoError(t, NewKnowledgeRepository(db).UpdateKnowledge(ctx, result.Knowledge))
	input, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: result.Knowledge.ID, ChunkIDs: []string{c.ID}})
	require.NoError(t, err)
	require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
	rows, err := NewRAGChunkSnapshotReader(db, store).HydrateRAGChunkSnapshots(ctx, []RAGChunkReadRequest{{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: result.Knowledge.ID, ChunkIDs: []string{c.ID}}})
	require.NoError(t, err)
	require.NoError(t, rows[0].SourceLineage.RequireComplete())
	oldAnswer := assistant.Content
	require.NoError(t, db.Model(&types.Message{}).Where("id = ?", assistant.ID).Update("content", "changed answer").Error)
	require.NoError(t, db.Model(&types.Message{}).Where("id = ?", assistant.ID).Update("content", oldAnswer).Error)
	require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof), "parent text ABA cannot restore the original committed version")
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
				copy := *user
				copy.ID = "duplicate"
				require.NoError(t, db.Create(&copy).Error)
			case "link-failure":
				require.NoError(t, db.Exec("CREATE TRIGGER deny_history_link BEFORE UPDATE OF knowledge_id ON messages BEGIN SELECT RAISE(ABORT,'injected link failure'); END").Error)
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
			result, err := store.CreateCommittedHistoryKnowledgeInput(historyInputAdmissionContext(), historyInputRequest(assistant))
			require.Error(t, err)
			require.Nil(t, result)
			var count int64
			require.NoError(t, db.Model(&types.Knowledge{}).Where("channel = 'internal_history'").Count(&count).Error)
			require.Zero(t, count)
			require.NoError(t, db.Table("knowledge_input_admissions").Where("origin_kind = 'history'").Count(&count).Error)
			require.Zero(t, count)
			require.NoError(t, db.Table("knowledge_input_history_parents").Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func TestPostgresKnowledgeHistoryAtomicPairAndParentWithdrawal(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	_, assistant := createHistoryInputParents(t, store, db)
	result, err := store.CreateCommittedHistoryKnowledgeInput(historyInputAdmissionContext(), historyInputRequest(assistant))
	require.NoError(t, err)
	ctx := WithKnowledgeBuildInputProof(context.Background(), store, result.Proof)
	c := &types.Chunk{ID: "history-pg-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: result.Knowledge.ID, Content: result.Passages[0], ChunkType: "text", IsEnabled: true, IndexStatus: "ready"}
	require.NoError(t, NewChunkRepository(db).CreateChunks(ctx, []*types.Chunk{c}))
	input, err := store.CaptureKnowledgeBuildInputs(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: result.Knowledge.ID, ChunkIDs: []string{c.ID}})
	require.NoError(t, err)
	oldAnswer := assistant.Content
	require.NoError(t, db.Model(assistant).Update("content", "changed").Error)
	require.NoError(t, db.Model(assistant).Update("content", oldAnswer).Error)
	require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, input.Proof))
}
