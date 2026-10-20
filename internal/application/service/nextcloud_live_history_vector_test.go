//go:build sqlite_fts5

package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// Count real query calls while delegating provider creation and all metadata
// to the production model service over this actor's encrypted persisted rows.
type documentHistoryVectorModels struct {
	interfaces.ModelService
	queries atomic.Int64
}

type documentHistoryVectorEmbedder struct {
	embedding.Embedder
	queries *atomic.Int64
}

func (e documentHistoryVectorEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.queries.Add(1)
	return e.Embedder.Embed(ctx, text)
}

func (m *documentHistoryVectorModels) GetEmbeddingModel(ctx context.Context, id string) (embedding.Embedder, error) {
	provider, err := m.ModelService.GetEmbeddingModel(ctx, id)
	if err != nil {
		return nil, err
	}
	return documentHistoryVectorEmbedder{Embedder: provider, queries: &m.queries}, nil
}

// No synthetic assistant, provenance envelope, child material, chunks or
// vector index is installed. IndexMessageToKB must select the real persisted
// RAG answer and queue the real committed-parent DocumentProcess payload.
func documentLiveHistoryVector(
	t *testing.T,
	m *machinePipelineFixture,
	live *documentLiveModels,
	base *knowledgeBaseService,
	history *documentLiveHistory,
	answer string,
) func() {
	t.Helper()
	if os.Getenv("WEKNORA_DOCUMENT_GRAPH_HISTORY_VECTOR") != "1" {
		return func() {}
	}
	require.NotNil(t, history)
	require.NotNil(t, live, "history vector requires actual configured models")
	require.Contains(
		t,
		strings.ToLower(live.embedder.GetModelName()),
		"bge",
		"this acceptance profile requires the existing real BGE model",
	)
	db := m.f.db
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(pool.Release)
	models := &documentHistoryVectorModels{
		ModelService: NewModelService(
			repository.NewModelRepository(db),
			repository.NewKnowledgeBaseRepository(db),
			nil,
			nil,
			embedding.NewBatchEmbedder(pool),
			nil,
		),
	}
	base.modelService = models
	base.groupAccess = NewGroupAccessService(repository.NewGroupAccessRepository(db))
	m.f.s.kbService = base
	m.f.post.kbService = base
	// The production post/summary entries must restore immutable original
	// message parents in every fresh worker context and close those guards.
	ConfigureKnowledgePostProcessHistoryMaterial(m.f.post, m.f.s)
	history.messages.kbService = base
	history.messages.modelService = models
	historyKB := &types.KnowledgeBase{
		ID:               history.session.ID + "-vector-kb",
		TenantID:         7,
		Name:             "Owned history vector acceptance",
		Type:             types.KnowledgeBaseTypeDocument,
		EmbeddingModelID: live.embeddingRow.ID,
		ChunkingConfig:   types.ChunkingConfig{ChunkSize: 512},
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true, KeywordEnabled: true},
	}
	require.NoError(
		t,
		m.f.store.CreateKnowledgeBaseWithInput(
			repository.WithIndependentKnowledgeBaseNameInput(
				repository.WithIndependentKnowledgeInput(history.ctx),
				historyKB.ID,
				historyKB.Name,
			),
			historyKB,
		),
	)
	var tenant types.Tenant
	require.NoError(t, db.Where("id=7").Take(&tenant).Error)
	tenant.ChatHistoryConfig = &types.ChatHistoryConfig{
		Enabled:          true,
		KnowledgeBaseID:  historyKB.ID,
		EmbeddingModelID: live.embeddingRow.ID,
	}
	// A zero threshold means production default .15, so choose an explicit
	// small positive threshold. No configured reranker exists in this model
	// export: this profile certifies real BGE/native vector + hybrid only.
	tenant.RetrievalConfig = &types.RetrievalConfig{
		EmbeddingTopK:   3,
		VectorThreshold: .01,
		RerankTopK:      3,
		RerankModelID:   "",
	}
	require.NoError(
		t,
		db.Model(&types.Tenant{}).
			Where("id=7").
			Updates(map[string]any{
				"chat_history_config": tenant.ChatHistoryConfig,
				"retrieval_config":    tenant.RetrievalConfig,
			}).
			Error,
	)
	history.ctx = context.WithValue(history.ctx, types.TenantInfoContextKey, &tenant)
	queueStart := len(m.f.queue.tasks)
	history.messages.IndexMessageToKB(
		history.ctx,
		"UNTRUSTED supplied question",
		"UNTRUSTED supplied answer",
		history.assistant.ID,
		history.session.ID,
	)
	var linked struct{ KnowledgeID string }
	require.NoError(
		t,
		db.Table("messages").
			Select("knowledge_id").
			Where("id=? AND session_id=?", history.assistant.ID, history.session.ID).
			Take(&linked).
			Error,
	)
	require.NotEmpty(t, linked.KnowledgeID, "real history producer must link its actual child")
	processed, postProcessed := false, false
	// This queue grows as the actual worker enqueues its next production
	// stage. A range over the initial slice would skip activation/finalizing.
	for i := queueStart; i < len(m.f.queue.tasks); i++ {
		task := m.f.queue.tasks[i]
		var target struct {
			KnowledgeID string `json:"knowledge_id"`
		}
		require.NoError(t, json.Unmarshal(task.Payload(), &target))
		require.Equal(
			t,
			linked.KnowledgeID,
			target.KnowledgeID,
			"the new queue suffix must belong to the actual history child",
		)
		switch task.Type() {
		case types.TypeDocumentProcess:
			var payload types.DocumentProcessPayload
			require.NoError(t, json.Unmarshal(task.Payload(), &payload))
			require.False(t, processed)
			require.NotNil(t, payload.OriginalInput)
			require.NotEmpty(t, payload.OriginalInput.SourceLineage.Sources)
			require.Equal(t, history.assistant.SourceLineage, payload.OriginalInput.SourceLineage)
			require.True(
				t,
				strings.Contains(strings.Join(payload.Passages, "\n"), answer),
				"actual committed child must contain the persisted RAG answer",
			)
			require.False(
				t,
				strings.Contains(strings.Join(payload.Passages, "\n"), "UNTRUSTED"),
				"caller supplied text must never become history child material",
			)
			// No fabricated NC build lease or creation-time parent guard.
			require.NoError(t, m.f.s.ProcessDocument(context.Background(), task))
			processed = true
		case types.TypeKnowledgePostProcess:
			require.False(t, postProcessed)
			require.NoError(t, m.f.post.Handle(context.Background(), task))
			postProcessed = true
		case types.TypeSummaryGeneration:
			// Empty SummaryModelID is the actual production skip/finalize
			// policy. This profile makes no history-summary model claim.
			require.NoError(t, m.f.s.ProcessSummaryGeneration(context.Background(), task))
		default:
			t.Fatalf("unexpected actual history child stage %s", task.Type())
		}
	}
	require.True(t, processed, "execute the actual queued history DocumentProcess")
	require.True(t, postProcessed, "execute the actual queued activation/finalizing stage")
	var child types.Knowledge
	require.NoError(t, db.Where("id=?", linked.KnowledgeID).Take(&child).Error)
	require.Equal(t, types.ParseStatusCompleted, child.ParseStatus)
	require.Equal(t, "enabled", child.EnableStatus)
	for _, table := range []string{"chunks", "lite_embeddings"} {
		var count int64
		require.NoError(t, db.Table(table).Where("knowledge_id=?", child.ID).Count(&count).Error)
		require.Positive(t, count, "actual history worker must produce native indexed material")
	}
	m.liveHistoryVectorKBID = historyKB.ID
	m.liveHistoryVectorKnowledgeID = child.ID
	m.liveHistoryVectorEmbeddingID = live.embeddingRow.ID
	answerHash := sha256.Sum256([]byte(answer))
	m.liveHistoryVectorAnswerSHA256 = hex.EncodeToString(answerHash[:])
	query := history.user.Content
	search := func(mode types.MessageSearchMode, revoked bool) {
		current := WithMessageMaterialOutputBoundary(history.ctx)
		defer CloseMessageMaterialOutputBoundary(current)
		before := models.queries.Load()
		result, err := history.messages.SearchMessages(
			current,
			&types.MessageSearchParams{Query: query, Mode: mode, Limit: 3, SessionIDs: []string{history.session.ID}},
		)
		if revoked {
			require.ErrorIs(
				t,
				err,
				access.ErrNextcloudPublicationDenied,
				"known actual NC revocation is an explicit vector/hybrid error, never complete zero success",
			)
			require.Nil(t, result)
			return
		}
		require.Greater(
			t,
			models.queries.Load(),
			before,
			"successful vector/hybrid search must use the real BGE query provider",
		)
		require.NoError(t, err)
		require.Equal(t, 1, result.Total)
		require.Len(t, result.Items, 1)
		require.Equal(t, history.session.ID, result.Items[0].SessionID)
		require.Equal(
			t,
			sha256.Sum256([]byte(history.user.Content)),
			sha256.Sum256([]byte(result.Items[0].QueryContent)),
			"history query must be the actual saved user body",
		)
		require.Equal(
			t,
			answerHash,
			sha256.Sum256([]byte(result.Items[0].AnswerContent)),
			"history answer must be the actual saved RAG body",
		)
		require.NoError(t, CheckMessageMaterialOutputBoundary(current))
	}
	for _, mode := range []types.MessageSearchMode{types.MessageSearchModeVector, types.MessageSearchModeHybrid} {
		search(mode, false)
	}
	retained := WithMessageMaterialOutputBoundary(history.ctx)
	t.Cleanup(func() { CloseMessageMaterialOutputBoundary(retained) })
	result, err := history.messages.SearchMessages(
		retained,
		&types.MessageSearchParams{
			Query:      query,
			Mode:       types.MessageSearchModeVector,
			Limit:      3,
			SessionIDs: []string{history.session.ID},
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Total)
	require.NoError(t, CheckMessageMaterialOutputBoundary(retained))
	t.Log(
		("REAL_HISTORY_VECTOR actual persisted RAG -> committed-history producer -> " +
			"fresh-context DocumentProcess/post/finalize -> native BGE vector/hybrid po" +
			"sitive passed; no reranker claim"),
	)
	return func() {
		require.ErrorIs(t, CheckMessageMaterialOutputBoundary(retained), access.ErrNextcloudPublicationDenied)
		CloseMessageMaterialOutputBoundary(retained)
		for _, mode := range []types.MessageSearchMode{types.MessageSearchModeVector, types.MessageSearchModeHybrid} {
			search(mode, true)
		}
		t.Log(
			("REAL_HISTORY_VECTOR actual NC revocation denied retained output and fresh " +
				"native vector/hybrid attempts explicitly"),
		)
	}
}
