//go:build sqlite_fts5

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Real providers are opt-in. No fallback to a synthetic response is allowed.
// The private JSON file contains model rows exported from the operator's DB.
type documentLiveModels struct {
	interfaces.ModelService
	embeddingRow, chatRow types.Model
	embedder              embedding.Embedder
	summary, graph        *documentLiveChat
}
type documentLiveChat struct {
	provider chat.Chat
	calls    atomic.Int64
	streams  atomic.Int64
}

func (m *documentLiveChat) Chat(
	ctx context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (*types.ChatResponse, error) {
	m.calls.Add(1)
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	return m.provider.Chat(ctx, messages, options)
}

func (m *documentLiveModels) GetEmbeddingModel(_ context.Context, id string) (embedding.Embedder, error) {
	if id != m.embeddingRow.ID {
		return nil, fmt.Errorf("unexpected live embedding model ID")
	}
	return m.embedder, nil
}

func (m *documentLiveModels) GetChatModel(_ context.Context, id string) (chat.Chat, error) {
	if id != m.chatRow.ID {
		return nil, fmt.Errorf("unexpected live chat model ID")
	}
	return m.summary, nil
}
func (m *documentLiveChat) GetModelID() string   { return m.provider.GetModelID() }
func (m *documentLiveChat) GetModelName() string { return m.provider.GetModelName() }

func (m *documentLiveChat) ChatStream(
	ctx context.Context,
	messages []chat.Message,
	options *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.streams.Add(1)
	return m.provider.ChatStream(ctx, messages, options)
}

type documentLiveGraphModels struct{ *documentLiveModels }

func (m documentLiveGraphModels) GetChatModel(_ context.Context, id string) (chat.Chat, error) {
	if id != m.chatRow.ID {
		return nil, fmt.Errorf("unexpected live graph model ID")
	}
	return m.graph, nil
}

func loadDocumentLiveModels(t *testing.T) (*documentLiveModels, *types.PromptTemplateStructured) {
	t.Helper()
	path := os.Getenv("WEKNORA_DOCUMENT_GRAPH_MODEL_CONFIG")
	if path == "" {
		return nil, nil
	}
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Zero(t, info.Mode().Perm()&0o077, "model config must be private")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var rows struct {
		SourceAESKey string      `json:"source_aes_key"`
		Embedding    types.Model `json:"embedding"`
		Chat         types.Model `json:"chat"`
	}
	require.NoError(t, json.Unmarshal(raw, &rows))
	require.Len(t, rows.SourceAESKey, 32, "source model encryption key required")
	for _, row := range []*types.Model{&rows.Embedding, &rows.Chat} {
		plain, e := utils.DecryptAESGCM(row.Parameters.APIKey, []byte(rows.SourceAESKey))
		require.NoError(t, e, "model credential decryption failed")
		require.NotEmpty(t, plain, "configured model credential required")
		row.Parameters.APIKey = plain
		row.Parameters.AppSecret, e = utils.DecryptAESGCM(row.Parameters.AppSecret, []byte(rows.SourceAESKey))
		require.NoError(t, e)
	}
	rows.SourceAESKey = ""

	require.Equal(t, types.ModelTypeEmbedding, rows.Embedding.Type)
	require.Equal(t, types.ModelTypeKnowledgeQA, rows.Chat.Type)
	require.Positive(t, rows.Embedding.Parameters.EmbeddingParameters.Dimension)
	require.NotEmpty(t, rows.Embedding.ID)
	require.NotEmpty(t, rows.Chat.ID)
	rows.Embedding.TenantID = 7
	rows.Chat.TenantID = 7
	// Bound this fixture's provider concurrency without mutating the source DB.
	rows.Embedding.Parameters.MaxConcurrency = 1
	rows.Chat.Parameters.MaxConcurrency = 1
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(pool.Release)
	embedder, err := embedding.NewEmbedder(
		embedding.ConfigFromModel(&rows.Embedding, "", ""),
		embedding.NewBatchEmbedder(pool),
		nil,
	)
	require.NoError(t, err)
	provider, err := chat.NewChat(chat.ConfigFromModel(&rows.Chat, "", ""), nil)
	require.NoError(t, err)
	models := &documentLiveModels{
		embeddingRow: rows.Embedding,
		chatRow:      rows.Chat,
		embedder:     embedder,
		summary:      &documentLiveChat{provider: provider},
		graph:        &documentLiveChat{provider: provider},
	}
	raw, err = os.ReadFile("../../../config/config.yaml")
	require.NoError(t, err)
	var cfg config.Config
	require.NoError(t, yaml.Unmarshal(raw, &cfg))
	require.NotNil(t, cfg.ExtractManager)
	template, err := config.BuiltInGraphTemplateOrigin(cfg.ExtractManager.ExtractGraph, "graph")
	require.NoError(t, err)
	return models, template
}
