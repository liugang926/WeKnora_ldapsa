package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type graphReplyChat struct {
	resp  *types.ChatResponse
	calls int
	err   error
}

func (c *graphReplyChat) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	c.calls++
	return c.resp, c.err
}

func (c *graphReplyChat) ChatStream(
	context.Context, []chat.Message, *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	return nil, errors.New("not implemented")
}
func (c *graphReplyChat) GetModelName() string { return "graph-reply" }
func (c *graphReplyChat) GetModelID() string   { return "graph-reply" }

// Issue #3600: a prose refusal finishes the per-chunk task (no asynq retry),
// while the same prose cut off by the output budget stays a retriable error.
func TestChunkExtractHandle_ModelDeclinedIsSkipNotRetry(t *testing.T) {
	const prose = "抱歉，该文本为目录页，没有可抽取的实体和关系。"
	_, store, db, _, ctx := knowledgeInputServiceFixture(t)
	kb := &types.KnowledgeBase{
		ID: "graph-kb", TenantID: 7, Name: "graph extraction", SummaryModelID: "graph-reply",
		ExtractConfig: &types.ExtractConfig{Enabled: true},
	}
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeInput(ctx), kb))
	namespace := types.NameSpace{KnowledgeBase: kb.ID, Knowledge: "source-doc"}
	_, proof := graphAdmittedInput(t, db, store, namespace)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	template, err := types.BindGraphPromptTemplateOrigin(
		&types.PromptTemplateStructured{Description: "extract"}, lineage,
	)
	require.NoError(t, err)
	task := func(receipt *types.KnowledgeBuildInputProof) *asynq.Task {
		payload, err := json.Marshal(types.ExtractChunkPayload{
			TenantID: kb.TenantID, ChunkID: namespace.Knowledge + "-chunk", KnowledgeID: namespace.Knowledge,
			ModelID: kb.SummaryModelID, OriginalInput: receipt,
		})
		require.NoError(t, err)
		return asynq.NewTask(types.TypeChunkExtract, payload)
	}
	model := &graphReplyChat{}
	svc := &ChunkExtractService{
		buildInputs: store, template: template,
		modelService: &stubModelService{chatModel: model},
	}
	require.ErrorIs(t, svc.Handle(ctx, task(nil)), types.ErrGraphSourceReceiptUnknown)
	require.Zero(t, model.calls)

	cases := []struct {
		name    string
		finish  string
		wantErr bool
	}{
		{"model stopped on its own: skip", "stop", false},
		{"output truncated: retry", "length", true},
		{"output truncated (max_tokens): retry", "max_tokens", true},
		{"stream broke before stop: retry", types.FinishReasonIncomplete, true},
		{"no finish reason reported: retry", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model.resp = &types.ChatResponse{Content: prose, FinishReason: tc.finish}
			calls := model.calls
			err := svc.Handle(ctx, task(proof))
			require.Equal(t, calls+1, model.calls)
			if tc.wantErr {
				require.Error(t, err)
				require.NotErrorIs(t, err, asynq.SkipRetry)
			} else {
				require.NoError(t, err)
			}
		})
	}
	model.err = errors.New("transport failure")
	require.ErrorIs(t, svc.Handle(ctx, task(proof)), model.err)
}
