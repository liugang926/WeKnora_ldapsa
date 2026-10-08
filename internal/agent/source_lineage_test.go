package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	agenttools "github.com/Tencent/WeKnora/internal/agent/tools"
	"github.com/Tencent/WeKnora/internal/event"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func agentLineageIdentity(file, etag string, epoch uint64) types.SourceIdentity {
	return types.SourceIdentity{Provider: "nextcloud", TenantID: 12, KnowledgeBaseID: "kb", DataSourceID: "ds",
		PairOperationID: "pair", InstanceID: "instance", BindingID: "binding", FileID: file,
		ExternalID: "nextcloud:instance:" + file, KnowledgeID: "knowledge-" + file, Revision: epoch, ETag: etag}
}
func agentComplete(t *testing.T, sources ...types.SourceIdentity) *types.SourceLineage {
	t.Helper()
	l, err := types.NewCompleteSourceLineage(sources...)
	require.NoError(t, err)
	return l
}

type accountedSourceTool struct {
	agenttools.BaseTool
	results      []*types.ToolResult
	calls        int
	beforeReturn func()
}

func newAccountedSourceTool(results ...*types.ToolResult) *accountedSourceTool {
	return &accountedSourceTool{BaseTool: agenttools.NewBaseTool("source_reader", "static trusted reader schema", json.RawMessage(`{"type":"object"}`)), results: results}
}
func (t *accountedSourceTool) DefinitionSourceLineageFor(types.FunctionDefinition) *types.SourceLineage {
	return independentAgentInput()
}
func (t *accountedSourceTool) Execute(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	return t.ExecuteSourceLineage(ctx, args)
}
func (t *accountedSourceTool) ExecuteSourceLineage(context.Context, json.RawMessage) (*types.ToolResult, error) {
	if t.calls >= len(t.results) {
		return nil, errors.New("unexpected reader call")
	}
	result := t.results[t.calls]
	t.calls++
	if t.beforeReturn != nil {
		t.beforeReturn()
	}
	return result, nil
}

func lineageCall() mockResponse {
	return mockResponse{chunks: []types.StreamResponse{{ResponseType: types.ResponseTypeToolCall, ToolCalls: []types.LLMToolCall{{ID: "read", Type: "function", Function: types.FunctionCall{Name: "source_reader", Arguments: `{}`}}}, FinishReason: "tool_calls", Done: true}}}
}
func lineageAnswer(content string) mockResponse {
	return mockResponse{chunks: []types.StreamResponse{{ResponseType: types.ResponseTypeAnswer, Content: content, FinishReason: "stop", Done: true}}}
}
func lineageEngine(t *testing.T, model chat.Chat, tool *accountedSourceTool, check func(context.Context, *types.SourceLineage) error) (*AgentEngine, *[]event.Event) {
	t.Helper()
	e := newTestEngine(t, model)
	e.toolRegistry = agenttools.NewToolRegistry()
	if tool != nil {
		e.toolRegistry.RegisterTool(tool)
	}
	e.SetSourceLineageVerifier(check)
	events := &[]event.Event{}
	for _, kind := range []event.EventType{event.EventAgentFinalAnswer, event.EventAgentToolResult, event.EventAgentTool, event.EventAgentToolCall, event.EventAgentThought, event.EventAgentComplete, event.EventError} {
		e.eventBus.On(kind, func(_ context.Context, ev event.Event) error {
			l, ok := ev.SourceLineage.(*types.SourceLineage)
			require.True(t, ok)
			require.NoError(t, l.RequireComplete())
			*events = append(*events, ev)
			return nil
		})
	}
	return e, events
}
func grantChecker(allowed map[types.SourceIdentity]bool) func(context.Context, *types.SourceLineage) error {
	return func(_ context.Context, l *types.SourceLineage) error {
		if err := l.RequireComplete(); err != nil {
			return err
		}
		for _, source := range l.Sources {
			if !allowed[source] {
				return errors.New("current grant/version denied")
			}
		}
		return nil
	}
}

func TestAgentLineageSourceToolCitationFreeNextTurnRevocation(t *testing.T) {
	source := agentLineageIdentity("1", "original", 7)
	allowed := map[types.SourceIdentity]bool{source: true}
	tool := newAccountedSourceTool(&types.ToolResult{Success: true, Output: "confidential retrieved text", Data: map[string]any{"uncited_title": "confidential name"}, SourceLineage: agentComplete(t, source)})
	model := &mockChat{responses: []mockResponse{lineageCall(), lineageAnswer("A citation-free paraphrase.")}}
	e, events := lineageEngine(t, model, tool, grantChecker(allowed))
	state, err := e.Execute(context.Background(), "session", "turn-1", "read and explain", nil)
	require.NoError(t, err)
	require.Equal(t, "A citation-free paraphrase.", state.FinalAnswer)
	require.Equal(t, agentComplete(t, source), state.SourceLineage)
	require.Equal(t, 2, model.callCount)
	require.NotContains(t, state.FinalAnswer, "<kb")
	require.NotContains(t, state.FinalAnswer, "citation=")
	for _, sent := range model.calls {
		for _, message := range sent {
			require.NoError(t, message.SourceLineage.RequireComplete())
		}
	}
	followModel := &mockChat{responses: []mockResponse{lineageAnswer("The same conclusion, without a new search.")}}
	follow, _ := lineageEngine(t, followModel, nil, grantChecker(allowed))
	history := []chat.Message{{Role: "assistant", Content: state.FinalAnswer, SourceLineage: state.SourceLineage}}
	followState, err := follow.Execute(context.Background(), "session", "turn-2", "explain again", history)
	require.NoError(t, err)
	require.Equal(t, state.SourceLineage, followState.SourceLineage)
	allowed[source] = false
	blockedModel := &mockChat{responses: []mockResponse{lineageAnswer("must not be sent")}}
	blocked, blockedEvents := lineageEngine(t, blockedModel, nil, grantChecker(allowed))
	_, err = blocked.Execute(context.Background(), "session", "turn-3", "continue", []chat.Message{{Role: "assistant", Content: followState.FinalAnswer, SourceLineage: followState.SourceLineage}})
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Zero(t, blockedModel.callCount)
	require.Empty(t, *blockedEvents)
	require.NotEmpty(t, *events)
}

func TestAgentLineageUnionPreservesTwoFilesEpochsAndEmptyTool(t *testing.T) {
	a := agentLineageIdentity("1", "etag-old", 7)
	b := agentLineageIdentity("1", "etag-new", 8)
	c := agentLineageIdentity("2", "second", 5)
	allowed := map[types.SourceIdentity]bool{a: true, b: true, c: true}
	tool := newAccountedSourceTool(
		&types.ToolResult{Success: true, Output: "first", SourceLineage: agentComplete(t, a)},
		&types.ToolResult{Success: true, Output: "new revision and second file", SourceLineage: agentComplete(t, b, c)},
		&types.ToolResult{Success: true, Output: "No rows", Data: map[string]any{"results": []any{}}, SourceLineage: agentComplete(t)})
	model := &mockChat{responses: []mockResponse{lineageCall(), lineageCall(), lineageCall(), lineageAnswer("all paraphrased")}}
	e, _ := lineageEngine(t, model, tool, grantChecker(allowed))
	state, err := e.Execute(context.Background(), "s", "m", "question", nil)
	require.NoError(t, err)
	require.Equal(t, agentComplete(t, a, b, c), state.SourceLineage)
	allowed[c] = false
	require.Error(t, e.checkLineage(context.Background()))
}

type revokeAfterSendModel struct {
	*mockChat
	revoke func()
}

func (m *revokeAfterSendModel) ChatStream(ctx context.Context, messages []chat.Message, opts *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	stream, err := m.mockChat.ChatStream(ctx, messages, opts)
	if m.revoke != nil {
		m.revoke()
	}
	return stream, err
}
func TestAgentLineageRevocationAtActualModelOutputBlocksAllEvents(t *testing.T) {
	source := agentLineageIdentity("1", "original", 7)
	allowed := map[types.SourceIdentity]bool{source: true}
	model := &revokeAfterSendModel{mockChat: &mockChat{responses: []mockResponse{lineageAnswer("revoked secret")}}, revoke: func() { allowed[source] = false }}
	e, events := lineageEngine(t, model, nil, grantChecker(allowed))
	_, err := e.Execute(context.Background(), "s", "m", "query", []chat.Message{{Role: "assistant", Content: "previous uncited secret", SourceLineage: agentComplete(t, source)}})
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Equal(t, 1, model.callCount)
	require.Empty(t, *events)
}

func TestAgentLineageRevocationAtActualEventPublicationBlocksCollectedModelText(t *testing.T) {
	source := agentLineageIdentity("1", "original", 7)
	model := &mockChat{responses: []mockResponse{lineageAnswer("source title and secret body")}}
	checks := 0
	e, events := lineageEngine(t, model, nil, func(_ context.Context, l *types.SourceLineage) error {
		checks++
		// The initial message boundary, wire model-send, and chunk boundary
		// succeeded. Revoke at the real event publication callback.
		if checks == 4 {
			return errors.New("revoked before publish")
		}
		return l.RequireComplete()
	})
	_, err := e.Execute(context.Background(), "s", "m", "query", []chat.Message{{Role: "assistant", Content: "earlier source prose", SourceLineage: agentComplete(t, source)}})
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Equal(t, 1, model.callCount)
	require.Empty(t, *events)
}
func TestAgentLineageRevocationAtToolReturnBlocksResultMetadataAndNextSend(t *testing.T) {
	source := agentLineageIdentity("1", "original", 7)
	allowed := map[types.SourceIdentity]bool{source: true}
	tool := newAccountedSourceTool(&types.ToolResult{Success: true, Output: "secret", Error: "source title diagnostic", Data: map[string]any{"title": "secret name"}, SourceLineage: agentComplete(t, source)})
	tool.beforeReturn = func() { allowed[source] = false }
	model := &mockChat{responses: []mockResponse{lineageCall(), lineageAnswer("not sent")}}
	e, events := lineageEngine(t, model, tool, grantChecker(allowed))
	_, err := e.Execute(context.Background(), "s", "m", "query", nil)
	require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
	require.Equal(t, 1, model.callCount)
	for _, ev := range *events {
		require.NotEqual(t, event.EventAgentToolResult, ev.Type)
		require.NotEqual(t, event.EventAgentComplete, ev.Type)
	}
}
func TestAgentLineageFreshSourceIndependentAndMandatoryUnknown(t *testing.T) {
	for _, kind := range []string{"missing verifier", "legacy nil history", "unaccounted memory", "unaccounted image", "unaccounted profile"} {
		t.Run(kind, func(t *testing.T) {
			model := &mockChat{responses: []mockResponse{lineageAnswer("no")}}
			e, ev := lineageEngine(t, model, nil, grantChecker(map[types.SourceIdentity]bool{}))
			var history []chat.Message
			var images []string
			switch kind {
			case "missing verifier":
				e.SetSourceLineageVerifier(nil)
			case "legacy nil history":
				history = []chat.Message{{Role: "assistant", Content: "no citations"}}
			case "unaccounted memory":
				e.SetMemoryPrompt("prior source paraphrase")
			case "unaccounted image":
				images = []string{"data:image/png;base64,AAAA"}
			case "unaccounted profile":
				e.knowledgeBasesInfo = []*KnowledgeBaseInfo{{ID: "manual-kb", Name: "ordinary", Profile: &types.KnowledgeBaseProfile{}}}
			}
			_, err := e.Execute(context.Background(), "s", "m", "raw human text", history, images)
			require.ErrorIs(t, err, ErrAgentSourceLineageDenied)
			require.Zero(t, model.callCount)
			require.Empty(t, *ev)
		})
	}
	model := &mockChat{responses: []mockResponse{lineageAnswer("Hello.")}}
	e, _ := lineageEngine(t, model, nil, grantChecker(map[types.SourceIdentity]bool{}))
	state, err := e.Execute(context.Background(), "s", "m", "raw human greeting", nil)
	require.NoError(t, err)
	require.Equal(t, agentComplete(t), state.SourceLineage)
	require.Equal(t, "Hello.", state.FinalAnswer)
}

func TestAgentLineageSanitizeAndHistoryRedactionRetainUnion(t *testing.T) {
	a := agentComplete(t, agentLineageIdentity("1", "original", 7))
	b := agentComplete(t, agentLineageIdentity("2", "second", 5))
	merged := agenttools.SanitizeMessages([]chat.Message{{Role: "user", Content: "first", SourceLineage: a}, {Role: "user", Content: "second", SourceLineage: b}})
	require.Len(t, merged, 1)
	union, _ := types.UnionSourceLineage(a, b)
	require.Equal(t, union, merged[0].SourceLineage)
	redacted := HistoryAsSent([]chat.Message{{Role: "tool", Name: agenttools.ToolReadDocument, Content: "secret text", SourceLineage: a}}, false)
	require.NotContains(t, redacted[0].Content, "secret text")
	require.Equal(t, a, redacted[0].SourceLineage)
	encoded, err := json.Marshal(types.ToolResult{Success: true, Output: "public", SourceLineage: a})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "source_lineage")
	require.NotContains(t, string(encoded), "original")
	var forged types.ToolResult
	require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf(`{"success":true,"source_lineage":%s}`, mustAgentJSON(t, a))), &forged))
	require.Nil(t, forged.SourceLineage)
}
func mustAgentJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}
