package chatpipeline

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type callerPrivacyLog struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (w *callerPrivacyLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.Write(p)
}

func (w *callerPrivacyLog) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

func callerPrivacyContext(private bool) (context.Context, *callerPrivacyLog) {
	output := &callerPrivacyLog{}
	log := logrus.New()
	log.SetOutput(output)
	ctx := context.WithValue(context.Background(), types.LoggerContextKey, logrus.NewEntry(log))
	if private {
		ctx = logprivacy.SuppressContent(ctx)
	}
	return ctx, output
}

type callerPrivacyFailureModel struct{ err error }

func (m *callerPrivacyFailureModel) GetModelName() string { return "controlled" }
func (m *callerPrivacyFailureModel) GetModelID() string   { return "controlled" }
func (m *callerPrivacyFailureModel) Chat(context.Context, []chat.Message, *chat.ChatOptions) (*types.ChatResponse, error) {
	return nil, m.err
}
func (m *callerPrivacyFailureModel) ChatStream(context.Context, []chat.Message, *chat.ChatOptions) (<-chan types.StreamResponse, error) {
	return nil, m.err
}

func TestSourceContentCompletionErrorLogsPreserveOriginalFailure(t *testing.T) {
	for _, private := range []bool{true, false} {
		for _, stream := range []bool{false, true} {
			ctx, output := callerPrivacyContext(private)
			original := &api.HTTPError{StatusCode: 400, Body: "PRIVATE_VENDOR_CALL_FAILURE"}
			model := &callerPrivacyFailureModel{err: original}
			manage := &types.ChatManage{}
			manage.EventBus = &syncEventBus{}
			var result *PluginError
			if stream {
				plugin := &PluginChatCompletionStream{modelService: &stubModelService{model: model}}
				result = plugin.OnEvent(ctx, types.CHAT_COMPLETION_STREAM, manage, func() *PluginError { return nil })
			} else {
				plugin := &PluginChatCompletion{modelService: &stubModelService{model: model}}
				result = plugin.OnEvent(ctx, types.CHAT_COMPLETION, manage, func() *PluginError { return nil })
			}
			require.NotNil(t, result)
			require.Same(t, original, result.Err)
			require.Equal(t, ErrModelCall.ErrorType, result.ErrorType)
			require.Equal(t, ErrModelCall.Description, result.Description)
			if private {
				require.NotContains(t, output.String(), original.Body)
				require.Contains(t, output.String(), "content logging suppressed")
			} else {
				require.Contains(t, output.String(), original.Body)
			}
		}
	}
}

func TestSourceContentCompletionStreamErrorLogPreservesFailureControl(t *testing.T) {
	for _, private := range []bool{true, false} {
		ctx, output := callerPrivacyContext(private)
		if private {
			ctx = types.WithRAGSourceLineage(ctx, types.NewRAGSourceLineage(func(context.Context, *types.SourceLineage) error { return nil }))
		}
		model := &openStreamChat{closeStream: true, chunks: []types.StreamResponse{{ResponseType: types.ResponseTypeError, Content: "PRIVATE_VENDOR_STREAM_FAILURE", Done: true}}}
		bus := &syncEventBus{}
		manage := &types.ChatManage{}
		manage.EventBus = bus
		plugin := &PluginChatCompletionStream{modelService: &stubModelService{model: model}}
		require.Nil(t, plugin.OnEvent(ctx, types.CHAT_COMPLETION_STREAM, manage, func() *PluginError { return nil }))
		if private {
			select {
			case err := <-manage.SourceStreamResult:
				require.EqualError(t, err, "model stream reported failure")
			case <-time.After(2 * time.Second):
				t.Fatal("protected error stream did not finish")
			}
			require.NotContains(t, output.String(), "PRIVATE_VENDOR_STREAM_FAILURE")
		} else {
			require.Eventually(t, func() bool { return bytes.Contains([]byte(output.String()), []byte("PRIVATE_VENDOR_STREAM_FAILURE")) }, 2*time.Second, time.Millisecond)
		}
		require.Eventually(t, func() bool { bus.mu.Lock(); defer bus.mu.Unlock(); return len(bus.events) > 0 }, 2*time.Second, time.Millisecond)
		require.Empty(t, bus.finalAnswerContents())
		require.Equal(t, "PRIVATE_VENDOR_STREAM_FAILURE", model.chunks[0].Content)
	}
}
