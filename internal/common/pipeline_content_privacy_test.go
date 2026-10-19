package common

import (
	"bytes"
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/Tencent/WeKnora/internal/models/api"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestSourceContentPipelineLogsSuppressBodiesAndPreserveOrdinary(t *testing.T) {
	var output bytes.Buffer
	log := logrus.New()
	log.SetOutput(&output)
	ctx := context.WithValue(context.Background(), types.LoggerContextKey, logrus.NewEntry(log))
	err := &api.HTTPError{StatusCode: 400, Body: "PRIVATE_VENDOR_BODY"}
	fields := map[string]interface{}{"error": err, "content": "PRIVATE_OTHER_FIELD", "answer_preview": "PRIVATE_ANSWER"}
	for _, emit := range []func(context.Context, string, string, map[string]interface{}){
		PipelineInfo,
		PipelineWarn,
		PipelineError,
	} {
		output.Reset()
		emit(logprivacy.SuppressContent(ctx), "PRIVATE_STAGE", "PRIVATE_ACTION", fields)
		require.Empty(t, output.String(), "protected diagnostics never reach the caller's content logger")
		require.NotContains(t, output.String(), "PRIVATE_")
		require.Equal(t, "PRIVATE_VENDOR_BODY", err.Body)
		require.Same(t, err, fields["error"])
		require.Equal(t, "PRIVATE_ANSWER", fields["answer_preview"])
		output.Reset()
		emit(ctx, "Completion", "model_call", fields)
		require.Contains(t, output.String(), "PRIVATE_VENDOR_BODY")
		require.Contains(t, output.String(), "PRIVATE_OTHER_FIELD")
		require.Contains(t, output.String(), "PRIVATE_ANSWER")
	}
}
