package logger

import (
	"bytes"
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/logprivacy"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type forbiddenLogValue struct{}

func (forbiddenLogValue) String() string { panic("private value must not be formatted") }
func (forbiddenLogValue) Error() string  { panic("private error must not be formatted") }
func TestSourceWorkerDiagnosticsDoNotFormatContentOrInheritedFields(t *testing.T) {
	var out bytes.Buffer
	oldOutput, oldLevel := appLogger.Out, appLogger.Level
	appLogger.SetOutput(&out)
	appLogger.SetLevel(logrus.DebugLevel)
	t.Cleanup(func() { appLogger.SetOutput(oldOutput); appLogger.SetLevel(oldLevel) })
	ctx := WithFields(logprivacy.SuppressContent(context.Background()), Fields{"document": "PRIVATE_SOURCE_FIELD"})
	for _, c := range []context.Context{ctx, CloneContext(ctx), CloneContextWithoutTrace(ctx)} {
		GetLogger(c).WithField("private", forbiddenLogValue{}).Errorf("PRIVATE_SOURCE %s", forbiddenLogValue{})
		Debug(c, forbiddenLogValue{})
		Debugf(c, "PRIVATE_SOURCE_BODY %s", forbiddenLogValue{})
		Info(c, forbiddenLogValue{})
		Infof(c, "PRIVATE_SOURCE_BODY %s", forbiddenLogValue{})
		Warn(c, forbiddenLogValue{})
		Warnf(c, "PRIVATE_SOURCE_BODY %s", forbiddenLogValue{})
		Error(c, forbiddenLogValue{})
		Errorf(c, "PRIVATE_SOURCE_BODY %s", forbiddenLogValue{})
		WarnWithFields(c, Fields{"secret": forbiddenLogValue{}}, "PRIVATE_SOURCE_WARNING")
		ErrorWithFields(c, forbiddenLogValue{}, Fields{"secret": forbiddenLogValue{}})
	}
	require.Contains(t, out.String(), "Protected source diagnostic")
	require.NotContains(t, out.String(), "PRIVATE_SOURCE")
	require.NotContains(t, out.String(), "PANIC")
	out.Reset()
	Infof(WithFields(context.Background(), Fields{"ordinary": "PUBLIC_FIELD"}), "PUBLIC_MESSAGE %s", "PUBLIC_VALUE")
	require.Contains(t, out.String(), "PUBLIC_MESSAGE PUBLIC_VALUE")
	require.Contains(t, out.String(), "PUBLIC_FIELD")
}
