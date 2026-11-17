package container

import (
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/Tencent/WeKnora/internal/runtime"
)

func init() {
	must(runtime.GetContainer().Decorate(handler.WithDingTalkScheduleStatus))
}
