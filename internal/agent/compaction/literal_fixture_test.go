package compaction

import (
	"context"
	"errors"

	agenttoken "github.com/Tencent/WeKnora/internal/agent/token"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
)

// literalCompactionInput accounts only text constructed by these unit fixtures.
// It is never a loader that certifies arbitrary stored or external messages.
func literalCompactionInput() *types.SourceLineage {
	lineage, err := types.NewCompleteSourceLineage()
	if err != nil {
		panic(err)
	}
	return lineage
}

func newLiteralTestCompactor(model chat.Chat, estimator *agenttoken.Estimator, settings Settings) *Compactor {
	compactor := New(model, estimator, settings)
	if compactor != nil {
		compactor.SetSourceLineageVerifier(func(_ context.Context, lineage *types.SourceLineage) error {
			if err := lineage.RequireComplete(); err != nil {
				return err
			}
			if len(lineage.Sources) != 0 {
				return errors.New("literal fixture cannot certify external sources")
			}
			return nil
		})
	}
	return compactor
}
