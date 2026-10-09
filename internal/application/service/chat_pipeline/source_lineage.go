package chatpipeline

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

// BeginSourceLineage is the mandatory ordinary RAG entry boundary. Only text,
// persisted accounted history and trusted hydrated document results are covered.
// Other adapters remain unknown until they supply their own dependency contract.
func (e *EventManager) BeginSourceLineage(ctx context.Context, cm *types.ChatManage, scope access.LineageReadScope) (context.Context, error) {
	turn := types.NewRAGSourceLineage(func(ctx context.Context, lineage *types.SourceLineage) error {
		if err := types.CheckHistoryMaterialLifetime(ctx); err != nil {
			return err
		}
		if e == nil || e.sourceLineageGuard == nil {
			return access.ErrNextcloudPublicationUnavailable
		}
		return e.sourceLineageGuard.Check(ctx, lineage, scope)
	})
	ctx = types.WithRAGSourceLineage(ctx, turn)
	if cm == nil || len(cm.Images) > 0 || len(cm.Attachments) > 0 || cm.QuotedContext != "" ||
		cm.ImageDescription != "" || cm.WebSearchEnabled || cm.DataAnalysisEnabled || len(cm.Entity) > 0 || cm.GraphResult != nil {
		_ = turn.Add(types.UnknownSourceLineage())
	}
	if err := CheckSourceLineageInputs(ctx, cm); err != nil {
		return ctx, err
	}
	return ctx, nil
}

func ConfigureSourceLineage(manager *EventManager, guard *access.NextcloudSourceLineageGuard) {
	manager.sourceLineageGuard = guard
}

func ConfigureRAGChunkMergeHydration(manager *EventManager, base interfaces.KnowledgeBaseService) {
	reader, _ := base.(interfaces.RAGChunkAugmentationReader)
	for _, plugin := range manager.listeners[types.CHUNK_MERGE] {
		if merge, ok := plugin.(*PluginMerge); ok {
			ConfigurePluginMergeRAGChunkHydration(merge, reader)
		}
	}
}

// AccountHistory runs on the actual retained pairs, including history loaded
// independently for rewriting. It never reconstructs proof from citations.
func AccountHistory(ctx context.Context, history []*types.History) error {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return nil
	} // Non-RAG pipelines have not activated this producer.
	for _, h := range history {
		if h == nil {
			_ = turn.Add(nil)
			continue
		}
		if err := turn.Add(h.SourceLineage); err != nil {
			return err
		}
	}
	return turn.Check(ctx)
}

func AccountResults(ctx context.Context, results []*types.SearchResult) error {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return nil
	}
	for _, result := range results {
		if result == nil {
			_ = turn.Add(nil)
			continue
		}
		if err := turn.Add(result.SourceLineage); err != nil {
			return err
		}
	}
	return turn.Check(ctx)
}

// CheckSourceLineageInputs checks every adapter used by the ordinary final
// prompt and every earlier influencing call. Unknown state is sticky.
func CheckSourceLineageInputs(ctx context.Context, cm *types.ChatManage) error {
	if err := types.CheckHistoryMaterialLifetime(ctx); err != nil {
		return err
	}
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return nil
	}
	if cm == nil {
		_ = turn.Add(nil)
		return turn.Check(ctx)
	}
	if len(cm.Images) > 0 || len(cm.Attachments) > 0 || cm.QuotedContext != "" || cm.ImageDescription != "" ||
		cm.MemoryPrompt != "" || len(cm.UsedMemories) > 0 || cm.GraphResult != nil || len(cm.Entity) > 0 {
		_ = turn.Add(types.UnknownSourceLineage())
	}
	if err := AccountHistory(ctx, cm.History); err != nil {
		return err
	}
	return turn.Check(ctx)
}

func sourceLineagePluginError(err error) *PluginError {
	if errors.Is(err, types.ErrSourceLineageUnknown) || errors.Is(err, types.ErrSourceLineageInvalid) || errors.Is(err, types.ErrSourceLineageLimit) {
		err = access.ErrNextcloudPublicationDenied
	}
	return &PluginError{Err: err, Description: "Source input authorization failed", ErrorType: "source_lineage_failed"}
}

func stampModelInputLineage(ctx context.Context, messages []chat.Message) error {
	if turn := types.RAGSourceLineageFromContext(ctx); turn != nil {
		lineage, err := turn.CheckedSnapshot(ctx)
		if err != nil {
			return err
		}
		for i := range messages {
			messages[i].SourceLineage, _ = types.UnionSourceLineage(lineage)
		}
	}
	return nil
}

func emitSourceLineageEvent(ctx context.Context, bus types.EventBusInterface, evt types.Event) error {
	if err := types.CheckHistoryMaterialLifetime(ctx); err != nil {
		return err
	}
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn != nil {
		lineage, err := turn.CheckedSnapshot(ctx)
		if err != nil {
			return err
		}
		evt.SourceLineage = lineage
	}
	err := bus.Emit(ctx, evt)
	if err != nil && turn != nil {
		_ = turn.Add(types.UnknownSourceLineage())
	}
	return err
}
