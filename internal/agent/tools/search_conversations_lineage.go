package tools

import (
	"context"
	"encoding/json"

	"github.com/Tencent/WeKnora/internal/types"
)

// This port is implemented by the native message service. It admits exact
// original message/title bodies before search and retains their current
// guards and returned projection on the turn's actual material boundary.
type conversationInputReader interface {
	SearchConversationsWithInputs(context.Context, *types.MessageSearchParams) (
		*types.MessageSearchResult, *types.SourceLineage, error)
}

func (t *SearchConversationsTool) ExecuteSourceLineage(
	ctx context.Context, args json.RawMessage,
) (result *types.ToolResult, err error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	reader, ok := t.messageService.(conversationInputReader)
	if turn == nil || !ok || reader == nil {
		return nil, ErrToolSourceLineageUnavailable
	}
	accounted := false
	defer func() {
		if err != nil && !accounted {
			_ = turn.Add(types.UnknownSourceLineage())
		}
	}()
	if err := turn.Check(ctx); err != nil {
		return nil, err
	}
	var actual *types.SourceLineage
	called := false
	result, err = t.executeConversationSearch(ctx, args, func(searchCtx context.Context,
		params *types.MessageSearchParams,
	) (*types.MessageSearchResult, error) {
		called = true
		found, lineage, readErr := reader.SearchConversationsWithInputs(searchCtx, params)
		if readErr != nil {
			return nil, readErr
		}
		if found == nil || lineage.RequireComplete() != nil {
			return nil, ErrToolSourceLineageUnavailable
		}
		actual = lineage
		return found, nil
	})
	// Before the native callback, this shared executor performs only local
	// JSON/control validation. Its ordinary diagnostic has no external input;
	// preserve the enclosing authorized prefix so the model can correct args.
	if !called && result != nil {
		actual = independentToolInput()
	} else if err != nil || result == nil || !called || actual.RequireComplete() != nil {
		if err == nil {
			err = ErrToolSourceLineageUnavailable
		}
		return nil, err
	}
	if checkErr := turn.Add(actual); checkErr != nil {
		return nil, checkErr
	}
	if checkErr := turn.Check(ctx); checkErr != nil {
		return nil, checkErr
	}
	lineage, cloneErr := types.UnionSourceLineage(actual)
	if cloneErr != nil {
		return nil, cloneErr
	}
	result.SourceLineage = lineage
	accounted = true
	return result, err
}
