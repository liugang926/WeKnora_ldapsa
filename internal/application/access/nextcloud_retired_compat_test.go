package access

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

type retiredPanicContext struct{ context.Context }

func (retiredPanicContext) Value(any) any { panic("retired guard consulted the request context") }

type retiredPanicKnowledge struct{}

func (retiredPanicKnowledge) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	panic("retired guard consulted knowledge")
}

type retiredPanicSources struct{}

func (retiredPanicSources) FindByKnowledgeBaseIncludingDeleted(context.Context, string) ([]*types.DataSource, error) {
	panic("retired guard consulted sources")
}

func (retiredPanicSources) HasEverNextcloudSourceForTenant(context.Context, uint64) (bool, error) {
	panic("retired guard consulted tenant history")
}

func (retiredPanicSources) HasEverNextcloudSourceForKnowledgeBase(context.Context, string) (bool, error) {
	panic("retired guard consulted KB history")
}

func (retiredPanicSources) HasEverNextcloudSourceGlobally(context.Context) (bool, error) {
	panic("retired guard consulted instance history")
}

func TestNextcloudRetiredCompat(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "nil guards"
		var history *NextcloudHistoryGuard
		var publication *NextcloudPublicationGuard
		var lineage *NextcloudSourceLineageGuard
		if configured {
			name = "configured guards"
			publication = NewNextcloudPublicationGuard(nil, nil)
			history = &NextcloudHistoryGuard{
				knowledge: retiredPanicKnowledge{}, sources: retiredPanicSources{},
				instanceSources: retiredPanicSources{}, publication: publication,
			}
			lineage = NewNextcloudSourceLineageGuard(nil, nil, nil, nil, nil, publication)
		}
		t.Run(name, func(t *testing.T) {
			contexts := []context.Context{nil, context.Background(), retiredPanicContext{context.Background()}}
			for _, ctx := range contexts {
				messages := []*types.Message{nil, {Role: "user"}, {Role: "assistant", AgentID: "legacy-agent"}}
				for _, message := range messages {
					for _, err := range []error{
						history.CheckMessage(ctx, message), history.checkMessage(ctx, message, true),
						history.checkMessage(ctx, message, false), history.CheckLiveMessage(ctx, message),
						history.CheckAgentHistoryReplay(ctx),
					} {
						if err != nil {
							t.Fatalf("retired history check: %v", err)
						}
					}
				}
				for _, knowledge := range []*types.Knowledge{nil, {ID: "legacy", KnowledgeBaseID: "unavailable"}} {
					if err := publication.CheckKnowledge(ctx, knowledge); err != nil {
						t.Fatalf("retired knowledge check: %v", err)
					}
				}
				if err := publication.CheckSourceRoot(ctx, nil, "unknown", "unknown"); err != nil {
					t.Fatal(err)
				}
				if err := publication.CheckSourceRoot(ctx, &types.DataSource{}, "", ""); err != nil {
					t.Fatal(err)
				}
				for _, source := range []*types.SourceLineage{nil, {}, types.UnknownSourceLineage()} {
					if err := lineage.Check(ctx, source, LineageReadScope{AgentID: "legacy"}); err != nil {
						t.Fatal(err)
					}
					reads := []SourceLineageRead{{Lineage: source, Scope: LineageReadScope{AgentID: "legacy"}}}
					if err := lineage.CheckBatch(ctx, reads); err != nil {
						t.Fatal(err)
					}
				}
				if err := lineage.CheckBatch(ctx, nil); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
