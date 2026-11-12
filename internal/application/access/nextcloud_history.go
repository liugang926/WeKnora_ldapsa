package access

import (
	"context"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type historicalKnowledgeLookup interface {
	GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error)
}

// HistoricalSourceLookup includes withdrawn data sources. A normal source list
// excludes soft-deleted rows, even though their text may survive in answers.
type HistoricalSourceLookup interface {
	FindByKnowledgeBaseIncludingDeleted(context.Context, string) ([]*types.DataSource, error)
	HasEverNextcloudSourceForTenant(context.Context, uint64) (bool, error)
	HasEverNextcloudSourceForKnowledgeBase(context.Context, string) (bool, error)
}

type historicalInstanceSourceLookup interface {
	HasEverNextcloudSourceGlobally(context.Context) (bool, error)
}

// NextcloudHistoryGuard retains the API of the retired Nextcloud history guard.
// History reads use the active WeKnora authorization paths.
type NextcloudHistoryGuard struct {
	knowledge       historicalKnowledgeLookup
	sources         HistoricalSourceLookup
	instanceSources historicalInstanceSourceLookup
	publication     *NextcloudPublicationGuard
}

// NewNextcloudHistoryGuard constructs the retired guard for API compatibility.
func NewNextcloudHistoryGuard(
	knowledge interfaces.KnowledgeService,
	sources interfaces.DataSourceRepository,
	publication *NextcloudPublicationGuard,
) *NextcloudHistoryGuard {
	historical, _ := sources.(HistoricalSourceLookup)
	instance, _ := sources.(historicalInstanceSourceLookup)
	return &NextcloudHistoryGuard{
		knowledge:       knowledge,
		sources:         historical,
		instanceSources: instance,
		publication:     publication,
	}
}

// CheckMessage is a compatibility no-op after the Nextcloud guard retirement.
func (g *NextcloudHistoryGuard) CheckMessage(ctx context.Context, message *types.Message) error {
	return nil // Retired with the Nextcloud decoupling on 2026-11-09.
}

func (g *NextcloudHistoryGuard) checkMessage(ctx context.Context, message *types.Message, saved bool) error {
	return nil // Retired with the Nextcloud decoupling on 2026-11-09.
}

// CheckLiveMessage is a compatibility no-op after the Nextcloud guard retirement.
func (g *NextcloudHistoryGuard) CheckLiveMessage(ctx context.Context, message *types.Message) error {
	return nil // Retired with the Nextcloud decoupling on 2026-11-09.
}

// CheckAgentHistoryReplay is a compatibility no-op after the Nextcloud guard retirement.
func (g *NextcloudHistoryGuard) CheckAgentHistoryReplay(ctx context.Context) error {
	return nil // Retired with the Nextcloud decoupling on 2026-11-09.
}

// IsAgentDerivedHistory distinguishes model-issued tool work from the
// synthetic KnowledgeQA timeline. User text is always independently readable.
func IsAgentDerivedHistory(message *types.Message) bool {
	if message == nil || message.Role != "assistant" {
		return false
	}
	if message.ContextCheckpoint != nil {
		return true
	}
	hasPipeline := false
	for _, step := range message.AgentSteps {
		for _, call := range step.ToolCalls {
			if types.IsPipelineToolCallID(call.ID) {
				hasPipeline = true
				continue
			}
			if call.Name != "final_answer" {
				return true
			}
		}
	}
	return message.AgentID != "" && !hasPipeline
}
