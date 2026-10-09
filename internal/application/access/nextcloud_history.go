package access

import (
	"context"
	"fmt"
	"strings"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

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

// NextcloudHistoryGuard checks persisted answer provenance. A saved answer is
// a copy of source content and must be authorized again at every read, even if
// the original search or connector sync was authorized at generation time.
type NextcloudHistoryGuard struct {
	knowledge       historicalKnowledgeLookup
	sources         HistoricalSourceLookup
	instanceSources historicalInstanceSourceLookup
	publication     *NextcloudPublicationGuard
}

// NewNextcloudHistoryGuard returns the live source-history authorization guard.
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

// CheckMessage returns nil for user-authored text and ordinary answers. For
// answers that could contain Nextcloud material it checks every identified
// document and refuses broad or ambiguous old scopes. An answer generated
// against an entire KB does not carry an exhaustive list of every retrieved
// chunk, so checking only its visible citations would be unsafe.
func (g *NextcloudHistoryGuard) CheckMessage(ctx context.Context, message *types.Message) error {
	return g.checkMessage(ctx, message, true)
}

func (g *NextcloudHistoryGuard) checkMessage(ctx context.Context, message *types.Message, saved bool) error {
	if message == nil {
		return ErrNextcloudPublicationDenied
	}
	if message.Role != "assistant" {
		return nil
	}
	if saved && IsAgentDerivedHistory(message) {
		if err := g.CheckAgentHistoryReplay(ctx); err != nil {
			return err
		}
	}
	seenDocuments := make(map[string]struct{})
	checkDocument := func(id string, marked bool) error {
		id = strings.TrimSpace(id)
		if id == "" {
			if marked {
				return apperrors.NewProtocolError(fmt.Errorf(
					"%w: source reference lacks document identity",
					ErrNextcloudPublicationDenied,
				), fmt.Sprintf("%s: source reference lacks document identity", apperrors.PublicMessage(
					ErrNextcloudPublicationDenied,
				)))
			}
			return nil
		}
		if _, seen := seenDocuments[id]; seen {
			return nil
		}
		seenDocuments[id] = struct{}{}
		if g == nil || g.knowledge == nil {
			return ErrNextcloudPublicationUnavailable
		}
		knowledge, err := g.knowledge.GetKnowledgeByIDOnly(ctx, id)
		if err != nil || knowledge == nil || knowledge.ID != id {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: historical document is unavailable",
				ErrNextcloudPublicationDenied,
			), fmt.Sprintf("%s: historical document is unavailable", apperrors.PublicMessage(
				ErrNextcloudPublicationDenied,
			)))
		}
		return g.publication.CheckKnowledge(ctx, knowledge)
	}
	for _, ref := range message.KnowledgeReferences {
		if ref == nil {
			continue
		}
		marked := ref.KnowledgeChannel == types.ConnectorTypeNextcloud ||
			ref.Metadata["nextcloud_file_id"] != "" ||
			ref.Metadata["nextcloud_binding_id"] != ""
		if err := checkDocument(ref.KnowledgeID, marked); err != nil {
			return err
		}
	}
	for _, id := range message.ExecutionContext.KnowledgeIDs {
		if err := checkDocument(id, false); err != nil {
			return err
		}
	}

	// The explicit request scope, tag scopes, and a selected Agent's configured
	// KBs are all possible source pools. A saved answer does not prove which
	// chunks from such a pool influenced its text; any KB with a Nextcloud
	// source, including a deleted one, makes the answer unsafe to replay.
	kbIDs := append([]string(nil), message.ExecutionContext.KnowledgeBaseIDs...)
	kbIDs = append(kbIDs, message.ExecutionContext.AgentKnowledgeBaseIDs...)
	for _, scope := range message.ExecutionContext.TagScopes {
		kbIDs = append(kbIDs, scope.KnowledgeBaseID)
	}
	seenKBs := make(map[string]struct{})
	for _, kbID := range kbIDs {
		if kbID == "" {
			continue
		}
		if _, seen := seenKBs[kbID]; seen {
			continue
		}
		seenKBs[kbID] = struct{}{}
		if g == nil || g.sources == nil {
			return ErrNextcloudPublicationUnavailable
		}
		ever, err := g.sources.HasEverNextcloudSourceForKnowledgeBase(ctx, kbID)
		if err != nil {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: KB source history unavailable: %v",
				ErrNextcloudPublicationUnavailable,
				err,
			), fmt.Sprintf("%s: KB source history unavailable: %v", apperrors.PublicMessage(
				ErrNextcloudPublicationUnavailable,
			), func() any {
				if err == nil {
					return nil
				}
				return apperrors.PublicMessage(err)
			}()))
		}
		if ever {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: answer has non-exhaustive Nextcloud KB scope",
				ErrNextcloudPublicationDenied,
			), fmt.Sprintf("%s: answer has non-exhaustive Nextcloud KB scope", apperrors.PublicMessage(
				ErrNextcloudPublicationDenied,
			)))
		}

		sources, err := g.sources.FindByKnowledgeBaseIncludingDeleted(ctx, kbID)
		if err != nil {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: historical source lookup failed: %v",
				ErrNextcloudPublicationUnavailable,
				err,
			), fmt.Sprintf("%s: historical source lookup failed: %v", apperrors.PublicMessage(
				ErrNextcloudPublicationUnavailable,
			), func() any {
				if err == nil {
					return nil
				}
				return apperrors.PublicMessage(err)
			}()))
		}
		for _, source := range sources {
			if source != nil && source.Type == types.ConnectorTypeNextcloud {
				return apperrors.NewProtocolError(fmt.Errorf(
					"%w: answer has non-exhaustive Nextcloud KB scope",
					ErrNextcloudPublicationDenied,
				), fmt.Sprintf("%s: answer has non-exhaustive Nextcloud KB scope", apperrors.PublicMessage(
					ErrNextcloudPublicationDenied,
				)))
			}
		}
	}

	// Older Agent messages did not snapshot resolved KBs. All-mode Agents can
	// query the whole tenant. When any Nextcloud source has ever existed in that
	// tenant, an answer lacking an exhaustive source list must remain hidden.
	mode := message.ExecutionContext.AgentKBSelectionMode
	if IsAgentDerivedHistory(message) && (mode == "" || mode == "all") {
		tenantID := message.AgentTenantID
		if tenantID == 0 {
			tenantID = types.CallerFromContext(ctx).TenantID
		}
		if g == nil || g.sources == nil || tenantID == 0 {
			return ErrNextcloudPublicationUnavailable
		}
		hasSource, err := g.sources.HasEverNextcloudSourceForTenant(ctx, tenantID)
		if err != nil {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: historical tenant source lookup failed: %v",
				ErrNextcloudPublicationUnavailable,
				err,
			), fmt.Sprintf("%s: historical tenant source lookup failed: %v", apperrors.PublicMessage(
				ErrNextcloudPublicationUnavailable,
			), func() any {
				if err == nil {
					return nil
				}
				return apperrors.PublicMessage(err)
			}()))
		}
		if hasSource {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: legacy Agent source scope is ambiguous",
				ErrNextcloudPublicationDenied,
			), fmt.Sprintf("%s: legacy Agent source scope is ambiguous", apperrors.PublicMessage(
				ErrNextcloudPublicationDenied,
			)))
		}
	}
	return nil
}

// CheckLiveMessage validates every source document already identified by the
// running turn. Unlike a saved answer, a live RAG turn has a pre-answer
// references event that identifies its retrieved chunks; Agent knowledge tool
// results also identify their documents. A broad Nextcloud scope without any
// such provenance is withheld until the source documents are known.
func (g *NextcloudHistoryGuard) CheckLiveMessage(ctx context.Context, message *types.Message) error {
	if message == nil {
		return ErrNextcloudPublicationDenied
	}
	cloned := *message
	cloned.ExecutionContext = message.ExecutionContext
	cloned.ExecutionContext.KnowledgeBaseIDs = nil
	cloned.ExecutionContext.AgentKnowledgeBaseIDs = nil
	cloned.ExecutionContext.TagScopes = nil
	cloned.ExecutionContext.AgentKBSelectionMode = "none"
	cloned.AgentID = ""
	cloned.AgentSteps = nil
	cloned.ContextCheckpoint = nil
	if err := g.checkMessage(ctx, &cloned, false); err != nil {
		return err
	}
	for _, ref := range message.KnowledgeReferences {
		if ref != nil && strings.TrimSpace(ref.KnowledgeID) != "" {
			return nil
		}
	}
	// With no observed source document, preserve the historical fail-closed
	// rule. Ordinary KBs still pass; a Nextcloud KB or all-mode Agent waits
	// for a provenance event before any answer or tool output is emitted.
	scope := *message
	scope.AgentSteps = nil
	scope.ContextCheckpoint = nil
	return g.checkMessage(ctx, &scope, false)
}

// CheckAgentHistoryReplay admits model history only on an instance positively
// verified never to have contained a Nextcloud source. Per-message citations
// cannot prove that newer paraphrases or compaction summaries are independent
// of an older revoked turn. This conservative barrier is an interim fallback
// until complete transitive lineage can be persisted and reauthorized.
func (g *NextcloudHistoryGuard) CheckAgentHistoryReplay(ctx context.Context) error {
	if g == nil {
		return ErrNextcloudPublicationUnavailable
	}
	lookup := g.instanceSources
	if lookup == nil {
		lookup, _ = g.sources.(historicalInstanceSourceLookup)
	}
	if lookup == nil {
		return ErrNextcloudPublicationUnavailable
	}
	seen, err := lookup.HasEverNextcloudSourceGlobally(ctx)
	if err != nil {
		return apperrors.NewProtocolError(fmt.Errorf(
			"%w: Agent source history lookup failed: %v",
			ErrNextcloudPublicationUnavailable,
			err,
		), fmt.Sprintf("%s: Agent source history lookup failed: %v", apperrors.PublicMessage(
			ErrNextcloudPublicationUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	if seen {
		return apperrors.NewProtocolError(fmt.Errorf(("%w: Agent history has no exhaustive transitive source l"+
			"ineage"),
			ErrNextcloudPublicationDenied),
			fmt.Sprintf(
				("%s: Agent history has no exhaustive transitive source l"+
					"ineage"), apperrors.PublicMessage(ErrNextcloudPublicationDenied)))
	}
	return nil
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
