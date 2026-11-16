package storageurl

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
)

// RequestResolver selects an authorized file service for each reference in an HTTP response.
type RequestResolver func(context.Context, interfaces.FileService, interfaces.StorageBackendResolver) Resolver

// NewHTTPResourceResolver wires the same live workspace and knowledge-base checks used
// to publish HTTP search results. The result text can contain caller-supplied handles.
func NewHTTPResourceResolver(
	catalog interfaces.ResourceCatalog, tenants interfaces.TenantService, guard *access.NextcloudSourceLineageGuard,
) RequestResolver {
	return func(
		ctx context.Context, fallback interfaces.FileService, backends interfaces.StorageBackendResolver,
	) Resolver {
		tenant, _ := types.TenantInfoFromContext(ctx)
		callerID := types.CallerFromContext(ctx).TenantID
		if callerID == 0 || catalog == nil || tenants == nil || guard == nil || backends == nil {
			return nil
		}
		if tenant == nil || tenant.ID != callerID {
			var err error
			tenant, err = tenants.GetTenantByID(ctx, callerID)
			if err != nil || tenant == nil || tenant.ID != callerID {
				return nil
			}
		}
		resolver := NewFileServiceResolver(tenant, fallback, backends)
		resolver.httpPublication = true
		return resolver.WithContext(ctx).WithResources(catalog, tenants).
			WithAuthorization(func(ctx context.Context, reference string) error {
				return authorizeHTTPResource(ctx, catalog, guard, reference)
			})
	}
}

func authorizeHTTPResource(
	ctx context.Context, catalog interfaces.ResourceCatalog,
	guard *access.NextcloudSourceLineageGuard, reference string,
) error {
	physical, resource, err := catalog.ResolvePath(ctx, reference)
	if err != nil {
		return err
	}
	owner := secutils.ParseTenantIDFromStoragePath(physical)
	if resource != nil {
		owner = resource.TenantID
	}
	if owner == 0 || secutils.ValidateStoragePathTenant(physical, owner) != nil {
		return access.ErrForbidden
	}
	if owner != types.CallerFromContext(ctx).TenantID {
		ids, err := catalog.ListKnowledgeBaseIDs(ctx, owner, reference)
		if err != nil {
			return err
		}
		// Only a registered derived resource, explicitly bound to a currently accessible KB,
		// can select another workspace's storage credentials.
		if _, handle := types.ParseResourcePath(reference); !handle || resource == nil || len(ids) == 0 ||
			secutils.ValidateKBScopedStoragePath(physical, owner) != nil {
			return access.ErrForbidden
		}
	}
	lookup, ok := catalog.(interfaces.ResourceKnowledgeLookup)
	if !ok {
		return access.ErrForbidden
	}
	owners, known, err := lookup.ListResourceKnowledgeOwners(ctx, owner, reference)
	if err != nil {
		return err
	}
	if !known || (owner != types.CallerFromContext(ctx).TenantID && len(owners) == 0) {
		return access.ErrForbidden
	}
	scope := access.LineageReadScope{}
	for _, knowledge := range owners {
		if knowledge == nil || knowledge.DeletedAt.Valid ||
			knowledge.TenantID != owner || knowledge.KnowledgeBaseID == "" {
			return access.ErrForbidden
		}
		scope.Targets = append(scope.Targets, &types.SearchTarget{
			KnowledgeBaseID: knowledge.KnowledgeBaseID, TenantID: owner,
		})
	}
	return guard.CheckWorkspaceRead(ctx, scope)
}

// NewRequestRewriter builds a response rewriter. Public URLs require a resolver
// wired to current credentials and catalog bindings. Without it, handles remain
// available to the authenticated file proxy.
func NewRequestRewriter(
	ctx context.Context, mode Mode, defaultSvc interfaces.FileService,
	storageResolver interfaces.StorageBackendResolver, resources ...RequestResolver,
) *Rewriter {
	if mode != ModePublic || len(resources) != 1 || resources[0] == nil {
		return NewRewriter(nil, "API")
	}
	return NewRewriter(resources[0](ctx, defaultSvc, storageResolver), "API")
}

// RewriteMessagesResponse returns rewritten message copies so callers can keep
// the originals untouched (for example objects returned from a service cache).
func (w *Rewriter) RewriteMessagesResponse(ctx context.Context, messages []*types.Message) []*types.Message {
	if !w.Enabled() || len(messages) == 0 {
		return messages
	}
	out := cloneMessages(messages)
	w.RewriteMessages(ctx, out)
	return out
}

// cloneMessages deep-copies messages via JSON so nested slices and agent steps
// are independent of the source slice.
func cloneMessages(messages []*types.Message) []*types.Message {
	data, err := json.Marshal(messages)
	if err != nil {
		return messages
	}
	var out []*types.Message
	if err := json.Unmarshal(data, &out); err != nil {
		return messages
	}
	return out
}

// RewriteMessages replaces storage references in a message history response so
// clients receive loadable image URLs. Messages are mutated in place.
func (w *Rewriter) RewriteMessages(ctx context.Context, messages []*types.Message) {
	if !w.Enabled() {
		return
	}
	for _, message := range messages {
		if message == nil {
			continue
		}
		message.Content = w.String(ctx, message.Content)
		for i := range message.Images {
			message.Images[i].URL = w.Ref(ctx, message.Images[i].URL)
			message.Images[i].Caption = w.String(ctx, message.Images[i].Caption)
		}
		message.KnowledgeReferences = w.CopyReferences(ctx, message.KnowledgeReferences)
		w.rewriteAgentSteps(ctx, message.AgentSteps)
	}
}

// CopyReferences returns rewritten copies of retrieval results, covering both the
// chunk text and the structured image_info payload.
//
// Copies rather than in-place edits because an SSE references payload shares its
// *SearchResult pointers with the stream replay buffer and the assistant message
// being persisted; rewriting those in place would corrupt both.
func (w *Rewriter) CopyReferences(ctx context.Context, refs []*types.SearchResult) []*types.SearchResult {
	if !w.Enabled() || refs == nil {
		return refs
	}
	out := make([]*types.SearchResult, len(refs))
	for i, ref := range refs {
		if ref == nil {
			continue
		}
		rewritten := *ref
		rewritten.Content = w.String(ctx, ref.Content)
		rewritten.MatchedContent = w.String(ctx, ref.MatchedContent)
		rewritten.ImageInfo = w.String(ctx, ref.ImageInfo)
		rewritten.MatchedImages = slices.Clone(ref.MatchedImages)
		for j := range rewritten.MatchedImages {
			rewritten.MatchedImages[j].URL = w.Ref(ctx, ref.MatchedImages[j].URL)
		}
		out[i] = &rewritten
	}
	return out
}

// CopyData returns a rewritten copy of an SSE metadata map, or data itself when
// it holds no storage reference. Agent tool results put renderable Markdown into
// this map, and its shape is tool-defined, so every string leaf is rewritten.
func (w *Rewriter) CopyData(ctx context.Context, data map[string]interface{}) map[string]interface{} {
	if !w.Enabled() || data == nil {
		return data
	}
	rewritten, changed := w.copyValue(ctx, data, 0)
	if !changed {
		return data
	}
	out, _ := rewritten.(map[string]interface{})
	return out
}

// CopyValue is CopyData for a decoded JSON value of any shape (object, array
// or scalar). Unchanged values are returned as-is.
func (w *Rewriter) CopyValue(ctx context.Context, value interface{}) interface{} {
	if !w.Enabled() {
		return value
	}
	rewritten, _ := w.copyValue(ctx, value, 0)
	return rewritten
}

// maxDataDepth bounds recursion into tool-defined metadata. Renderable content
// sits within a couple of levels; the cap only guards against a pathologically
// nested payload.
const maxDataDepth = 8

func (w *Rewriter) copyValue(ctx context.Context, value interface{}, depth int) (interface{}, bool) {
	if depth > maxDataDepth {
		return value, false
	}
	switch typed := value.(type) {
	case string:
		out := w.String(ctx, typed)
		return out, out != typed
	// The references event carries its results twice: once in
	// StreamResponse.KnowledgeReferences and once in Data. With an in-memory
	// stream manager these arrive as the typed slice rather than the decoded
	// []interface{} a Redis round-trip produces, so both forms need a case or
	// the Data copy leaks handles the caller asked to have resolved.
	case types.References:
		return types.References(w.CopyReferences(ctx, typed)), true
	case []*types.SearchResult:
		return w.CopyReferences(ctx, typed), true
	case []string:
		out := make([]string, len(typed))
		changed := false
		for i, item := range typed {
			out[i] = w.String(ctx, item)
			changed = changed || out[i] != item
		}
		return out, changed
	case map[string]string:
		out := make(map[string]string, len(typed))
		changed := false
		for key, item := range typed {
			out[key] = w.String(ctx, item)
			changed = changed || out[key] != item
		}
		return out, changed
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		changed := false
		for key, item := range typed {
			converted, itemChanged := w.copyValue(ctx, item, depth+1)
			out[key] = converted
			changed = changed || itemChanged
		}
		return out, changed
	case []interface{}:
		out := make([]interface{}, len(typed))
		changed := false
		for i, item := range typed {
			converted, itemChanged := w.copyValue(ctx, item, depth+1)
			out[i] = converted
			changed = changed || itemChanged
		}
		return out, changed
	default:
		return value, false
	}
}

// rewriteAgentSteps covers the reasoning trace, whose tool output embeds Markdown
// images for retrieved figures and generated charts.
func (w *Rewriter) rewriteAgentSteps(ctx context.Context, steps types.AgentSteps) {
	for i := range steps {
		steps[i].Thought = w.String(ctx, steps[i].Thought)
		steps[i].ReasoningContent = w.String(ctx, steps[i].ReasoningContent)
		for j := range steps[i].ToolCalls {
			call := &steps[i].ToolCalls[j]
			call.Reflection = w.String(ctx, call.Reflection)
			if call.Result != nil {
				call.Result.Output = w.String(ctx, call.Result.Output)
			}
		}
	}
}
