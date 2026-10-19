package session

import (
	"context"
	stderrors "errors"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
)

func rawSearchPotentialSource(result *types.SearchResult) bool {
	if result == nil {
		return false
	}
	metadata := result.Metadata
	return result.KnowledgeChannel == types.ConnectorTypeNextcloud ||
		metadata["datasource_id"] != "" || metadata["nextcloud_instance_id"] != "" ||
		metadata["nextcloud_binding_id"] != "" || metadata["nextcloud_file_id"] != ""
}

func rawSearchLeaseHTTPError(err error) error {
	if stderrors.Is(err, repository.ErrNextcloudContentLeaseDenied) ||
		stderrors.Is(err, repository.ErrNextcloudContentLeaseExpired) {
		return errors.NewForbiddenError("Nextcloud content is no longer available")
	}
	return errors.NewServiceUnavailableError("Cannot verify current Nextcloud content lease")
}

// Resolve every requested search target to a trusted KB before retrieval.
// An unresolvable direct document or tag scope cannot be searched without a
// KB-wide lease, so the request fails closed instead of using a late lease.
func (h *Handler) beginRawSearchRead(c *gin.Context, kbIDs, knowledgeIDs []string,
	tagScopes []types.TagScope,
) (*readlease.NextcloudReadGuard, error) {
	// Hand-built legacy test handlers may omit the store for ordinary results.
	// A source result still fails closed in protectRawSearchResults. Production
	// always configures the durable store before registering this route.
	if h.searchContentLeases == nil {
		return nil, nil
	}
	if h.knowledgebaseService == nil || h.searchKnowledgeService == nil {
		return nil, errors.NewServiceUnavailableError("Cannot resolve knowledge search lease scopes")
	}
	allKBIDs := make(map[string]bool)
	for _, kbID := range kbIDs {
		if kbID == "" {
			return nil, errors.NewBadRequestError("Knowledge base ID cannot be empty")
		}
		allKBIDs[kbID] = true
	}
	for _, scope := range tagScopes {
		if scope.KnowledgeBaseID == "" {
			return nil, errors.NewBadRequestError("Tag scope requires a knowledge base")
		}
		allKBIDs[scope.KnowledgeBaseID] = true
	}
	for _, id := range knowledgeIDs {
		if id == "" {
			return nil, errors.NewBadRequestError("Knowledge ID cannot be empty")
		}
		row, err := h.searchKnowledgeService.GetKnowledgeByIDOnly(c.Request.Context(), id)
		if err != nil || row == nil || row.ID != id || row.KnowledgeBaseID == "" {
			return nil, errors.NewForbiddenError("Cannot resolve knowledge search target")
		}
		allKBIDs[row.KnowledgeBaseID] = true
	}
	if len(allKBIDs) == 0 {
		return nil, errors.NewBadRequestError("Knowledge search requires a knowledge base")
	}
	scopes := make([]readlease.NextcloudKBReadScope, 0, len(allKBIDs))
	for kbID := range allKBIDs {
		kb, err := h.knowledgebaseService.GetKnowledgeBaseByIDOnly(c.Request.Context(), kbID)
		if err != nil || kb == nil || kb.ID != kbID || kb.TenantID == 0 {
			return nil, errors.NewForbiddenError("Cannot resolve knowledge search target")
		}
		if err := h.checkCurrentRawSearchKBAccess(c.Request.Context(), c,
			&types.Knowledge{KnowledgeBaseID: kb.ID, TenantID: kb.TenantID}); err != nil {
			return nil, err
		}
		scopes = append(scopes, readlease.NextcloudKBReadScope{TenantID: kb.TenantID, KBID: kb.ID})
	}
	guard, err := readlease.BeginNextcloudKBRead(c.Request.Context(), h.searchContentLeases, scopes)
	if err != nil {
		return nil, rawSearchLeaseHTTPError(err)
	}
	return guard, nil
}

// Re-resolve every result with a persisted document ID so a missing
// channel/metadata field cannot turn a Nextcloud chunk into an unguarded row.
// Upgrade the existing broad lease to exact generation leases before output.
func (h *Handler) protectRawSearchResults(c *gin.Context,
	guard *readlease.NextcloudReadGuard, results []*types.SearchResult,
) error {
	rows, err := h.checkCurrentRawSearchResults(c.Request.Context(), c, results)
	if err != nil || len(rows) == 0 {
		return err
	}
	if guard == nil {
		return errors.NewServiceUnavailableError("Nextcloud content lease store unavailable")
	}
	for _, row := range rows {
		if err := guard.PinKnowledge(row); err != nil {
			return rawSearchLeaseHTTPError(err)
		}
	}
	if err := guard.Verify(); err != nil {
		return rawSearchLeaseHTTPError(err)
	}
	return nil
}

func (h *Handler) checkCurrentRawSearchResults(ctx context.Context, c *gin.Context,
	results []*types.SearchResult,
) ([]*types.Knowledge, error) {
	rows := make([]*types.Knowledge, 0)
	seen := make(map[string]*types.Knowledge)
	for _, result := range results {
		if result == nil {
			return nil, errors.NewServiceUnavailableError("Cannot verify search result identity")
		}
		if result.KnowledgeID == "" {
			return nil, errors.NewServiceUnavailableError("Cannot verify search result identity")
		}
		if h.searchKnowledgeService == nil {
			return nil, errors.NewServiceUnavailableError("Cannot verify search result source")
		}
		row := seen[result.KnowledgeID]
		if row == nil {
			var err error
			row, err = h.searchKnowledgeService.GetKnowledgeByIDOnly(ctx, result.KnowledgeID)
			if err != nil || row == nil || row.ID != result.KnowledgeID {
				return nil, errors.NewForbiddenError("Search result source is no longer available")
			}
			seen[result.KnowledgeID] = row
		}
		scope, source, err := readlease.NextcloudKnowledgeLeaseScope(row)
		if err != nil {
			return nil, errors.NewServiceUnavailableError("Cannot verify source result identity")
		}
		if !source && !rawSearchPotentialSource(result) {
			continue
		}
		if result.KnowledgeBaseID != row.KnowledgeBaseID ||
			result.KnowledgeChannel != row.Channel ||
			!rawSearchMetadataMatchesSource(result.Metadata, row.GetMetadata()) {
			return nil, errors.NewForbiddenError("Search result source changed")
		}
		checker, ok := h.searchKnowledgeService.(interface {
			CheckKnowledgePublication(context.Context, *types.Knowledge) error
		})
		if !ok {
			return nil, errors.NewServiceUnavailableError("Cannot verify current source publication")
		}
		if source {
			if err := h.checkCurrentRawSearchKBAccess(ctx, c, row); err != nil {
				return nil, err
			}
		}
		if err := checker.CheckKnowledgePublication(ctx, row); err != nil {
			if stderrors.Is(err, access.ErrNextcloudPublicationUnavailable) {
				return nil, errors.NewServiceUnavailableError("Cannot verify current Nextcloud file access")
			}
			return nil, errors.NewForbiddenError("Current Nextcloud file access is required")
		}
		if source && scope.KnowledgeID != "" {
			// Duplicate chunks share one generation lease; the guard itself
			// deduplicates PinKnowledge for the same persisted source tuple.
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// Reranking adds score diagnostics to the result, not to the persisted source.
// Preserve every source key exactly, including any persisted score key. Only
// the two finite numeric diagnostics produced by PluginRerank may be extra;
// unknown additions and source-key changes still fail closed. Neither map is
// changed, and nil/empty retain the previous exact-match distinction.
func rawSearchMetadataMatchesSource(result, source map[string]string) bool {
	if result == nil || source == nil {
		return reflect.DeepEqual(result, source)
	}
	for key, value := range source {
		actual, exists := result[key]
		if !exists || actual != value {
			return false
		}
	}
	for key, value := range result {
		if _, persisted := source[key]; persisted {
			continue
		}
		if key != "base_score" && key != "model_score" {
			return false
		}
		score, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(score) || math.IsInf(score, 0) {
			return false
		}
	}
	return true
}

func (h *Handler) checkCurrentRawSearchKBAccess(ctx context.Context, c *gin.Context,
	row *types.Knowledge,
) error {
	if h.knowledgebaseService == nil || row == nil {
		return errors.NewServiceUnavailableError("Cannot verify current knowledge base access")
	}
	ids := []string(nil)
	if row.ID != "" {
		ids = []string{row.ID}
	}
	if err := types.AuthorizeTenantAPIKeyKnowledgeTargets(ctx,
		[]string{row.KnowledgeBaseID}, ids); err != nil {
		return err
	}
	kb, err := h.knowledgebaseService.GetKnowledgeBaseByIDOnly(ctx, row.KnowledgeBaseID)
	if err != nil || kb == nil || kb.ID != row.KnowledgeBaseID || kb.TenantID != row.TenantID {
		return errors.NewServiceUnavailableError("Cannot verify current knowledge base access")
	}
	grant, err := access.ResolveKB(ctx, middleware.KBAccessRequest(c), kb,
		types.OrgRoleViewer, h.kbShareService, h.agentShareService)
	if err != nil || grant == nil || grant.EffectiveTenantID != row.TenantID {
		return errors.NewForbiddenError("Current knowledge base access is required")
	}
	if h.groupAccess != nil {
		permission, groupErr := h.groupAccess.EffectivePermission(ctx, kb.TenantID,
			types.GroupResourceTypeKnowledgeBase, kb.ID, types.ResourceActionRead, time.Now().UTC())
		if groupErr != nil {
			return errors.NewServiceUnavailableError("Cannot verify current directory group access")
		}
		if !permission.Allowed {
			return errors.NewForbiddenError("Current directory group access is required")
		}
	}
	return nil
}

// Gin may send headers before the JSON body. Verify at both boundaries so a
// revoked result cannot be reported as a successful empty 200 or written
// after it was rendered in memory.
type rawSearchAuthorizationWriter struct {
	gin.ResponseWriter
	guard  *readlease.NextcloudReadGuard
	check  func(context.Context) error
	denied error
}

func (w *rawSearchAuthorizationWriter) beforeWrite() error {
	// Once this response was denied it must not resume if a later check
	// succeeds. Before the first denial every boundary still checks afresh.
	if w.denied != nil {
		return w.denied
	}
	if err := w.guard.Verify(); err != nil {
		w.denied = rawSearchLeaseHTTPError(err)
		return w.denied
	}
	w.denied = w.check(w.guard.Context())
	return w.denied
}

func (w *rawSearchAuthorizationWriter) denyWrite(err error) {
	if w.Written() {
		return
	}
	status := http.StatusForbidden
	if appErr, ok := errors.IsAppError(err); ok {
		status = appErr.HTTPCode
	}
	w.ResponseWriter.Header().Del("Content-Type")
	w.ResponseWriter.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
	w.ResponseWriter.WriteHeaderNow()
}

func (w *rawSearchAuthorizationWriter) WriteHeader(code int) {
	if err := w.beforeWrite(); err != nil {
		w.denyWrite(err)
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *rawSearchAuthorizationWriter) WriteHeaderNow() {
	if err := w.beforeWrite(); err != nil {
		w.denyWrite(err)
		return
	}
	w.ResponseWriter.WriteHeaderNow()
}

func (w *rawSearchAuthorizationWriter) Write(p []byte) (int, error) {
	if err := w.beforeWrite(); err != nil {
		w.denyWrite(err)
		return 0, err
	}
	return w.ResponseWriter.Write(p)
}

func (w *rawSearchAuthorizationWriter) WriteString(s string) (int, error) {
	if err := w.beforeWrite(); err != nil {
		w.denyWrite(err)
		return 0, err
	}
	return w.ResponseWriter.WriteString(s)
}

func guardRawSearchResponseWrites(c *gin.Context, guard *readlease.NextcloudReadGuard,
	check func(context.Context) error,
) func() {
	prior := c.Writer
	c.Writer = &rawSearchAuthorizationWriter{ResponseWriter: prior, guard: guard, check: check}
	return func() { c.Writer = prior }
}
