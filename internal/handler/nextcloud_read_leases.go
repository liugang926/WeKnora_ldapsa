package handler

import (
	"bufio"
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	nextcloudHTTPReadLeaseTTL       = 2 * time.Minute
	nextcloudHTTPReadLeaseHeartbeat = 30 * time.Second
	nextcloudHTTPReadCheckpoint     = 2 * time.Second
	nextcloudDirectResponseBuffer   = 1 * 1024 * 1024
)

// Kept narrow so handler tests can force lease expiry/retirement without an
// external source. Production wires the durable repository implementation.
type nextcloudHTTPReadLeaseStore interface {
	AcquireKBRead(context.Context, uint64, string, string, time.Duration) (repository.NextcloudContentLease, error)
	AcquireKnowledge(context.Context, repository.NextcloudContentScope,
		repository.NextcloudContentLeaseKind, string, time.Duration) (repository.NextcloudContentLease, error)
	RenewLease(context.Context, string, time.Duration) (repository.NextcloudContentLease, error)
	ReleaseLease(context.Context, string) error
}

// A list query does not know which source generations its page and total will
// read. Pin the KB before issuing the query, then add exact leases for the
// returned rows before they can be serialized.
func acquireNextcloudHTTPKBReadLease(ctx context.Context, store nextcloudHTTPReadLeaseStore,
	tenantID uint64, kbID string, heartbeat time.Duration,
) (*nextcloudHTTPReadLeases, error) {
	if store == nil {
		return nil, errors.NewServiceUnavailableError("Nextcloud content lease store unavailable")
	}
	if heartbeat <= 0 {
		heartbeat = nextcloudHTTPReadLeaseHeartbeat
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	group := &nextcloudHTTPReadLeases{
		store: store, ctx: leaseCtx, cancel: cancel,
		heartbeat: heartbeat, rows: make([]repository.NextcloudContentLease, 0, 1),
	}
	row, err := store.AcquireKBRead(leaseCtx, tenantID, kbID, uuid.NewString(),
		nextcloudHTTPReadLeaseTTL)
	if err != nil {
		group.Close()
		return nil, nextcloudHTTPLeaseError(err)
	}
	group.rows = append(group.rows, row)
	group.stop.Add(1)
	go group.heartbeatLoop()
	return group, nil
}

func nextcloudHTTPLeaseError(err error) error {
	if stderrors.Is(err, repository.ErrNextcloudContentLeaseDenied) {
		return errors.NewForbiddenError("Nextcloud content is no longer available")
	}
	return errors.NewServiceUnavailableError("Cannot verify current Nextcloud content lease")
}

func nextcloudHTTPLeaseScope(knowledge *types.Knowledge) (repository.NextcloudContentScope, bool, error) {
	var scope repository.NextcloudContentScope
	if knowledge == nil {
		return scope, false, errors.NewNotFoundError("Knowledge not found")
	}
	if knowledge.Channel != types.ConnectorTypeNextcloud {
		return scope, false, nil
	}
	var metadata struct {
		DataSourceID string `json:"datasource_id"`
		ExternalID   string `json:"external_id"`
	}
	if err := json.Unmarshal(knowledge.Metadata, &metadata); err != nil ||
		knowledge.TenantID == 0 || knowledge.KnowledgeBaseID == "" ||
		knowledge.ID == "" || metadata.DataSourceID == "" || metadata.ExternalID == "" {
		return scope, true, errors.NewServiceUnavailableError("Cannot verify Nextcloud content identity")
	}
	return repository.NextcloudContentScope{
		TenantID: knowledge.TenantID, KnowledgeBaseID: knowledge.KnowledgeBaseID,
		KnowledgeID: knowledge.ID, DataSourceID: metadata.DataSourceID,
		ExternalID: metadata.ExternalID,
	}, true, nil
}

type nextcloudHTTPReadLeases struct {
	store     nextcloudHTTPReadLeaseStore
	ctx       context.Context
	cancel    context.CancelFunc
	rows      []repository.NextcloudContentLease
	heartbeat time.Duration
	stop      sync.WaitGroup
	renewMu   sync.Mutex
	errMu     sync.Mutex
	err       error
	closeOnce sync.Once
}

// acquireNextcloudHTTPReadLeases is called only after ordinary KB and live
// source authorization. Acquisition never grants source access by itself.
func acquireNextcloudHTTPReadLeases(ctx context.Context, store nextcloudHTTPReadLeaseStore,
	knowledges []*types.Knowledge, heartbeat time.Duration,
) (*nextcloudHTTPReadLeases, error) {
	unique := make(map[repository.NextcloudContentScope]struct{}, len(knowledges))
	scopes := make([]repository.NextcloudContentScope, 0, len(knowledges))
	for _, knowledge := range knowledges {
		scope, marked, err := nextcloudHTTPLeaseScope(knowledge)
		if err != nil {
			return nil, err
		}
		if !marked {
			continue
		}
		if _, found := unique[scope]; found {
			continue
		}
		unique[scope] = struct{}{}
		scopes = append(scopes, scope)
	}
	if len(scopes) == 0 {
		return nil, nil
	}
	if store == nil {
		return nil, errors.NewServiceUnavailableError("Nextcloud content lease store unavailable")
	}
	if heartbeat <= 0 {
		heartbeat = nextcloudHTTPReadLeaseHeartbeat
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	group := &nextcloudHTTPReadLeases{
		store: store, ctx: leaseCtx, cancel: cancel,
		heartbeat: heartbeat, rows: make([]repository.NextcloudContentLease, 0, len(scopes)),
	}
	owner := uuid.NewString()
	for _, scope := range scopes {
		row, err := store.AcquireKnowledge(leaseCtx, scope, repository.NextcloudContentReadLease,
			owner, nextcloudHTTPReadLeaseTTL)
		if err != nil {
			group.Close()
			return nil, nextcloudHTTPLeaseError(err)
		}
		group.rows = append(group.rows, row)
	}
	group.stop.Add(1)
	go group.heartbeatLoop()
	return group, nil
}

func (g *nextcloudHTTPReadLeases) Context(fallback context.Context) context.Context {
	if g == nil {
		return fallback
	}
	return g.ctx
}

func attachNextcloudLeasedRequest(c *gin.Context, leases *nextcloudHTTPReadLeases) {
	if c != nil && leases != nil {
		c.Request = c.Request.WithContext(leases.ctx)
	}
}

func (h *KnowledgeHandler) beginNextcloudRead(ctx context.Context, c *gin.Context,
	knowledges []*types.Knowledge,
) (*nextcloudHTTPReadLeases, error) {
	leases, err := acquireNextcloudHTTPReadLeases(ctx, h.contentLeases, knowledges, h.leaseHeartbeat)
	if err != nil {
		return nil, err
	}
	attachNextcloudLeasedRequest(c, leases)
	return leases, nil
}

func (h *KnowledgeHandler) verifyNextcloudRead(ctx context.Context,
	leases *nextcloudHTTPReadLeases, knowledges []*types.Knowledge,
) error {
	if err := leases.Verify(); err != nil {
		return nextcloudHTTPLeaseError(err)
	}
	return h.checkPublicationList(ctx, knowledges)
}

// A global knowledge search may span several KBs. The service has already
// narrowed its results to the caller's allowed scopes; each returned source
// generation is then pinned before its title, description or metadata leaves
// the process. The write boundary rechecks publication after JSON rendering.
func (h *KnowledgeHandler) respondKnowledgeSearch(ctx context.Context, c *gin.Context,
	knowledges []*types.Knowledge, hasMore bool, total int64,
) {
	if err := h.checkPublicationList(ctx, knowledges); err != nil {
		_ = c.Error(err)
		return
	}
	leases, err := h.beginNextcloudRead(ctx, c, knowledges)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer leases.Close()
	ctx = leases.Context(ctx)
	if err := h.verifyNextcloudRead(ctx, leases, knowledges); err != nil {
		_ = c.Error(err)
		return
	}
	defer guardNextcloudResponseWrites(c, leases, func(checkCtx context.Context) error {
		return h.checkPublicationList(checkCtx, knowledges)
	})()
	c.JSON(http.StatusOK, gin.H{
		"success":  true,
		"data":     knowledges,
		"has_more": hasMore,
		"total":    total,
	})
}

// Re-resolve the live KB/share/group grant at the output boundary. A route
// grant captured at request start cannot authorize bytes after the share or
// directory group permission has been revoked during a long download.
func (h *KnowledgeHandler) checkCurrentNextcloudKBAccess(ctx context.Context,
	c *gin.Context, knowledge *types.Knowledge, required types.OrgMemberRole,
) error {
	if h.kbService == nil || knowledge == nil {
		return errors.NewServiceUnavailableError("Cannot verify current knowledge base access")
	}
	kb, err := h.kbService.GetKnowledgeBaseByIDOnly(ctx, knowledge.KnowledgeBaseID)
	if err != nil || kb == nil || kb.ID != knowledge.KnowledgeBaseID || kb.TenantID != knowledge.TenantID {
		return errors.NewServiceUnavailableError("Cannot verify current knowledge base access")
	}
	grant, err := access.ResolveKB(ctx, middleware.KBAccessRequest(c), kb, required,
		h.kbShareService, h.agentShareService)
	if err != nil {
		return kbAccessHTTPError(err)
	}
	if grant.EffectiveTenantID != knowledge.TenantID {
		return errors.NewForbiddenError("Permission denied to access this knowledge")
	}
	action := types.ResourceActionRead
	if required != types.OrgRoleViewer {
		action = types.ResourceActionEdit
	}
	return h.authorizeKBGroupAccess(ctx, kb, action)
}

func (h *ChunkHandler) beginNextcloudRead(ctx context.Context, c *gin.Context,
	knowledge *types.Knowledge,
) (*nextcloudHTTPReadLeases, error) {
	leases, err := acquireNextcloudHTTPReadLeases(ctx, h.contentLeases,
		[]*types.Knowledge{knowledge}, h.leaseHeartbeat)
	if err != nil {
		return nil, err
	}
	attachNextcloudLeasedRequest(c, leases)
	return leases, nil
}

func (g *nextcloudHTTPReadLeases) fail(err error) error {
	if g == nil || err == nil {
		return err
	}
	g.errMu.Lock()
	if g.err == nil {
		g.err = err
		g.cancel()
	}
	actual := g.err
	g.errMu.Unlock()
	return actual
}

func (g *nextcloudHTTPReadLeases) Err() error {
	if g == nil {
		return nil
	}
	g.errMu.Lock()
	err := g.err
	g.errMu.Unlock()
	if err != nil {
		return err
	}
	return g.ctx.Err()
}

// Verify renews all exact leases and fails closed if any generation retired,
// the durable store failed, or the request was canceled.
func (g *nextcloudHTTPReadLeases) Verify() error {
	if g == nil {
		return nil
	}
	g.renewMu.Lock()
	defer g.renewMu.Unlock()
	if err := g.Err(); err != nil {
		return err
	}
	for _, row := range g.rows {
		if _, err := g.store.RenewLease(g.ctx, row.ID, nextcloudHTTPReadLeaseTTL); err != nil {
			return g.fail(err)
		}
	}
	return g.Err()
}

func (g *nextcloudHTTPReadLeases) heartbeatLoop() {
	defer g.stop.Done()
	ticker := time.NewTicker(g.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			if err := g.Verify(); err != nil {
				return
			}
		}
	}
}

// Close waits for in-flight renewals, then releases with a detached bounded
// context. A canceled HTTP request must not leave a lease live until TTL.
func (g *nextcloudHTTPReadLeases) Close() {
	if g == nil {
		return
	}
	g.closeOnce.Do(func() {
		g.cancel()
		g.stop.Wait()
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := len(g.rows) - 1; i >= 0; i-- {
			if err := g.store.ReleaseLease(releaseCtx, g.rows[i].ID); err != nil {
				logger.Warnf(releaseCtx, "[NextcloudHTTPReadLease] release failed: %v", err)
			}
		}
	})
}

// nextcloudLeasedFile checks the durable fence and live publication at stream
// checkpoints. It preserves Seek for Range/ServeContent while ensuring a
// blocked backend read cannot return bytes after a failed checkpoint.
type nextcloudLeasedFile struct {
	file      io.ReadCloser
	leases    *nextcloudHTTPReadLeases
	check     func(context.Context) error
	every     time.Duration
	lastCheck time.Time
}

func (f *nextcloudLeasedFile) checkpoint(force bool) error {
	if f == nil || f.leases == nil {
		return nil
	}
	if err := f.leases.Err(); err != nil {
		return err
	}
	if force || f.lastCheck.IsZero() || time.Since(f.lastCheck) >= f.every {
		if err := f.leases.Verify(); err != nil {
			return err
		}
		if f.check != nil {
			if err := f.check(f.leases.ctx); err != nil {
				return f.leases.fail(err)
			}
		}
		f.lastCheck = time.Now()
	}
	return f.leases.Err()
}

func (f *nextcloudLeasedFile) Read(p []byte) (int, error) {
	if err := f.checkpoint(false); err != nil {
		return 0, err
	}
	n, err := f.file.Read(p)
	if err2 := f.checkpoint(false); err2 != nil {
		return 0, err2
	}
	return n, err
}

func (f *nextcloudLeasedFile) Close() error { return f.file.Close() }

type nextcloudLeasedSeekFile struct{ *nextcloudLeasedFile }

func (f *nextcloudLeasedSeekFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.checkpoint(false); err != nil {
		return 0, err
	}
	return f.file.(io.Seeker).Seek(offset, whence)
}

func wrapNextcloudLeasedFile(file io.ReadCloser, leases *nextcloudHTTPReadLeases,
	check func(context.Context) error, every time.Duration,
) io.ReadCloser {
	if leases == nil {
		return file
	}
	if every <= 0 {
		every = nextcloudHTTPReadCheckpoint
	}
	wrapped := &nextcloudLeasedFile{file: file, leases: leases, check: check, every: every}
	if _, ok := file.(io.Seeker); ok {
		return &nextcloudLeasedSeekFile{wrapped}
	}
	return wrapped
}

// Gin and net/http may buffer a successful Read before calling Write. Check
// the live source again at the actual output boundary, including WriteString.
// The embedded Gin writer preserves Flush/Hijack/CloseNotify behavior.
type nextcloudAuthorizationWriter struct {
	gin.ResponseWriter
	leases *nextcloudHTTPReadLeases
	check  func(context.Context) error
}

func (w *nextcloudAuthorizationWriter) beforeWrite() error {
	if w.leases == nil {
		return nil
	}
	if err := w.leases.Err(); err != nil {
		return err
	}
	if err := w.leases.Verify(); err != nil {
		return err
	}
	if w.check != nil {
		if err := w.check(w.leases.ctx); err != nil {
			return w.leases.fail(err)
		}
	}
	return w.leases.Err()
}

func (w *nextcloudAuthorizationWriter) Write(p []byte) (int, error) {
	if err := w.beforeWrite(); err != nil {
		w.denyWrite(err)
		return 0, err
	}
	return w.ResponseWriter.Write(p)
}

func (w *nextcloudAuthorizationWriter) WriteString(s string) (int, error) {
	if err := w.beforeWrite(); err != nil {
		w.denyWrite(err)
		return 0, err
	}
	return w.ResponseWriter.WriteString(s)
}

func (w *nextcloudAuthorizationWriter) WriteHeaderNow() {
	if err := w.beforeWrite(); err != nil {
		w.denyWrite(err)
		return
	}
	w.ResponseWriter.WriteHeaderNow()
}

func (w *nextcloudAuthorizationWriter) denyWrite(err error) {
	if w.Written() {
		return
	}
	// Gin's JSON renderer can call Write without a preceding WriteHeaderNow.
	// Commit the denial through the underlying writer so a suppressed body
	// cannot still be reported as an empty 200 response.
	status := http.StatusForbidden
	if appErr, ok := errors.IsAppError(err); ok {
		status = appErr.HTTPCode
	}
	w.ResponseWriter.Header().Del("Content-Type")
	w.ResponseWriter.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
	w.ResponseWriter.WriteHeaderNow()
}

func (w *nextcloudAuthorizationWriter) WriteHeader(code int) {
	if err := w.beforeWrite(); err != nil {
		// HEAD and Range responses may send file metadata without a body.
		// Strip it when access changed between the handler check and headers.
		header := w.Header()
		header.Del("Content-Disposition")
		header.Del("Content-Description")
		header.Del("Content-Length")
		header.Del("Content-Range")
		header.Del("Content-Type")
		header.Del("Last-Modified")
		header.Del("ETag")
		header.Del("Accept-Ranges")
		w.denyWrite(err)
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func guardNextcloudResponseWrites(c *gin.Context, leases *nextcloudHTTPReadLeases,
	check func(context.Context) error,
) func() {
	if leases == nil {
		return func() {}
	}
	prior := c.Writer
	c.Writer = &nextcloudAuthorizationWriter{ResponseWriter: prior, leases: leases, check: check}
	return func() { c.Writer = prior }
}

// Keep small ServeContent writes in memory until an actual response write.
// The guarded underlying writer then verifies source and current KB access
// immediately before the buffered bytes leave the process.
type nextcloudBufferedResponseWriter struct {
	http.ResponseWriter
	buffer *bufio.Writer
}

func newNextcloudBufferedResponseWriter(w http.ResponseWriter, size int) *nextcloudBufferedResponseWriter {
	return &nextcloudBufferedResponseWriter{
		ResponseWriter: w,
		buffer:         bufio.NewWriterSize(w, size),
	}
}

func (w *nextcloudBufferedResponseWriter) Write(p []byte) (int, error) {
	return w.buffer.Write(p)
}

func (w *nextcloudBufferedResponseWriter) Flush() error {
	return w.buffer.Flush()
}

func bufferedNextcloudResponse(w http.ResponseWriter, leases *nextcloudHTTPReadLeases,
	size int,
) (http.ResponseWriter, func() error) {
	if leases == nil {
		return w, func() error { return nil }
	}
	buffered := newNextcloudBufferedResponseWriter(w, size)
	return buffered, buffered.Flush
}
