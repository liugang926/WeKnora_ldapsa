package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
)

const nativeLiveFrameTTL = 2 * time.Minute
const nativeLiveMaxBytes = 8 << 20

type nativeLiveMaterialService interface {
	HasOriginalMessageMaterialStore() bool
	MessageNativeLiveIdentity(context.Context, string, string) (*types.Message, string, error)
	CaptureNativeLiveMaterialParents(context.Context) ([]*types.MessageMaterialSnapshot, error)
	BeginNativeLiveMaterialReceiver(context.Context, []*types.MessageMaterialSnapshot) (func() error, func(), error)
	CheckNativeLiveIdentity(context.Context, *types.Message, string) error
	CheckNativeLiveFramePublication(context.Context, *types.Message, *types.SourceLineage) error
	BeginNativeLiveFrameSources(context.Context, *types.Message, *types.SourceLineage) (func() error, func(), error)
}

type nativeLiveKey struct {
	Service                                      nativeLiveMaterialService
	Tenant                                       uint64
	Owner, Session, Message, Request, Generation string
}
type nativeLiveFrame struct {
	Sequence       uint64
	Body           []byte
	Digest         [32]byte
	Parents        []byte
	Lineage        []byte
	StaticTerminal bool
	Expires        time.Time
}
type nativeLiveRun struct {
	mu       sync.Mutex
	key      nativeLiveKey
	control  *types.Message
	frames   []nativeLiveFrame
	sequence uint64
	bytes    int
	expires  time.Time
	timer    *time.Timer
	closed   bool
}

var nativeLiveRuns = struct {
	sync.Mutex
	items map[nativeLiveKey]*nativeLiveRun
}{items: make(map[nativeLiveKey]*nativeLiveRun)}

func nativeLiveOwner(ctx context.Context) string {
	if principal, ok := types.PrincipalFromContext(ctx); ok && principal.Type == types.PrincipalWebUser {
		return principal.ID
	}
	return types.SessionOwnerIDFromContext(ctx)
}

func eraseNativeLiveRun(run *nativeLiveRun) {
	nativeLiveRuns.Lock()
	if nativeLiveRuns.items[run.key] == run {
		delete(nativeLiveRuns.items, run.key)
	}
	nativeLiveRuns.Unlock()
	run.mu.Lock()
	run.closed = true
	for i := range run.frames {
		run.frames[i].Body = nil
		run.frames[i].Parents = nil
		run.frames[i].Lineage = nil
	}
	run.frames = nil
	run.bytes = 0
	if run.timer != nil {
		run.timer.Stop()
	}
	run.mu.Unlock()
}

type nativeLiveAppender struct {
	interfaces.StreamManager
	service         nativeLiveMaterialService
	producer        context.Context
	run             *nativeLiveRun
	registrationErr error
}

func (h *Handler) nativeLiveStreamManager(ctx context.Context, tenant uint64, session, message, request string) interfaces.StreamManager {
	actual, ok := h.messageService.(nativeLiveMaterialService)
	if !ok || !actual.HasOriginalMessageMaterialStore() {
		return h.streamManager
	}
	appender := &nativeLiveAppender{StreamManager: h.streamManager, service: actual, producer: ctx}
	control, generation, err := actual.MessageNativeLiveIdentity(ctx, session, message)
	if err != nil {
		appender.registrationErr = err
		return appender
	}
	if control == nil || control.IsCompleted || control.Role != "assistant" || control.RequestID != request || generation == "" || nativeLiveOwner(ctx) == "" {
		appender.registrationErr = access.ErrNextcloudPublicationUnavailable
		return appender
	}
	// The metadata API already checked this exact owner in its authority tx.
	key := nativeLiveKey{actual, tenant, nativeLiveOwner(ctx), session, message, request, generation}
	copy, err := cloneNativeLiveControl(control)
	if err != nil {
		appender.registrationErr = err
		return appender
	}
	run := &nativeLiveRun{key: key, control: copy, expires: time.Now().Add(nativeLiveFrameTTL)}
	nativeLiveRuns.Lock()
	prior := nativeLiveRuns.items[key]
	if prior == nil {
		nativeLiveRuns.items[key] = run
	} else {
		run = prior
	}
	nativeLiveRuns.Unlock()
	run.mu.Lock()
	if run.timer == nil {
		run.timer = time.AfterFunc(nativeLiveFrameTTL, func() { eraseNativeLiveRun(run) })
	}
	run.mu.Unlock()
	appender.run = run
	return appender
}

func cloneNativeLiveControl(original *types.Message) (*types.Message, error) {
	encoded, err := json.Marshal(struct {
		Body        *types.Message
		Execution   types.MessageExecutionContext
		AgentTenant uint64
	}{original, original.ExecutionContext, original.AgentTenantID})
	if err != nil {
		return nil, err
	}
	var copy struct {
		Body        *types.Message
		Execution   types.MessageExecutionContext
		AgentTenant uint64
	}
	if err := json.Unmarshal(encoded, &copy); err != nil {
		return nil, err
	}
	copy.Body.ExecutionContext = copy.Execution
	copy.Body.AgentTenantID = copy.AgentTenant
	return copy.Body, nil
}

func (a *nativeLiveAppender) AppendEvent(ctx context.Context, session, message string, frame interfaces.StreamEvent) error {
	// Completed rows have their own saved-original receipt and replay path.
	if frame.Type == types.ResponseTypeComplete {
		if a.registrationErr != nil {
			return a.registrationErr
		}
		if a.run == nil || session != a.run.key.Session || message != a.run.key.Message {
			return access.ErrNextcloudPublicationUnavailable
		}
		current, generation, err := a.service.MessageNativeLiveIdentity(ctx, session, message)
		if err != nil {
			return err
		}
		if current == nil || !current.IsCompleted || generation != a.run.key.Generation || current.RequestID != a.run.key.Request {
			return access.ErrNextcloudPublicationDenied
		}
		err = a.StreamManager.AppendEvent(ctx, session, message, frame)
		if err == nil && a.run != nil {
			eraseNativeLiveRun(a.run)
		}
		return err
	}
	if a.registrationErr != nil {
		return a.registrationErr
	}
	if a.run == nil || session != a.run.key.Session || message != a.run.key.Message {
		return access.ErrNextcloudPublicationUnavailable
	}
	static := frame.Type == types.ResponseTypeError && frame.Done
	var parents []*types.MessageMaterialSnapshot
	if static {
		// Only a fixed code-origin terminal recipe enters the native registry.
		// The raw provider/error metadata is never stored as authenticated bytes.
		frame = interfaces.StreamEvent{ID: frame.ID, Type: types.ResponseTypeError, Content: "Generation could not complete; please retry after checking access", Done: true, Timestamp: time.Now(), Data: map[string]interface{}{"error_code": "generation_failed"}}
	} else {
		if frame.SourceLineage.RequireComplete() != nil {
			return access.ErrNextcloudPublicationDenied
		}
		var err error
		parents, err = a.service.CaptureNativeLiveMaterialParents(a.producer)
		if err != nil {
			return err
		}
		if len(parents) == 0 {
			return access.ErrNextcloudPublicationUnavailable
		}
		for _, parent := range parents {
			if parent == nil || parent.Messages != nil {
				return access.ErrNextcloudPublicationDenied
			}
		}
		if err := a.service.CheckNativeLiveIdentity(a.producer, a.run.control, a.run.key.Generation); err != nil {
			return err
		}
		if err := a.service.CheckNativeLiveFramePublication(a.producer, a.run.control, frame.SourceLineage); err != nil {
			return err
		}
	}
	if frame.Timestamp.IsZero() {
		frame.Timestamp = time.Now()
	}
	body, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	parentBytes, err := json.Marshal(parents)
	if err != nil {
		return err
	}
	lineageBytes, err := json.Marshal(frame.SourceLineage)
	if err != nil {
		return err
	}
	run := a.run
	run.mu.Lock()
	if run.closed || time.Now().After(run.expires) || run.bytes+len(body)+len(parentBytes)+len(lineageBytes) > nativeLiveMaxBytes {
		run.mu.Unlock()
		eraseNativeLiveRun(run)
		return access.ErrNextcloudPublicationUnavailable
	}
	// Freeze and publish in actual append order; delta event IDs repeat.
	err = a.StreamManager.AppendEvent(ctx, session, message, frame)
	if err == nil {
		run.sequence++
		now := time.Now()
		kept := run.frames[:0]
		for i := range run.frames {
			old := run.frames[i]
			if old.Expires.After(now) {
				kept = append(kept, old)
			} else {
				run.bytes -= len(old.Body) + len(old.Parents) + len(old.Lineage)
				run.frames[i].Body = nil
				run.frames[i].Parents = nil
				run.frames[i].Lineage = nil
			}
		}
		run.frames = append(kept, nativeLiveFrame{Sequence: run.sequence, Body: body, Digest: sha256.Sum256(body), Parents: parentBytes, Lineage: lineageBytes, StaticTerminal: static, Expires: now.Add(nativeLiveFrameTTL)})
		run.bytes += len(body) + len(parentBytes) + len(lineageBytes)
		run.expires = now.Add(nativeLiveFrameTTL)
		run.timer.Reset(nativeLiveFrameTTL)
	}
	run.mu.Unlock()
	return err
}

func (run *nativeLiveRun) read(after uint64) ([]nativeLiveFrame, error) {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.closed || time.Now().After(run.expires) {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	result := []nativeLiveFrame{}
	for _, entry := range run.frames {
		if entry.Sequence <= after {
			continue
		}
		if time.Now().After(entry.Expires) {
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		copy := entry
		copy.Body = nil // Scope and original proof are acquired before body access.
		copy.Parents = append([]byte(nil), entry.Parents...)
		copy.Lineage = append([]byte(nil), entry.Lineage...)
		result = append(result, copy)
	}
	return result, nil
}

func (run *nativeLiveRun) readBody(metadata nativeLiveFrame) ([]byte, error) {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.closed || time.Now().After(run.expires) {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	for _, current := range run.frames {
		if current.Sequence != metadata.Sequence {
			continue
		}
		if time.Now().After(current.Expires) || current.Digest != metadata.Digest || !current.Expires.Equal(metadata.Expires) || !bytes.Equal(current.Parents, metadata.Parents) || !bytes.Equal(current.Lineage, metadata.Lineage) || current.StaticTerminal != metadata.StaticTerminal {
			return nil, access.ErrNextcloudPublicationDenied
		}
		if sha256.Sum256(current.Body) != current.Digest {
			return nil, access.ErrNextcloudPublicationDenied
		}
		return append([]byte(nil), current.Body...), nil
	}
	return nil, access.ErrNextcloudPublicationUnavailable
}

func (h *Handler) tryNativeLiveReplay(ctx context.Context, c *gin.Context, session, message string, rewriter *storageurl.StreamRewriter) bool {
	actual, ok := h.messageService.(nativeLiveMaterialService)
	if !ok || !actual.HasOriginalMessageMaterialStore() {
		return false
	}
	control, generation, err := actual.MessageNativeLiveIdentity(ctx, session, message)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if control == nil {
		_ = c.Error(nativeSavedReplayHTTPError(access.ErrNextcloudPublicationUnavailable))
		return true
	}
	if control.IsCompleted {
		return h.tryNativeSavedReplay(ctx, c, session, message, rewriter)
	}
	tenant := types.MustTenantIDFromContext(ctx)
	key := nativeLiveKey{actual, tenant, nativeLiveOwner(ctx), session, message, control.RequestID, generation}
	nativeLiveRuns.Lock()
	run := nativeLiveRuns.items[key]
	nativeLiveRuns.Unlock()
	if run == nil {
		_ = c.Error(nativeSavedReplayHTTPError(access.ErrNextcloudPublicationUnavailable))
		return true
	}
	prior := c.Writer
	defer func() { c.Writer = prior }()
	checks := []func() error{}
	closes := []func(){}
	parentHeld := map[[32]byte]bool{}
	sourceHeld := map[[32]byte]bool{}
	defer func() {
		for i := len(closes) - 1; i >= 0; i-- {
			closes[i]()
		}
	}()
	check := func() error {
		if err := actual.CheckNativeLiveIdentity(ctx, run.control, generation); err != nil {
			return err
		}
		for _, check := range checks {
			if err := check(); err != nil {
				return err
			}
		}
		return nil
	}
	writer := &nativeSavedReplayWriter{ResponseWriter: prior, ctx: c.Request.Context(), check: check}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var offset uint64
	for {
		current, currentGeneration, err := actual.MessageNativeLiveIdentity(ctx, session, message)
		if err != nil {
			if !c.Writer.Written() {
				_ = c.Error(nativeSavedReplayHTTPError(err))
			}
			return true
		}
		if currentGeneration != generation {
			return true
		}
		if current.IsCompleted {
			// Discard the old live holdback and acquire the completed saved receipt.
			c.Writer = prior
			fresh, err := h.resolveStreamRewriter(c)
			if err == nil {
				h.tryNativeSavedReplay(ctx, c, session, message, fresh)
			}
			return true
		}
		entries, err := run.read(offset)
		if err != nil {
			if !c.Writer.Written() {
				_ = c.Error(nativeSavedReplayHTTPError(err))
			}
			return true
		}
		for _, entry := range entries {
			if !entry.StaticTerminal {
				var parents []*types.MessageMaterialSnapshot
				if err := json.Unmarshal(entry.Parents, &parents); err != nil {
					return true
				}
				parentKey := sha256.Sum256(entry.Parents)
				if !parentHeld[parentKey] {
					parentCheck, parentClose, err := actual.BeginNativeLiveMaterialReceiver(ctx, parents)
					if err != nil {
						current, _, controlErr := actual.MessageNativeLiveIdentity(ctx, session, message)
						if controlErr == nil && current.IsCompleted {
							c.Writer = prior
							fresh, _ := h.resolveStreamRewriter(c)
							h.tryNativeSavedReplay(ctx, c, session, message, fresh)
						}
						return true
					}
					if parentCheck == nil || parentClose == nil {
						if parentClose != nil {
							parentClose()
						}
						return true
					}
					closes = append(closes, parentClose)
					checks = append(checks, parentCheck)
					parentHeld[parentKey] = true
				}
				var lineage *types.SourceLineage
				if err := json.Unmarshal(entry.Lineage, &lineage); err != nil {
					return true
				}
				sourceKey := sha256.Sum256(entry.Lineage)
				if !sourceHeld[sourceKey] {
					sourceCheck, sourceClose, err := actual.BeginNativeLiveFrameSources(ctx, run.control, lineage)
					if err != nil {
						return true
					}
					if sourceCheck == nil || sourceClose == nil {
						if sourceClose != nil {
							sourceClose()
						}
						return true
					}
					closes = append(closes, sourceClose)
					checks = append(checks, sourceCheck)
					sourceHeld[sourceKey] = true
				}
			}
			if entry.StaticTerminal {
				writer.check = func() error { return actual.CheckNativeLiveIdentity(ctx, run.control, generation) }
			} else {
				writer.check = check
			}
			if err := writer.beforeWrite(); err != nil {
				return true
			}
			body, err := run.readBody(entry)
			if err != nil {
				return true
			}
			var frame interfaces.StreamEvent
			if err := json.Unmarshal(body, &frame); err != nil {
				return true
			}
			frameCheck := writer.check
			writer.check = func() error {
				if err := frameCheck(); err != nil {
					return err
				}
				if _, err := run.readBody(entry); err != nil {
					return err
				}
				return nil
			}
			c.Writer = writer
			if err := writer.check(); err != nil {
				return true
			}
			setSSEHeaders(c)
			if entry.StaticTerminal {
				emitSafeTerminalGenerationError(c, control.RequestID)
				return true
			}
			response := buildStreamResponseFor(ctx, frame, control.RequestID, rewriter)
			var encoded bytes.Buffer
			if err := sse.Encode(&encoded, sse.Event{Event: "message", Data: response}); err != nil {
				return true
			}
			if _, err := writer.Write(encoded.Bytes()); err != nil {
				return true
			}
			writer.Flush()
			if writer.denied != nil {
				return true
			}
			offset = entry.Sequence
		}
		select {
		case <-c.Request.Context().Done():
			return true
		case <-ticker.C:
		}
	}
}
