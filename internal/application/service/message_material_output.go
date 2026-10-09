package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

// Initialization is not an authorization decision. HTTP may wait for this
// state, but must still run the original/current checks once setup finishes.
var ErrMessageMaterialHTTPInitializing = fmt.Errorf("%w: original message material initialization pending", access.ErrNextcloudPublicationUnavailable)

type messageMaterialOutputKey struct{}
type messageMaterialExternalKey struct{}
type messageMaterialOutputSlot struct {
	mu            sync.Mutex
	Guards        []*MessageMaterialGuard
	PhysicalReads []*readlease.NextcloudReadGuard
	Bodies        []string
	Outputs       [][]*types.Message
	OutputBodies  []string
	Ready         bool
	Failed        error
	Refs          int
	Closed        bool
}

func WithMessageMaterialOutputBoundary(ctx context.Context) context.Context {
	return context.WithValue(ctx, messageMaterialOutputKey{}, &messageMaterialOutputSlot{})
}

// BeginMessageMaterialHTTPBoundary creates a private shared owner. The model
// and the HTTP sender each retain one reference; only the last release closes
// the actual original guards. No caller text or row header grants admission.
func BeginMessageMaterialHTTPBoundary(ctx context.Context) (context.Context, func(context.Context) context.Context, func() error, func()) {
	slot := &messageMaterialOutputSlot{Refs: 1}
	attach := func(target context.Context) context.Context {
		target = context.WithValue(target, messageMaterialOutputKey{}, slot)
		return context.WithValue(target, messageMaterialExternalKey{}, slot)
	}
	marked := attach(ctx)
	check := func() error {
		slot.mu.Lock()
		ready, failed, closed := slot.Ready, slot.Failed, slot.Closed
		slot.mu.Unlock()
		if failed != nil {
			return failed
		}
		if closed {
			return access.ErrNextcloudPublicationUnavailable
		}
		if !ready {
			return ErrMessageMaterialHTTPInitializing
		}
		return CheckMessageMaterialOutputBoundary(marked)
	}
	var once sync.Once
	close := func() { once.Do(func() { releaseExternalMessageMaterial(slot) }) }
	return marked, attach, check, close
}
func releaseExternalMessageMaterial(slot *messageMaterialOutputSlot) {
	slot.mu.Lock()
	slot.Refs--
	if slot.Refs > 0 || slot.Closed {
		slot.mu.Unlock()
		return
	}
	slot.Closed = true
	guards := append([]*MessageMaterialGuard{}, slot.Guards...)
	physical := append([]*readlease.NextcloudReadGuard{}, slot.PhysicalReads...)
	slot.mu.Unlock()
	for _, guard := range guards {
		guard.Close()
	}
	for _, guard := range physical {
		_ = guard.Close()
	}
}
func retainMessageMaterialProducer(ctx context.Context) (context.Context, func(), bool) {
	slot, ok := ctx.Value(messageMaterialExternalKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return ctx, nil, false
	}
	slot.mu.Lock()
	if slot.Closed {
		slot.mu.Unlock()
		return ctx, func() {}, true
	}
	slot.Refs++
	slot.mu.Unlock()
	var once sync.Once
	return ctx, func() { once.Do(func() { releaseExternalMessageMaterial(slot) }) }, true
}
func markMessageMaterialReady(ctx context.Context) {
	if slot, ok := ctx.Value(messageMaterialExternalKey{}).(*messageMaterialOutputSlot); ok {
		slot.mu.Lock()
		slot.Ready = true
		slot.mu.Unlock()
	}
}

func CloseMessageMaterialOutputBoundary(ctx context.Context) {
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		slot.mu.Lock()
		if !slot.Closed {
			slot.Closed = true
		}
		guards := append([]*MessageMaterialGuard{}, slot.Guards...)
		physical := append([]*readlease.NextcloudReadGuard{}, slot.PhysicalReads...)
		slot.mu.Unlock()
		for _, guard := range guards {
			guard.Close()
		}
		for _, guard := range physical {
			_ = guard.Close()
		}
	}
}
func CheckMessageMaterialOutputBoundary(ctx context.Context) error {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return access.ErrNextcloudPublicationUnavailable
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.Closed {
		return access.ErrNextcloudPublicationUnavailable
	}
	if slot.Failed != nil {
		return slot.Failed
	}
	for _, guard := range slot.PhysicalReads {
		if err := guard.Verify(); err != nil {
			return err
		}
	}
	for i, guard := range slot.Guards {
		if err := guard.Recheck(); err != nil {
			return err
		}
		guard.mu.Lock()
		body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
		guard.mu.Unlock()
		if err != nil {
			return err
		}
		if string(body) != slot.Bodies[i] {
			return access.ErrNextcloudPublicationDenied
		}
	}
	for i, output := range slot.Outputs {
		body, err := messageBoundaryBodyDigest(output)
		if err != nil {
			return err
		}
		if string(body) != slot.OutputBodies[i] {
			return access.ErrNextcloudPublicationDenied
		}
	}
	return nil
}

// A turn owns each actual retrieval guard before index/body access. Its model
// and HTTP references release independently; cached bytes never prolong a
// retired generation's access. Non-turn calls retain the normal method scope.
func holdRAGPhysicalReadGuard(ctx context.Context, guard *readlease.NextcloudReadGuard) (bool, error) {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return false, nil
	}
	if guard == nil {
		return false, access.ErrNextcloudPublicationUnavailable
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.Closed || slot.Failed != nil {
		return false, access.ErrNextcloudPublicationUnavailable
	}
	if err := guard.Verify(); err != nil {
		return false, err
	}
	for _, prior := range slot.PhysicalReads {
		if prior == guard {
			return true, nil
		}
	}
	slot.PhysicalReads = append(slot.PhysicalReads, guard)
	return true, nil
}

// Borrowed request guards can close at their caller's return. A turn reuses
// only a guard it owns, or acquires its own guard before touching any content.
func ragTurnPhysicalReadGuard(ctx context.Context, scopes []readlease.NextcloudKBReadScope) (*readlease.NextcloudReadGuard, bool) {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return nil, false
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	for _, guard := range slot.PhysicalReads {
		if guard.Covers(scopes) {
			return guard, true
		}
	}
	return nil, true
}
func (s *messageService) admittedMessagePage(ctx context.Context, sessionID string, before time.Time, limit, page int, descending bool) ([]*types.Message, error) {
	tenant, ok := sessionTenantIDForLookup(ctx)
	if !ok {
		return nil, access.ErrNextcloudPublicationDenied
	}
	exclude, _ := ctx.Value(agentHistoryCurrentRequestKey{}).(string)
	if exclude != "" {
		return s.admittedMessages(ctx, types.MessageMaterialReadRequest{TenantID: tenant, SessionID: sessionID, AllSession: true, CompletedOnly: true, ExcludeRequestID: exclude, IncludeOriginalUserFields: true})
	}
	request, err := s.materialStore.MessagePageMaterialRequest(ctx, tenant, sessionID, before, limit, page, descending)
	if err != nil {
		return nil, err
	}
	if len(request.MessageIDs) == 0 {
		return []*types.Message{}, nil
	}
	return s.admittedMessages(ctx, request)
}
func (s *messageService) admittedMessages(ctx context.Context, request types.MessageMaterialReadRequest) ([]*types.Message, error) {
	guard, err := s.beginMessageMaterialRead(ctx, request)
	if err != nil {
		return nil, err
	}
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
		if err != nil {
			guard.Close()
			return nil, err
		}
		slot.mu.Lock()
		if slot.Closed {
			slot.mu.Unlock()
			guard.Close()
			return nil, access.ErrNextcloudPublicationUnavailable
		}
		slot.Guards = append(slot.Guards, guard)
		slot.Bodies = append(slot.Bodies, string(body))
		slot.mu.Unlock()
	} else {
		defer guard.Close()
	}
	copied, err := cloneCollection(guard.snapshot.Messages)
	if err != nil {
		guard.Close()
		return nil, err
	}
	for i, row := range guard.snapshot.Messages {
		if row == nil {
			continue
		}
		copied[i].SourceLineage, _ = types.UnionSourceLineage(row.SourceLineage)
		copied[i].ExecutionContext = row.ExecutionContext
		copied[i].AgentTenantID = row.AgentTenantID
		copied[i].RenderedContent = row.RenderedContent
		copied[i].ContextCheckpoint, err = cloneCollection(row.ContextCheckpoint)
		if err != nil {
			return nil, err
		}
	}
	if !request.IncludeOriginalUserFields {
		copied = rawUserContentOnly(copied)
	}
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		body, err := messageBoundaryBodyDigest(copied)
		if err != nil {
			return nil, err
		}
		slot.mu.Lock()
		slot.Outputs = append(slot.Outputs, copied)
		slot.OutputBodies = append(slot.OutputBodies, string(body))
		slot.mu.Unlock()
	}
	return copied, nil
}

func messageMaterialBoundarySnapshots(ctx context.Context) ([]*types.MessageMaterialSnapshot, error) {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
		return nil, err
	}
	slot.mu.Lock()
	guards := append([]*MessageMaterialGuard{}, slot.Guards...)
	slot.mu.Unlock()
	if len(guards) == 0 {
		return nil, types.ErrSourceLineageUnknown
	}
	result := []*types.MessageMaterialSnapshot{}
	for _, guard := range guards {
		copy, err := guard.OriginalSnapshot()
		if err != nil {
			return nil, err
		}
		result = append(result, copy)
	}
	return result, nil
}

func advanceMessageMaterialBoundaryAfterOwnField(ctx context.Context) error {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	for i, guard := range slot.Guards {
		if guard.Recheck() == nil {
			continue
		}
		if err := guard.advanceOwnGeneratedField(ctx); err != nil {
			return err
		}
		body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
		if err != nil {
			return err
		}
		slot.Bodies[i] = string(body)
	}
	return nil
}

func messageMaterialGeneratedWriteContext(ctx context.Context) (context.Context, error) {
	slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	if !ok {
		return ctx, types.ErrSourceLineageUnknown
	}
	slot.mu.Lock()
	guards := append([]*MessageMaterialGuard{}, slot.Guards...)
	slot.mu.Unlock()
	if len(guards) == 0 {
		return ctx, types.ErrSourceLineageUnknown
	}
	leases := []repository.NextcloudContentLease{}
	var err error
	for _, guard := range guards {
		guard.mu.Lock()
		plan := guard.plan
		rows := append([]repository.NextcloudContentLease{}, guard.leases...)
		guard.mu.Unlock()
		ctx, err = repository.WithMessageMaterialReadPlan(ctx, plan)
		if err != nil {
			return ctx, err
		}
		leases = append(leases, rows...)
	}
	if len(leases) > 0 {
		ctx, err = repository.WithFAQReadLeaseCoverage(ctx, leases)
	}
	return ctx, err
}

func messageBoundaryBodyDigest(messages []*types.Message) ([]byte, error) {
	type material struct {
		Body        *types.Message
		Lineage     *types.SourceLineage
		Execution   types.MessageExecutionContext
		AgentTenant uint64
		Rendered    string
		Checkpoint  *types.ContextCheckpoint
	}
	values := []material{}
	for _, message := range messages {
		if message == nil {
			values = append(values, material{})
			continue
		}
		values = append(values, material{message, message.SourceLineage, message.ExecutionContext, message.AgentTenantID, message.RenderedContent, message.ContextCheckpoint})
	}
	return json.Marshal(values)
}

func (s *messageService) actualGeneratedMessageFieldContext(ctx context.Context, session, message, field string, value any) (context.Context, bool, error) {
	turn := types.RAGSourceLineageFromContext(ctx)
	if turn == nil {
		return ctx, false, nil
	}
	lineage, err := turn.CheckedSnapshot(ctx)
	if err != nil {
		return ctx, true, err
	}
	if s.materialStore == nil {
		return ctx, true, types.ErrSourceLineageUnknown
	}
	ctx, err = messageMaterialGeneratedWriteContext(ctx)
	if err != nil {
		return ctx, true, err
	}
	parents, err := messageMaterialBoundarySnapshots(ctx)
	if err != nil {
		return ctx, true, err
	}
	ctx, err = s.materialStore.WithActualMessageGeneratedField(ctx, session, message, field, value, parents, lineage)
	return ctx, true, err
}

func (s *messageService) beginMessageMaterialCompletion(ctx context.Context, message *types.Message) (context.Context, func(error) error, error) {
	slot, ok := ctx.Value(messageMaterialExternalKey{}).(*messageMaterialOutputSlot)
	if !ok || message == nil || message.Role != "assistant" || !message.IsCompleted {
		return ctx, nil, nil
	}
	slot.mu.Lock()
	fail := func(err error) (context.Context, func(error) error, error) { slot.mu.Unlock(); return ctx, nil, err }
	if slot.Closed || !slot.Ready || slot.Failed != nil {
		return fail(access.ErrNextcloudPublicationUnavailable)
	}
	leases := []repository.NextcloudContentLease{}
	for _, guard := range slot.Guards {
		if err := guard.Recheck(); err != nil {
			return fail(err)
		}
		guard.mu.Lock()
		plan := guard.plan
		rows := append([]repository.NextcloudContentLease{}, guard.leases...)
		guard.mu.Unlock()
		var err error
		ctx, err = repository.WithMessageMaterialReadPlan(ctx, plan)
		if err != nil {
			return fail(err)
		}
		leases = append(leases, rows...)
	}
	var err error
	if len(leases) > 0 {
		ctx, err = repository.WithFAQReadLeaseCoverage(ctx, leases)
		if err != nil {
			return fail(err)
		}
	}
	ctx, err = s.materialStore.WithMessageProducerAdvance(ctx, message)
	if err != nil {
		return fail(err)
	}
	// Block heartbeat observation of the committed own-write transition until
	// its exact immutable edge has transferred the corresponding old controls.
	for _, guard := range slot.Guards {
		guard.mu.Lock()
	}
	finish := func(writeErr error) error {
		defer slot.mu.Unlock()
		defer func() {
			for i := len(slot.Guards) - 1; i >= 0; i-- {
				slot.Guards[i].mu.Unlock()
			}
		}()
		if writeErr != nil {
			slot.Failed = writeErr
			return writeErr
		}
		for i, guard := range slot.Guards {
			if guard.store.RecheckMessageMaterialInputs(guard.ctx, guard.snapshot) == nil {
				continue
			}
			if err := guard.advanceOwnCompletionLocked(ctx); err != nil {
				slot.Failed = err
				return err
			}
			body, err := messageBoundaryBodyDigest(guard.snapshot.Messages)
			if err != nil {
				slot.Failed = err
				return err
			}
			slot.Bodies[i] = string(body)
		}
		return nil
	}
	return ctx, finish, nil
}

func RetainMessageMaterialHTTPProducer(ctx context.Context) func() {
	_, release, external := retainMessageMaterialProducer(ctx)
	if !external {
		return func() {}
	}
	return release
}
