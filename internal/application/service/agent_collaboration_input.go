package service

import (
	"context"
	"reflect"
	"slices"
	"sync"

	"github.com/Tencent/WeKnora/internal/agent/collaboration"
	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
)

// The scheduling transaction stores identities of already admitted inputs. A
// worker opens fresh guards; holding a lease on a collaboration is not message
// access, and the generated child query is not an independent human input.
func (s *messageService) captureDelegatedInput(
	ctx context.Context, scope collaboration.Scope, parent string,
) (*collaboration.InputOrigin, error) {
	if s.materialStore == nil || s.sourceLineageGuard == nil {
		return nil, access.ErrNextcloudPublicationUnavailable
	}
	caller := types.CallerFromContext(ctx)
	if caller.TenantID != scope.TenantID || types.SessionOwnerIDFromContext(ctx) != scope.UserID {
		return nil, access.ErrNextcloudPublicationDenied
	}
	control, generation, err := s.materialStore.MessageLiveControl(ctx, scope.SessionID, parent)
	if err != nil {
		return nil, err
	}
	if control.Role != "assistant" || control.IsCompleted {
		return nil, access.ErrNextcloudPublicationDenied
	}
	pair, err := s.materialStore.CommittedMessageParentRequest(ctx, scope.TenantID, scope.SessionID, parent)
	if err != nil {
		return nil, err
	}
	pair.MessageIDs = slices.DeleteFunc(pair.MessageIDs, func(id string) bool { return id == parent })
	if len(pair.MessageIDs) != 1 {
		return nil, access.ErrNextcloudPublicationDenied
	}
	pair.IncludeOriginalUserFields = true
	initial, err := s.beginMessageMaterialRead(ctx, pair)
	if err != nil {
		return nil, err
	}
	defer initial.Close()
	input := &collaboration.InputOrigin{ParentSession: scope.SessionID, ParentMessage: parent, Generation: generation}
	var snapshots []*types.MessageMaterialSnapshot
	if slot, ok := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot); ok {
		if err := CheckMessageMaterialOutputBoundary(ctx); err != nil {
			return nil, err
		}
		slot.mu.Lock()
		guards := slices.Clone(slot.Guards)
		slot.mu.Unlock()
		for _, guard := range guards {
			snapshot, err := guard.OriginalSnapshot()
			if err != nil {
				return nil, err
			}
			snapshots = append(snapshots, snapshot)
		}
	}
	snapshots = append(snapshots, initial.snapshot)
	var sources []*types.SourceLineage
	if turn := types.RAGSourceLineageFromContext(ctx); turn != nil {
		current, err := turn.CheckedSnapshot(ctx)
		if err != nil {
			return nil, err
		}
		sources = append(sources, current)
	}
	for _, snapshot := range snapshots {
		if snapshot.Request.TenantID != scope.TenantID || snapshot.SessionOwnerID != scope.UserID {
			return nil, access.ErrNextcloudPublicationDenied
		}
		for _, proof := range snapshot.Proofs {
			if proof == nil || proof.AdmissionID == "" {
				return nil, access.ErrNextcloudPublicationDenied
			}
			ref := delegatedMessageRef(proof, snapshot.Request.IncludeOriginalUserFields)
			if !slices.ContainsFunc(input.Messages, func(prev collaboration.InputMessage) bool {
				return reflect.DeepEqual(prev, ref)
			}) {
				input.Messages = append(input.Messages, ref)
			}
			sources = append(sources, proof.SourceLineage)
			for _, field := range proof.GeneratedFields {
				sources = append(sources, field.SourceLineage)
			}
		}
	}
	input.Lineage, err = types.UnionSourceLineage(sources...)
	if err != nil {
		return nil, err
	}
	if err := input.Lineage.RequireComplete(); err != nil {
		return nil, err
	}
	return input, nil
}

func delegatedMessageRef(p *types.MessageMaterialProof, originalFields bool) collaboration.InputMessage {
	ref := collaboration.InputMessage{
		Session: p.SessionID, Message: p.MessageID, Admission: p.AdmissionID,
		OriginalFields: originalFields,
	}
	for _, field := range p.GeneratedFields {
		ref.Generated = append(ref.Generated, field.AdmissionID)
	}
	return ref
}

type delegatedWorkerInput struct {
	work                collaboration.Work
	service             *AgentCollaboration
	native              *messageService
	userID, assistantID string
	mu                  sync.Mutex
	opened              []collaboration.InputOrigin
	sources             *types.SourceLineage
}

type delegatedWorkerInputKey struct{}

func (d *delegatedWorkerInput) check(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	w := d.work
	if types.CallerFromContext(ctx).TenantID != w.Scope.TenantID ||
		types.SessionOwnerIDFromContext(ctx) != w.Scope.UserID {
		return access.ErrNextcloudPublicationDenied
	}
	member, run, err := d.service.Runtime.OwnedRun(ctx, w)
	if err != nil {
		return err
	}
	if member.ID != w.Member.ID || member.SessionID != w.Member.SessionID || member.AgentID != w.Member.AgentID ||
		member.ConfigJSON != w.Member.ConfigJSON || !run.MatchesInput(w.Scope, member) || run.Query != w.Run.Query ||
		run.UserMessageID != d.userID || run.MessageID != d.assistantID ||
		len(w.Member.InputOrigins) == 0 || len(member.InputOrigins) < len(w.Member.InputOrigins) ||
		!reflect.DeepEqual(member.InputOrigins[:len(w.Member.InputOrigins)], w.Member.InputOrigins) ||
		len(member.InputOrigins) < len(d.opened) ||
		len(d.opened) > 0 && !reflect.DeepEqual(member.InputOrigins[:len(d.opened)], d.opened) {
		return access.ErrNextcloudPublicationDenied
	}
	var sources []*types.SourceLineage
	for i, input := range member.InputOrigins {
		control, generation, err := d.native.materialStore.MessageLiveControl(
			ctx, input.ParentSession, input.ParentMessage,
		)
		if err != nil {
			return err
		}
		if control.Role != "assistant" || generation != input.Generation || input.ParentSession != w.Scope.SessionID {
			return access.ErrNextcloudPublicationDenied
		}
		scope, err := d.native.messageSourceLineageScope(ctx, control)
		if err != nil {
			return err
		}
		if err := d.native.sourceLineageGuard.Check(ctx, input.Lineage, scope); err != nil {
			return err
		}
		if i >= len(d.opened) {
			if err := d.reopenMessages(ctx, input.Messages); err != nil {
				return err
			}
		}
		sources = append(sources, input.Lineage)
	}
	d.sources, err = types.UnionSourceLineage(sources...)
	if err != nil {
		return err
	}
	d.opened = member.InputOrigins
	return nil
}

func (d *delegatedWorkerInput) lineage() *types.SourceLineage {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sources // replaced, never mutated, when new guidance arrives
}

func (d *delegatedWorkerInput) reopenMessages(ctx context.Context, messages []collaboration.InputMessage) error {
	if len(messages) == 0 {
		return access.ErrNextcloudPublicationDenied
	}
	for _, ref := range messages {
		req := types.MessageMaterialReadRequest{
			TenantID: d.work.Scope.TenantID, SessionID: ref.Session, MessageIDs: []string{ref.Message},
			IncludeOriginalUserFields: ref.OriginalFields,
		}
		guard, err := d.native.beginMessageMaterialRead(ctx, req)
		if err != nil {
			return err
		}
		if len(guard.snapshot.Proofs) != 1 || guard.snapshot.SessionOwnerID != d.work.Scope.UserID ||
			!reflect.DeepEqual(delegatedMessageRef(guard.snapshot.Proofs[0], ref.OriginalFields), ref) {
			guard.Close()
			return access.ErrNextcloudPublicationDenied
		}
		if _, err = d.native.bindAdmittedMessageGuard(ctx, guard, req); err != nil {
			return err
		}
	}
	return nil
}

func (d *delegatedWorkerInput) bindChild(ctx context.Context, userID, assistantID string) error {
	d.userID, d.assistantID = userID, assistantID
	if err := d.check(ctx); err != nil {
		return err
	}
	req := types.MessageMaterialReadRequest{
		TenantID: d.work.Scope.TenantID, SessionID: d.work.Member.SessionID,
		MessageIDs: []string{userID}, IncludeOriginalUserFields: true,
	}
	rows, err := d.native.admittedMessages(ctx, req)
	if err != nil {
		return err
	}
	if len(rows) != 1 || rows[0].Role != "user" || rows[0].RequestID != d.work.Run.ID ||
		rows[0].Content != d.work.Run.Query {
		return access.ErrNextcloudPublicationDenied
	}
	return d.native.bindInitialMessageMaterial(ctx, assistantID, userID, d.work.Run.ID)
}
