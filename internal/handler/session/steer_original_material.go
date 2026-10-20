package session

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/access"
	applicationservice "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type actualSteerMaterialService interface {
	HasOriginalMessageMaterialStore() bool
	MessageNativeLiveIdentity(context.Context, string, string) (*types.Message, string, error)
	AdmitActualSteerInput(context.Context, *types.SteerAuthoredRequest) (*types.SteerMaterialInput, error)
	BeginSteerMaterialRead(
		context.Context,
		*types.SteerMaterialRequest,
	) (*types.SteerMaterialSnapshot, func() error, func(), error)
	ApplyActualSteerControl(context.Context, *types.SteerControlRequest) (*types.SteerCandidateControl, error)
	CheckActualSteerControl(context.Context, *types.SteerCandidateControl) error
	ConsumeActualSteerIntoMessage(
		context.Context,
		*types.SteerMaterialInput,
		string,
		string,
		string,
	) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error)
	ConsumeActualSteerFollowUpIntoMessage(
		context.Context,
		*types.SteerMaterialInput,
		string,
		string,
		string,
	) (*types.Message, *types.SteerConsumptionProof, func() error, func(), error)
	CreateActualSteerFollowUpAssistant(
		context.Context,
		*types.SteerMaterialInput,
		*types.Message,
	) (*types.Message, error)
}

// Only explicit service-package algorithm fixtures can use the old Redis
// algorithms. Missing actual production provenance never grants a fallback.
type steerAlgorithmFixture interface{ steerAlgorithmFixture() }

func actualSteerService(messages interfaces.MessageService) (actualSteerMaterialService, bool, error) {
	if actual, ok := messages.(actualSteerMaterialService); ok && actual.HasOriginalMessageMaterialStore() {
		return actual, false, nil
	}
	if _, ok := messages.(steerAlgorithmFixture); ok {
		return nil, true, nil
	}
	return nil, false, access.ErrNextcloudPublicationUnavailable
}

func beginActualSteerRead(
	ctx context.Context,
	actual actualSteerMaterialService,
	session, run string,
	use types.SteerMaterialUse,
	ids []string,
) (*types.SteerMaterialSnapshot, func() error, func(), error) {
	control, generation, err := actual.MessageNativeLiveIdentity(ctx, session, run)
	if err != nil {
		return nil, nil, nil, err
	}
	if control == nil || generation == "" {
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	request := &types.SteerMaterialRequest{
		TenantID:        types.MustTenantIDFromContext(ctx),
		SessionID:       session,
		RunMessageID:    run,
		RunGenerationID: generation,
		SteerIDs:        ids,
		Use:             use,
	}
	snapshot, check, closeRead, err := actual.BeginSteerMaterialRead(ctx, request)
	if err != nil {
		return nil, nil, nil, err
	}
	if snapshot == nil || snapshot.Plan == nil || check == nil || closeRead == nil {
		if closeRead != nil {
			closeRead()
		}
		return nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if err := check(); err != nil {
		closeRead()
		return nil, nil, nil, err
	}
	return snapshot, check, closeRead, nil
}

func writeActualSteerJSON(c *gin.Context, check func() error, closeRead2 func(), body interface{}) {
	if closeRead2 != nil {
		defer closeRead2()
	}
	if check == nil {
		_ = c.Error(errors.NewServiceUnavailableError("Steer material is unavailable"))
		return
	}
	if err := check(); err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return
	}
	prior := c.Writer
	c.Writer = &nativeSavedReplayWriter{ResponseWriter: prior, ctx: c.Request.Context(), check: check}
	defer func() { c.Writer = prior }()
	c.JSON(200, body)
}

func steerMaterialMirror(input *types.SteerMaterialInput) interfaces.StreamEvent {
	event := steerEvent(input.Proof.SteerID, input.Query, input.MentionedItems, input.Channel)
	event.Data["delivery"] = string(input.Control.Delivery)
	event.Data[steerDataConsumed] = input.Control.Consumed || input.Control.Discarded || input.Control.Deleted
	event.Data[steerDataUserMessageID] = input.Control.UserMessageID
	event.Data["original_admission_id"] = input.Proof.AdmissionID
	event.Data["control_head_id"] = input.Control.HeadID
	event.Data["control_revision"] = input.Control.Revision
	return event
}

func (h *Handler) tryActualSteerEnqueue(
	ctx context.Context, c *gin.Context,

	session, run string,
	req *SteerMessageRequest,
	query, delivery string,
) bool {
	actual, algorithm, err := actualSteerService(h.messageService)
	if algorithm {
		return false
	}
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if run == "" {
		c.JSON(200, gin.H{"success": true, "status": "new_run"})
		return true
	}
	if req.ExpectedAssistantMessageID != "" && req.ExpectedAssistantMessageID != run {
		_ = c.Error(errors.NewConflictError("The active turn changed; retry the message"))
		return true
	}
	control, generation, err := actual.MessageNativeLiveIdentity(ctx, session, run)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if control == nil || control.IsCompleted {
		_ = c.Error(errors.NewConflictError("The active turn changed; retry the message"))
		return true
	}
	steerID := req.SteerID
	if steerID == "" {
		steerID = uuid.NewString()
	} else if _, err := uuid.Parse(steerID); err != nil {
		_ = c.Error(errors.NewBadRequestError("invalid steer_id"))
		return true
	}
	channel := strings.TrimSpace(req.Channel)
	if channel == "" {
		channel = "web"
	}
	rawInput, err := json.Marshal(req)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	request := &types.SteerAuthoredRequest{
		RawInput:        rawInput,
		TenantID:        types.MustTenantIDFromContext(ctx),
		SessionID:       session,
		RunMessageID:    run,
		RunGenerationID: generation,
		RunRequestID:    control.RequestID,
		ClientSteerID:   steerID,
		Query:           query,
		MentionedItems:  convertMentionedItems(req.MentionedItems),
		Channel:         channel,
		Delivery:        types.SteerDelivery(delivery),
		ActorContext:    ctx,
		SourceContext:   ctx,
	}
	ctx, err = applicationservice.WithActualSteerHTTPInput(ctx, request)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	input, err := actual.AdmitActualSteerInput(ctx, request)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if input == nil || input.Proof == nil || input.Control == nil {
		_ = c.Error(nativeSavedReplayHTTPError(access.ErrNextcloudPublicationUnavailable))
		return true
	}
	// Redis is a best-effort mirror of the original SQL admission/control.
	_ = h.streamManager.AppendSteerEvents(
		ctx,
		session,
		input.Control.RunMessageID,
		[]interfaces.StreamEvent{steerMaterialMirror(input)},
	)
	status := "queued"
	if input.Control.Consumed {
		status = "already_injected"
	}
	writeActualSteerJSON(
		c,
		func() error { return actual.CheckActualSteerControl(ctx, input.Control) },
		nil,
		gin.H{
			"success":              true,
			"status":               status,
			"steer_id":             input.Proof.SteerID,
			"delivery":             input.Control.Delivery,
			"assistant_message_id": input.Control.RunMessageID,
		},
	)
	return true
}

func (h *Handler) tryActualSteerControl(
	ctx context.Context, c *gin.Context,

	session, run, steer string,
	operation types.SteerControlOperation,
) bool {
	actual, algorithm, err := actualSteerService(h.messageService)
	if algorithm {
		return false
	}
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if run == "" {
		c.JSON(200, gin.H{"success": true, "status": "new_run"})
		return true
	}
	snapshot, check, closeRead3, err := beginActualSteerRead(
		ctx,
		actual,
		session,
		run,
		types.SteerMaterialPending,
		[]string{steer},
	)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	defer closeRead3()
	var selected *types.SteerCandidateControl
	for _, candidate := range snapshot.Plan.CandidateControls {
		if candidate != nil && candidate.SteerID == steer {
			selected = candidate
			break
		}
	}
	if selected == nil {
		_ = c.Error(errors.NewNotFoundError("Queued message not found"))
		return true
	}
	if selected.Consumed {
		writeActualSteerJSON(
			c,
			check,
			nil,
			gin.H{"success": true, "status": "already_injected", "removed": false, "steer_id": steer},
		)
		return true
	}
	if err := check(); err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	newControl, err := actual.ApplyActualSteerControl(
		ctx,
		&types.SteerControlRequest{
			TenantID:              snapshot.Plan.Request.TenantID,
			SessionID:             session,
			SteerID:               steer,
			OriginalAdmissionID:   selected.OriginalAdmissionID,
			ExpectedHeadID:        selected.HeadID,
			ExpectedRevision:      selected.Revision,
			Operation:             operation,
			TargetRunMessageID:    selected.RunMessageID,
			TargetRunGenerationID: selected.RunGenerationID,
			TargetRunRequestID:    selected.RunRequestID,
		},
	)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if newControl == nil {
		_ = c.Error(nativeSavedReplayHTTPError(access.ErrNextcloudPublicationUnavailable))
		return true
	}
	if operation == types.SteerControlDelete {
		_, _ = h.streamManager.DeleteSteerEvent(ctx, session, run, steer)
	} else {
		_, _ = h.streamManager.UpdateSteerEventData(
			ctx,
			session,
			run,
			steer,
			map[string]interface{}{
				"delivery":         string(newControl.Delivery),
				"control_head_id":  newControl.HeadID,
				"control_revision": newControl.Revision,
			},
		)
	}
	writeActualSteerJSON(
		c,
		func() error { return actual.CheckActualSteerControl(ctx, newControl) },
		nil,
		gin.H{
			"success":              true,
			"status":               "queued",
			"removed":              newControl.Deleted,
			"steer_id":             steer,
			"delivery":             newControl.Delivery,
			"assistant_message_id": newControl.RunMessageID,
		},
	)
	return true
}

func (h *Handler) tryActualSteerList(ctx context.Context, c *gin.Context, session, run string) bool {
	actual, algorithm, err := actualSteerService(h.messageService)
	if algorithm {
		return false
	}
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	if run == "" {
		c.JSON(200, gin.H{"success": true, "items": []interface{}{}})
		return true
	}
	snapshot, check, closeRead4, err := beginActualSteerRead(ctx, actual, session, run, types.SteerMaterialPending, nil)
	if err != nil {
		_ = c.Error(nativeSavedReplayHTTPError(err))
		return true
	}
	items := []map[string]interface{}{}
	for _, input := range snapshot.Inputs {
		if input == nil || input.Proof == nil || input.Control == nil {
			closeRead4()
			_ = c.Error(nativeSavedReplayHTTPError(access.ErrNextcloudPublicationUnavailable))
			return true
		}
		items = append(
			items,
			map[string]interface{}{
				"steer_id":        input.Proof.SteerID,
				"content":         input.Query,
				"delivery":        input.Control.Delivery,
				"mentioned_items": input.MentionedItems,
			},
		)
	}
	writeActualSteerJSON(c, check, closeRead4, gin.H{"success": true, "assistant_message_id": run, "items": items})
	return true
}

type heldSteerMaterial struct {
	input *types.SteerMaterialInput
	check func() error
	close func()
}

func (s *steerSink) pollActualSteer(
	ctx context.Context,
	session, run string,
) ([]map[string]interface{}, int, bool, error) {
	actual, algorithm, err := actualSteerService(s.messageService)
	if algorithm {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, true, err
	}
	if err := s.CheckOriginalSteerInputs(ctx); err != nil {
		return nil, 0, true, err
	}
	snapshot, check, closeRead5, err := beginActualSteerRead(ctx, actual, session, run, types.SteerMaterialInject, nil)
	if err != nil {
		return nil, 0, true, err
	}
	if len(snapshot.Inputs) == 0 {
		closeRead5()
		return nil, len(snapshot.Plan.CandidateControls), true, nil
	}
	s.mu.Lock()
	if s.originalInputs == nil {
		s.originalInputs = map[string]heldSteerMaterial{}
	}
	out := []map[string]interface{}{}
	for _, input := range snapshot.Inputs {
		if input == nil || input.Proof == nil || input.Control == nil {
			err = access.ErrNextcloudPublicationUnavailable
			break
		}
		if _, used := s.injectedIDs[input.Proof.SteerID]; used {
			continue
		}
		s.originalInputs[input.Proof.SteerID] = heldSteerMaterial{input, check, closeRead5}
		out = append(
			out,
			map[string]interface{}{
				"id":                    input.Proof.SteerID,
				"content":               input.Query,
				"mentioned_items":       mentionedItemsToRaw(input.MentionedItems),
				"channel":               input.Channel,
				"original_admission_id": input.Proof.AdmissionID,
				"control_revision":      input.Control.Revision,
			},
		)
		break // One exact consume/head transfer per actual round drain.
	}
	s.originalPollCloses = append(s.originalPollCloses, closeRead5)
	s.mu.Unlock()
	if err != nil {
		closeRead5()
	}
	return out, len(snapshot.Plan.CandidateControls), true, err
}

func (s *steerSink) PersistSteerMaterial(
	ctx context.Context,
	session, run, steer, content string,
	mentions types.MentionedItems,
	channel string,
) (string, *types.SourceLineage, func() error, func(), error) {
	actual, algorithm, err := actualSteerService(s.messageService)
	if algorithm {
		id := s.PersistSteerMessage(ctx, session, run, steer, content, mentions, channel)
		if id == "" {
			return "", nil, nil, nil, access.ErrNextcloudPublicationUnavailable
		}
		lineage, _ := types.NewCompleteSourceLineage()
		return id, lineage, func() error { return nil }, func() {}, nil
	}
	if err != nil {
		return "", nil, nil, nil, err
	}
	s.mu.Lock()
	held, found := s.originalInputs[steer]
	s.mu.Unlock()
	if !found || held.input == nil ||
		!equalSteerOriginalFields(
			&types.SteerMaterialInput{
				Query:          types.SteerCanonicalQuery(content),
				MentionedItems: mentions,
				Channel:        channel,
			},
			held.input,
		) {
		return "", nil, nil, nil, access.ErrNextcloudPublicationDenied
	}
	if err := held.check(); err != nil {
		return "", nil, nil, nil, err
	}
	control, generation, err := actual.MessageNativeLiveIdentity(ctx, session, run)
	if err != nil {
		return "", nil, nil, nil, err
	}
	message, proof, check, closeRead6, err := actual.ConsumeActualSteerIntoMessage(
		ctx,
		held.input,
		run,
		generation,
		control.RequestID,
	)
	if err != nil {
		return "", nil, nil, nil, err
	}
	if message == nil || proof == nil || check == nil || closeRead6 == nil ||
		message.SourceLineage.RequireComplete() != nil {
		if closeRead6 != nil {
			closeRead6()
		}
		return "", nil, nil, nil, access.ErrNextcloudPublicationUnavailable
	}
	if err := check(); err != nil {
		closeRead6()
		return "", nil, nil, nil, err
	}
	s.mu.Lock()
	s.originalChecks = append(s.originalChecks, check)
	s.originalCloses = append(s.originalCloses, closeRead6)
	s.lastUserMessageID = message.ID
	s.injectedIDs[steer] = struct{}{}
	delete(s.originalInputs, steer)
	pollCloses := s.originalPollCloses
	s.originalPollCloses = nil
	s.mu.Unlock()
	for _, close := range pollCloses {
		close()
	}
	_, _ = s.streamManager.UpdateSteerEventData(
		ctx,
		session,
		run,
		steer,
		map[string]interface{}{
			steerDataConsumed:      true,
			steerDataUserMessageID: message.ID,
			"control_head_id":      proof.ResultHeadID,
			"control_revision":     proof.ResultRevision,
		},
	)
	lineage, _ := types.UnionSourceLineage(message.SourceLineage)
	return message.ID, lineage, check, closeRead6, nil
}

func (s *steerSink) CheckOriginalSteerInputs(context.Context) error {
	s.mu.Lock()
	checks := append([]func() error(nil), s.originalChecks...)
	s.mu.Unlock()
	for _, check := range checks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (s *steerSink) closeOriginalSteerInputs() {
	s.mu.Lock()
	closes := append(append([]func(){}, s.originalCloses...), s.originalPollCloses...)
	s.originalCloses = nil
	s.originalPollCloses = nil
	s.mu.Unlock()
	for _, close := range closes {
		close()
	}
}

func equalSteerOriginalFields(a, b *types.SteerMaterialInput) bool {
	if a == nil || b == nil {
		return false
	}
	mentionsA, mentionsB := a.MentionedItems, b.MentionedItems
	if len(mentionsA) == 0 {
		mentionsA = types.MentionedItems{}
	}
	if len(mentionsB) == 0 {
		mentionsB = types.MentionedItems{}
	}
	x, err := json.Marshal(struct {
		Query    string
		Mentions types.MentionedItems
		Channel  string
	}{a.Query, mentionsA, a.Channel})
	if err != nil {
		return false
	}
	y, err := json.Marshal(struct {
		Query    string
		Mentions types.MentionedItems
		Channel  string
	}{b.Query, mentionsB, b.Channel})
	return err == nil && string(x) == string(y)
}

func (h *Handler) discardActualSteer(ctx context.Context, session, run string) (bool, error) {
	actual, algorithm, err := actualSteerService(h.messageService)
	if algorithm {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	snapshot, check, closeRead7, err := beginActualSteerRead(
		ctx,
		actual,
		session,
		run,
		types.SteerMaterialFollowUp,
		nil,
	)
	if err != nil {
		return true, err
	}
	defer closeRead7()
	if err := check(); err != nil {
		return true, err
	}
	for _, input := range snapshot.Inputs {
		if input == nil || input.Control == nil {
			return true, access.ErrNextcloudPublicationUnavailable
		}
		control := input.Control
		after, err := actual.ApplyActualSteerControl(
			ctx,
			&types.SteerControlRequest{
				TenantID:              snapshot.Plan.Request.TenantID,
				SessionID:             session,
				SteerID:               control.SteerID,
				OriginalAdmissionID:   control.OriginalAdmissionID,
				ExpectedHeadID:        control.HeadID,
				ExpectedRevision:      control.Revision,
				Operation:             types.SteerControlDiscard,
				TargetRunMessageID:    run,
				TargetRunGenerationID: control.RunGenerationID,
				TargetRunRequestID:    control.RunRequestID,
			},
		)
		if err != nil {
			return true, err
		}
		if after == nil {
			return true, access.ErrNextcloudPublicationUnavailable
		}
		_, _ = h.streamManager.UpdateSteerEventData(
			ctx,
			session,
			run,
			control.SteerID,
			map[string]interface{}{
				steerDataConsumed:  true,
				"control_head_id":  after.HeadID,
				"control_revision": after.Revision,
			},
		)
	}
	return true, nil
}

func (h *Handler) claimActualSteerFollowUp(
	ctx context.Context,
	previous *qaRequestContext,
	previousStream *sseStreamContext,
) (*qaRequestContext, bool, bool) {
	actual, algorithm, err := actualSteerService(h.messageService)
	if algorithm {
		return nil, false, false
	}
	if err != nil {
		return nil, false, true
	}
	if previousStream.materialCheck != nil && previousStream.materialCheck() != nil {
		return nil, false, true
	}
	if previousStream.actualSteerClaimAttempted {
		return nil, false, true
	}
	snapshot, check, closeRead8, err := beginActualSteerRead(
		ctx,
		actual,
		previous.sessionID,
		previousStream.assistantMessage.ID,
		types.SteerMaterialFollowUp,
		nil,
	)
	if err != nil {
		return nil, false, true
	}
	defer closeRead8()
	if len(snapshot.Inputs) == 0 {
		return nil, false, true
	}
	previousStream.actualSteerClaimAttempted = true
	first := snapshot.Inputs[0]
	if first == nil || first.Proof == nil || first.Control == nil {
		return nil, false, true
	}
	if err := check(); err != nil {
		return nil, false, true
	}
	follow := *previous
	follow.ctx = applicationservice.MessageMaterialOwnerContext(ctx)
	follow.query = first.Query
	follow.channel = first.Channel
	follow.requestID = uuid.NewString()
	follow.userMessageID = ""
	follow.steerSink = nil
	follow.images = nil
	follow.attachments = nil
	follow.attachmentIDs = nil
	follow.attachmentMetas = nil
	follow.suggestionAttribution = nil
	follow.skipSSE = true
	follow.steerCarryOver = nil
	h.applyFollowUpMentions(ctx, &follow, mentionedItemsToRaw(first.MentionedItems))
	// The only ordinary Create here is the actual empty assistant producer.
	// The user row is derived exclusively from the admitted authored receipt.
	follow.assistantMessage = &types.Message{
		SessionID:        previous.sessionID,
		RequestID:        follow.requestID,
		Role:             "assistant",
		ExecutionContext: previous.assistantMessage.ExecutionContext,
		AgentID:          previous.assistantMessage.AgentID,
		AgentTenantID:    previous.assistantMessage.AgentTenantID,
	}
	created, err := actual.CreateActualSteerFollowUpAssistant(ctx, first, follow.assistantMessage)
	if err != nil {
		return nil, false, true
	}
	follow.assistantMessage = created
	control, generation, err := actual.MessageNativeLiveIdentity(ctx, previous.sessionID, created.ID)
	if err != nil {
		h.rollbackTurnMessages(ctx, &follow, false, true)
		return nil, false, true
	}
	user, consumed, readCheck, readClose, err := actual.ConsumeActualSteerFollowUpIntoMessage(
		ctx,
		first,
		created.ID,
		generation,
		control.RequestID,
	)
	if err != nil || user == nil || consumed == nil || readCheck == nil || readClose == nil {
		if readClose != nil {
			readClose()
		}
		return nil, false, true
	}
	if err := readCheck(); err != nil {
		readClose()
		return nil, false, true
	}
	follow.userMessageID = user.ID
	follow.userCreatedAt = user.CreatedAt
	if err := h.streamManager.ClaimLiveRun(ctx, previous.sessionID, created.ID, follow.requestID); err != nil {
		readClose()
		_, _ = actual.ApplyActualSteerControl(
			ctx,
			&types.SteerControlRequest{
				TenantID:            first.Proof.TenantID,
				SessionID:           previous.sessionID,
				SteerID:             first.Proof.SteerID,
				OriginalAdmissionID: first.Proof.OriginID,
				ExpectedHeadID:      consumed.ResultHeadID,
				ExpectedRevision:    consumed.ResultRevision,
				Operation:           types.SteerControlClaimFailed,
			},
		)
		return nil, false, true
	}
	// Subsequent QA receives its actual saved user receipt. Keep this original
	// consume capability alive across the handoff; the producer context owns it.
	follow.steerInputClose = readClose
	context.AfterFunc(follow.ctx, readClose)
	_, _ = h.streamManager.UpdateSteerEventData(
		ctx,
		previous.sessionID,
		previousStream.assistantMessage.ID,
		first.Proof.SteerID,
		map[string]interface{}{
			steerDataConsumed:      true,
			steerDataUserMessageID: user.ID,
			"control_head_id":      consumed.ResultHeadID,
			"control_revision":     consumed.ResultRevision,
		},
	)
	for _, input := range snapshot.Inputs[1:] {
		if input == nil || input.Control == nil || input.Proof == nil {
			continue
		}
		old := input.Control
		after, err := actual.ApplyActualSteerControl(
			ctx,
			&types.SteerControlRequest{
				TenantID:              snapshot.Plan.Request.TenantID,
				SessionID:             previous.sessionID,
				SteerID:               old.SteerID,
				OriginalAdmissionID:   old.OriginalAdmissionID,
				ExpectedHeadID:        old.HeadID,
				ExpectedRevision:      old.Revision,
				Operation:             types.SteerControlRebind,
				TargetRunMessageID:    created.ID,
				TargetRunGenerationID: generation,
				TargetRunRequestID:    follow.requestID,
			},
		)
		if err != nil || after == nil {
			continue
		}
		mirror := *input
		mirror.Control = after
		_ = h.streamManager.AppendSteerEvents(
			ctx,
			previous.sessionID,
			created.ID,
			[]interfaces.StreamEvent{steerMaterialMirror(&mirror)},
		)
		_, _ = h.streamManager.DeleteSteerEvent(
			ctx,
			previous.sessionID,
			previousStream.assistantMessage.ID,
			old.SteerID,
		)
	}
	return &follow, true, true
}
