package repository

import (
	"context"
	"encoding/json"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type messageCompletionInputManifestRow struct {
	AssistantAdmissionID                   string `gorm:"primaryKey"`
	Recipe, ParentManifest, ManifestDigest string
}

func (messageCompletionInputManifestRow) TableName() string {
	return "message_completion_input_manifests"
}

type messageCompletionInputKey struct{}
type messageCompletionInputBoundary struct {
	Store    *KnowledgeInputProvenanceStore
	Manifest *types.MessageCompletionInputManifest
}

func (s *KnowledgeInputProvenanceStore) WithActualMessageCompletionInputs(ctx context.Context, manifest *types.MessageCompletionInputManifest) (context.Context, error) {
	if manifest == nil || manifest.Initial == nil || manifest.Recipe != "history-initial-and-consumed-steer-v1" || manifest.SourceLineage.RequireComplete() != nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return ctx, err
	}
	var copy types.MessageCompletionInputManifest
	if err := json.Unmarshal(body, &copy); err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, messageCompletionInputKey{}, messageCompletionInputBoundary{s, &copy}), nil
}
func (s *KnowledgeInputProvenanceStore) sealMessageCompletionInputsInTx(tx *gorm.DB, origin *messageGenerationOrigin, admission *messageMaterialAdmission, message *types.Message) error {
	boundary, ok := tx.Statement.Context.Value(messageCompletionInputKey{}).(messageCompletionInputBoundary)
	if !ok {
		return nil
	}
	if boundary.Store != s || message.Role != "assistant" || !message.IsCompleted {
		return types.ErrSourceLineageUnknown
	}
	manifest := boundary.Manifest
	if manifest.AssistantMessageID != message.ID || manifest.AssistantGenerationID != origin.ID || manifest.SessionID != message.SessionID || manifest.RequestID != message.RequestID || manifest.OwnerID != origin.OwnerID || manifest.OwnerRevision != origin.OwnerRevision || manifest.TenantID != origin.TenantID {
		return ErrKnowledgeInputChanged
	}
	manifest.AssistantAdmissionID = admission.ID
	sources := []*types.SourceLineage{message.SourceLineage}
	for _, snapshot := range manifest.Inputs {
		if snapshot == nil || snapshot.Messages != nil {
			return types.ErrSourceLineageUnknown
		}

		for _, proof := range snapshot.Proofs {
			sources = append(sources, proof.SourceLineage)
		}
	}
	for _, steer := range manifest.Steers {
		if steer == nil || steer.Consumption == nil || steer.User == nil || steer.Original == nil {
			return types.ErrSourceLineageUnknown
		}
		if err := s.checkSteerConsumptionInTx(tx, steer.Consumption); err != nil {
			return err
		}
		sources = append(sources, steer.OriginalSource, steer.User.SourceLineage)
	}
	union, err := types.UnionSourceLineage(sources...)
	if err != nil {
		return err
	}
	if !faqInputEqual(union, manifest.SourceLineage) {
		return ErrKnowledgeInputChanged
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	digest, err := KnowledgeInputDigest(json.RawMessage(raw))
	if err != nil {
		return err
	}
	return tx.Create(&messageCompletionInputManifestRow{admission.ID, manifest.Recipe, string(raw), digest}).Error
}

func (s *KnowledgeInputProvenanceStore) validateMessageCompletionInputsInTx(tx *gorm.DB, message *types.Message) error {
	boundary, ok := tx.Statement.Context.Value(messageCompletionInputKey{}).(messageCompletionInputBoundary)
	if !ok {
		return nil
	}
	if boundary.Store != s || boundary.Manifest == nil || boundary.Manifest.AssistantMessageID != message.ID {
		return types.ErrSourceLineageUnknown
	}
	for _, snapshot := range boundary.Manifest.Inputs {
		actual, err := s.captureMessageMaterialInTx(tx, snapshot.Request)
		if err != nil {
			return err
		}
		a, b := *actual, *snapshot
		a.Messages = nil
		b.Messages = nil
		if !faqInputEqual(a, b) {
			return ErrKnowledgeInputChanged
		}
	}
	for _, parent := range boundary.Manifest.Steers {
		if err := s.checkSteerConsumptionInTx(tx, parent.Consumption); err != nil {
			return err
		}
	}
	return nil
}
