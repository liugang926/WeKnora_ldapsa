package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

// Only a verified durable Auto completion intent can re-emit this task. It
// reacquires the actual current build/configuration context, proves the old
// immutable typed task input, and reuses a fixed ID across recovery scans.
func (s *KnowledgeInputProvenanceStore) PrepareNextcloudAutoTagRecovery(ctx context.Context,
	raw []byte,
) ([]byte, string, func(), error) {
	noop := func() {}
	var p types.KnowledgeAutoTagPayload
	if json.Unmarshal(raw, &p) != nil || p.TaskInput == nil {
		return nil, "", noop, ErrNextcloudMachineTaskInvalid
	}
	var k types.Knowledge
	if err := s.db.WithContext(ctx).Select("id,tenant_id,knowledge_base_id,channel,metadata").
		Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND deleted_at IS NULL",
			p.KnowledgeID, p.TenantID, p.KnowledgeBaseID).Take(&k).Error; err != nil {
		return nil, "", noop, err
	}
	if k.Channel != types.ConnectorTypeNextcloud {
		return raw, "", noop, nil
	}
	managed, err := NewNextcloudOnboardingStore(s.db).ManagedKB(ctx, k.TenantID, k.KnowledgeBaseID)
	if err != nil {
		return nil, "", noop, err
	}
	if !managed {
		return raw, "", noop, nil
	}
	var metadata struct {
		DataSourceID string `json:"datasource_id"`
		ExternalID   string `json:"external_id"`
	}
	if json.Unmarshal(k.Metadata, &metadata) != nil || metadata.DataSourceID == "" || metadata.ExternalID == "" {
		return nil, "", noop, ErrNextcloudMachineTaskInvalid
	}
	scope := NextcloudContentScope{
		TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID,
		KnowledgeID: k.ID, DataSourceID: metadata.DataSourceID, ExternalID: metadata.ExternalID,
	}
	leases := NewNextcloudContentLeaseStore(s.db)
	lease, err := leases.AcquireKnowledge(ctx, scope, NextcloudContentBuildLease,
		"nc-auto-recovery:"+uuid.NewString(), 2*time.Minute)
	if err != nil {
		return nil, "", noop, err
	}
	releaseLease := func() {
		release, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = leases.ReleaseLease(release, lease.ID)
	}
	current := WithNextcloudBuildLease(types.WithTaskAuthorization(types.WithBackgroundTask(ctx), p.TenantID,
		p.Initiator), leases, lease, scope)
	current, closeConfiguration, err := s.BeginNextcloudBuildConfigurationRead(current)
	if err != nil {
		releaseLease()
		return nil, "", noop, err
	}
	close := func() { closeConfiguration(); releaseLease() }
	if err := s.VerifyKnowledgeAutoTagTaskInput(current, p); err != nil {
		close()
		return nil, "", noop, err
	}
	current = WithKnowledgeBuildInputProof(current, s, p.OriginalInput)
	prepared, id, err := s.PrepareNextcloudMachineTask(current, types.TypeKnowledgeAutoTag, raw,
		"nc-auto:"+p.TaskInput.ID)
	if err != nil {
		close()
		return nil, "", noop, err
	}
	return prepared, id, close, nil
}
