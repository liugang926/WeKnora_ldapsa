package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// DingTalkScheduleAuthorizationStatus checks both the viewing editor and the
// grant's recorded editor. It never confers or renews a scheduled identity.
func (s *KnowledgeInputProvenanceStore) DingTalkScheduleAuthorizationStatus(
	ctx context.Context, tenant uint64, id string,
) (string, error) {
	principal, _ := types.PrincipalFromContext(ctx)
	if principal.Type != types.PrincipalWebUser || types.IsBackgroundTask(ctx) ||
		types.CallerFromContext(ctx).TenantID != tenant {
		return "", access.ErrForbidden
	}
	var status string
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ds types.DataSource
		if err := tx.Select("id", "tenant_id", "knowledge_base_id").
			Where("id = ? AND tenant_id = ? AND type = ?", id, tenant, types.ConnectorTypeDingTalk).
			Take(&ds).Error; err != nil {
			return err
		}
		kb := &types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID}
		if _, err := s.templateAuthority(tx).AuthorizeCurrentKnowledgeInputWrite(ctx, kb); err != nil {
			return err
		}
		grant, err := findDingTalkScheduleAuthorization(tx, id, tenant)
		if err != nil {
			return err
		}
		if grant.GrantID == "" {
			status = "required"
			return nil
		}
		_, err = (dingTalkScheduledActor{store: s, grant: grant}).authorize(ctx, tx)
		if errors.Is(err, ErrDingTalkScheduleAuthorizationRevoked) {
			status = "revoked"
			return nil
		}
		if err != nil {
			return err
		}
		status = "authorized"
		return nil
	})
	return status, err
}

// ConsecutiveFailures lets a scheduled run use a replacement grant at its next
// normal tick, even for sources saved before authorization errors were cleared.
// A real fetch error or a failure under the current grant retains its backoff.
func (r ConnectorScheduleLogs) ConsecutiveFailures(ctx context.Context, source string) (int, time.Time, error) {
	failures, last, err := r.SyncLogRepository.ConsecutiveFailures(ctx, source)
	if err != nil || failures < 2 || r.ScheduleAuthority == nil {
		return failures, last, err
	}
	log, err := r.SyncLogRepository.FindLatest(ctx, source)
	if err != nil || log == nil || log.Status != types.SyncLogStatusFailed ||
		log.RecoveryTrigger != "schedule" || log.QueueTaskID != log.ScheduledMinuteTaskID() ||
		!isDingTalkScheduleMessage(log.ErrorMessage) {
		return failures, last, err
	}
	grant, err := findDingTalkScheduleAuthorization(r.DB.WithContext(ctx), source, log.TenantID)
	if err != nil || grant.GrantID == "" || grant.GrantID == log.DingTalkScheduleGrant {
		return failures, last, err
	}
	_, err = (dingTalkScheduledActor{store: r.ScheduleAuthority, grant: grant}).authorize(ctx, r.DB)
	if errors.Is(err, ErrDingTalkScheduleAuthorizationRevoked) {
		return failures, last, nil
	}
	if err != nil {
		return failures, last, err
	}
	return 0, time.Time{}, nil
}
