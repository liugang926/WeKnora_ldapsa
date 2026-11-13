package repository

import (
	"context"
	"errors"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var (
	// ErrDingTalkScheduleAuthorizationRequired identifies a schedule awaiting an explicit authorized save.
	ErrDingTalkScheduleAuthorizationRequired = errors.New("dingtalk_schedule_authorization_required")
	// ErrDingTalkScheduleAuthorizationRevoked identifies a schedule whose saved actor no longer has access.
	ErrDingTalkScheduleAuthorizationRevoked = errors.New("dingtalk_schedule_authorization_revoked")
)

func isDingTalkScheduleMessage(message string) bool {
	return message == ErrDingTalkScheduleAuthorizationRequired.Error() ||
		message == ErrDingTalkScheduleAuthorizationRevoked.Error()
}

type (
	dingTalkScheduleEditKey   struct{}
	dingTalkScheduledActorKey struct{}
	dingTalkScheduledActor    struct {
		store      *KnowledgeInputProvenanceStore
		grant      dingTalkScheduleAuthorization
		run, token string
	}
)

type dingTalkScheduleAuthorization struct {
	DataSourceID              string `gorm:"primaryKey"`
	TenantID                  uint64
	KnowledgeBaseID           string
	SourceRevision            uint64
	Schedule, GrantID, UserID string
}

func (dingTalkScheduleAuthorization) TableName() string { return "dingtalk_schedule_authorizations" }

// WithDingTalkScheduleEdit marks an explicit source save/resume after HTTP edit authorization.
func WithDingTalkScheduleEdit(ctx context.Context) context.Context {
	return context.WithValue(ctx, dingTalkScheduleEditKey{}, true)
}

func saveDingTalkScheduleAuthorization(ctx context.Context, tx *gorm.DB, ds *types.DataSource,
	factory KnowledgeDisplayAuthorityFactory,
) error {
	if ctx.Value(dingTalkScheduleEditKey{}) != true || ds.Type != types.ConnectorTypeDingTalk {
		return nil
	}
	if err := tx.Where("data_source_id = ?", ds.ID).Delete(&dingTalkScheduleAuthorization{}).Error; err != nil {
		return err
	}
	principal, _ := types.PrincipalFromContext(ctx)
	if ds.SyncSchedule == "" || ds.Status == types.DataSourceStatusPaused ||
		principal.Type != types.PrincipalWebUser || types.IsBackgroundTask(ctx) {
		return nil
	}
	actor, err := captureKnowledgeActorSelector(ctx, "")
	if err != nil {
		return err
	}
	if actor.CallerTenantID != ds.TenantID || factory == nil {
		return types.ErrSourceLineageUnknown
	}
	authority := factory(tx)
	if authority == nil || authority.db != tx {
		return types.ErrSourceLineageUnknown
	}
	kb := &types.KnowledgeBase{ID: ds.KnowledgeBaseID, TenantID: ds.TenantID}
	if _, err := authority.AuthorizeCurrentKnowledgeInputWrite(ctx, kb); err != nil {
		return err
	}
	q := tx.Table("dingtalk_source_input_revisions").Where("datasource_id = ?", ds.ID)
	var source struct{ Revision uint64 }
	if err := q.Take(&source).Error; err != nil {
		return err
	}
	return tx.Create(&dingTalkScheduleAuthorization{
		DataSourceID: ds.ID, TenantID: ds.TenantID, KnowledgeBaseID: ds.KnowledgeBaseID,
		SourceRevision: source.Revision, Schedule: ds.SyncSchedule, GrantID: uuid.NewString(), UserID: actor.UserID,
	}).Error
}

// Only an explicit schedule edit records a selector. Old audits, manual runs and source creator IDs are not grants.
func findDingTalkScheduleAuthorization(
	tx *gorm.DB, source string, tenant uint64,
) (dingTalkScheduleAuthorization, error) {
	var grant dingTalkScheduleAuthorization
	err := tx.Raw(`SELECT a.* FROM dingtalk_schedule_authorizations a
        JOIN data_sources d ON d.id = a.data_source_id AND d.tenant_id = a.tenant_id
          AND d.knowledge_base_id = a.knowledge_base_id AND d.sync_schedule = a.schedule
        JOIN dingtalk_source_input_revisions r ON r.datasource_id = d.id AND r.revision = a.source_revision
        WHERE d.id = ? AND d.tenant_id = ? AND d.deleted_at IS NULL AND d.type = ?
          AND d.status IN (?, ?) AND COALESCE(d.sync_enabled, TRUE) AND d.sync_schedule <> ''`,
		source, tenant, types.ConnectorTypeDingTalk, types.DataSourceStatusActive, types.DataSourceStatusError,
	).Scan(&grant).Error
	return grant, err
}

// RestoreDingTalkScheduledActor resolves the server-recorded grant of the exact claimed scheduled run.
func (s *KnowledgeInputProvenanceStore) RestoreDingTalkScheduledActor(ctx context.Context) (context.Context, error) {
	claim, ok := ctx.Value(dingTalkExecutionKey{}).(dingTalkRunClaim)
	if !ok {
		return ctx, ErrDingTalkRunExpired
	}
	var fresh context.Context
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var log types.SyncLog
		if err := tx.Where("id = ? AND data_source_id = ? AND tenant_id = ?",
			claim.SyncLogID, claim.DataSourceID, claim.TenantID).Take(&log).Error; err != nil {
			return err
		}
		if log.QueueTaskID != claim.TaskID || log.Status != types.SyncLogStatusRunning ||
			claim.TaskID != log.ScheduledMinuteTaskID() {
			return ErrDingTalkRunExpired
		}
		if log.RecoveryTrigger != "schedule" || log.DingTalkScheduleGrant == "" {
			return ErrDingTalkScheduleAuthorizationRequired
		}
		f := dingTalkFetch{
			Tenant: claim.TenantID, KB: claim.KnowledgeBaseID, Source: claim.DataSourceID,
			Run: claim.SyncLogID, Revision: claim.SourceRevision, ExecutionToken: claim.Token,
		}
		if err := checkDingTalkExecution(tx, f); err != nil {
			return err
		}
		grant, err := findDingTalkScheduleAuthorization(tx, claim.DataSourceID, claim.TenantID)
		if err != nil {
			return err
		}
		if grant.GrantID != log.DingTalkScheduleGrant || grant.KnowledgeBaseID != claim.KnowledgeBaseID ||
			grant.SourceRevision != claim.SourceRevision {
			return ErrDingTalkScheduleAuthorizationRequired
		}
		actor := dingTalkScheduledActor{store: s, grant: grant, run: claim.SyncLogID, token: claim.Token}
		fresh, err = actor.authorize(ctx, tx)
		if err == nil {
			fresh = context.WithValue(fresh, dingTalkScheduledActorKey{}, actor)
		}
		return err
	})
	if err != nil {
		return ctx, err
	}
	return fresh, nil
}

func (a dingTalkScheduledActor) authorize(ctx context.Context, tx *gorm.DB) (context.Context, error) {
	ctx = types.WithTaskAuthorization(ctx, a.grant.TenantID,
		types.TaskInitiator{CallerTenantID: a.grant.TenantID, UserID: a.grant.UserID})
	kb := &types.KnowledgeBase{ID: a.grant.KnowledgeBaseID, TenantID: a.grant.TenantID}
	fresh, err := a.store.templateAuthority(tx).AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, access.ErrForbidden) ||
		errors.Is(err, access.ErrNotFound) || errors.Is(err, ErrKnowledgeInputChanged) {
		return ctx, ErrDingTalkScheduleAuthorizationRevoked
	}
	return fresh, err
}

func checkDingTalkScheduledActor(tx *gorm.DB, f dingTalkFetch, log *types.SyncLog) error {
	if log.RecoveryTrigger != "schedule" {
		return nil
	}
	ctx := tx.Statement.Context
	actor, ok := ctx.Value(dingTalkScheduledActorKey{}).(dingTalkScheduledActor)
	if !ok || actor.token != f.ExecutionToken || actor.run != log.ID ||
		actor.grant.GrantID != log.DingTalkScheduleGrant {
		return ErrDingTalkScheduleAuthorizationRequired
	}
	grant, err := findDingTalkScheduleAuthorization(tx, f.Source, f.Tenant)
	if err != nil {
		return err
	}
	if grant != actor.grant {
		return ErrDingTalkScheduleAuthorizationRequired
	}
	_, err = actor.authorize(ctx, tx)
	return err
}
