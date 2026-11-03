package repository

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
)

// ErrDingTalkBindingConflict signals that a concurrent writer won the race
// for the natural key or the per-user active-binding slot. Callers treat it
// as a rejected login (audit binding_conflict), never as a server failure.
var ErrDingTalkBindingConflict = errors.New("dingtalk identity binding conflicts with an existing binding")

type dingTalkRepository struct {
	db *gorm.DB
}

// NewDingTalkRepository persists DingTalk SSO bindings and audit events.
func NewDingTalkRepository(db *gorm.DB) interfaces.DingTalkRepository {
	return &dingTalkRepository{db: db}
}

func (r *dingTalkRepository) FindActiveByDingTalkID(
	ctx context.Context,
	corpID, dingTalkUserID string,
) (*types.DingTalkIdentity, error) {
	var binding types.DingTalkIdentity
	err := r.db.WithContext(ctx).
		Where(
			"corp_id = ? AND dingtalk_user_id = ? AND status = ? AND user_id IS NOT NULL",
			corpID, dingTalkUserID, types.DingTalkIdentityActive,
		).
		First(&binding).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &binding, nil
}

func (r *dingTalkRepository) FindActiveByUserID(
	ctx context.Context,
	userID string,
) (*types.DingTalkIdentity, error) {
	if strings.TrimSpace(userID) == "" {
		return nil, nil
	}
	var binding types.DingTalkIdentity
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND status = ?", userID, types.DingTalkIdentityActive).
		First(&binding).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &binding, nil
}

// CreateBinding performs the check-then-insert sequence inside a single
// transaction. The unique constraints remain the authoritative guard: when
// two logins race to bind the same job number (or the same DingTalk user),
// exactly one insert succeeds and the loser receives
// ErrDingTalkBindingConflict instead of a duplicate row.
//
// The natural key (corp_id, dingtalk_user_id) spans ALL rows, including
// unlinked history, so re-binding a previously unlinked DingTalk user
// re-activates the existing row instead of inserting a second one.
func (r *dingTalkRepository) CreateBinding(ctx context.Context, binding *types.DingTalkIdentity) error {
	if binding == nil {
		return errors.New("dingtalk binding is required")
	}
	if binding.UserID == nil || strings.TrimSpace(*binding.UserID) == "" {
		return errors.New("dingtalk binding requires a linked user")
	}
	userID := strings.TrimSpace(*binding.UserID)
	now := time.Now().UTC()

	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Advisory per-user check inside the transaction; the partial unique
		// index on user_id still backstops it under concurrency.
		var userSlot types.DingTalkIdentity
		err := tx.Where(
			"user_id = ? AND status = ?",
			userID, types.DingTalkIdentityActive,
		).First(&userSlot).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			return ErrDingTalkBindingConflict
		}

		var existing types.DingTalkIdentity
		err = tx.Where(
			"corp_id = ? AND dingtalk_user_id = ?",
			binding.CorpID, binding.DingTalkUserID,
		).First(&existing).Error
		switch {
		case err != nil && !errors.Is(err, gorm.ErrRecordNotFound):
			return err
		case err == nil && existing.Status == types.DingTalkIdentityActive:
			// An active row for this DingTalk user exists. Same user means
			// our own concurrent attempt won (idempotent); any other user is
			// a conflict.
			if existing.UserID != nil && *existing.UserID == userID {
				binding.ID = existing.ID
				binding.BoundAt = existing.BoundAt
				binding.CreatedAt = existing.CreatedAt
				return nil
			}
			return ErrDingTalkBindingConflict
		case err == nil:
			// Re-activate the historical (unlinked) row.
			updates := map[string]any{
				"user_id":       userID,
				"union_id":      binding.UnionID,
				"job_number":    binding.JobNumber,
				"display_name":  binding.DisplayName,
				"status":        types.DingTalkIdentityActive,
				"bound_at":      now,
				"unbound_at":    nil,
				"last_login_at": nil,
				"updated_at":    now,
			}
			res := tx.Model(&types.DingTalkIdentity{}).Where("id = ?", existing.ID).Updates(updates)
			if res.Error != nil {
				if isDingTalkUniqueViolation(res.Error) {
					return ErrDingTalkBindingConflict
				}
				return res.Error
			}
			if res.RowsAffected == 0 {
				return ErrDingTalkBindingConflict
			}
			binding.ID = existing.ID
			binding.BoundAt = now
			binding.CreatedAt = existing.CreatedAt
			return nil
		}

		binding.Status = types.DingTalkIdentityActive
		binding.BoundAt = now
		binding.CreatedAt = now
		binding.UpdatedAt = now
		if err := tx.Create(binding).Error; err != nil {
			if isDingTalkUniqueViolation(err) {
				return ErrDingTalkBindingConflict
			}
			return err
		}
		return nil
	})
}

// UpdateSnapshot refreshes the DingTalk profile snapshot after a legitimate
// job number change. The linked user_id is deliberately not updatable here:
// re-pointing a binding at a different account must go through unbind+bind
// so the audit trail records both halves.
func (r *dingTalkRepository) UpdateSnapshot(
	ctx context.Context,
	id uint64,
	jobNumber, displayName, unionID string,
) error {
	res := r.db.WithContext(ctx).Model(&types.DingTalkIdentity{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"job_number":   jobNumber,
			"display_name": displayName,
			"union_id":     unionID,
			"updated_at":   time.Now().UTC(),
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// MarkUnlinked clears the user link so the partial unique index
// uq_dingtalk_identities_user stops covering the row and a future binding
// for the same user can be created.
func (r *dingTalkRepository) MarkUnlinked(ctx context.Context, id uint64) error {
	now := time.Now().UTC()
	res := r.db.WithContext(ctx).Model(&types.DingTalkIdentity{}).
		Where("id = ? AND status = ?", id, types.DingTalkIdentityActive).
		Updates(map[string]any{
			"user_id":    nil,
			"status":     types.DingTalkIdentityUnlinked,
			"unbound_at": now,
			"updated_at": now,
		})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *dingTalkRepository) TouchLastLogin(ctx context.Context, id uint64) error {
	now := time.Now().UTC()
	return r.db.WithContext(ctx).Model(&types.DingTalkIdentity{}).
		Where("id = ?", id).
		Updates(map[string]any{"last_login_at": now, "updated_at": now}).Error
}

func (r *dingTalkRepository) InsertEvent(ctx context.Context, event *types.DingTalkAuthEvent) error {
	if event == nil {
		return nil
	}
	event.CreatedAt = time.Now().UTC()
	return r.db.WithContext(ctx).Create(event).Error
}

// dingTalkSettingsID is the fixed primary key of the single-row settings
// table; the application layer guarantees only this row is ever written.
const dingTalkSettingsID = 1

// GetSettings loads the database-managed provider configuration row, or nil
// when no administrator has saved one yet (env baseline applies).
func (r *dingTalkRepository) GetSettings(ctx context.Context) (*types.DingTalkAuthSettings, error) {
	var settings types.DingTalkAuthSettings
	err := r.db.WithContext(ctx).
		Where("id = ?", dingTalkSettingsID).
		First(&settings).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &settings, nil
}

// SaveSettings upserts the single configuration row in one statement so two
// concurrent administrators cannot interleave a create-then-update race.
func (r *dingTalkRepository) SaveSettings(
	ctx context.Context,
	settings *types.DingTalkAuthSettings,
) error {
	if settings == nil {
		return errors.New("dingtalk settings are required")
	}
	settings.ID = dingTalkSettingsID
	now := time.Now().UTC()
	settings.UpdatedAt = now

	existing, err := r.GetSettings(ctx)
	if err != nil {
		return err
	}
	if existing == nil {
		settings.CreatedAt = now
		return r.db.WithContext(ctx).Create(settings).Error
	}
	settings.CreatedAt = existing.CreatedAt
	return r.db.WithContext(ctx).
		Model(&types.DingTalkAuthSettings{}).
		Where("id = ?", dingTalkSettingsID).
		Updates(map[string]any{
			"enabled":               settings.Enabled,
			"corp_id":               settings.CorpID,
			"app_key":               settings.AppKey,
			"agent_id":              settings.AgentID,
			"display_name":          settings.DisplayName,
			"redirect_url":          settings.RedirectURL,
			"app_secret_ciphertext": settings.AppSecretCiphertext,
			"config_version":        settings.ConfigVersion,
			"updated_by":            settings.UpdatedBy,
			"updated_at":            settings.UpdatedAt,
		}).Error
}

// FindADIdentitiesBySAMAccountName resolves the DingTalk job number against
// linked AD identities. The comparison is exact and case-sensitive on
// purpose: job numbers with leading zeros ("00123" vs "123") are different
// employees, so callers must not normalize the input.
func (r *dingTalkRepository) FindADIdentitiesBySAMAccountName(
	ctx context.Context,
	samAccountName string,
) ([]*types.DirectoryIdentity, error) {
	if samAccountName == "" {
		return []*types.DirectoryIdentity{}, nil
	}
	var identities []*types.DirectoryIdentity
	// Plain `=` keeps the match byte-exact on PostgreSQL and SQLite; the
	// value is bound as a parameter so no quoting can mangle leading zeros.
	// Deliberately NOT filtered on user_id: first-login provisioning (方案 A)
	// resolves unique ACTIVE-but-unlinked identities through the same query
	// and lets the service layer decide between loading the linked user and
	// provisioning the account.
	err := r.db.WithContext(ctx).
		Where("sam_account_name = ?", samAccountName).
		Order("directory_id ASC, id ASC").
		Find(&identities).Error
	if err != nil {
		return nil, err
	}
	return identities, nil
}

func isDingTalkUniqueViolation(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	return isDirectoryIdentityUniqueViolation(err)
}
