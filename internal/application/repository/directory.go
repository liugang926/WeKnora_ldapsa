package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mattn/go-sqlite3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type directoryRepository struct {
	db *gorm.DB
}

var ErrDirectoryConfigVersionChanged = errors.New("directory configuration changed during synchronization")
var ErrDirectoryIdentityLinkConflict = errors.New("directory identity link conflicts with an existing link")

func NewDirectoryRepository(db *gorm.DB) interfaces.DirectoryRepository {
	return &directoryRepository{db: db}
}

func (r *directoryRepository) Create(ctx context.Context, directory *types.Directory) error {
	if directory.ID == "" {
		directory.ID = uuid.NewString()
	}
	return r.db.WithContext(ctx).Create(directory).Error
}

func (r *directoryRepository) Update(ctx context.Context, directory *types.Directory) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current types.Directory
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", directory.ID).First(&current).Error; err != nil {
			return err
		}
		tenantIDs, err := directoryAffectedTenantIDs(tx, directory.ID)
		if err != nil {
			return err
		}
		res := tx.Model(&types.Directory{}).
			Where("id = ?", directory.ID).
			Select(
				"name", "protocol", "enabled", "config_source", "tls_mode", "server_urls", "server_names",
				"base_dn", "user_base_dn", "group_base_dn", "user_filter", "group_filter", "allowed_login_filter",
				"service_account_dn", "password_ciphertext", "enterprise_ca_pem",
				"security_config_fingerprint",
				"connect_timeout_seconds", "query_timeout_seconds", "page_size", "result_limit",
				"sync_interval_seconds", "stale_after_seconds", "updated_at",
			).Updates(directory)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		if err := tx.Model(&types.Directory{}).Where("id = ?", directory.ID).
			UpdateColumn("config_version", gorm.Expr("config_version + 1")).Error; err != nil {
			return err
		}
		directory.ConfigVersion = current.ConfigVersion + 1
		if directorySessionSecurityChanged(&current, directory) {
			if err := revokeDirectoryTokens(tx, directory.ID, false); err != nil {
				return err
			}
		}
		if directoryRequiresFreshSnapshot(&current, directory) {
			// Preserve the last complete snapshot for the configured grace period,
			// but block new LDAP logins and make the scheduler run immediately.
			// A later successful atomic snapshot clears this marker.
			if err := tx.Model(&types.Directory{}).Where("id = ?", directory.ID).Updates(map[string]any{
				"last_sync_attempt_at": nil,
				"last_sync_error":      "configuration changed; synchronization required",
			}).Error; err != nil {
				return err
			}
		}
		for _, tenantID := range tenantIDs {
			if _, err := bumpPermissionVersion(tx, tenantID); err != nil {
				return err
			}
		}
		return nil
	})
}

func directorySessionSecurityChanged(current, next *types.Directory) bool {
	if current == nil || next == nil {
		return current != next
	}
	return current.Enabled != next.Enabled || current.Protocol != next.Protocol || current.TLSMode != next.TLSMode ||
		strings.Join(current.ServerURLs, "\x00") != strings.Join(next.ServerURLs, "\x00") ||
		strings.Join(current.ServerNames, "\x00") != strings.Join(next.ServerNames, "\x00") ||
		current.BaseDN != next.BaseDN || current.UserBaseDN != next.UserBaseDN || current.GroupBaseDN != next.GroupBaseDN ||
		current.UserFilter != next.UserFilter || current.GroupFilter != next.GroupFilter ||
		current.AllowedLoginFilter != next.AllowedLoginFilter || current.ServiceAccountDN != next.ServiceAccountDN ||
		current.PasswordCiphertext != next.PasswordCiphertext || current.EnterpriseCAPEM != next.EnterpriseCAPEM ||
		current.SecurityConfigFingerprint != next.SecurityConfigFingerprint ||
		current.PageSize != next.PageSize || current.ResultLimit != next.ResultLimit
}

// revokeDirectoryTokens is deliberately part of the same transaction as the
// directory state change. This closes the cross-pod window where a config or
// identity update commits but best-effort application-level revocation fails.
func revokeDirectoryTokens(tx *gorm.DB, directoryID string, onlyInactive bool) error {
	linkedUsers := tx.Model(&types.DirectoryIdentity{}).
		Select("user_id").
		Where("directory_id = ? AND user_id IS NOT NULL AND user_id <> ''", directoryID)
	if onlyInactive {
		linkedUsers = linkedUsers.Where("status <> ?", types.DirectoryObjectActive)
	}
	return tx.Model(&types.AuthToken{}).
		Where("user_id IN (?) AND is_revoked = ?", linkedUsers, false).
		Updates(map[string]any{"is_revoked": true, "updated_at": time.Now().UTC()}).Error
}

func directoryRequiresFreshSnapshot(current, next *types.Directory) bool {
	if current == nil || next == nil || !next.Enabled {
		return false
	}
	return !current.Enabled || current.Protocol != next.Protocol || current.TLSMode != next.TLSMode ||
		strings.Join(current.ServerURLs, "\x00") != strings.Join(next.ServerURLs, "\x00") ||
		strings.Join(current.ServerNames, "\x00") != strings.Join(next.ServerNames, "\x00") ||
		current.BaseDN != next.BaseDN || current.UserBaseDN != next.UserBaseDN || current.GroupBaseDN != next.GroupBaseDN ||
		current.UserFilter != next.UserFilter || current.GroupFilter != next.GroupFilter ||
		current.AllowedLoginFilter != next.AllowedLoginFilter || current.ServiceAccountDN != next.ServiceAccountDN ||
		current.PasswordCiphertext != next.PasswordCiphertext || current.EnterpriseCAPEM != next.EnterpriseCAPEM ||
		current.SecurityConfigFingerprint != next.SecurityConfigFingerprint ||
		current.PageSize != next.PageSize || current.ResultLimit != next.ResultLimit
}

func (r *directoryRepository) Get(ctx context.Context, id string) (*types.Directory, error) {
	var directory types.Directory
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&directory).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &directory, nil
}

func (r *directoryRepository) List(ctx context.Context) ([]*types.Directory, error) {
	var rows []*types.Directory
	if err := r.db.WithContext(ctx).Order("name ASC, id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *directoryRepository) Delete(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		tenantIDs, err := directoryAffectedTenantIDs(tx, id)
		if err != nil {
			return err
		}
		res := tx.Where("id = ?", id).Delete(&types.Directory{})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		for _, tenantID := range tenantIDs {
			if _, err := bumpPermissionVersion(tx, tenantID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *directoryRepository) GetIdentity(ctx context.Context, id string) (*types.DirectoryIdentity, error) {
	var identity types.DirectoryIdentity
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&identity).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &identity, nil
}

func (r *directoryRepository) GetIdentityByObjectGUID(ctx context.Context, directoryID, objectGUID string) (*types.DirectoryIdentity, error) {
	var identity types.DirectoryIdentity
	if err := r.db.WithContext(ctx).
		Where("directory_id = ? AND object_guid = ?", directoryID, objectGUID).
		First(&identity).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &identity, nil
}

// GetLoginSnapshot holds a shared lock on the directory version while loading
// the identity and its effective groups. ApplySnapshot takes an update lock on
// the same row, so PostgreSQL cannot expose a mixture of two complete
// snapshots; SQLite's read transaction provides the equivalent stable view.
func (r *directoryRepository) GetLoginSnapshot(
	ctx context.Context,
	directoryID, objectGUID string,
) (*types.DirectoryLoginSnapshot, error) {
	var result *types.DirectoryLoginSnapshot
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var directory types.Directory
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ?", directoryID).First(&directory).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				result = nil
				return nil
			}
			return err
		}

		var identity types.DirectoryIdentity
		err := tx.Where(
			"directory_id = ? AND object_guid = ? AND snapshot_version = ? AND status = ?",
			directory.ID, objectGUID, directory.SnapshotVersion, types.DirectoryObjectActive,
		).First(&identity).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			result = &types.DirectoryLoginSnapshot{Directory: &directory}
			return nil
		}
		if err != nil {
			return err
		}

		var rows []struct{ ObjectGUID string }
		if err := tx.Table("directory_group_memberships AS gm").
			Select("DISTINCT g.object_guid AS object_guid").
			Joins("JOIN directory_groups AS g ON g.id = gm.group_id AND g.directory_id = gm.directory_id").
			Where(
				"gm.directory_id = ? AND gm.identity_id = ? AND gm.snapshot_version = ? AND g.snapshot_version = ? AND g.status = ?",
				directory.ID, identity.ID, directory.SnapshotVersion, directory.SnapshotVersion, types.DirectoryObjectActive,
			).
			Order("g.object_guid ASC").Scan(&rows).Error; err != nil {
			return err
		}
		groupGUIDs := make([]string, 0, len(rows))
		for _, row := range rows {
			if strings.TrimSpace(row.ObjectGUID) == "" {
				return fmt.Errorf("directory login snapshot contains an empty group objectGUID")
			}
			groupGUIDs = append(groupGUIDs, row.ObjectGUID)
		}
		result = &types.DirectoryLoginSnapshot{
			Directory: &directory, Identity: &identity, EffectiveGroupObjectGUIDs: groupGUIDs,
		}
		return nil
	})
	return result, err
}

func (r *directoryRepository) GetIdentityByUserID(ctx context.Context, userID string) ([]*types.DirectoryIdentity, error) {
	var identities []*types.DirectoryIdentity
	if err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("directory_id ASC, id ASC").Find(&identities).Error; err != nil {
		return nil, err
	}
	return identities, nil
}

func (r *directoryRepository) LinkIdentity(ctx context.Context, identityID, userID string) error {
	// Read immutable routing data before the write transaction. Keeping the
	// transaction write-first avoids SQLite's deferred read-to-write upgrade
	// race while the conditional UPDATE provides the same no-overwrite guard
	// as a row lock on PostgreSQL.
	var target types.DirectoryIdentity
	if err := r.db.WithContext(ctx).Where("id = ?", identityID).First(&target).Error; err != nil {
		return err
	}
	if target.UserID != nil && strings.TrimSpace(*target.UserID) != "" {
		if *target.UserID == userID {
			return nil
		}
		return ErrDirectoryIdentityLinkConflict
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		update := tx.Model(&types.DirectoryIdentity{}).
			Where("id = ? AND (user_id IS NULL OR user_id = '')", identityID).
			Updates(map[string]any{"user_id": userID, "updated_at": time.Now().UTC()})
		if update.Error != nil {
			if isDirectoryIdentityUniqueViolation(update.Error) {
				return ErrDirectoryIdentityLinkConflict
			}
			return update.Error
		}
		if update.RowsAffected != 1 {
			var current types.DirectoryIdentity
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", identityID).First(&current).Error; err != nil {
				return err
			}
			if current.UserID != nil && *current.UserID == userID {
				return nil
			}
			return ErrDirectoryIdentityLinkConflict
		}
		tenantIDs, err := directoryAffectedTenantIDs(tx, target.DirectoryID)
		if err != nil {
			return err
		}
		for _, tenantID := range tenantIDs {
			if _, err := bumpPermissionVersion(tx, tenantID); err != nil {
				return err
			}
		}
		return nil
	})
}

func isDirectoryIdentityUniqueViolation(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return true
	}
	var sqliteErr sqlite3.Error
	return errors.As(err, &sqliteErr) &&
		(sqliteErr.ExtendedCode == sqlite3.ErrConstraintUnique || sqliteErr.ExtendedCode == sqlite3.ErrConstraintPrimaryKey)
}

func (r *directoryRepository) UnlinkIdentity(ctx context.Context, identityID string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var identity types.DirectoryIdentity
		if err := tx.Where("id = ?", identityID).First(&identity).Error; err != nil {
			return err
		}
		res := tx.Model(&types.DirectoryIdentity{}).Where("id = ?", identityID).
			Updates(map[string]any{"user_id": nil, "updated_at": time.Now().UTC()})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		tenantIDs, err := directoryAffectedTenantIDs(tx, identity.DirectoryID)
		if err != nil {
			return err
		}
		for _, tenantID := range tenantIDs {
			if _, err := bumpPermissionVersion(tx, tenantID); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *directoryRepository) ListIdentities(ctx context.Context, directoryID string, offset, limit int) ([]*types.DirectoryIdentity, error) {
	var rows []*types.DirectoryIdentity
	if err := r.db.WithContext(ctx).Where("directory_id = ?", directoryID).
		Order("display_name ASC, object_guid ASC").Offset(offset).Limit(normalizeDirectoryLimit(limit)).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *directoryRepository) ListGroups(ctx context.Context, directoryID, query string, offset, limit int) ([]*types.DirectoryGroup, error) {
	var rows []*types.DirectoryGroup
	db := r.db.WithContext(ctx).Where("directory_id = ?", directoryID)
	if query = strings.TrimSpace(query); query != "" {
		like := "%" + escapeLikePattern(query) + "%"
		db = db.Where("LOWER(display_name) LIKE LOWER(?) OR LOWER(dn) LIKE LOWER(?)", like, like)
	}
	if err := db.Order("display_name ASC, object_guid ASC").Offset(offset).
		Limit(normalizeDirectoryLimit(limit)).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func normalizeDirectoryLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func (r *directoryRepository) ListGroupEdges(ctx context.Context, directoryID string) ([]*types.DirectoryGroupEdge, error) {
	var rows []*types.DirectoryGroupEdge
	if err := r.db.WithContext(ctx).Where("directory_id = ?", directoryID).
		Order("parent_group_id ASC, child_group_id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *directoryRepository) ListGroupMemberships(ctx context.Context, groupID string) ([]*types.DirectoryGroupMembership, error) {
	var rows []*types.DirectoryGroupMembership
	if err := r.db.WithContext(ctx).Where("group_id = ?", groupID).
		Order("identity_id ASC, depth ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *directoryRepository) ListDirectoryMemberships(ctx context.Context, directoryID string) ([]*types.DirectoryGroupMembership, error) {
	var rows []*types.DirectoryGroupMembership
	if err := r.db.WithContext(ctx).Where("directory_id = ?", directoryID).
		Order("group_id ASC, identity_id ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func syncLeaseExpiryExpression(db *gorm.DB, ttl time.Duration) clause.Expr {
	seconds := int64(ttl / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	if db.Dialector.Name() == "postgres" {
		return gorm.Expr(fmt.Sprintf("CURRENT_TIMESTAMP + INTERVAL '%d seconds'", seconds))
	}
	if db.Dialector.Name() == "sqlite" {
		return gorm.Expr("datetime(CURRENT_TIMESTAMP, ?)", fmt.Sprintf("+%d seconds", seconds))
	}
	return gorm.Expr("?", time.Now().UTC().Add(time.Duration(seconds)*time.Second))
}

func (r *directoryRepository) TryAcquireSyncLease(
	ctx context.Context,
	directoryID, owner string,
	ttl time.Duration,
) (bool, error) {
	if strings.TrimSpace(directoryID) == "" || strings.TrimSpace(owner) == "" || ttl <= 0 {
		return false, errors.New("directory sync lease requires directory, owner, and positive ttl")
	}
	result := r.db.WithContext(ctx).Model(&types.Directory{}).
		Where("id = ?", directoryID).
		Where("sync_lease_owner = '' OR sync_lease_owner IS NULL OR sync_lease_expires_at IS NULL OR sync_lease_expires_at <= CURRENT_TIMESTAMP").
		Updates(map[string]any{
			"sync_lease_owner":      owner,
			"sync_lease_expires_at": syncLeaseExpiryExpression(r.db, ttl),
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

func (r *directoryRepository) RenewSyncLease(
	ctx context.Context,
	directoryID, owner string,
	ttl time.Duration,
) (bool, error) {
	if strings.TrimSpace(directoryID) == "" || strings.TrimSpace(owner) == "" || ttl <= 0 {
		return false, errors.New("directory sync lease requires directory, owner, and positive ttl")
	}
	result := r.db.WithContext(ctx).Model(&types.Directory{}).
		Where("id = ? AND sync_lease_owner = ?", directoryID, owner).
		Update("sync_lease_expires_at", syncLeaseExpiryExpression(r.db, ttl))
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

func (r *directoryRepository) ReleaseSyncLease(ctx context.Context, directoryID, owner string) error {
	if strings.TrimSpace(directoryID) == "" || strings.TrimSpace(owner) == "" {
		return nil
	}
	return r.db.WithContext(ctx).Model(&types.Directory{}).
		Where("id = ? AND sync_lease_owner = ?", directoryID, owner).
		Updates(map[string]any{"sync_lease_owner": "", "sync_lease_expires_at": nil}).Error
}

func (r *directoryRepository) ApplySnapshot(ctx context.Context, snapshot *types.DirectorySnapshot) (*types.DirectorySnapshotResult, error) {
	var result *types.DirectorySnapshotResult
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var directory types.Directory
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", snapshot.DirectoryID).First(&directory).Error; err != nil {
			return err
		}
		if snapshot.ExpectedConfigVersion != 0 && directory.ConfigVersion != snapshot.ExpectedConfigVersion {
			return fmt.Errorf("%w: expected %d, current %d", ErrDirectoryConfigVersionChanged, snapshot.ExpectedConfigVersion, directory.ConfigVersion)
		}
		now := time.Now().UTC()
		version := directory.SnapshotVersion + 1

		// Mark first, then reactivate objects present in the complete snapshot.
		// Any failure rolls the transaction back, preserving the prior snapshot.
		if err := tx.Model(&types.DirectoryIdentity{}).Where("directory_id = ?", directory.ID).
			Updates(map[string]any{"status": types.DirectoryObjectOutOfScope, "disabled_reason": "not present in latest complete snapshot", "updated_at": now}).Error; err != nil {
			return err
		}
		for _, input := range snapshot.Identities {
			status := types.DirectoryObjectActive
			disabledReason := ""
			if !input.Enabled {
				status = types.DirectoryObjectDisabled
				disabledReason = "disabled in directory"
			}
			identity := &types.DirectoryIdentity{
				ID: uuid.NewString(), DirectoryID: directory.ID, ObjectGUID: input.ObjectGUID,
				ObjectSID: input.ObjectSID, DN: input.DN, SAMAccountName: input.SAMAccountName,
				UPN: input.UPN, DisplayName: input.DisplayName, Email: input.Email,
				PrimaryGroupSID: input.PrimaryGroupSID, Status: status, DisabledReason: disabledReason,
				SnapshotVersion: version, LastSeenAt: now,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "directory_id"}, {Name: "object_guid"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"object_sid", "dn", "sam_account_name", "upn", "display_name", "email",
					"primary_group_sid", "status", "disabled_reason", "snapshot_version", "last_seen_at", "updated_at",
				}),
			}).Create(identity).Error; err != nil {
				return err
			}
		}

		if err := tx.Model(&types.DirectoryGroup{}).Where("directory_id = ?", directory.ID).
			Updates(map[string]any{"status": types.DirectoryObjectOutOfScope, "updated_at": now}).Error; err != nil {
			return err
		}
		for _, input := range snapshot.Groups {
			group := &types.DirectoryGroup{
				ID: uuid.NewString(), DirectoryID: directory.ID, ObjectGUID: input.ObjectGUID,
				ObjectSID: input.ObjectSID, DN: input.DN, SAMAccountName: input.SAMAccountName,
				DisplayName: input.DisplayName, Email: input.Email,
				Status: types.DirectoryObjectActive, SnapshotVersion: version, LastSeenAt: now,
			}
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "directory_id"}, {Name: "object_guid"}},
				DoUpdates: clause.AssignmentColumns([]string{
					"object_sid", "dn", "sam_account_name", "display_name", "email", "status",
					"snapshot_version", "last_seen_at", "updated_at",
				}),
			}).Create(group).Error; err != nil {
				return err
			}
		}

		var identities []types.DirectoryIdentity
		if err := tx.Where("directory_id = ? AND snapshot_version = ?", directory.ID, version).
			Find(&identities).Error; err != nil {
			return err
		}
		identityByGUID := make(map[string]string, len(identities))
		for _, identity := range identities {
			identityByGUID[identity.ObjectGUID] = identity.ID
		}
		var groups []types.DirectoryGroup
		if err := tx.Where("directory_id = ? AND snapshot_version = ?", directory.ID, version).
			Find(&groups).Error; err != nil {
			return err
		}
		groupByGUID := make(map[string]string, len(groups))
		for _, group := range groups {
			groupByGUID[group.ObjectGUID] = group.ID
		}

		if err := tx.Where("directory_id = ?", directory.ID).Delete(&types.DirectoryGroupEdge{}).Error; err != nil {
			return err
		}
		if err := tx.Where("directory_id = ?", directory.ID).Delete(&types.DirectoryGroupMembership{}).Error; err != nil {
			return err
		}
		edges := make([]types.DirectoryGroupEdge, 0, len(snapshot.GroupEdges))
		for _, input := range snapshot.GroupEdges {
			edges = append(edges, types.DirectoryGroupEdge{
				DirectoryID: directory.ID, ParentGroupID: groupByGUID[input.ParentGroupObjectGUID],
				ChildGroupID: groupByGUID[input.ChildGroupObjectGUID], SnapshotVersion: version, CreatedAt: now,
			})
		}
		if len(edges) > 0 {
			if err := tx.CreateInBatches(edges, 500).Error; err != nil {
				return err
			}
		}
		memberships := make([]types.DirectoryGroupMembership, 0, len(snapshot.Memberships))
		for _, input := range snapshot.Memberships {
			memberships = append(memberships, types.DirectoryGroupMembership{
				DirectoryID: directory.ID, GroupID: groupByGUID[input.GroupObjectGUID],
				IdentityID: identityByGUID[input.UserObjectGUID], Direct: input.Direct,
				Primary: input.Primary, Depth: input.Depth, Source: input.Source,
				SnapshotVersion: version, CreatedAt: now,
			})
		}
		if len(memberships) > 0 {
			if err := tx.CreateInBatches(memberships, 500).Error; err != nil {
				return err
			}
		}

		if err := revokeDirectoryTokens(tx, directory.ID, true); err != nil {
			return err
		}

		if err := tx.Model(&types.Directory{}).Where("id = ?", directory.ID).Updates(map[string]any{
			"snapshot_version": version, "last_successful_sync_at": now, "last_sync_attempt_at": now,
			"last_sync_error": "", "updated_at": now,
		}).Error; err != nil {
			return err
		}
		completed := now
		run := &types.DirectorySyncRun{
			ID: uuid.NewString(), DirectoryID: directory.ID, Status: types.DirectorySyncSuccess,
			Trigger:         snapshot.Trigger,
			SnapshotVersion: version, UserCount: len(snapshot.Identities), GroupCount: len(snapshot.Groups),
			MembershipCount: len(snapshot.Memberships), StartedAt: snapshot.StartedAt,
			CompletedAt: &completed, CreatedAt: now,
		}
		if run.StartedAt.IsZero() {
			run.StartedAt = now
		}
		if run.Trigger == "" {
			run.Trigger = types.DirectorySyncTriggerManual
		}
		if err := tx.Create(run).Error; err != nil {
			return err
		}

		tenantIDs, err := directoryAffectedTenantIDs(tx, directory.ID)
		if err != nil {
			return err
		}
		versions := make(map[uint64]uint64, len(tenantIDs))
		for _, tenantID := range tenantIDs {
			permissionVersion, err := bumpPermissionVersion(tx, tenantID)
			if err != nil {
				return err
			}
			versions[tenantID] = permissionVersion
		}
		result = &types.DirectorySnapshotResult{SyncRun: run, AffectedTenants: versions}
		return nil
	})
	return result, err
}

func directoryAffectedTenantIDs(tx *gorm.DB, directoryID string) ([]uint64, error) {
	var rows []struct{ TenantID uint64 }
	err := tx.Raw(`
		SELECT DISTINCT tenant_id FROM tenant_group_role_grants
		 WHERE directory_group_id IN (SELECT id FROM directory_groups WHERE directory_id = ?)
		UNION
		SELECT DISTINCT tenant_id FROM resource_group_grants
		 WHERE directory_group_id IN (SELECT id FROM directory_groups WHERE directory_id = ?)
	`, directoryID, directoryID).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.TenantID)
	}
	return ids, nil
}

func bumpPermissionVersion(tx *gorm.DB, tenantID uint64) (uint64, error) {
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}},
		DoNothing: true,
	}).Create(&types.PermissionVersion{TenantID: tenantID, Version: 0}).Error; err != nil {
		return 0, err
	}
	now := time.Now().UTC()
	if err := tx.Model(&types.PermissionVersion{}).Where("tenant_id = ?", tenantID).
		Updates(map[string]any{"version": gorm.Expr("version + 1"), "updated_at": now}).Error; err != nil {
		return 0, err
	}
	var row types.PermissionVersion
	if err := tx.Where("tenant_id = ?", tenantID).First(&row).Error; err != nil {
		return 0, err
	}
	return row.Version, nil
}

func (r *directoryRepository) RecordSyncFailure(ctx context.Context, directoryID, code, message string, startedAt time.Time, expectedSnapshotVersion, expectedConfigVersion uint64, trigger types.DirectorySyncTrigger) (*types.DirectorySyncRun, error) {
	now := time.Now().UTC()
	if startedAt.IsZero() {
		startedAt = now
	}
	completed := now
	if trigger != types.DirectorySyncTriggerScheduled && trigger != types.DirectorySyncTriggerLogin {
		trigger = types.DirectorySyncTriggerManual
	}
	run := &types.DirectorySyncRun{
		ID: uuid.NewString(), DirectoryID: directoryID, Status: types.DirectorySyncFailed,
		Trigger: trigger, SnapshotVersion: expectedSnapshotVersion,
		ErrorCode: code, ErrorMessage: message, StartedAt: startedAt, CompletedAt: &completed, CreatedAt: now,
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&types.Directory{}).
			Where("id = ? AND snapshot_version = ? AND config_version = ?", directoryID, expectedSnapshotVersion, expectedConfigVersion).
			Where("last_successful_sync_at IS NULL OR last_successful_sync_at <= ?", startedAt).
			Where("last_sync_attempt_at IS NULL OR last_sync_attempt_at <= ?", startedAt).
			Updates(map[string]any{"last_sync_attempt_at": now, "last_sync_error": message, "updated_at": now})
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			var count int64
			if err := tx.Model(&types.Directory{}).Where("id = ?", directoryID).Count(&count).Error; err != nil {
				return err
			}
			if count == 0 {
				return gorm.ErrRecordNotFound
			}
			// A newer attempt, successful snapshot, or configuration update won
			// the race. Keep its health fields, but retain this failed run below.
		}
		return tx.Create(run).Error
	})
	if err != nil {
		return nil, err
	}
	return run, nil
}

func (r *directoryRepository) ListSyncRuns(ctx context.Context, directoryID string, offset, limit int) ([]*types.DirectorySyncRun, error) {
	var rows []*types.DirectorySyncRun
	if err := r.db.WithContext(ctx).Where("directory_id = ?", directoryID).
		Order("started_at DESC, id DESC").Offset(offset).Limit(normalizeDirectoryLimit(limit)).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}
