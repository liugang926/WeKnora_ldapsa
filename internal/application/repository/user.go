package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrCannotDisableSelf prevents administrators from disabling their own account.
	ErrCannotDisableSelf = errors.New("cannot disable your own account")
	// ErrCannotDisableLastAdmin preserves at least one active system administrator.
	ErrCannotDisableLastAdmin = errors.New("cannot disable the last system administrator")
	// ErrUserNotFound indicates that the requested user does not exist.
	ErrUserNotFound = errors.New("user not found")
	// ErrUserAlreadyExists indicates a duplicate user account.
	ErrUserAlreadyExists = errors.New("user already exists")
	// ErrTokenNotFound indicates that the requested token does not exist.
	ErrTokenNotFound = errors.New("token not found")
	// ErrCannotRevokeSelf prevents administrators from revoking their own role.
	ErrCannotRevokeSelf = errors.New("cannot revoke your own system admin privileges")
	// ErrLastSystemAdmin prevents revoking the last active system administrator's role.
	ErrLastSystemAdmin = errors.New("cannot revoke the last remaining system administrator")
	// ErrUserNotSystemAdmin indicates that the user has no system-admin role to revoke.
	ErrUserNotSystemAdmin = errors.New("user is not a system administrator")
)

// userRepository implements user repository interface
type userRepository struct {
	db *gorm.DB
}

// NewUserRepository creates a new user repository
func NewUserRepository(db *gorm.DB) interfaces.UserRepository {
	return &userRepository{db: db}
}

// CreateUser creates a user
func (r *userRepository) CreateUser(ctx context.Context, user *types.User) error {
	// users.tenant_id is nullable in both PostgreSQL and SQLite. GORM would
	// otherwise serialise the uint64 zero value as 0, which violates the
	// PostgreSQL FK and loses the distinction between "not provisioned yet"
	// and a real tenant. Omitting the column stores SQL NULL; reads hydrate it
	// back as zero, the domain sentinel used by tenantless auth flows.
	if user != nil && user.TenantID == 0 {
		return r.db.WithContext(ctx).Omit("tenant_id").Create(user).Error
	}
	return r.db.WithContext(ctx).Create(user).Error
}

// GetUserByID gets a user by ID
func (r *userRepository) GetUserByID(ctx context.Context, id string) (*types.User, error) {
	var user types.User
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &user, nil
}

// GetUsersByIDs batch-fetches users by id with a single SELECT … WHERE id IN (…)
// and projects the result into a map keyed by user id. Returns an empty
// map for an empty input slice. Missing ids are silently absent from
// the result (consistent with the interface contract used by tenant
// member hydration).
func (r *userRepository) GetUsersByIDs(ctx context.Context, ids []string) (map[string]*types.User, error) {
	out := make(map[string]*types.User, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	var users []*types.User
	if err := r.db.WithContext(ctx).Where("id IN ?", ids).Find(&users).Error; err != nil {
		return nil, err
	}
	for _, u := range users {
		out[u.ID] = u
	}
	return out, nil
}

// GetUserByEmail gets a user by email
func (r *userRepository) GetUserByEmail(ctx context.Context, email string) (*types.User, error) {
	var user types.User
	if err := r.db.WithContext(ctx).Where("email = ?", email).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &user, nil
}

// GetUserByUsername gets a user by username
func (r *userRepository) GetUserByUsername(ctx context.Context, username string) (*types.User, error) {
	var user types.User
	if err := r.db.WithContext(ctx).Where("username = ?", username).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &user, nil
}

// GetUserByTenantID gets the first user (owner) of a tenant
func (r *userRepository) GetUserByTenantID(ctx context.Context, tenantID uint64) (*types.User, error) {
	var user types.User
	if err := r.db.WithContext(ctx).Where("tenant_id = ?", tenantID).Order("created_at ASC").First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return &user, nil
}

// UpdateUser updates user data while preserving account status. Status changes
// must go through SetUserActive so stale profile/password writes cannot undo
// an administrator's enable/disable action.
func (r *userRepository) UpdateUser(ctx context.Context, user *types.User) error {
	if user != nil && user.TenantID == 0 {
		return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// Keep status and the nullable tenant column out of the struct
			// write, then explicitly store NULL for the tenant.
			// Writing uint64(0) would violate the PostgreSQL tenant FK.
			if err := tx.Omit("tenant_id", "is_active").Save(user).Error; err != nil {
				return err
			}
			return tx.Model(&types.User{}).
				Where("id = ?", user.ID).
				UpdateColumn("tenant_id", nil).Error
		})
	}
	return r.db.WithContext(ctx).Omit("is_active").Save(user).Error
}

// DeleteUser deletes a user
func (r *userRepository) DeleteUser(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&types.User{}).Error
}

// DeleteUserRetiringSessions locks the web user, retires every session visible
// under that lock, and soft-deletes the user in the same transaction. A user
// row that is already soft-deleted is still held for the re-read; its sessions
// are retired and the call returns nil.
func (r *userRepository) DeleteUserRetiringSessions(ctx context.Context, id string, at time.Time) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := lockLiveWebUser(tx, id)
		if err != nil && !errors.Is(err, errWebUserDeleted) {
			return err
		}
		ids, err := ownerSessionIDs(tx, id)
		if err != nil {
			return err
		}
		if _, err := RetireSessionsTx(tx, ids, at); err != nil {
			return err
		}
		return tx.Where("id = ?", id).Delete(&types.User{}).Error
	})
}

// applyUserKeywordFilter adds a dialect-aware, LIKE-escaped substring match
// against username or email.
func applyUserKeywordFilter(db *gorm.DB, keyword string) *gorm.DB {
	userLike := "LOWER(username) LIKE LOWER(?) ESCAPE ?"
	mailLike := "LOWER(email) LIKE LOWER(?) ESCAPE ?"
	if db.Name() == "postgres" {
		userLike = "username ILIKE ? ESCAPE ?"
		mailLike = "email ILIKE ? ESCAPE ?"
	}
	searchPattern := "%" + escapeLikeKeyword(keyword) + "%"
	return db.Where("("+userLike+" OR "+mailLike+")", searchPattern, likeEscapeChar, searchPattern, likeEscapeChar)
}

// ListUsers lists users with pagination and an optional username/email filter.
// Both active and disabled users are included so administrators can locate
// disabled accounts across pages. Ties on created_at are broken by id ASC for
// deterministic pagination.
func (r *userRepository) ListUsers(ctx context.Context, query string, offset, limit int) ([]*types.User, error) {
	var users []*types.User
	dbQuery := r.db.WithContext(ctx)
	if q := strings.TrimSpace(query); q != "" {
		dbQuery = applyUserKeywordFilter(dbQuery, q)
	}
	dbQuery = dbQuery.Order("created_at DESC, id ASC")

	if limit > 0 {
		dbQuery = dbQuery.Limit(limit)
	}

	if offset > 0 {
		dbQuery = dbQuery.Offset(offset)
	}

	if err := dbQuery.Find(&users).Error; err != nil {
		return nil, err
	}
	return users, nil
}

// ListSystemAdmins lists users where is_system_admin = true.
//
// Walks idx_users_is_system_admin (created in migration 000052), so the
// query stays cheap even on a large users table — only the small subset
// of system admins is scanned. Returns total count alongside the page so
// the management UI can render pagination without a second roundtrip.
//
// Ordered by created_at DESC for stable, newest-first listing; ties are
// further broken by id to keep paging deterministic across boundaries.
// limit <= 0 means "no limit" (matches ListUsers semantics); callers in
// production pass a sane page size.
func (r *userRepository) ListSystemAdmins(ctx context.Context, offset, limit int) ([]*types.User, int64, error) {
	var users []*types.User
	var total int64

	base := r.db.WithContext(ctx).Model(&types.User{}).Where("is_system_admin = ?", true)
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	query := base.Order("created_at DESC, id ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	if offset > 0 {
		query = query.Offset(offset)
	}
	if err := query.Find(&users).Error; err != nil {
		return nil, 0, err
	}
	return users, total, nil
}

// lockSystemAdmins uses the same lock order for status changes and role
// revocation: all admin rows by ID, then the target user. SQLite has no row
// locks, so acquire its writer lock before reading the administrator set.
func lockSystemAdmins(tx *gorm.DB) ([]types.User, error) {
	if tx.Name() == "sqlite" {
		if err := tx.Model(&types.User{}).Where("is_system_admin = ?", true).
			UpdateColumn("is_system_admin", true).Error; err != nil {
			return nil, err
		}
	}
	var admins []types.User
	err := lockUserRows(tx).Where("is_system_admin = ?", true).Order("id ASC").Find(&admins).Error
	return admins, err
}

func lockUserRows(tx *gorm.DB) *gorm.DB {
	switch tx.Name() {
	case "postgres", "mysql":
		return tx.Clauses(clause.Locking{Strength: "UPDATE"})
	default:
		return tx
	}
}

func activeSystemAdminCount(admins []types.User) int {
	n := 0
	for _, admin := range admins {
		if admin.IsActive {
			n++
		}
	}
	return n
}

// SetUserActive persists status and session revocation in one transaction.
// Locking the admin set also serializes this operation with role revocation.
func (r *userRepository) SetUserActive(ctx context.Context, userID, actorID string, active bool) (*types.User, error) {
	var user types.User
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		admins, err := lockSystemAdmins(tx)
		if err != nil {
			return err
		}
		if err := lockUserRows(tx).Where("id = ?", userID).First(&user).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUserNotFound
			}
			return err
		}
		if user.IsActive == active {
			return nil
		}
		if !active {
			if userID == actorID {
				return ErrCannotDisableSelf
			}
			if user.IsSystemAdmin && activeSystemAdminCount(admins) <= 1 {
				return ErrCannotDisableLastAdmin
			}
		}
		user.IsActive = active
		user.UpdatedAt = time.Now()
		if err := tx.Model(&user).Updates(map[string]any{
			"is_active": active, "updated_at": user.UpdatedAt,
		}).Error; err != nil {
			return err
		}
		if !active {
			return tx.Model(&types.AuthToken{}).Where("user_id = ?", userID).
				Update("is_revoked", true).Error
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// RevokeSystemAdmin revokes system-admin privileges inside a transaction.
// It locks the current admin rows before counting so concurrent revokes
// cannot both observe "two admins" and leave the platform with zero.
//
// Return contract:
//   - (user, nil): revoke actually happened; user.IsSystemAdmin == false
//   - (user, ErrUserNotSystemAdmin): target was already not an admin;
//     no row was written. Caller should treat as idempotent success but
//     MUST distinguish it from a real revoke for audit purposes — the
//     surfaced `user` is the unchanged DB row.
//   - (nil, ErrCannotRevokeSelf | ErrLastSystemAdmin | ErrUserNotFound | …):
//     hard rejection; no row written.
func (r *userRepository) RevokeSystemAdmin(ctx context.Context, userID, actorID string) (*types.User, error) {
	if userID == actorID {
		return nil, ErrCannotRevokeSelf
	}

	var revoked *types.User
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		admins, err := lockSystemAdmins(tx)
		if err != nil {
			return err
		}
		var user types.User
		if err := lockUserRows(tx).
			Where("id = ?", userID).
			First(&user).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrUserNotFound
			}
			return err
		}
		if !user.IsSystemAdmin {
			revoked = &user
			return ErrUserNotSystemAdmin
		}

		if len(admins) <= 1 || (user.IsActive && activeSystemAdminCount(admins) <= 1) {
			return ErrLastSystemAdmin
		}

		user.IsSystemAdmin = false
		if err := tx.Model(&user).Update("is_system_admin", false).Error; err != nil {
			return err
		}
		revoked = &user
		return nil
	})
	// Propagate ErrUserNotSystemAdmin up to the handler alongside the
	// (unchanged) user row. The handler treats it as idempotent success
	// but emits an audit row with changed=false so a probing pattern
	// ("revoke every random user id we know") still leaves a trail.
	if errors.Is(err, ErrUserNotSystemAdmin) {
		return revoked, err
	}
	if err != nil {
		return nil, err
	}
	return revoked, nil
}

// SearchUsers searches users by username or email
func (r *userRepository) SearchUsers(ctx context.Context, query string, limit int) ([]*types.User, error) {
	var users []*types.User
	// Dialect-aware bits so the same query works on Postgres and SQLite (Lite
	// build), which has no ILIKE. The keyword is escaped either way: % and _
	// are LIKE wildcards, so an unescaped search for "a_b" also matches "axb"
	// and "%" returns every user.
	dbQuery := applyUserKeywordFilter(r.db.WithContext(ctx), query).
		Where("is_active = ?", true).
		Order("username ASC")

	if limit > 0 {
		dbQuery = dbQuery.Limit(limit)
	} else {
		dbQuery = dbQuery.Limit(20) // default limit
	}

	if err := dbQuery.Find(&users).Error; err != nil {
		return nil, err
	}
	return users, nil
}

// FindUserByEmailOrUsernameFold performs exact case-insensitive collision
// detection for directory auto-provisioning. It intentionally does not filter
// is_active: an inactive local account must still require explicit linking.
func (r *userRepository) FindUserByEmailOrUsernameFold(
	ctx context.Context,
	email, username string,
) (*types.User, error) {
	query := r.db.WithContext(ctx)
	conditions := make([]string, 0, 2)
	args := make([]any, 0, 2)
	if email = strings.TrimSpace(email); email != "" {
		conditions = append(conditions, "LOWER(email) = LOWER(?)")
		args = append(args, email)
	}
	if username = strings.TrimSpace(username); username != "" {
		conditions = append(conditions, "LOWER(username) = LOWER(?)")
		args = append(args, username)
	}
	if len(conditions) == 0 {
		return nil, nil
	}

	var user types.User
	if err := query.Where("("+strings.Join(conditions, " OR ")+")", args...).
		Order("id ASC").First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &user, nil
}

// authTokenRepository implements auth token repository interface
type authTokenRepository struct {
	db *gorm.DB
}

// NewAuthTokenRepository creates a new auth token repository
func NewAuthTokenRepository(db *gorm.DB) interfaces.AuthTokenRepository {
	return &authTokenRepository{db: db}
}

// CreateTokenPair serializes issuance with account status changes and consumes a
// refresh token only if the replacement pair commits.
func (r *authTokenRepository) CreateTokenPair(
	ctx context.Context, access, refresh *types.AuthToken, previousRefresh string,
) error {
	if access == nil || refresh == nil || access.UserID == "" || access.UserID != refresh.UserID ||
		access.TokenType != "access_token" || refresh.TokenType != "refresh_token" {
		return errors.New("invalid token pair")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockLoginUser(tx, access.UserID); err != nil {
			return err
		}
		family := refresh.ID
		if previousRefresh != "" {
			var source types.AuthToken
			err := tx.Where("user_id = ? AND token = ? AND is_revoked = ? AND expires_at > ?",
				access.UserID, previousRefresh, false, time.Now()).First(&source).Error
			if err != nil {
				return fmt.Errorf("load live source token: %w", err)
			}
			if source.TokenType != "access_token" && source.TokenType != "refresh_token" {
				return errors.New("invalid session source")
			}
			family = source.SessionID
			if family == "" {
				// Without a known pair, rotation would strand the other old token.
				return errors.New("legacy login cannot be rotated; sign in again")
			}
			if source.TokenType == "refresh_token" {
				if err := tx.Model(&source).Update("is_revoked", true).Error; err != nil {
					return err
				}
			}
		}
		access.SessionID, refresh.SessionID = family, family
		if err := tx.Create(access).Error; err != nil {
			return fmt.Errorf("persist access token: %w", err)
		}
		if err := tx.Create(refresh).Error; err != nil {
			return fmt.Errorf("persist refresh token: %w", err)
		}
		return nil
	})
}

func lockLoginUser(tx *gorm.DB, userID string) error {
	account := tx.Model(&types.User{}).Where("id = ? AND is_active = ?", userID, true)
	var locked *gorm.DB
	if tx.Name() == "sqlite" {
		// SQLite has no row locks. Take its writer lock before reading or
		// rotating tokens without modifying the user's status or timestamps.
		locked = account.UpdateColumn("is_active", true)
	} else {
		var user types.User
		locked = lockUserRows(account).Select("id").Find(&user)
	}
	if locked.Error != nil {
		return locked.Error
	}
	if locked.RowsAffected == 0 {
		return errors.New("account is disabled or missing")
	}
	return nil
}

// RevokeLogin serializes with rotation: either the successor is revoked here
// or its issuer observes an already revoked source.
func (r *authTokenRepository) RevokeLogin(ctx context.Context, userID, token, companionRefresh string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockLoginUser(tx, userID); err != nil {
			return err
		}
		var source types.AuthToken
		if err := tx.Where("user_id = ? AND token = ?", userID, token).First(&source).Error; err != nil {
			return err
		}
		family := source.SessionID
		ids := []string{source.ID}
		if companionRefresh != "" && source.TokenType == "access_token" {
			var refresh types.AuthToken
			err := tx.Where("user_id = ? AND token_type = ? AND token = ?",
				userID, "refresh_token", companionRefresh).First(&refresh).Error
			if err != nil {
				return err
			}
			if family != refresh.SessionID {
				return errors.New("logout credentials belong to different sessions")
			}
			ids = append(ids, refresh.ID)
		} else if family == "" {
			return errors.New("legacy login requires both access and refresh credentials")
		}
		revoke := tx.Model(&types.AuthToken{}).Where("user_id = ? AND is_revoked = ?", userID, false)
		if family != "" {
			revoke = revoke.Where("(session_id = ? OR id IN ?)", family, ids)
		} else {
			revoke = revoke.Where("id IN ?", ids)
		}
		return revoke.Update("is_revoked", true).Error
	})
}

// GetTokenByValue gets a token by its value
func (r *authTokenRepository) GetTokenByValue(ctx context.Context, tokenValue string) (*types.AuthToken, error) {
	var token types.AuthToken
	if err := r.db.WithContext(ctx).Where("token = ?", tokenValue).First(&token).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrTokenNotFound
		}
		return nil, err
	}
	return &token, nil
}

// GetTokenByID gets a token by its primary key
func (r *authTokenRepository) GetTokenByID(ctx context.Context, id string) (*types.AuthToken, error) {
	var token types.AuthToken
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&token).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrTokenNotFound
		}
		return nil, err
	}
	return &token, nil
}

// GetTokensByUserID gets all tokens for a user
func (r *authTokenRepository) GetTokensByUserID(ctx context.Context, userID string) ([]*types.AuthToken, error) {
	var tokens []*types.AuthToken
	if err := r.db.WithContext(ctx).Where("user_id = ?", userID).Find(&tokens).Error; err != nil {
		return nil, err
	}
	return tokens, nil
}

// UpdateToken updates a token
func (r *authTokenRepository) UpdateToken(ctx context.Context, token *types.AuthToken) error {
	return r.db.WithContext(ctx).Save(token).Error
}

// DeleteToken deletes a token
func (r *authTokenRepository) DeleteToken(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&types.AuthToken{}).Error
}

// DeleteExpiredTokens deletes all expired tokens
func (r *authTokenRepository) DeleteExpiredTokens(ctx context.Context) error {
	return r.db.WithContext(ctx).Where("expires_at < NOW()").Delete(&types.AuthToken{}).Error
}

// RevokeTokensByUserID revokes all tokens for a user
func (r *authTokenRepository) RevokeTokensByUserID(ctx context.Context, userID string) error {
	return r.db.WithContext(ctx).Model(&types.AuthToken{}).Where("user_id = ?", userID).Update("is_revoked", true).Error
}
