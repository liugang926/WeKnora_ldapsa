package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
)

// ErrDingTalkBindingNotFound is returned by UnlinkDingTalkIdentity when the
// target user has no active DingTalk binding.
var ErrDingTalkBindingNotFound = errors.New("no active dingtalk binding for user")

// DingTalkRejectionError marks a login attempt that was definitively
// rejected (as opposed to a transient failure). Reason is a stable
// machine-readable code shared with the audit trail and the
// #dingtalk_error= redirect fragment; handlers map it to 401/403.
type DingTalkRejectionError struct {
	Reason  string
	Message string
}

func (e *DingTalkRejectionError) Error() string { return e.Message }

// dingTalkRejectionMessage keeps user-facing text stable per reason code.
func dingTalkRejectionMessage(reason string) string {
	switch reason {
	case types.DingTalkRejectDisabled:
		return "DingTalk login is disabled"
	case types.DingTalkRejectWrongCorp:
		return "DingTalk account belongs to a different enterprise"
	case types.DingTalkRejectNotEnterpriseMember:
		return "DingTalk account is not a member of the configured enterprise"
	case types.DingTalkRejectUserInactive:
		return "Linked WeKnora account is disabled"
	case types.DingTalkRejectEmptyJobNumber:
		return "DingTalk profile has no job number"
	case types.DingTalkRejectNoADMatch:
		return "No corporate account matches this job number; contact an administrator"
	case types.DingTalkRejectDuplicateJobNumber:
		return "Job number matches multiple corporate accounts"
	case types.DingTalkRejectBindingConflict:
		return "Job number is already bound to another DingTalk account"
	case types.DingTalkRejectADOrUserDisabled:
		return "Corporate account or linked user is disabled"
	case types.DingTalkRejectUnavailable:
		return "DingTalk authentication is temporarily unavailable"
	default:
		return "DingTalk login rejected"
	}
}

// dingTalkAuditDetailCap mirrors the dingtalk_auth_events.detail column size.
const dingTalkAuditDetailCap = 512

func (s *userService) dingTalkAudit(ctx context.Context, event *types.DingTalkAuthEvent) {
	if s.dingtalkRepo == nil || event == nil {
		return
	}
	if len(event.Detail) > dingTalkAuditDetailCap {
		event.Detail = event.Detail[:dingTalkAuditDetailCap]
	}
	if err := s.dingtalkRepo.InsertEvent(ctx, event); err != nil {
		// Audit rows are best-effort: a full events table must not lock
		// every DingTalk login out of the product.
		logger.Warnf(ctx, "DingTalk auth: failed to insert %s audit event: %v", event.EventType, err)
	}
}

// rejectDingTalkLogin records a login_rejected audit row and returns the
// typed rejection the handler translates into an HTTP status / redirect
// reason. The audit event carries whatever identifiers the flow had
// resolved at the point of rejection.
func (s *userService) rejectDingTalkLogin(
	ctx context.Context,
	reason string,
	audit types.DingTalkAuthEvent,
) error {
	audit.EventType = types.DingTalkEventLoginRejected
	if audit.Reason == "" {
		audit.Reason = reason
	}
	s.dingTalkAudit(ctx, &audit)
	return &DingTalkRejectionError{Reason: reason, Message: dingTalkRejectionMessage(reason)}
}

// dingTalkEnabledConfig gates every DingTalk entry point on the module
// switch. A disabled module is a 403-class rejection, not a 503.
func (s *userService) dingTalkEnabledConfig() (*config.DingTalkAuthConfig, error) {
	if s.config == nil || s.config.DingTalkAuth == nil || !s.config.DingTalkAuth.Enable {
		return nil, &DingTalkRejectionError{
			Reason:  types.DingTalkRejectDisabled,
			Message: dingTalkRejectionMessage(types.DingTalkRejectDisabled),
		}
	}
	return s.config.DingTalkAuth, nil
}

// dingTalkRuntime returns the config plus the client/repository pair the
// login flows need. A missing wiring is an availability failure.
func (s *userService) dingTalkRuntime() (*config.DingTalkAuthConfig, DingTalkClient, error) {
	cfg, err := s.dingTalkEnabledConfig()
	if err != nil {
		return nil, nil, err
	}
	if s.dingtalkClient == nil || s.dingtalkRepo == nil {
		return nil, nil, &DingTalkRejectionError{
			Reason:  types.DingTalkRejectUnavailable,
			Message: dingTalkRejectionMessage(types.DingTalkRejectUnavailable),
		}
	}
	return cfg, s.dingtalkClient, nil
}

// GetDingTalkAuthConfig reports the public provider configuration. CorpID
// stays empty when the module is disabled so the response leaks nothing.
func (s *userService) GetDingTalkAuthConfig(
	ctx context.Context,
) (*types.DingTalkConfigResponse, error) {
	resp := &types.DingTalkConfigResponse{Success: true}
	if s.config != nil && s.config.DingTalkAuth != nil && s.config.DingTalkAuth.Enable {
		resp.Enabled = true
		resp.CorpID = strings.TrimSpace(s.config.DingTalkAuth.CorpID)
		resp.ProviderDisplayName = dingTalkProviderDisplayName(s.config.DingTalkAuth)
	}
	return resp, nil
}

func dingTalkProviderDisplayName(cfg *config.DingTalkAuthConfig) string {
	if cfg == nil {
		return ""
	}
	if name := strings.TrimSpace(cfg.ProviderDisplayName); name != "" {
		return name
	}
	return "钉钉"
}

// BuildDingTalkAuthorizeURL composes the DingTalk authorization page URL
// (the page renders the QR code) and the signed state bound to the caller's
// browser via the dingtalk_state cookie. The nonce never leaves the server
// except through that HttpOnly cookie.
func (s *userService) BuildDingTalkAuthorizeURL(
	ctx context.Context,
	callbackURL string,
) (*types.DingTalkAuthURLResponse, error) {
	cfg, client, err := s.dingTalkRuntime()
	if err != nil {
		return nil, err
	}
	callbackURL = strings.TrimSpace(callbackURL)
	if callbackURL == "" {
		return nil, errors.New("dingtalk callback url is required")
	}

	nonceBytes := make([]byte, 32)
	if _, err := rand.Read(nonceBytes); err != nil {
		return nil, fmt.Errorf("failed to generate dingtalk state nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)
	state, err := secutils.SignDingTalkState(&secutils.DingTalkStatePayload{Nonce: nonce})
	if err != nil {
		return nil, fmt.Errorf("failed to encode dingtalk state: %w", err)
	}

	return &types.DingTalkAuthURLResponse{
		Success:             true,
		ProviderDisplayName: dingTalkProviderDisplayName(cfg),
		AuthorizationURL:    client.BuildAuthorizeURL(callbackURL, state),
		State:               state,
		Nonce:               nonce,
	}, nil
}

// LoginWithDingTalkCode completes the web authorization-code flow. The
// handler has already verified the signed state against the browser cookie
// before calling this; the one-time DingTalk code is exchanged here.
func (s *userService) LoginWithDingTalkCode(
	ctx context.Context,
	code string,
) (*types.OIDCCallbackResponse, error) {
	if strings.TrimSpace(code) == "" {
		return nil, errors.New("code is required")
	}
	cfg, client, err := s.dingTalkRuntime()
	if err != nil {
		return nil, err
	}

	// The authorization code is one-time; never log it or the tokens below.
	userAccessToken, err := client.ExchangeUserAccessToken(ctx, code)
	if err != nil {
		return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectUnavailable, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: types.DingTalkRejectUnavailable, Detail: "exchange user access token failed",
		})
	}
	unionID, corpID, nick, err := client.GetUserContactMe(ctx, userAccessToken)
	if err != nil {
		return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectUnavailable, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: types.DingTalkRejectUnavailable, Detail: "load contact profile failed",
		})
	}
	if corpID != cfg.CorpID {
		return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectWrongCorp, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: types.DingTalkRejectWrongCorp, Detail: "token corp does not match configuration",
		})
	}

	// Real-time enterprise membership (AC-07): a personal DingTalk account
	// that was removed from the enterprise fails here even if a binding row
	// from its employed days still exists.
	dingTalkUserID, err := client.GetUserIDByUnionID(ctx, unionID)
	if err != nil {
		reason := types.DingTalkRejectUnavailable
		if errors.Is(err, ErrDingTalkNotEnterpriseMember) {
			reason = types.DingTalkRejectNotEnterpriseMember
		}
		return nil, s.rejectDingTalkLogin(ctx, reason, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: reason, Detail: "resolve userid by unionId failed",
		})
	}

	jobNumber, name, profileUnionID, err := client.GetUserDetail(ctx, dingTalkUserID)
	if err != nil {
		reason := types.DingTalkRejectUnavailable
		if errors.Is(err, ErrDingTalkNotEnterpriseMember) {
			reason = types.DingTalkRejectNotEnterpriseMember
		}
		return nil, s.rejectDingTalkLogin(ctx, reason, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID,
			Reason: reason, Detail: "load user detail failed",
		})
	}
	displayName := name
	if strings.TrimSpace(displayName) == "" {
		displayName = nick
	}
	if strings.TrimSpace(profileUnionID) != "" {
		unionID = profileUnionID
	}

	login, err := s.resolveDingTalkLogin(ctx, cfg, dingTalkUserID, jobNumber, displayName, unionID)
	if err != nil {
		return nil, err
	}
	return &types.OIDCCallbackResponse{
		Success:      true,
		Message:      login.Message,
		User:         login.User,
		Tenant:       login.ActiveTenant,
		Memberships:  login.Memberships,
		Token:        login.Token,
		RefreshToken: login.RefreshToken,
		// IsNewUser stays false: DingTalk SSO never auto-creates accounts.
	}, nil
}

// LoginWithDingTalkAuthCode completes the DingTalk workbench免登 flow. The
// in-app auth code is issued by the DingTalk client for this enterprise's
// app, so resolving it already proves enterprise membership — the corp and
// unionId checks of the web flow are intentionally skipped.
func (s *userService) LoginWithDingTalkAuthCode(
	ctx context.Context,
	authCode string,
) (*types.LoginResponse, error) {
	if strings.TrimSpace(authCode) == "" {
		return nil, errors.New("auth_code is required")
	}
	cfg, client, err := s.dingTalkRuntime()
	if err != nil {
		return nil, err
	}

	dingTalkUserID, err := client.GetInAppUserID(ctx, authCode)
	if err != nil {
		reason := types.DingTalkRejectUnavailable
		if errors.Is(err, ErrDingTalkNotEnterpriseMember) {
			reason = types.DingTalkRejectNotEnterpriseMember
		}
		return nil, s.rejectDingTalkLogin(ctx, reason, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: reason, Detail: "resolve in-app auth code failed",
		})
	}

	jobNumber, name, unionID, err := client.GetUserDetail(ctx, dingTalkUserID)
	if err != nil {
		reason := types.DingTalkRejectUnavailable
		if errors.Is(err, ErrDingTalkNotEnterpriseMember) {
			reason = types.DingTalkRejectNotEnterpriseMember
		}
		return nil, s.rejectDingTalkLogin(ctx, reason, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID,
			Reason: reason, Detail: "load user detail failed",
		})
	}

	return s.resolveDingTalkLogin(ctx, cfg, dingTalkUserID, jobNumber, name, unionID)
}

// resolveDingTalkLogin is the shared tail of both flows: transactional
// binding resolution followed by the exact token/membership assembly the
// OIDC login uses (resolveLoginTenantID / generateTokensForTenant /
// buildMembershipsForUser), so every login path returns one shape.
//
// Binding rules (no account is ever created, AC-04):
//   - an active binding keyed by (corp_id, dingtalk_user_id) wins; a changed
//     job number only refreshes the snapshot and keeps the linked user (AC-05);
//   - without a binding, the verbatim job number must match exactly one
//     linked directory identity (sam_account_name, case-sensitive, leading
//     zeros preserved); anything else is rejected with an audited reason.
func (s *userService) resolveDingTalkLogin(
	ctx context.Context,
	cfg *config.DingTalkAuthConfig,
	dingTalkUserID, jobNumber, displayName, unionID string,
) (*types.LoginResponse, error) {
	auditBase := types.DingTalkAuthEvent{
		CorpID:         cfg.CorpID,
		DingTalkUserID: dingTalkUserID,
		JobNumber:      jobNumber,
	}

	binding, err := s.dingtalkRepo.FindActiveByDingTalkID(ctx, cfg.CorpID, dingTalkUserID)
	if err != nil {
		return nil, fmt.Errorf("failed to query dingtalk binding: %w", err)
	}

	var user *types.User
	var bindingID uint64
	if binding != nil && binding.UserID != nil && strings.TrimSpace(*binding.UserID) != "" {
		user, err = s.userRepo.GetUserByID(ctx, *binding.UserID)
		if err != nil || user == nil {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectUserInactive, types.DingTalkAuthEvent{
				CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID, UserID: *binding.UserID,
				Reason: types.DingTalkRejectUserInactive, Detail: "linked user missing",
			})
		}
		if !user.IsActive {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectUserInactive, types.DingTalkAuthEvent{
				CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID, UserID: user.ID,
				Reason: types.DingTalkRejectUserInactive, Detail: "linked user disabled",
			})
		}
		// Directory-linked users must keep an eligible session identity
		// (same check as token refresh), mirroring the LDAP path (AC-08).
		if err := s.validateUserSessionEligibility(ctx, user); err != nil {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectADOrUserDisabled, types.DingTalkAuthEvent{
				CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID, UserID: user.ID,
				Reason: types.DingTalkRejectADOrUserDisabled, Detail: "session eligibility check failed",
			})
		}
		bindingID = binding.ID
		// A legitimately changed job number updates the snapshot; the
		// binding itself is keyed by the stable DingTalk user id and keeps
		// the same linked user (AC-05).
		if jobNumber != "" && jobNumber != binding.JobNumber {
			if err := s.dingtalkRepo.UpdateSnapshot(ctx, binding.ID, jobNumber, displayName, unionID); err != nil {
				logger.Warnf(ctx,
					"DingTalk login: failed to refresh job-number snapshot for binding %d: %v",
					binding.ID, err)
			} else {
				s.dingTalkAudit(ctx, &types.DingTalkAuthEvent{
					EventType:      types.DingTalkEventSnapshotUpdate,
					CorpID:         cfg.CorpID,
					DingTalkUserID: dingTalkUserID,
					JobNumber:      jobNumber,
					UserID:         user.ID,
					Detail:         fmt.Sprintf("job number changed from %q", binding.JobNumber),
				})
			}
		}
	} else {
		if jobNumber == "" {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectEmptyJobNumber, auditBase)
		}
		// Exact, case-sensitive match on the verbatim job number. No
		// trimming/normalization: "00123" and "123" are different employees.
		identities, err := s.dingtalkRepo.FindADIdentitiesBySAMAccountName(ctx, jobNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to match job number against directory identities: %w", err)
		}
		switch {
		case len(identities) == 0:
			// No matching AD account: never fall back to auto-creation.
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectNoADMatch, auditBase)
		case len(identities) > 1:
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectDuplicateJobNumber, auditBase)
		}
		identity := identities[0]
		if identity == nil || identity.UserID == nil || strings.TrimSpace(*identity.UserID) == "" {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectNoADMatch, auditBase)
		}
		if identity.Status != types.DirectoryObjectActive {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectADOrUserDisabled, types.DingTalkAuthEvent{
				CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID, JobNumber: jobNumber,
				Reason: types.DingTalkRejectADOrUserDisabled, Detail: "directory identity not active",
			})
		}
		user, err = s.userRepo.GetUserByID(ctx, *identity.UserID)
		if err != nil || user == nil || !user.IsActive {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectADOrUserDisabled, types.DingTalkAuthEvent{
				CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID, JobNumber: jobNumber,
				Reason: types.DingTalkRejectADOrUserDisabled, Detail: "linked user missing or disabled",
			})
		}
		// Job-number reuse guard: the matched account must not already have
		// an active DingTalk binding held by a different DingTalk user.
		existing, err := s.dingtalkRepo.FindActiveByUserID(ctx, user.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to query existing dingtalk binding: %w", err)
		}
		if existing != nil {
			return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectBindingConflict, types.DingTalkAuthEvent{
				CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID, JobNumber: jobNumber, UserID: user.ID,
				Reason: types.DingTalkRejectBindingConflict,
				Detail: "job number already bound to another dingtalk account",
			})
		}
		created := &types.DingTalkIdentity{
			CorpID:         cfg.CorpID,
			DingTalkUserID: dingTalkUserID,
			UnionID:        unionID,
			JobNumber:      jobNumber,
			DisplayName:    displayName,
			UserID:         &user.ID,
			Status:         types.DingTalkIdentityActive,
		}
		if err := s.dingtalkRepo.CreateBinding(ctx, created); err != nil {
			if !errors.Is(err, apprepo.ErrDingTalkBindingConflict) {
				return nil, fmt.Errorf("failed to create dingtalk binding: %w", err)
			}
			// Lost a race (AC-04). Re-read: only proceed when the winner is
			// this very (corp, dingtalk_user) pair bound to the same user —
			// i.e. our own concurrent attempt — otherwise reject.
			current, rerr := s.dingtalkRepo.FindActiveByDingTalkID(ctx, cfg.CorpID, dingTalkUserID)
			if rerr != nil || current == nil || current.UserID == nil || *current.UserID != user.ID {
				return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectBindingConflict, types.DingTalkAuthEvent{
					CorpID: cfg.CorpID, DingTalkUserID: dingTalkUserID, JobNumber: jobNumber, UserID: user.ID,
					Reason: types.DingTalkRejectBindingConflict, Detail: "concurrent binding race lost",
				})
			}
			created = current
		} else {
			s.dingTalkAudit(ctx, &types.DingTalkAuthEvent{
				EventType:      types.DingTalkEventBind,
				CorpID:         cfg.CorpID,
				DingTalkUserID: dingTalkUserID,
				JobNumber:      jobNumber,
				UserID:         user.ID,
				Detail:         "bound via job number match",
			})
		}
		bindingID = created.ID
	}

	// Token generation mirrors the OIDC login exactly so all three login
	// paths (password / OIDC / DingTalk) produce identical response shapes.
	resolvedTenantID := s.resolveLoginTenantID(ctx, user)
	accessToken, refreshToken, err := s.generateTokensForTenant(ctx, user, resolvedTenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to generate local tokens: %w", err)
	}
	var tenant *types.Tenant
	if resolvedTenantID > 0 && s.tenantService != nil {
		if t, terr := s.tenantService.GetTenantByID(ctx, resolvedTenantID); terr == nil {
			tenant = t
		} else {
			logger.Warnf(ctx, "DingTalk login: failed to load tenant %d for user %s: %v",
				resolvedTenantID, user.ID, terr)
		}
	}
	memberships := s.buildMembershipsForUser(ctx, user, tenant)

	if bindingID != 0 {
		if err := s.dingtalkRepo.TouchLastLogin(ctx, bindingID); err != nil {
			logger.Warnf(ctx, "DingTalk login: failed to touch last login for binding %d: %v", bindingID, err)
		}
	}
	s.dingTalkAudit(ctx, &types.DingTalkAuthEvent{
		EventType:      types.DingTalkEventLoginSuccess,
		CorpID:         cfg.CorpID,
		DingTalkUserID: dingTalkUserID,
		JobNumber:      jobNumber,
		UserID:         user.ID,
	})

	logger.Infof(ctx, "DingTalk login succeeded for user %s", user.ID)
	return &types.LoginResponse{
		Success:      true,
		Message:      "登录成功",
		User:         user,
		ActiveTenant: tenant,
		Memberships:  memberships,
		Token:        accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// UnlinkDingTalkIdentity removes the user's active DingTalk binding and
// revokes their sessions so in-flight tokens stop working immediately.
// operatorID is recorded in the unbind audit detail (the caller is
// responsible for authorization; BE2 wires the admin surface).
func (s *userService) UnlinkDingTalkIdentity(ctx context.Context, operatorID, targetUserID string) error {
	if strings.TrimSpace(targetUserID) == "" {
		return errors.New("target user id is required")
	}
	if s.dingtalkRepo == nil {
		return errors.New("dingtalk auth is not configured")
	}
	binding, err := s.dingtalkRepo.FindActiveByUserID(ctx, targetUserID)
	if err != nil {
		return err
	}
	if binding == nil {
		return ErrDingTalkBindingNotFound
	}
	if err := s.dingtalkRepo.MarkUnlinked(ctx, binding.ID); err != nil {
		return err
	}
	if s.tokenRepo != nil {
		if err := s.tokenRepo.RevokeTokensByUserID(ctx, targetUserID); err != nil {
			logger.Warnf(ctx, "DingTalk unlink: failed to revoke tokens for user %s: %v", targetUserID, err)
		}
	}
	s.dingTalkAudit(ctx, &types.DingTalkAuthEvent{
		EventType:      types.DingTalkEventUnbind,
		CorpID:         binding.CorpID,
		DingTalkUserID: binding.DingTalkUserID,
		JobNumber:      binding.JobNumber,
		UserID:         targetUserID,
		Detail:         fmt.Sprintf("operator=%s", operatorID),
	})
	return nil
}
