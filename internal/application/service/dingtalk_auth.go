package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	secutils "github.com/Tencent/WeKnora/internal/utils"
)

// ErrDingTalkBindingNotFound is returned by UnlinkDingTalkIdentity when the
// target user has no active DingTalk binding.
var ErrDingTalkBindingNotFound = errors.New("no active dingtalk binding for user")

// ErrDingTalkConfigFileManaged rejects admin writes when the deployment
// keeps the DingTalk configuration in env/YAML (config_source=file), the
// same read-only contract the directory module exposes.
var ErrDingTalkConfigFileManaged = errors.New(
	"dingtalk configuration is managed by deployment files and is read-only")

// ErrInvalidDingTalkConfig marks administrator input that would leave the
// module unusable (enabled without corp id / app key / usable secret).
var ErrInvalidDingTalkConfig = errors.New("invalid dingtalk configuration")

// dingTalkConfigCacheTTL bounds how long a database-resolved configuration
// stays cached. DingTalk login is low-frequency, so correctness (fresh
// config) wins over avoiding one indexed single-row read.
const dingTalkConfigCacheTTL = 30 * time.Second

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

// truncateDingTalkDetail keeps an audit detail within the column size; the
// client error strings are already redacted (endpoint URLs without tokens).
func truncateDingTalkDetail(detail string) string {
	if len(detail) > dingTalkAuditDetailCap-1 {
		return detail[:dingTalkAuditDetailCap-1]
	}
	return detail
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

// dingTalkEnvConfig returns the deployment baseline (env/YAML) or an
// equivalent empty section when the module was never configured.
func (s *userService) dingTalkEnvConfig() *config.DingTalkAuthConfig {
	if s.config == nil || s.config.DingTalkAuth == nil {
		return &config.DingTalkAuthConfig{}
	}
	return s.config.DingTalkAuth
}

// dingTalkDatabaseManaged reports whether the settings UI owns the DingTalk
// configuration:
//
//   - DINGTALK_AUTH_CONFIG_SOURCE=file — hard rollback switch, env/YAML
//     stays authoritative and the settings row is never consulted;
//   - DINGTALK_AUTH_CONFIG_SOURCE=database — explicit opt-in;
//   - unset (legacy default) — database-managed once a settings row exists,
//     so adopting the env configuration from the admin UI flips the module
//     without a restart.
//
// The row-exists probe rides the same 30s cache as the effective config and
// is invalidated on every save.
func (s *userService) dingTalkDatabaseManaged(ctx context.Context) bool {
	switch s.dingTalkEnvConfig().ConfigSource {
	case config.DingTalkConfigSourceFile:
		return false
	case config.DingTalkConfigSourceDatabase:
		return true
	}
	return s.dingTalkSettingsRowExists(ctx)
}

func (s *userService) dingTalkSettingsRowExists(ctx context.Context) bool {
	if s.dingtalkRepo == nil {
		return false
	}
	s.dingTalkConfigMu.Lock()
	defer s.dingTalkConfigMu.Unlock()
	if s.dingTalkRowExists != nil && s.dingTalkClock().Before(s.dingTalkRowExistsExpires) {
		return *s.dingTalkRowExists
	}
	exists := false
	if row, err := s.dingtalkRepo.GetSettings(ctx); err == nil && row != nil {
		exists = true
	}
	s.dingTalkRowExists = &exists
	s.dingTalkRowExistsExpires = s.dingTalkClock().Add(dingTalkConfigCacheTTL)
	return exists
}

func (s *userService) dingTalkClock() time.Time {
	if s.dingTalkNow != nil {
		return s.dingTalkNow()
	}
	return time.Now()
}

// dingTalkEffectiveConfig resolves the configuration every login/URL/config
// read must use:
//
//   - file (default): env/YAML is authoritative — today's behaviour, byte
//     for byte; the settings table is never consulted.
//   - database: the dingtalk_auth_settings row overlays the env baseline.
//     Non-empty row fields win over their env counterparts; a stored secret
//     replaces the env secret (empty ciphertext falls back to env); enabled
//     is taken from the row. No row means the env baseline applies.
//
// The merged result is cached for dingTalkConfigCacheTTL; saves invalidate
// it immediately. Decryption failures propagate as plain errors so callers
// fail closed (503) instead of silently degrading to the env secret.
func (s *userService) dingTalkEffectiveConfig(
	ctx context.Context,
) (*config.DingTalkAuthConfig, error) {
	env := s.dingTalkEnvConfig()
	// Resolution gate is the service-level management mode (explicit env
	// choice, or — with the source unset — a stored settings row), probed
	// BEFORE taking the config cache lock: the probe locks the same mutex.
	if !s.dingTalkDatabaseManaged(ctx) {
		return env, nil
	}
	s.dingTalkConfigMu.Lock()
	defer s.dingTalkConfigMu.Unlock()
	if s.dingTalkConfigCached != nil && s.dingTalkClock().Before(s.dingTalkConfigExpires) {
		return s.dingTalkConfigCached, nil
	}
	merged := *env
	if s.dingtalkRepo != nil {
		row, err := s.dingtalkRepo.GetSettings(ctx)
		if err != nil {
			return nil, fmt.Errorf("load dingtalk settings: %w", err)
		}
		if row != nil {
			// enabled always follows the database row, never env, so an
			// administrator can switch the module off without redeploying.
			merged.Enable = row.Enabled
			merged.CorpID = dingTalkOverlay(merged.CorpID, row.CorpID)
			merged.AppKey = dingTalkOverlay(merged.AppKey, row.AppKey)
			merged.AgentID = dingTalkOverlay(merged.AgentID, row.AgentID)
			merged.ProviderDisplayName = dingTalkOverlay(merged.ProviderDisplayName, row.DisplayName)
			merged.RedirectURL = dingTalkOverlay(merged.RedirectURL, row.RedirectURL)
			if row.AppSecretCiphertext != "" {
				secret, err := secutils.DecryptStoredSecret(row.AppSecretCiphertext)
				if err != nil {
					return nil, fmt.Errorf("decrypt dingtalk app secret: %w", err)
				}
				merged.AppSecret = secret
			}
		}
	}
	s.dingTalkConfigCached = &merged
	s.dingTalkConfigExpires = s.dingTalkClock().Add(dingTalkConfigCacheTTL)
	return &merged, nil
}

// dingTalkOverlay lets a stored row value replace the env baseline value;
// an empty row value keeps the baseline.
func dingTalkOverlay(envValue, rowValue string) string {
	if strings.TrimSpace(rowValue) != "" {
		return rowValue
	}
	return envValue
}

// invalidateDingTalkConfigCache drops the cached effective configuration so
// the next read observes a just-saved row immediately.
func (s *userService) invalidateDingTalkConfigCache() {
	s.dingTalkRowExists = nil
	s.dingTalkConfigMu.Lock()
	defer s.dingTalkConfigMu.Unlock()
	s.dingTalkConfigCached = nil
}

// dingTalkEnabledConfig gates every DingTalk entry point on the module
// switch. A disabled module is a 403-class rejection, not a 503.
func (s *userService) dingTalkEnabledConfig(ctx context.Context) (*config.DingTalkAuthConfig, error) {
	cfg, err := s.dingTalkEffectiveConfig(context.Background())
	if err != nil {
		return nil, err
	}
	if cfg == nil || !cfg.Enable {
		return nil, &DingTalkRejectionError{
			Reason:  types.DingTalkRejectDisabled,
			Message: dingTalkRejectionMessage(types.DingTalkRejectDisabled),
		}
	}
	return cfg, nil
}

// dingTalkNewClient builds a credential-bound client, honouring the test
// factory override when one is installed.
func (s *userService) dingTalkNewClient(cfg *config.DingTalkAuthConfig) DingTalkClient {
	if s.dingTalkClientFactory != nil {
		return s.dingTalkClientFactory(cfg)
	}
	return NewDingTalkClient(cfg)
}

// dingTalkRuntime returns the config plus the client/repository pair the
// login flows need. A missing wiring is an availability failure. In database
// mode the client is rebuilt from the effective config so a UI-saved secret
// is honoured (the injected client still carries the env credentials);
// file mode keeps the injected client, exactly as before.
func (s *userService) dingTalkRuntime(ctx context.Context) (*config.DingTalkAuthConfig, DingTalkClient, error) {
	cfg, err := s.dingTalkEnabledConfig(ctx)
	if err != nil {
		return nil, nil, err
	}
	client := s.dingtalkClient
	if s.dingTalkDatabaseManaged(ctx) {
		client = s.dingTalkNewClient(cfg)
	}
	if client == nil || s.dingtalkRepo == nil {
		return nil, nil, &DingTalkRejectionError{
			Reason:  types.DingTalkRejectUnavailable,
			Message: dingTalkRejectionMessage(types.DingTalkRejectUnavailable),
		}
	}
	return cfg, client, nil
}

// GetDingTalkAuthConfig reports the public provider configuration. CorpID
// stays empty when the module is disabled so the response leaks nothing.
func (s *userService) GetDingTalkAuthConfig(
	ctx context.Context,
) (*types.DingTalkConfigResponse, error) {
	resp := &types.DingTalkConfigResponse{Success: true}
	cfg, err := s.dingTalkEffectiveConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg != nil && cfg.Enable {
		resp.Enabled = true
		resp.CorpID = strings.TrimSpace(cfg.CorpID)
		resp.ProviderDisplayName = dingTalkProviderDisplayName(cfg)
	}
	return resp, nil
}

// DingTalkEffectiveAuthInfo reports the secret-free runtime state the auth
// handler gates public endpoints on: whether the effective (file- or
// database-resolved) configuration enables the module, and the registered
// redirect URL to fall back from.
func (s *userService) DingTalkEffectiveAuthInfo(
	ctx context.Context,
) (*types.DingTalkAuthInfo, error) {
	cfg, err := s.dingTalkEffectiveConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &config.DingTalkAuthConfig{}
	}
	return &types.DingTalkAuthInfo{
		Enabled:     cfg.Enable,
		RedirectURL: strings.TrimSpace(cfg.RedirectURL),
	}, nil
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

// GetDingTalkAdminConfig returns the secret-free management view. Values are
// the EFFECTIVE configuration (database row overlaid on env), so the form
// round-trips exactly what a login would use. ConfigVersion / UpdatedBy /
// UpdatedAt come from the stored row when one exists.
func (s *userService) GetDingTalkAdminConfig(
	ctx context.Context,
) (*types.DingTalkAdminConfig, error) {
	cfg, err := s.dingTalkEffectiveConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &config.DingTalkAuthConfig{}
	}
	var row *types.DingTalkAuthSettings
	if s.dingtalkRepo != nil && s.dingTalkDatabaseManaged(ctx) {
		if row, err = s.dingtalkRepo.GetSettings(ctx); err != nil {
			return nil, err
		}
	}
	view := &types.DingTalkAdminConfig{
		Success:      true,
		ConfigSource: s.dingTalkConfigSourceLabel(ctx),
		Enabled:      cfg.Enable,
		CorpID:       strings.TrimSpace(cfg.CorpID),
		AppKey:       strings.TrimSpace(cfg.AppKey),
		AgentID:      strings.TrimSpace(cfg.AgentID),
		DisplayName:  strings.TrimSpace(cfg.ProviderDisplayName),
		RedirectURL:  strings.TrimSpace(cfg.RedirectURL),
		HasSecret:    strings.TrimSpace(cfg.AppSecret) != "",
		SecretFrom:   types.DingTalkSecretSourceNone,
	}
	if view.HasSecret {
		if row != nil && row.AppSecretCiphertext != "" {
			view.SecretFrom = types.DingTalkSecretSourceDatabase
		} else {
			view.SecretFrom = types.DingTalkSecretSourceEnv
		}
	}
	if row != nil {
		view.ConfigVersion = row.ConfigVersion
		view.UpdatedBy = row.UpdatedBy
		if !row.UpdatedAt.IsZero() {
			view.UpdatedAt = row.UpdatedAt.UTC().Format(time.RFC3339)
		}
	}
	return view, nil
}

// dingTalkConfigSourceLabel normalizes the deployment's config source for
// the management view ("file" / "database").
func (s *userService) dingTalkConfigSourceLabel(ctx context.Context) string {
	if s.dingTalkDatabaseManaged(ctx) {
		return config.DingTalkConfigSourceDatabase
	}
	return config.DingTalkConfigSourceFile
}

// UpdateDingTalkAdminConfig stores the UI-managed settings row. Non-secret
// fields are replaced wholesale (an empty value simply means "fall back to
// env" at resolution time); the secret keeps its current ciphertext unless a
// replacement is supplied or clear_secret is set. Saving an ENABLED module
// without corp id / app key / any usable secret is rejected so the switch
// never produces a module that cannot log anybody in.
func (s *userService) UpdateDingTalkAdminConfig(
	ctx context.Context,
	update *types.DingTalkAdminConfigUpdate,
) (*types.DingTalkAdminConfig, error) {
	if s.dingTalkDatabaseManaged(ctx) {
		return s.writeDingTalkAdminConfig(ctx, update)
	}
	// File-managed deployments keep env/YAML authoritative; the settings UI
	// is read-only for this module (same contract as the directory module).
	// Adoption below is the one-shot path out of that state.
	return nil, ErrDingTalkConfigFileManaged
}

// ErrDingTalkConfigAlreadyManaged rejects adoption when the settings row
// already exists (the UI is already authoritative).
var ErrDingTalkConfigAlreadyManaged = errors.New(
	"dingtalk configuration is already managed in the settings UI")

// AdoptDingTalkEnvConfig copies the current env/YAML configuration —
// including the app secret, re-sealed for storage — into the settings row,
// flipping the module to database management in one click without a restart.
// DINGTALK_AUTH_CONFIG_SOURCE=file remains the hard rollback switch and
// refuses the adoption; the env values can be cleaned up afterwards.
func (s *userService) AdoptDingTalkEnvConfig(
	ctx context.Context,
) (*types.DingTalkAdminConfig, error) {
	if s.dingtalkRepo == nil {
		return nil, errors.New("dingtalk auth settings repository is not configured")
	}
	env := s.dingTalkEnvConfig()
	if env.ConfigSource == config.DingTalkConfigSourceFile {
		return nil, ErrDingTalkConfigFileManaged
	}
	if s.dingTalkDatabaseManaged(ctx) {
		return nil, ErrDingTalkConfigAlreadyManaged
	}
	if strings.TrimSpace(env.CorpID) == "" || strings.TrimSpace(env.AppKey) == "" {
		return nil, fmt.Errorf(
			"%w: environment configuration lacks corp_id/app_key to adopt", ErrInvalidDingTalkConfig)
	}

	row := &types.DingTalkAuthSettings{
		ID:          1,
		Enabled:     env.Enable,
		CorpID:      strings.TrimSpace(env.CorpID),
		AppKey:      strings.TrimSpace(env.AppKey),
		AgentID:     strings.TrimSpace(env.AgentID),
		DisplayName: strings.TrimSpace(env.ProviderDisplayName),
		RedirectURL: strings.TrimSpace(env.RedirectURL),
	}
	if secret := strings.TrimSpace(env.AppSecret); secret != "" {
		key := secutils.GetAESKey()
		if len(key) != 32 {
			return nil, ErrDirectoryEncryptionKey
		}
		ciphertext, err := secutils.EncryptAESGCM(secret, key)
		if err != nil {
			return nil, fmt.Errorf("encrypt dingtalk app secret: %w", err)
		}
		row.AppSecretCiphertext = ciphertext
	}
	if env.Enable && row.AppSecretCiphertext == "" {
		return nil, fmt.Errorf(
			"%w: environment has no app secret to adopt for an enabled module", ErrInvalidDingTalkConfig)
	}

	operatorID, _ := types.UserIDFromContext(ctx)
	row.UpdatedBy = operatorID
	row.ConfigVersion = 1
	if err := s.dingtalkRepo.SaveSettings(ctx, row); err != nil {
		return nil, err
	}
	s.invalidateDingTalkConfigCache()
	s.dingTalkAudit(ctx, &types.DingTalkAuthEvent{
		EventType: types.DingTalkEventConfigUpdate,
		Detail: fmt.Sprintf("adopted environment configuration (enabled=%t, secret=%t) operator=%s",
			env.Enable, row.AppSecretCiphertext != "", operatorID),
	})
	return s.GetDingTalkAdminConfig(ctx)
}

func (s *userService) writeDingTalkAdminConfig(
	ctx context.Context,
	update *types.DingTalkAdminConfigUpdate,
) (*types.DingTalkAdminConfig, error) {
	if update == nil {
		return nil, fmt.Errorf("%w: empty update", ErrInvalidDingTalkConfig)
	}
	if s.dingtalkRepo == nil {
		return nil, errors.New("dingtalk auth settings repository is not configured")
	}
	before, err := s.dingTalkEffectiveConfig(ctx)
	if err != nil {
		return nil, err
	}
	if before == nil {
		before = &config.DingTalkAuthConfig{}
	}
	existing, err := s.dingtalkRepo.GetSettings(ctx)
	if err != nil {
		return nil, err
	}

	candidate := &config.DingTalkAuthConfig{
		Enable:              update.Enabled,
		CorpID:              strings.TrimSpace(update.CorpID),
		AppKey:              strings.TrimSpace(update.AppKey),
		AgentID:             strings.TrimSpace(update.AgentID),
		ProviderDisplayName: strings.TrimSpace(update.DisplayName),
		RedirectURL:         strings.TrimSpace(update.RedirectURL),
		AppSecret:           before.AppSecret,
	}
	row := &types.DingTalkAuthSettings{
		ID:          1,
		Enabled:     update.Enabled,
		CorpID:      candidate.CorpID,
		AppKey:      candidate.AppKey,
		AgentID:     candidate.AgentID,
		DisplayName: candidate.ProviderDisplayName,
		RedirectURL: candidate.RedirectURL,
	}
	switch {
	case update.ClearSecret:
		// Explicitly drop the stored secret; effective resolution falls back
		// to the env value (which may be empty — validated below).
		row.AppSecretCiphertext = ""
		candidate.AppSecret = s.dingTalkEnvConfig().AppSecret
	case strings.TrimSpace(update.AppSecret) != "":
		key := secutils.GetAESKey()
		if len(key) != 32 {
			return nil, ErrDirectoryEncryptionKey
		}
		ciphertext, err := secutils.EncryptAESGCM(strings.TrimSpace(update.AppSecret), key)
		if err != nil {
			return nil, fmt.Errorf("encrypt dingtalk app secret: %w", err)
		}
		row.AppSecretCiphertext = ciphertext
		candidate.AppSecret = strings.TrimSpace(update.AppSecret)
	default:
		if existing != nil {
			row.AppSecretCiphertext = existing.AppSecretCiphertext
		}
	}

	// "Enabled must be usable" (same contract as the directory bind
	// password): corp id, app key and a secret from some source are all
	// required before the switch can be saved on.
	if candidate.Enable {
		if candidate.CorpID == "" || candidate.AppKey == "" {
			return nil, fmt.Errorf(
				"%w: corp_id and app_key are required when dingtalk auth is enabled",
				ErrInvalidDingTalkConfig)
		}
		if strings.TrimSpace(candidate.AppSecret) == "" {
			return nil, fmt.Errorf(
				"%w: an app secret from the database or the environment is required when dingtalk auth is enabled",
				ErrInvalidDingTalkConfig)
		}
	}

	operatorID, _ := types.UserIDFromContext(ctx)
	row.UpdatedBy = operatorID
	row.ConfigVersion = 1
	if existing != nil {
		row.ConfigVersion = existing.ConfigVersion + 1
	}
	if err := s.dingtalkRepo.SaveSettings(ctx, row); err != nil {
		return nil, err
	}
	s.invalidateDingTalkConfigCache()

	s.dingTalkAudit(ctx, &types.DingTalkAuthEvent{
		EventType: types.DingTalkEventConfigUpdate,
		Detail: fmt.Sprintf("changed=%s operator=%s",
			strings.Join(dingTalkChangedFields(before, candidate, existing, row), ","),
			operatorID),
	})
	return s.GetDingTalkAdminConfig(ctx)
}

// dingTalkChangedFields lists which EFFECTIVE fields a save touched. Names
// only — values (especially the secret) never enter the audit trail. The
// secret counts as changed when its provenance or value changed; for the
// stored secret that is detected via the ciphertext.
func dingTalkChangedFields(
	before, after *config.DingTalkAuthConfig,
	existing *types.DingTalkAuthSettings,
	row *types.DingTalkAuthSettings,
) []string {
	changed := make([]string, 0, 7)
	appendIf := func(name string, differs bool) {
		if differs {
			changed = append(changed, name)
		}
	}
	appendIf("enabled", before.Enable != after.Enable)
	appendIf("corp_id", before.CorpID != after.CorpID)
	appendIf("app_key", before.AppKey != after.AppKey)
	appendIf("agent_id", before.AgentID != after.AgentID)
	appendIf("display_name", before.ProviderDisplayName != after.ProviderDisplayName)
	appendIf("redirect_url", before.RedirectURL != after.RedirectURL)
	previousCiphertext := ""
	if existing != nil {
		previousCiphertext = existing.AppSecretCiphertext
	}
	appendIf("app_secret", previousCiphertext != row.AppSecretCiphertext)
	return changed
}

// TestDingTalkConnection verifies candidate credentials against DingTalk's
// enterprise access-token endpoint. Fields omitted from the candidate fall
// back to the current effective values, so the administrator can test the
// form as edited without saving first (directory-module interaction).
// File-managed deployments ignore echoed form values and test the deployment
// configuration itself, mirroring directoryRuntimeService.TestConnection.
func (s *userService) TestDingTalkConnection(
	ctx context.Context,
	candidate *types.DingTalkAdminConfigUpdate,
) (*types.DingTalkTestResult, error) {
	effective, err := s.dingTalkEffectiveConfig(ctx)
	if err != nil {
		return nil, err
	}
	if effective == nil {
		effective = &config.DingTalkAuthConfig{}
	}
	probe := *effective
	if candidate != nil && s.dingTalkDatabaseManaged(ctx) {
		probe.CorpID = dingTalkOverlay(probe.CorpID, strings.TrimSpace(candidate.CorpID))
		probe.AppKey = dingTalkOverlay(probe.AppKey, strings.TrimSpace(candidate.AppKey))
		probe.AgentID = dingTalkOverlay(probe.AgentID, strings.TrimSpace(candidate.AgentID))
		probe.ProviderDisplayName = dingTalkOverlay(
			probe.ProviderDisplayName, strings.TrimSpace(candidate.DisplayName))
		probe.RedirectURL = dingTalkOverlay(probe.RedirectURL, strings.TrimSpace(candidate.RedirectURL))
		if secret := strings.TrimSpace(candidate.AppSecret); secret != "" {
			probe.AppSecret = secret
		}
	}
	if strings.TrimSpace(probe.CorpID) == "" || strings.TrimSpace(probe.AppKey) == "" ||
		strings.TrimSpace(probe.AppSecret) == "" {
		return nil, fmt.Errorf(
			"%w: corp_id, app_key and an app secret are required to test the connection",
			ErrInvalidDingTalkConfig)
	}

	result := &types.DingTalkTestResult{}
	switch err := s.dingTalkNewClient(&probe).VerifyCredentials(ctx); {
	case err == nil:
		result.OK = true
		result.Message = fmt.Sprintf(
			"DingTalk accepted the app credentials for corp %s",
			strings.TrimSpace(probe.CorpID))
	case errors.Is(err, ErrDingTalkConfig):
		return nil, fmt.Errorf(
			"%w: corp_id, app_key and an app secret are required to test the connection",
			ErrInvalidDingTalkConfig)
	default:
		var credentialErr *DingTalkCredentialError
		if errors.As(err, &credentialErr) {
			result.ErrorCode = credentialErr.Code
			result.Message = credentialErr.Message
		} else {
			// Transport-level failure: no error code, generic message that
			// never echoes credentials or endpoint details.
			result.Message = "DingTalk API is unreachable; the credentials were not verified"
		}
	}
	s.dingTalkAudit(ctx, &types.DingTalkAuthEvent{
		EventType: types.DingTalkEventConfigTest,
		Detail:    fmt.Sprintf("ok=%t", result.OK),
	})
	return result, nil
}

// BuildDingTalkAuthorizeURL composes the DingTalk authorization page URL
// (the page renders the QR code) and the signed state bound to the caller's
// browser via the dingtalk_state cookie. The nonce never leaves the server
// except through that HttpOnly cookie.
func (s *userService) BuildDingTalkAuthorizeURL(
	ctx context.Context,
	callbackURL string,
) (*types.DingTalkAuthURLResponse, error) {
	cfg, client, err := s.dingTalkRuntime(ctx)
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
	cfg, client, err := s.dingTalkRuntime(ctx)
	if err != nil {
		return nil, err
	}

	// The authorization code is one-time; never log it or the tokens below.
	userAccessToken, err := client.ExchangeUserAccessToken(ctx, code)
	if err != nil {
		return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectUnavailable, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: types.DingTalkRejectUnavailable, Detail: truncateDingTalkDetail(fmt.Sprintf("exchange user access token failed: %v", err)),
		})
	}
	unionID, corpID, nick, err := client.GetUserContactMe(ctx, userAccessToken)
	if err != nil {
		return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectUnavailable, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: types.DingTalkRejectUnavailable, Detail: truncateDingTalkDetail(fmt.Sprintf("load contact profile failed: %v", err)),
		})
	}
	// AC-03 enterprise lock: a token that carries a corp id must carry OURS.
	// An empty corp id (DingTalk omits it for some permission/scope
	// combinations of /contact/users/me) is not a mismatch — enterprise
	// membership is still proven authoritatively by the corp-token
	// unionId→userid resolution right below, which fails for any account
	// outside this enterprise.
	if corpID != "" && corpID != cfg.CorpID {
		return nil, s.rejectDingTalkLogin(ctx, types.DingTalkRejectWrongCorp, types.DingTalkAuthEvent{
			CorpID: cfg.CorpID, Reason: types.DingTalkRejectWrongCorp,
			Detail: truncateDingTalkDetail(
				fmt.Sprintf("token corp does not match configuration (received=%q)", corpID)),
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
			CorpID: cfg.CorpID, Reason: reason, Detail: truncateDingTalkDetail(fmt.Sprintf("resolve userid by unionId failed: %v", err)),
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
			Reason: reason, Detail: truncateDingTalkDetail(fmt.Sprintf("load user detail failed: %v", err)),
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
	cfg, client, err := s.dingTalkRuntime(ctx)
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
			Reason: reason, Detail: truncateDingTalkDetail(fmt.Sprintf("load user detail failed: %v", err)),
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
