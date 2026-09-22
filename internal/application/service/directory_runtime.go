package service

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/config"
	ldapdirectory "github.com/Tencent/WeKnora/internal/infrastructure/directory"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	secutils "github.com/Tencent/WeKnora/internal/utils"
)

var (
	ErrDirectoryDisabled              = errors.New("directory authentication is disabled")
	ErrDirectoryNotConfigured         = errors.New("directory authentication is not configured")
	ErrDirectoryUnavailable           = errors.New("directory is unavailable")
	ErrDirectoryIdentityUnavailable   = errors.New("directory identity is not active in the latest complete snapshot")
	ErrDirectoryMembershipMismatch    = errors.New("live directory group memberships do not match the latest complete snapshot")
	ErrDirectoryIdentityLinkRequired  = errors.New("directory identity conflicts with an existing local account; an administrator must link it explicitly")
	ErrDirectoryIdentityAlreadyLinked = errors.New("directory identity is already linked to a different user")
	ErrDirectoryEncryptionKey         = errors.New("SYSTEM_AES_KEY must contain exactly 32 bytes before a UI-managed directory credential can be saved")
)

const (
	directorySourceDatabase    = "database"
	directorySourceEnv         = "env"
	directorySourceFile        = "file"
	directorySourceDefault     = "default"
	directoryManualTrigger     = "manual"
	directoryScheduledTrigger  = "scheduled"
	directoryLoginTrigger      = "login"
	directoryMaxCABytes        = 1024 * 1024
	directoryLoginSyncCooldown = 30 * time.Second
)

type liveDirectoryAdapter interface {
	Authenticate(context.Context, string, string) (*ldapdirectory.AuthenticationResult, error)
	Sync(context.Context) (*ldapdirectory.Snapshot, error)
}

type directoryAdapterFactory func(ldapdirectory.Config) (liveDirectoryAdapter, error)

type directoryRuntimeService struct {
	config      *config.Config
	directories interfaces.DirectoryService
	repo        interfaces.DirectoryRepository
	users       interfaces.UserService
	members     interfaces.TenantMemberService
	tenants     interfaces.TenantService
	tokens      interfaces.AuthTokenRepository
	audit       interfaces.AuditLogService
	newAdapter  directoryAdapterFactory
	now         func() time.Time
	linkMu      sync.Mutex

	stateMu             sync.RWMutex
	syncing             int
	activeServer        string
	consecutiveFailures int
	startOnce           sync.Once
	stopOnce            sync.Once
	stop                chan struct{}
	wg                  sync.WaitGroup
}

func NewDirectoryRuntimeService(
	cfg *config.Config,
	directories interfaces.DirectoryService,
	repo interfaces.DirectoryRepository,
	users interfaces.UserService,
	tenants interfaces.TenantService,
	tokens interfaces.AuthTokenRepository,
	audit interfaces.AuditLogService,
	members interfaces.TenantMemberService,
) interfaces.DirectoryRuntimeService {
	runtime := newDirectoryRuntimeService(cfg, directories, repo, users, tenants, tokens, audit, func(c ldapdirectory.Config) (liveDirectoryAdapter, error) {
		return ldapdirectory.NewAdapter(c)
	})
	runtime.members = members
	return runtime
}

func newDirectoryRuntimeService(
	cfg *config.Config,
	directories interfaces.DirectoryService,
	repo interfaces.DirectoryRepository,
	users interfaces.UserService,
	tenants interfaces.TenantService,
	tokens interfaces.AuthTokenRepository,
	audit interfaces.AuditLogService,
	factory directoryAdapterFactory,
) *directoryRuntimeService {
	return &directoryRuntimeService{
		config: cfg, directories: directories, repo: repo, users: users, tenants: tenants, tokens: tokens, audit: audit,
		newAdapter: factory, now: time.Now, stop: make(chan struct{}),
	}
}

func (s *directoryRuntimeService) deploymentConfig() *config.DirectoryConfig {
	if s.config == nil || s.config.Directory == nil {
		return &config.DirectoryConfig{
			ManagementSource: config.DirectoryManagementFile,
			ID:               "default-ad", ProviderDisplayName: "Corporate directory",
			ConnectTimeout: 5 * time.Second, QueryTimeout: 10 * time.Second,
			PageSize: 500, ResultLimit: 10000, SyncInterval: 5 * time.Minute, StaleAfter: 15 * time.Minute,
			UserFilter: "(&(objectCategory=person)(objectClass=user))", GroupFilter: "(objectCategory=group)",
		}
	}
	return s.config.Directory
}

func (s *directoryRuntimeService) directoryID() string {
	id := strings.TrimSpace(s.deploymentConfig().ID)
	if id == "" {
		return "default-ad"
	}
	return id
}

func (s *directoryRuntimeService) fileManaged() bool {
	return s.deploymentConfig().ManagementSource != config.DirectoryManagementDatabase
}

func (s *directoryRuntimeService) GetConfig(ctx context.Context) (*types.DirectoryAdminConfig, error) {
	if s.fileManaged() {
		return adminConfigFromDeployment(s.deploymentConfig()), nil
	}
	directory, err := s.repo.Get(ctx, s.directoryID())
	if err != nil {
		return nil, err
	}
	if directory == nil {
		return defaultDatabaseAdminConfig(s.deploymentConfig()), nil
	}
	return adminConfigFromDirectory(directory), nil
}

func defaultDatabaseAdminConfig(deployment *config.DirectoryConfig) *types.DirectoryAdminConfig {
	result := adminConfigFromDeployment(deployment)
	result.Source = directorySourceDefault
	result.ReadOnlyFields = []string{}
	result.BindPasswordSource = ""
	result.CASource = ""
	return result
}

func adminConfigFromDeployment(deployment *config.DirectoryConfig) *types.DirectoryAdminConfig {
	servers := make([]types.DirectoryAdminServer, 0, len(deployment.Servers))
	transport := types.DirectoryTLSLDAPS
	for _, server := range deployment.Servers {
		servers = append(servers, types.DirectoryAdminServer{Address: server.URL, ServerName: server.ServerName})
		if server.TLSMode == config.DirectoryTLSStartTLS {
			transport = types.DirectoryTLSStartTLS
		}
	}
	source := directorySourceFile
	if deployment.ConfiguredFromEnvironment {
		source = directorySourceEnv
	}
	return &types.DirectoryAdminConfig{
		Enabled: deployment.Enabled, DisplayName: deployment.ProviderDisplayName,
		Servers: servers, Transport: transport, CAFile: deployment.CAFile,
		BaseDN: deployment.BaseDN, UserBaseDN: deployment.UserBaseDN, GroupBaseDN: deployment.GroupBaseDN,
		BindDN: deployment.BindDN, UserFilter: deployment.UserFilter, GroupFilter: deployment.GroupFilter,
		AllowedLoginFilter:    deployment.AllowedLoginFilter,
		LoginAttributes:       []string{"sAMAccountName", "userPrincipalName"},
		ConnectTimeoutSeconds: durationSeconds(deployment.ConnectTimeout, 5),
		QueryTimeoutSeconds:   durationSeconds(deployment.QueryTimeout, 10),
		ResultLimit:           defaultInt(deployment.ResultLimit, 10000), PageSize: defaultInt(int(deployment.PageSize), 500),
		SyncIntervalSeconds: durationSeconds(deployment.SyncInterval, 300),
		StaleAfterSeconds:   durationSeconds(deployment.StaleAfter, 900),
		HasBindPassword:     deployment.BindPassword != "", Source: source,
		ReadOnlyFields:     []string{"enabled", "display_name", "servers", "transport", "ca_file", "base_dn", "user_base_dn", "group_base_dn", "bind_dn", "bind_password", "user_filter", "group_filter", "allowed_login_filter", "login_attributes", "connect_timeout_seconds", "query_timeout_seconds", "result_limit", "page_size", "sync_interval_seconds", "stale_after_seconds"},
		BindPasswordSource: source, CASource: source,
	}
}

func adminConfigFromDirectory(directory *types.Directory) *types.DirectoryAdminConfig {
	servers := make([]types.DirectoryAdminServer, 0, len(directory.ServerURLs))
	for i, address := range directory.ServerURLs {
		serverName := ""
		if i < len(directory.ServerNames) {
			serverName = directory.ServerNames[i]
		}
		servers = append(servers, types.DirectoryAdminServer{Address: address, ServerName: serverName})
	}
	caSource := ""
	if directory.EnterpriseCAPEM != "" {
		caSource = directorySourceDatabase
	}
	return &types.DirectoryAdminConfig{
		Enabled: directory.Enabled, DisplayName: directory.Name, Servers: servers, Transport: directory.TLSMode,
		BaseDN: directory.BaseDN, UserBaseDN: directory.UserBaseDN, GroupBaseDN: directory.GroupBaseDN,
		BindDN: directory.ServiceAccountDN, UserFilter: directory.UserFilter, GroupFilter: directory.GroupFilter,
		AllowedLoginFilter:    directory.AllowedLoginFilter,
		LoginAttributes:       []string{"sAMAccountName", "userPrincipalName"},
		ConnectTimeoutSeconds: directory.ConnectTimeoutSeconds, QueryTimeoutSeconds: directory.QueryTimeoutSeconds,
		ResultLimit: directory.ResultLimit, PageSize: directory.PageSize,
		SyncIntervalSeconds: directory.SyncIntervalSeconds, StaleAfterSeconds: directory.StaleAfterSeconds,
		HasBindPassword: directory.PasswordCiphertext != "", Source: directorySourceDatabase,
		ReadOnlyFields: []string{}, BindPasswordSource: directorySourceDatabase, CASource: caSource,
	}
}

func durationSeconds(value time.Duration, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return int(value / time.Second)
}

func defaultInt(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func (s *directoryRuntimeService) UpdateConfig(ctx context.Context, update *types.DirectoryAdminConfigUpdate) (*types.DirectoryAdminConfig, error) {
	if s.fileManaged() {
		return nil, ErrDirectoryFileManaged
	}
	if update == nil {
		return nil, fmt.Errorf("%w: empty update", ErrInvalidDirectoryConfig)
	}
	existing, err := s.repo.Get(ctx, s.directoryID())
	if err != nil {
		return nil, err
	}
	directory, err := directoryFromAdminUpdate(s.directoryID(), update, existing)
	if err != nil {
		return nil, err
	}
	if update.BindPassword != nil {
		secret := *update.BindPassword
		if secret == "" {
			return nil, fmt.Errorf("%w: bind password cannot be empty", ErrInvalidDirectoryConfig)
		}
		key := secutils.GetAESKey()
		if len(key) != 32 {
			return nil, ErrDirectoryEncryptionKey
		}
		directory.PasswordCiphertext, err = secutils.EncryptAESGCM(secret, key)
		if err != nil {
			return nil, fmt.Errorf("encrypt directory bind password: %w", err)
		}
	} else if existing != nil {
		directory.PasswordCiphertext = existing.PasswordCiphertext
	}
	if directory.Enabled && directory.PasswordCiphertext == "" {
		return nil, fmt.Errorf("%w: bind password is required when enabled", ErrInvalidDirectoryConfig)
	}
	if existing == nil {
		if _, err = s.directories.Create(ctx, directory); err != nil {
			return nil, err
		}
	} else if existing.IsFileManaged() {
		// A deployment may deliberately switch authority from file to
		// database. The core CRUD service rejects file-managed edits, so this
		// one transition is validated here and written through the repository.
		if err = normalizeDirectory(directory); err == nil {
			err = s.repo.Update(ctx, directory)
		}
		if err != nil {
			return nil, err
		}
	} else {
		if _, err = s.directories.Update(ctx, directory); err != nil {
			return nil, err
		}
	}
	if existing != nil && directoryAuthenticationSettingsChanged(existing, directory) {
		s.revokeDirectorySessions(ctx, directory.ID)
	}
	s.emitAudit(ctx, types.AuditActionDirectoryConfigChanged, directory.ID, "directory", "", map[string]any{
		"enabled": directory.Enabled, "source": directorySourceDatabase,
		"changed_fields": []string{"enabled", "display_name", "servers", "transport", "ca", "search_scope", "service_account", "limits", "sync_policy"},
	})
	return s.GetConfig(ctx)
}

func directoryFromAdminUpdate(id string, update *types.DirectoryAdminConfigUpdate, existing *types.Directory) (*types.Directory, error) {
	if update.Transport == "" {
		update.Transport = types.DirectoryTLSLDAPS
	}
	urls := make(types.StringArray, 0, len(update.Servers))
	serverNames := make(types.StringArray, 0, len(update.Servers))
	for _, server := range update.Servers {
		address := strings.TrimSpace(server.Address)
		if address != "" {
			urls = append(urls, address)
			serverNames = append(serverNames, strings.TrimSpace(server.ServerName))
		}
	}
	caPEM := ""
	if existing != nil {
		caPEM = existing.EnterpriseCAPEM
	}
	if path := strings.TrimSpace(update.CAFile); path != "" {
		contents, err := readLimitedFile(path, directoryMaxCABytes)
		if err != nil {
			return nil, fmt.Errorf("read enterprise CA file: %w", err)
		}
		caPEM = string(contents)
	}
	directory := &types.Directory{
		ID: id, Name: strings.TrimSpace(update.DisplayName), Protocol: types.DirectoryProtocolAD,
		Enabled: update.Enabled, ConfigSource: types.DirectoryConfigSourceUI,
		TLSMode: update.Transport, ServerURLs: urls, ServerNames: serverNames, BaseDN: strings.TrimSpace(update.BaseDN),
		UserBaseDN: strings.TrimSpace(update.UserBaseDN), GroupBaseDN: strings.TrimSpace(update.GroupBaseDN),
		UserFilter: strings.TrimSpace(update.UserFilter), GroupFilter: strings.TrimSpace(update.GroupFilter),
		AllowedLoginFilter: strings.TrimSpace(update.AllowedLoginFilter),
		ServiceAccountDN:   strings.TrimSpace(update.BindDN), EnterpriseCAPEM: caPEM,
		ConnectTimeoutSeconds: update.ConnectTimeoutSeconds, QueryTimeoutSeconds: update.QueryTimeoutSeconds,
		PageSize: update.PageSize, ResultLimit: update.ResultLimit,
		SyncIntervalSeconds: update.SyncIntervalSeconds, StaleAfterSeconds: update.StaleAfterSeconds,
	}
	if directory.Name == "" {
		directory.Name = "Corporate directory"
	}
	if directory.UserBaseDN == "" {
		directory.UserBaseDN = directory.BaseDN
	}
	if directory.GroupBaseDN == "" {
		directory.GroupBaseDN = directory.BaseDN
	}
	if directory.UserFilter == "" {
		directory.UserFilter = "(&(objectCategory=person)(objectClass=user))"
	}
	if directory.GroupFilter == "" {
		directory.GroupFilter = "(objectCategory=group)"
	}
	return directory, nil
}

func readLimitedFile(path string, limit int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("file must be regular and no larger than %d bytes", limit)
	}
	return os.ReadFile(path)
}

func (s *directoryRuntimeService) persistedDirectory(ctx context.Context) (*types.Directory, error) {
	if s.fileManaged() {
		return s.ensureFileDirectory(ctx)
	}
	directory, err := s.repo.Get(ctx, s.directoryID())
	if err != nil {
		return nil, err
	}
	if directory == nil {
		return nil, ErrDirectoryNotConfigured
	}
	return directory, nil
}

func (s *directoryRuntimeService) ensureFileDirectory(ctx context.Context) (*types.Directory, error) {
	deployment := s.deploymentConfig()
	desired, err := directoryFromDeployment(deployment)
	if err != nil {
		return nil, err
	}
	existing, err := s.repo.Get(ctx, desired.ID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		// Do not create an empty persistence row for the default-off module.
		// Once a file-managed directory has existed, however, disabling it must
		// be persisted below so token validation cannot observe stale enabled
		// state from the previous process/configuration.
		if !desired.Enabled {
			return desired, nil
		}
		if err := s.repo.Create(ctx, desired); err != nil {
			return nil, err
		}
	} else if !sameDirectorySettings(existing, desired) {
		authChanged := directoryAuthenticationSettingsChanged(existing, desired)
		desired.SnapshotVersion = existing.SnapshotVersion
		desired.LastSuccessfulSyncAt = existing.LastSuccessfulSyncAt
		desired.LastSyncAttemptAt = existing.LastSyncAttemptAt
		desired.LastSyncError = existing.LastSyncError
		if err := s.repo.Update(ctx, desired); err != nil {
			return nil, err
		}
		if authChanged {
			s.revokeDirectorySessions(ctx, desired.ID)
		}
	}
	return s.repo.Get(ctx, desired.ID)
}

func directoryAuthenticationSettingsChanged(current, next *types.Directory) bool {
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

func directoryFromDeployment(deployment *config.DirectoryConfig) (*types.Directory, error) {
	urls := make(types.StringArray, 0, len(deployment.Servers))
	serverNames := make(types.StringArray, 0, len(deployment.Servers))
	mode := types.DirectoryTLSLDAPS
	for _, server := range deployment.Servers {
		urls = append(urls, strings.TrimSpace(server.URL))
		serverNames = append(serverNames, strings.TrimSpace(server.ServerName))
		if server.TLSMode == config.DirectoryTLSStartTLS {
			mode = types.DirectoryTLSStartTLS
		}
	}
	fingerprint, err := directorySecurityConfigFingerprint(deployment)
	if err != nil {
		return nil, err
	}
	return &types.Directory{
		ID: strings.TrimSpace(deployment.ID), Name: strings.TrimSpace(deployment.ProviderDisplayName),
		Protocol: types.DirectoryProtocolAD, Enabled: deployment.Enabled, ConfigSource: types.DirectoryConfigSourceFile,
		TLSMode: mode, ServerURLs: urls, ServerNames: serverNames, BaseDN: deployment.BaseDN, UserBaseDN: deployment.UserBaseDN,
		GroupBaseDN: deployment.GroupBaseDN, UserFilter: deployment.UserFilter, GroupFilter: deployment.GroupFilter,
		AllowedLoginFilter: deployment.AllowedLoginFilter, ServiceAccountDN: deployment.BindDN,
		SecurityConfigFingerprint: fingerprint,
		ConnectTimeoutSeconds:     durationSeconds(deployment.ConnectTimeout, 5),
		QueryTimeoutSeconds:       durationSeconds(deployment.QueryTimeout, 10),
		PageSize:                  defaultInt(int(deployment.PageSize), 500), ResultLimit: defaultInt(deployment.ResultLimit, 10000),
		SyncIntervalSeconds: durationSeconds(deployment.SyncInterval, 300), StaleAfterSeconds: durationSeconds(deployment.StaleAfter, 900),
	}, nil
}

func directorySecurityConfigFingerprint(deployment *config.DirectoryConfig) (string, error) {
	if deployment == nil {
		return "", ErrDirectoryNotConfigured
	}
	type fingerprintServer struct {
		URL        string `json:"url"`
		TLSMode    string `json:"tls_mode"`
		ServerName string `json:"server_name"`
	}
	servers := make([]fingerprintServer, 0, len(deployment.Servers))
	for _, server := range deployment.Servers {
		servers = append(servers, fingerprintServer{
			URL: strings.TrimSpace(server.URL), TLSMode: strings.TrimSpace(string(server.TLSMode)),
			ServerName: strings.TrimSpace(server.ServerName),
		})
	}
	var caPEM []byte
	if path := strings.TrimSpace(deployment.CAFile); path != "" && deployment.Enabled {
		var err error
		caPEM, err = readLimitedFile(path, 1024*1024)
		if err != nil {
			return "", fmt.Errorf("read directory CA file for security fingerprint: %w", err)
		}
	}
	payload := struct {
		Enabled            bool                `json:"enabled"`
		Servers            []fingerprintServer `json:"servers"`
		BindDN             string              `json:"bind_dn"`
		BindPassword       string              `json:"bind_password"`
		CAFile             string              `json:"ca_file"`
		CAPEM              []byte              `json:"ca_pem"`
		BaseDN             string              `json:"base_dn"`
		UserBaseDN         string              `json:"user_base_dn"`
		GroupBaseDN        string              `json:"group_base_dn"`
		UserFilter         string              `json:"user_filter"`
		GroupFilter        string              `json:"group_filter"`
		LoginFilter        string              `json:"login_filter"`
		AllowedLoginFilter string              `json:"allowed_login_filter"`
		ConnectTimeout     int64               `json:"connect_timeout_ns"`
		QueryTimeout       int64               `json:"query_timeout_ns"`
		PageSize           uint32              `json:"page_size"`
		ResultLimit        int                 `json:"result_limit"`
	}{
		Enabled: deployment.Enabled, Servers: servers,
		BindDN: strings.TrimSpace(deployment.BindDN), BindPassword: deployment.BindPassword,
		CAFile: strings.TrimSpace(deployment.CAFile), CAPEM: caPEM,
		BaseDN: strings.TrimSpace(deployment.BaseDN), UserBaseDN: strings.TrimSpace(deployment.UserBaseDN),
		GroupBaseDN: strings.TrimSpace(deployment.GroupBaseDN), UserFilter: strings.TrimSpace(deployment.UserFilter),
		GroupFilter: strings.TrimSpace(deployment.GroupFilter), LoginFilter: strings.TrimSpace(deployment.LoginFilter),
		AllowedLoginFilter: strings.TrimSpace(deployment.AllowedLoginFilter),
		ConnectTimeout:     int64(deployment.ConnectTimeout), QueryTimeout: int64(deployment.QueryTimeout),
		PageSize: deployment.PageSize, ResultLimit: deployment.ResultLimit,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode directory security fingerprint: %w", err)
	}
	// Key the digest so a database-only disclosure cannot be used as an
	// offline oracle for guessing a weak service-account password. When the
	// deployment intentionally has no stable JWT_SECRET, getJwtSecret generates
	// a process key; restart invalidates both JWTs and this fingerprint, which
	// safely forces a fresh sync and session revocation.
	mac := hmac.New(sha256.New, []byte(getJwtSecret()))
	_, _ = mac.Write(encoded)
	return fmt.Sprintf("%x", mac.Sum(nil)), nil
}

func sameDirectorySettings(a, b *types.Directory) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Name == b.Name && a.Protocol == b.Protocol && a.Enabled == b.Enabled && a.ConfigSource == b.ConfigSource &&
		a.TLSMode == b.TLSMode && strings.Join(a.ServerURLs, "\x00") == strings.Join(b.ServerURLs, "\x00") &&
		strings.Join(a.ServerNames, "\x00") == strings.Join(b.ServerNames, "\x00") &&
		a.BaseDN == b.BaseDN && a.UserBaseDN == b.UserBaseDN && a.GroupBaseDN == b.GroupBaseDN &&
		a.UserFilter == b.UserFilter && a.GroupFilter == b.GroupFilter && a.AllowedLoginFilter == b.AllowedLoginFilter &&
		a.SecurityConfigFingerprint == b.SecurityConfigFingerprint &&
		a.ServiceAccountDN == b.ServiceAccountDN && a.ConnectTimeoutSeconds == b.ConnectTimeoutSeconds &&
		a.QueryTimeoutSeconds == b.QueryTimeoutSeconds && a.PageSize == b.PageSize && a.ResultLimit == b.ResultLimit &&
		a.SyncIntervalSeconds == b.SyncIntervalSeconds && a.StaleAfterSeconds == b.StaleAfterSeconds
}

func (s *directoryRuntimeService) adapterFor(ctx context.Context, directory *types.Directory, replacementSecret string) (liveDirectoryAdapter, error) {
	if directory == nil {
		return nil, ErrDirectoryNotConfigured
	}
	bindPassword := replacementSecret
	if bindPassword == "" {
		if directory.IsFileManaged() {
			bindPassword = s.deploymentConfig().BindPassword
		} else {
			if directory.PasswordCiphertext != "" && !strings.HasPrefix(directory.PasswordCiphertext, secutils.EncPrefix) {
				return nil, fmt.Errorf("%w: database bind credential is not encrypted", ErrInvalidDirectoryConfig)
			}
			var err error
			bindPassword, err = secutils.DecryptStoredSecret(directory.PasswordCiphertext)
			if err != nil {
				return nil, fmt.Errorf("decrypt directory bind password: %w", err)
			}
		}
	}
	controllers := make([]ldapdirectory.Controller, 0, len(directory.ServerURLs))
	for i, address := range directory.ServerURLs {
		controller := ldapdirectory.Controller{URL: address, TLSMode: ldapdirectory.TLSMode(directory.TLSMode)}
		if i < len(directory.ServerNames) {
			controller.ServerName = directory.ServerNames[i]
		}
		if directory.IsFileManaged() && i < len(s.deploymentConfig().Servers) {
			controller.ServerName = s.deploymentConfig().Servers[i].ServerName
			if mode := s.deploymentConfig().Servers[i].TLSMode; mode != "" {
				controller.TLSMode = ldapdirectory.TLSMode(mode)
			}
		}
		controllers = append(controllers, controller)
	}
	userFilter := directory.UserFilter
	if allowed := strings.TrimSpace(directory.AllowedLoginFilter); allowed != "" {
		userFilter = combineLDAPFilters(userFilter, allowed)
	}
	// Database-managed directories deliberately keep the fixed, audited
	// sAMAccountName/UPN selector supplied by Adapter. File-managed deployments
	// may opt into a custom template validated at startup.
	loginFilter := ""
	if directory.IsFileManaged() {
		loginFilter = s.deploymentConfig().LoginFilter
	}
	tlsOptions := ldapdirectory.TLSOptions{}
	if directory.IsFileManaged() {
		tlsOptions.CAFile = s.deploymentConfig().CAFile
	} else if strings.TrimSpace(directory.EnterpriseCAPEM) != "" {
		tlsOptions.CAPEM = []byte(directory.EnterpriseCAPEM)
	}
	pageSize := directory.PageSize
	if pageSize <= 0 {
		pageSize = 500
	}
	limit := directory.ResultLimit
	if limit <= 0 {
		limit = 10000
	}
	maxPages := limit/pageSize + 2
	return s.newAdapter(ldapdirectory.Config{
		DirectoryID: directory.ID, Controllers: controllers,
		BindDN: directory.ServiceAccountDN, BindPassword: bindPassword,
		UserBaseDN: directory.UserBaseDN, GroupBaseDN: directory.GroupBaseDN,
		UserFilter: userFilter, GroupFilter: directory.GroupFilter, LoginFilter: loginFilter,
		ConnectTimeout: time.Duration(directory.ConnectTimeoutSeconds) * time.Second,
		QueryTimeout:   time.Duration(directory.QueryTimeoutSeconds) * time.Second,
		PageSize:       uint32(pageSize), MaxPages: maxPages, ResultLimit: limit, TLS: tlsOptions,
	})
}

func combineLDAPFilters(base, allowed string) string {
	base = strings.TrimSpace(base)
	allowed = strings.TrimSpace(allowed)
	if base == "" {
		return allowed
	}
	if allowed == "" {
		return base
	}
	return "(&" + base + allowed + ")"
}

func (s *directoryRuntimeService) GetStatus(ctx context.Context) (*types.DirectoryHealth, error) {
	directory, err := s.persistedDirectory(ctx)
	if errors.Is(err, ErrDirectoryNotConfigured) {
		return &types.DirectoryHealth{}, nil
	}
	if err != nil {
		return nil, err
	}
	s.stateMu.RLock()
	activeServer, failures, syncing := s.activeServer, s.consecutiveFailures, s.syncing > 0
	s.stateMu.RUnlock()
	now := s.now().UTC()
	status := &types.DirectoryHealth{
		Enabled: directory.Enabled, Available: directory.IsFresh(now) && directory.LastSyncError == "",
		AccessPaused: directory.Enabled && !directory.IsFresh(now), Syncing: syncing,
		ActiveServer: activeServer, LastAttemptAt: directory.LastSyncAttemptAt,
		LastSuccessAt: directory.LastSuccessfulSyncAt, LastError: directory.LastSyncError,
		ConsecutiveFailures: failures,
	}
	if directory.Enabled {
		base := directory.LastSyncAttemptAt
		if base == nil {
			base = directory.LastSuccessfulSyncAt
		}
		if base != nil {
			next := base.Add(time.Duration(defaultInt(directory.SyncIntervalSeconds, 300)) * time.Second)
			status.NextSyncAt = &next
		}
	}
	return status, nil
}

func (s *directoryRuntimeService) TestConnection(ctx context.Context, candidate *types.DirectoryAdminConfigUpdate) (*types.DirectoryTestResult, error) {
	directory, replacement, err := s.testCandidate(ctx, candidate)
	if err != nil {
		return nil, err
	}
	adapter, err := s.adapterFor(ctx, directory, replacement)
	if err != nil {
		return nil, err
	}
	started := s.now()
	snapshot, err := adapter.Sync(ctx)
	latency := s.now().Sub(started).Milliseconds()
	if err != nil {
		return &types.DirectoryTestResult{OK: false, LatencyMS: latency, Message: err.Error()}, nil
	}
	s.setActiveServer(snapshot.ControllerURL)
	return &types.DirectoryTestResult{OK: true, Server: snapshot.ControllerURL, LatencyMS: latency, Message: "Connection and paged search succeeded"}, nil
}

func (s *directoryRuntimeService) testCandidate(ctx context.Context, candidate *types.DirectoryAdminConfigUpdate) (*types.Directory, string, error) {
	current, err := s.persistedDirectory(ctx)
	if err != nil && !errors.Is(err, ErrDirectoryNotConfigured) {
		return nil, "", err
	}
	if candidate == nil {
		if current == nil {
			return nil, "", ErrDirectoryNotConfigured
		}
		return current, "", nil
	}
	if s.fileManaged() {
		// Deployment-owned fields are immutable in the UI. Ignore the echoed
		// read-only form values so the mounted password and per-controller
		// server names remain authoritative during a connection test.
		return current, "", nil
	}
	directory, err := directoryFromAdminUpdate(s.directoryID(), candidate, current)
	if err != nil {
		return nil, "", err
	}
	if current != nil {
		directory.PasswordCiphertext = current.PasswordCiphertext
	}
	replacement := ""
	if candidate.BindPassword != nil {
		replacement = *candidate.BindPassword
	}
	return directory, replacement, nil
}

func (s *directoryRuntimeService) liveSnapshot(ctx context.Context) (*ldapdirectory.Snapshot, *types.Directory, error) {
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !directory.Enabled {
		return nil, directory, ErrDirectoryDisabled
	}
	adapter, err := s.adapterFor(ctx, directory, "")
	if err != nil {
		return nil, directory, err
	}
	snapshot, err := adapter.Sync(ctx)
	if err != nil {
		return nil, directory, err
	}
	s.setActiveServer(snapshot.ControllerURL)
	return snapshot, directory, nil
}

func normalizeSearchLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func containsFold(query string, values ...string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return true
	}
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), query) {
			return true
		}
	}
	return false
}

func (s *directoryRuntimeService) QueryUsers(ctx context.Context, query string, limit, offset int) (*types.DirectoryObjectSearchResult, error) {
	snapshot, _, err := s.liveSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	persisted, err := s.listAllIdentities(ctx, snapshot.DirectoryID)
	if err != nil {
		return nil, err
	}
	identityByGUID := make(map[string]*types.DirectoryIdentity, len(persisted))
	for _, identity := range persisted {
		identityByGUID[identity.ObjectGUID] = identity
	}
	items := make([]types.DirectoryObjectSummary, 0)
	for _, user := range snapshot.Users {
		if !containsFold(query, user.SAMAccountName, user.UserPrincipalName, user.DisplayName, user.Email, user.DN) {
			continue
		}
		summary := types.DirectoryObjectSummary{
			DirectoryID: snapshot.DirectoryID, ObjectGUID: user.ObjectGUID, SID: user.SID, DN: user.DN,
			DisplayName: user.DisplayName, Email: user.Email, AccountName: user.SAMAccountName,
			UserPrincipalName: user.UserPrincipalName, Disabled: !user.Enabled,
		}
		if identity := identityByGUID[user.ObjectGUID]; identity != nil {
			summary.IdentityID = identity.ID
			summary.Status = string(identity.Status)
			if identity.UserID != nil {
				summary.LinkedUserID = *identity.UserID
			}
		}
		items = append(items, summary)
	}
	total := len(items)
	sort.Slice(items, func(i, j int) bool { return directoryObjectLess(items[i], items[j]) })
	items, more := directoryPage(items, limit, offset)
	return &types.DirectoryObjectSearchResult{Items: items, Total: total, Truncated: more}, nil
}

func (s *directoryRuntimeService) QueryGroups(ctx context.Context, query string, limit, offset int) (*types.DirectoryGroupSearchResult, error) {
	snapshot, _, err := s.liveSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	direct, effective, parents := map[string]int{}, map[string]int{}, map[string]int{}
	directSeen, effectiveSeen := map[string]struct{}{}, map[string]struct{}{}
	for _, member := range snapshot.DirectMemberships {
		if member.Source != ldapdirectory.MembershipDirect {
			continue
		}
		key := member.UserGUID + "\x00" + member.GroupGUID
		if _, ok := directSeen[key]; !ok {
			directSeen[key] = struct{}{}
			direct[member.GroupGUID]++
		}
	}
	for _, member := range snapshot.EffectiveMemberships {
		key := member.UserGUID + "\x00" + member.GroupGUID
		if _, ok := effectiveSeen[key]; !ok {
			effectiveSeen[key] = struct{}{}
			effective[member.GroupGUID]++
		}
	}
	for _, edge := range snapshot.GroupMemberships {
		parents[edge.MemberGroupGUID]++
	}
	items := make([]types.DirectoryGroupSummary, 0)
	for _, group := range snapshot.Groups {
		if !containsFold(query, group.SAMAccountName, group.DisplayName, group.Email, group.DN) {
			continue
		}
		items = append(items, types.DirectoryGroupSummary{
			DirectoryObjectSummary: types.DirectoryObjectSummary{
				DirectoryID: snapshot.DirectoryID, ObjectGUID: group.ObjectGUID, SID: group.SID, DN: group.DN,
				DisplayName: group.DisplayName, Email: group.Email, AccountName: group.SAMAccountName,
			},
			DirectMemberCount: direct[group.ObjectGUID], EffectiveMemberCount: effective[group.ObjectGUID],
			ParentGroupCount: parents[group.ObjectGUID],
		})
	}
	total := len(items)
	sort.Slice(items, func(i, j int) bool {
		return directoryObjectLess(items[i].DirectoryObjectSummary, items[j].DirectoryObjectSummary)
	})
	items, more := directoryPage(items, limit, offset)
	return &types.DirectoryGroupSearchResult{Items: items, Total: total, Truncated: more}, nil
}

func convertDirectorySnapshot(snapshot *ldapdirectory.Snapshot, started time.Time) (*types.DirectorySnapshot, []string, error) {
	if snapshot == nil {
		return nil, nil, ErrIncompleteDirectorySync
	}
	result := &types.DirectorySnapshot{
		DirectoryID: snapshot.DirectoryID, Complete: true, PaginationComplete: true, StartedAt: started,
		Identities:  make([]types.DirectoryIdentitySnapshot, 0, len(snapshot.Users)),
		Groups:      make([]types.DirectoryGroupSnapshot, 0, len(snapshot.Groups)),
		GroupEdges:  make([]types.DirectoryGroupEdgeSnapshot, 0, len(snapshot.GroupMemberships)),
		Memberships: make([]types.DirectoryMembershipSnapshot, 0, len(snapshot.DirectMemberships)),
	}
	for _, user := range snapshot.Users {
		primarySID, err := ldapdirectory.PrimaryGroupSID(user.SID, user.PrimaryGroupRID)
		if err != nil {
			return nil, nil, fmt.Errorf("derive primary group for %s: %w", user.ObjectGUID, err)
		}
		result.Identities = append(result.Identities, types.DirectoryIdentitySnapshot{
			ObjectGUID: user.ObjectGUID, ObjectSID: user.SID, DN: user.DN,
			SAMAccountName: user.SAMAccountName, UPN: user.UserPrincipalName,
			DisplayName: user.DisplayName, Email: user.Email, PrimaryGroupSID: primarySID, Enabled: user.Enabled,
		})
	}
	for _, group := range snapshot.Groups {
		result.Groups = append(result.Groups, types.DirectoryGroupSnapshot{
			ObjectGUID: group.ObjectGUID, ObjectSID: group.SID, DN: group.DN,
			SAMAccountName: group.SAMAccountName, DisplayName: group.DisplayName, Email: group.Email,
		})
	}
	for _, edge := range snapshot.GroupMemberships {
		result.GroupEdges = append(result.GroupEdges, types.DirectoryGroupEdgeSnapshot{
			ParentGroupObjectGUID: edge.ParentGroupGUID, ChildGroupObjectGUID: edge.MemberGroupGUID,
		})
	}
	for _, membership := range snapshot.DirectMemberships {
		primary := membership.Source == ldapdirectory.MembershipPrimary
		source := types.DirectoryMembershipDirect
		if primary {
			source = types.DirectoryMembershipPrimary
		}
		result.Memberships = append(result.Memberships, types.DirectoryMembershipSnapshot{
			GroupObjectGUID: membership.GroupGUID, UserObjectGUID: membership.UserGUID,
			Direct: true, Primary: primary, Depth: 0, Source: source,
		})
	}
	warnings := make([]string, 0)
	if len(snapshot.UnresolvedMembers) > 0 {
		warnings = append(warnings, fmt.Sprintf("%d group members were outside the selected user/group scopes", len(snapshot.UnresolvedMembers)))
	}
	return result, warnings, nil
}

func (s *directoryRuntimeService) PreviewSync(ctx context.Context) (*types.DirectorySyncPreview, error) {
	started := s.now().UTC()
	snapshot, _, err := s.liveSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	converted, warnings, err := convertDirectorySnapshot(snapshot, started)
	if err != nil {
		return nil, err
	}
	normalized, err := normalizeDirectorySnapshot(converted)
	if err != nil {
		return nil, err
	}
	preview, err := s.compareSnapshot(ctx, normalized)
	if err != nil {
		return nil, err
	}
	preview.Warnings = warnings
	preview.Complete = true
	return preview, nil
}

func (s *directoryRuntimeService) compareSnapshot(ctx context.Context, snapshot *types.DirectorySnapshot) (*types.DirectorySyncPreview, error) {
	identities, err := s.listAllIdentities(ctx, snapshot.DirectoryID)
	if err != nil {
		return nil, err
	}
	groups, err := s.listAllGroups(ctx, snapshot.DirectoryID)
	if err != nil {
		return nil, err
	}
	preview := &types.DirectorySyncPreview{}
	existingUsers := make(map[string]*types.DirectoryIdentity, len(identities))
	for _, identity := range identities {
		existingUsers[identity.ObjectGUID] = identity
	}
	seenUsers := make(map[string]struct{}, len(snapshot.Identities))
	for _, incoming := range snapshot.Identities {
		seenUsers[incoming.ObjectGUID] = struct{}{}
		existing := existingUsers[incoming.ObjectGUID]
		if existing == nil {
			preview.Users.Create++
		} else if !incoming.Enabled || identityChanged(existing, incoming) {
			preview.Users.Update++
		} else {
			preview.Users.Unchanged++
		}
	}
	for guid, existing := range existingUsers {
		if _, ok := seenUsers[guid]; !ok && existing.Status == types.DirectoryObjectActive {
			preview.Users.Disable++
		}
	}
	existingGroups := make(map[string]*types.DirectoryGroup, len(groups))
	for _, group := range groups {
		existingGroups[group.ObjectGUID] = group
	}
	seenGroups := make(map[string]struct{}, len(snapshot.Groups))
	for _, incoming := range snapshot.Groups {
		seenGroups[incoming.ObjectGUID] = struct{}{}
		existing := existingGroups[incoming.ObjectGUID]
		if existing == nil {
			preview.Groups.Create++
		} else if groupChanged(existing, incoming) {
			preview.Groups.Update++
		} else {
			preview.Groups.Unchanged++
		}
	}
	for guid, existing := range existingGroups {
		if _, ok := seenGroups[guid]; !ok && existing.Status == types.DirectoryObjectActive {
			preview.Groups.Remove++
		}
	}
	incomingMemberships := make(map[string]struct{}, len(snapshot.Memberships))
	for _, member := range snapshot.Memberships {
		incomingMemberships[member.UserObjectGUID+"\x00"+member.GroupObjectGUID] = struct{}{}
	}
	existingMemberships := make(map[string]struct{})
	identityGUIDByID := make(map[string]string, len(identities))
	for _, identity := range identities {
		identityGUIDByID[identity.ID] = identity.ObjectGUID
	}
	for _, group := range groups {
		members, listErr := s.repo.ListGroupMemberships(ctx, group.ID)
		if listErr != nil {
			return nil, listErr
		}
		for _, member := range members {
			existingMemberships[identityGUIDByID[member.IdentityID]+"\x00"+group.ObjectGUID] = struct{}{}
		}
	}
	for key := range incomingMemberships {
		if _, ok := existingMemberships[key]; ok {
			preview.Memberships.Unchanged++
		} else {
			preview.Memberships.Add++
		}
	}
	for key := range existingMemberships {
		if _, ok := incomingMemberships[key]; !ok {
			preview.Memberships.Remove++
		}
	}
	return preview, nil
}

func identityChanged(existing *types.DirectoryIdentity, incoming types.DirectoryIdentitySnapshot) bool {
	expectedStatus := types.DirectoryObjectActive
	if !incoming.Enabled {
		expectedStatus = types.DirectoryObjectDisabled
	}
	return existing.ObjectSID != incoming.ObjectSID || existing.DN != incoming.DN ||
		existing.SAMAccountName != incoming.SAMAccountName || existing.UPN != incoming.UPN ||
		existing.DisplayName != incoming.DisplayName || existing.Email != incoming.Email ||
		existing.PrimaryGroupSID != incoming.PrimaryGroupSID || existing.Status != expectedStatus
}

func groupChanged(existing *types.DirectoryGroup, incoming types.DirectoryGroupSnapshot) bool {
	return existing.ObjectSID != incoming.ObjectSID || existing.DN != incoming.DN ||
		existing.SAMAccountName != incoming.SAMAccountName || existing.DisplayName != incoming.DisplayName ||
		existing.Email != incoming.Email || existing.Status != types.DirectoryObjectActive
}

func (s *directoryRuntimeService) listAllIdentities(ctx context.Context, directoryID string) ([]*types.DirectoryIdentity, error) {
	result := make([]*types.DirectoryIdentity, 0)
	for offset := 0; ; offset += 1000 {
		page, err := s.repo.ListIdentities(ctx, directoryID, offset, 1000)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if len(page) < 1000 {
			return result, nil
		}
	}
}

func (s *directoryRuntimeService) listAllGroups(ctx context.Context, directoryID string) ([]*types.DirectoryGroup, error) {
	result := make([]*types.DirectoryGroup, 0)
	for offset := 0; ; offset += 1000 {
		page, err := s.repo.ListGroups(ctx, directoryID, "", offset, 1000)
		if err != nil {
			return nil, err
		}
		result = append(result, page...)
		if len(page) < 1000 {
			return result, nil
		}
	}
}

func (s *directoryRuntimeService) ManualSync(ctx context.Context) (*types.DirectorySyncRunView, error) {
	return s.runSync(ctx, directoryManualTrigger)
}

func (s *directoryRuntimeService) runSync(ctx context.Context, trigger string) (*types.DirectorySyncRunView, error) {
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return nil, err
	}
	if !directory.Enabled {
		return nil, ErrDirectoryDisabled
	}
	started := s.now().UTC()
	adapter, err := s.adapterFor(ctx, directory, "")
	if err != nil {
		s.recordDirectorySyncFailure(ctx, directory, trigger, started, err)
		return nil, err
	}
	s.setSyncing(true)
	defer s.setSyncing(false)
	var raw *ldapdirectory.Snapshot
	result, err := s.directories.RunSync(ctx, directory.ID, func(fetchCtx context.Context) (*types.DirectorySnapshot, error) {
		var fetchErr error
		raw, fetchErr = adapter.Sync(fetchCtx)
		if fetchErr != nil {
			return nil, fetchErr
		}
		converted, _, convertErr := convertDirectorySnapshot(raw, started)
		if converted != nil {
			converted.Trigger = types.DirectorySyncTrigger(trigger)
			converted.ExpectedConfigVersion = directory.ConfigVersion
		}
		return converted, convertErr
	})
	if err != nil {
		if !errors.Is(err, ErrDirectorySyncInProgress) {
			s.recordDirectorySyncFailure(ctx, directory, trigger, started, err)
		}
		return nil, err
	}
	s.clearFailures()
	if raw != nil {
		s.setActiveServer(raw.ControllerURL)
	}
	s.revokeSuspendedIdentitySessions(ctx, directory.ID)
	s.emitAudit(ctx, types.AuditActionDirectorySyncCompleted, directory.ID, "directory", "", map[string]any{
		"trigger": trigger, "users_seen": result.SyncRun.UserCount, "groups_seen": result.SyncRun.GroupCount,
		"memberships_seen": result.SyncRun.MembershipCount, "snapshot_version": result.SyncRun.SnapshotVersion,
	})
	return syncRunView(result.SyncRun, trigger), nil
}

func (s *directoryRuntimeService) recordDirectorySyncFailure(
	requestCtx context.Context,
	directory *types.Directory,
	trigger string,
	started time.Time,
	syncErr error,
) {
	// Request cancellation must not erase operational evidence. Bound the
	// detached write so a database outage still cannot leak a goroutine.
	failureCtx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), 5*time.Second)
	defer cancel()
	if s.directories != nil {
		_, _ = s.directories.RecordSyncFailure(
			failureCtx, directory.ID, directorySyncErrorCode(syncErr), syncErr.Error(), started,
			directory.SnapshotVersion, directory.ConfigVersion, types.DirectorySyncTrigger(trigger),
		)
	}
	s.recordFailure()
	s.emitAudit(failureCtx, types.AuditActionDirectorySyncFailed, directory.ID, "directory", "", map[string]any{
		"trigger": trigger, "error_code": directorySyncErrorCode(syncErr),
	})
}

func directorySyncErrorCode(err error) string {
	switch {
	case errors.Is(err, ldapdirectory.ErrInvalidServiceCredentials):
		return "invalid_service_credentials"
	case errors.Is(err, ldapdirectory.ErrIncompleteResults):
		return "incomplete_results"
	case errors.Is(err, ldapdirectory.ErrMembershipCycle), errors.Is(err, ErrDirectoryGroupCycle):
		return "group_cycle"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "directory_sync_failed"
	}
}

func syncRunView(run *types.DirectorySyncRun, trigger string) *types.DirectorySyncRunView {
	if run == nil {
		return nil
	}
	started := run.StartedAt
	if run.Trigger != "" {
		trigger = string(run.Trigger)
	}
	return &types.DirectorySyncRunView{
		ID: run.ID, Trigger: trigger, Status: string(run.Status), StartedAt: &started,
		FinishedAt: run.CompletedAt, UsersSeen: run.UserCount, GroupsSeen: run.GroupCount,
		MembershipsSeen: run.MembershipCount, Error: run.ErrorMessage,
	}
}

func (s *directoryRuntimeService) ListSyncRuns(ctx context.Context, limit int) (*types.DirectorySyncRunsResponse, error) {
	limit = normalizeSearchLimit(limit)
	runs, err := s.directories.ListSyncRuns(ctx, s.directoryID(), 0, limit)
	if err != nil {
		return nil, err
	}
	result := &types.DirectorySyncRunsResponse{Runs: make([]types.DirectorySyncRunView, 0, len(runs)), Total: len(runs)}
	for _, run := range runs {
		view := syncRunView(run, directoryManualTrigger)
		if view != nil {
			result.Runs = append(result.Runs, *view)
		}
	}
	return result, nil
}

func (s *directoryRuntimeService) LinkIdentity(ctx context.Context, objectGUID, userID string) error {
	objectGUID = strings.TrimSpace(objectGUID)
	userID = strings.TrimSpace(userID)
	if objectGUID == "" || userID == "" {
		return fmt.Errorf("%w: object_guid and user_id are required", ErrInvalidDirectoryConfig)
	}
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return err
	}
	identity, err := s.repo.GetIdentityByObjectGUID(ctx, directory.ID, objectGUID)
	if err != nil {
		return err
	}
	if identity == nil {
		return ErrDirectoryIdentityUnavailable
	}
	if identity.UserID != nil && *identity.UserID != "" {
		if *identity.UserID == userID {
			return nil
		}
		return ErrDirectoryIdentityAlreadyLinked
	}
	user, err := s.users.GetUserByID(ctx, userID)
	if err != nil {
		return err
	}
	if user == nil {
		return ErrDirectoryIdentityUnavailable
	}
	if err := s.directories.LinkIdentity(ctx, identity.ID, userID); err != nil {
		return err
	}
	if s.tokens != nil {
		_ = s.tokens.RevokeTokensByUserID(ctx, userID)
	}
	s.emitAudit(ctx, types.AuditActionDirectoryIdentityLinked, identity.ID, "directory_identity", userID, map[string]any{
		"directory_id": identity.DirectoryID, "object_guid": identity.ObjectGUID, "link_mode": "administrator",
	})
	return nil
}

func (s *directoryRuntimeService) UnlinkIdentity(ctx context.Context, objectGUID string) error {
	objectGUID = strings.TrimSpace(objectGUID)
	if objectGUID == "" {
		return fmt.Errorf("%w: object_guid is required", ErrInvalidDirectoryConfig)
	}
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return err
	}
	identity, err := s.repo.GetIdentityByObjectGUID(ctx, directory.ID, objectGUID)
	if err != nil {
		return err
	}
	if identity == nil {
		return ErrDirectoryIdentityUnavailable
	}
	if identity.UserID == nil || *identity.UserID == "" {
		return nil
	}
	userID := *identity.UserID
	if err := s.directories.UnlinkIdentity(ctx, identity.ID); err != nil {
		return err
	}
	if s.tokens != nil {
		_ = s.tokens.RevokeTokensByUserID(ctx, userID)
	}
	s.emitAudit(ctx, types.AuditActionDirectoryIdentityUnlinked, identity.ID, "directory_identity", userID, map[string]any{
		"directory_id": identity.DirectoryID, "object_guid": identity.ObjectGUID,
	})
	return nil
}

func (s *directoryRuntimeService) Login(ctx context.Context, identifier, password string) (*types.LoginResponse, error) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" || password == "" {
		return nil, ldapdirectory.ErrEmptyPassword
	}
	directory, err := s.persistedDirectory(ctx)
	if err != nil {
		return nil, err
	}
	if !directory.Enabled {
		return nil, ErrDirectoryDisabled
	}
	// A failed synchronization means the current directory state could not be
	// validated as a complete snapshot. Existing sessions may continue until
	// the stale threshold, but new LDAP logins fail closed until a complete
	// synchronization clears LastSyncError.
	if !directory.IsFresh(s.now().UTC()) || strings.TrimSpace(directory.LastSyncError) != "" {
		return nil, ErrDirectoryUnavailable
	}
	// Capture the configuration generation before any network work. Never read
	// it again from the potentially shared model pointer: only this generation
	// may authorize the bind result and token issuance.
	expectedConfigVersion := directory.ConfigVersion
	adapter, err := s.adapterFor(ctx, directory, "")
	if err != nil {
		return nil, err
	}
	authenticated, err := adapter.Authenticate(ctx, identifier, password)
	if err != nil {
		return nil, err
	}
	s.setActiveServer(authenticated.ControllerURL)
	loginSnapshot, err := s.repo.GetLoginSnapshot(ctx, directory.ID, authenticated.User.ObjectGUID)
	if err != nil {
		return nil, err
	}
	identity, err := s.validateLoginSnapshot(loginSnapshot, expectedConfigVersion)
	if err != nil {
		return nil, err
	}
	if !sameDirectoryGroupSet(authenticated.EffectiveGroupObjectGUIDs, loginSnapshot.EffectiveGroupObjectGUIDs) {
		// Never patch memberships from a login-time partial view. Attempt one
		// complete, lease-protected synchronization, then compare against the
		// newly applied atomic snapshot. Any failure or remaining difference is
		// fail-closed and no token has been issued yet.
		if s.directories != nil && directoryLoginSyncDue(directory, s.now().UTC()) {
			if _, syncErr := s.runSync(ctx, directoryLoginTrigger); syncErr == nil {
				loginSnapshot, err = s.repo.GetLoginSnapshot(ctx, directory.ID, authenticated.User.ObjectGUID)
				if err != nil {
					return nil, err
				}
				identity, err = s.validateLoginSnapshot(loginSnapshot, expectedConfigVersion)
				if err != nil {
					return nil, err
				}
				if sameDirectoryGroupSet(authenticated.EffectiveGroupObjectGUIDs, loginSnapshot.EffectiveGroupObjectGUIDs) {
					goto membershipsVerified
				}
			}
		}
		return nil, ErrDirectoryMembershipMismatch
	}

membershipsVerified:
	user, err := s.resolveDirectoryUser(ctx, identity, authenticated.User)
	if err != nil {
		return nil, err
	}
	if user == nil || !user.IsActive {
		return nil, ErrDirectoryIdentityUnavailable
	}
	accessToken, refreshToken, err := s.users.GenerateTokens(ctx, user)
	if err != nil {
		return nil, err
	}
	postSnapshot, postErr := s.repo.GetLoginSnapshot(ctx, directory.ID, authenticated.User.ObjectGUID)
	if postErr != nil {
		s.revokeGeneratedDirectoryTokens(ctx, user.ID)
		return nil, fmt.Errorf("%w: post-login snapshot read failed", ErrDirectoryUnavailable)
	}
	postIdentity, postErr := s.validateLoginSnapshot(postSnapshot, expectedConfigVersion)
	if postErr != nil || postIdentity.UserID == nil || *postIdentity.UserID != user.ID {
		s.revokeGeneratedDirectoryTokens(ctx, user.ID)
		if postErr != nil {
			return nil, postErr
		}
		return nil, ErrDirectoryIdentityUnavailable
	}
	if !sameDirectoryGroupSet(authenticated.EffectiveGroupObjectGUIDs, postSnapshot.EffectiveGroupObjectGUIDs) {
		s.revokeGeneratedDirectoryTokens(ctx, user.ID)
		return nil, ErrDirectoryMembershipMismatch
	}
	memberships := s.users.BuildLoginMemberships(ctx, user, nil)
	activeTenantID := user.TenantID
	if pref := user.Preferences.LastActiveTenantID; pref != nil && membershipContains(memberships, *pref) {
		activeTenantID = *pref
	}
	if activeTenantID == 0 && len(memberships) > 0 {
		activeTenantID = memberships[0].TenantID
	}
	var activeTenant *types.Tenant
	if activeTenantID > 0 && s.tenants != nil {
		activeTenant, _ = s.tenants.GetTenantByID(ctx, activeTenantID)
	}
	memberships = s.users.BuildLoginMemberships(ctx, user, activeTenant)
	return &types.LoginResponse{
		Success: true, Message: "Login successful", User: user, ActiveTenant: activeTenant,
		Memberships: memberships, Token: accessToken, RefreshToken: refreshToken,
	}, nil
}

func directoryLoginSyncDue(directory *types.Directory, now time.Time) bool {
	return directory != nil && (directory.LastSyncAttemptAt == nil || !now.Before(directory.LastSyncAttemptAt.Add(directoryLoginSyncCooldown)))
}

func (s *directoryRuntimeService) revokeGeneratedDirectoryTokens(ctx context.Context, userID string) {
	if s.tokens != nil && strings.TrimSpace(userID) != "" {
		_ = s.tokens.RevokeTokensByUserID(ctx, userID)
	}
}

func (s *directoryRuntimeService) validateLoginSnapshot(snapshot *types.DirectoryLoginSnapshot, expectedConfigVersion uint64) (*types.DirectoryIdentity, error) {
	if snapshot == nil || snapshot.Directory == nil || snapshot.Directory.ID != s.directoryID() {
		return nil, ErrDirectoryUnavailable
	}
	directory := snapshot.Directory
	if directory.ConfigVersion != expectedConfigVersion {
		return nil, ErrDirectoryUnavailable
	}
	if !directory.Enabled || !directory.IsFresh(s.now().UTC()) || strings.TrimSpace(directory.LastSyncError) != "" {
		return nil, ErrDirectoryUnavailable
	}
	if snapshot.Identity == nil || snapshot.Identity.Status != types.DirectoryObjectActive ||
		snapshot.Identity.SnapshotVersion != directory.SnapshotVersion {
		return nil, ErrDirectoryIdentityUnavailable
	}
	return snapshot.Identity, nil
}

func sameDirectoryGroupSet(live, persisted []string) bool {
	canonical := func(values []string) (map[string]struct{}, bool) {
		result := make(map[string]struct{}, len(values))
		for _, value := range values {
			value = strings.ToLower(strings.TrimSpace(value))
			if value == "" {
				return nil, false
			}
			result[value] = struct{}{}
		}
		return result, true
	}
	liveSet, liveOK := canonical(live)
	persistedSet, persistedOK := canonical(persisted)
	if !liveOK || !persistedOK || len(liveSet) != len(persistedSet) {
		return false
	}
	for guid := range liveSet {
		if _, ok := persistedSet[guid]; !ok {
			return false
		}
	}
	return true
}

func membershipContains(memberships []types.Membership, tenantID uint64) bool {
	for _, membership := range memberships {
		if membership.TenantID == tenantID {
			return true
		}
	}
	return false
}

func (s *directoryRuntimeService) resolveDirectoryUser(
	ctx context.Context,
	identity *types.DirectoryIdentity,
	directoryUser ldapdirectory.User,
) (*types.User, error) {
	if identity.UserID != nil && strings.TrimSpace(*identity.UserID) != "" {
		return s.users.GetUserByID(ctx, *identity.UserID)
	}
	// Serialize the rare first-login provisioning path. A second concurrent
	// bind for the same directory identity must observe the link created by
	// the first request instead of racing on users' unique indexes.
	s.linkMu.Lock()
	defer s.linkMu.Unlock()
	refreshed, err := s.repo.GetIdentity(ctx, identity.ID)
	if err != nil {
		return nil, err
	}
	if refreshed == nil || refreshed.Status != types.DirectoryObjectActive {
		return nil, ErrDirectoryIdentityUnavailable
	}
	identity = refreshed
	if identity.UserID != nil && strings.TrimSpace(*identity.UserID) != "" {
		return s.users.GetUserByID(ctx, *identity.UserID)
	}
	username := directoryUsername(directoryUser)
	email := strings.TrimSpace(directoryUser.Email)
	if email == "" {
		email = strings.ToLower(strings.TrimSpace(directoryUser.ObjectGUID)) + "@directory.invalid"
	}
	if conflict, err := s.directoryIdentityConflicts(ctx, email, username); err != nil {
		return nil, err
	} else if conflict {
		return nil, ErrDirectoryIdentityLinkRequired
	}
	password, err := randomDirectoryPlaceholderPassword()
	if err != nil {
		return nil, err
	}
	user, err := s.users.Register(ctx, &types.RegisterRequest{
		Username: username, Email: email, Password: password, TenantProvisioning: types.TenantProvisioningTenantless,
	})
	if err != nil {
		if errors.Is(err, ErrUserEmailExists) || errors.Is(err, ErrUserUsernameExists) {
			return nil, ErrDirectoryIdentityLinkRequired
		}
		return nil, err
	}
	if err := s.directories.LinkIdentity(ctx, identity.ID, user.ID); err != nil {
		_ = s.users.DeleteUser(ctx, user.ID)
		if errors.Is(err, ErrDirectoryIdentityAlreadyLinked) {
			refreshed, readErr := s.repo.GetIdentity(ctx, identity.ID)
			if readErr != nil {
				return nil, readErr
			}
			if refreshed != nil && refreshed.UserID != nil && strings.TrimSpace(*refreshed.UserID) != "" {
				return s.users.GetUserByID(ctx, *refreshed.UserID)
			}
			return nil, ErrDirectoryIdentityLinkRequired
		}
		return nil, fmt.Errorf("link directory identity: %w", err)
	}
	s.emitAudit(ctx, types.AuditActionDirectoryIdentityLinked, identity.ID, "directory_identity", user.ID, map[string]any{
		"directory_id": identity.DirectoryID, "object_guid": identity.ObjectGUID, "link_mode": "automatic_new_user",
	})
	return user, nil
}

func (s *directoryRuntimeService) directoryIdentityConflicts(ctx context.Context, email, username string) (bool, error) {
	collision, err := s.users.FindUserByEmailOrUsernameFold(ctx, email, username)
	if err != nil {
		return false, err
	}
	return collision != nil, nil
}

func (s *directoryRuntimeService) emitAudit(
	ctx context.Context,
	action types.AuditAction,
	targetID, targetType, targetUserID string,
	details map[string]any,
) {
	if s.audit == nil {
		return
	}
	actorID, _ := types.UserIDFromContext(ctx)
	var detailsJSON types.JSON
	if encoded, err := json.Marshal(details); err == nil {
		detailsJSON = types.JSON(encoded)
	}
	outcome := types.AuditOutcomeSuccess
	if action == types.AuditActionDirectorySyncFailed {
		outcome = types.AuditOutcomeFailed
	}
	_ = s.audit.Log(ctx, &types.AuditLog{
		TenantID: 0, ActorUserID: actorID, ActorRole: "system_admin", Action: action,
		TargetType: targetType, TargetID: targetID, TargetUserID: targetUserID,
		Outcome: outcome, Details: detailsJSON,
	})
}

func directoryUsername(user ldapdirectory.User) string {
	username := strings.TrimSpace(user.SAMAccountName)
	if username == "" {
		username = strings.TrimSpace(user.UserPrincipalName)
		if at := strings.IndexByte(username, '@'); at > 0 {
			username = username[:at]
		}
	}
	if username == "" {
		username = "directory-" + strings.TrimSpace(user.ObjectGUID)
	}
	if len(username) > 50 {
		username = username[:50]
	}
	return username
}

func randomDirectoryPlaceholderPassword() (string, error) {
	randomBytes := make([]byte, 20)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("generate directory placeholder password: %w", err)
	}
	return "Aa1!" + base64.RawURLEncoding.EncodeToString(randomBytes), nil
}

func (s *directoryRuntimeService) revokeSuspendedIdentitySessions(ctx context.Context, directoryID string) {
	if s.tokens == nil {
		return
	}
	identities, err := s.listAllIdentities(ctx, directoryID)
	if err != nil {
		return
	}
	for _, identity := range identities {
		if identity.Status == types.DirectoryObjectActive || identity.UserID == nil || *identity.UserID == "" {
			continue
		}
		_ = s.tokens.RevokeTokensByUserID(ctx, *identity.UserID)
	}
}

func (s *directoryRuntimeService) revokeDirectorySessions(ctx context.Context, directoryID string) {
	if s.tokens == nil {
		return
	}
	identities, err := s.listAllIdentities(ctx, directoryID)
	if err != nil {
		return
	}
	seen := map[string]struct{}{}
	for _, identity := range identities {
		if identity.UserID == nil || *identity.UserID == "" {
			continue
		}
		if _, ok := seen[*identity.UserID]; ok {
			continue
		}
		seen[*identity.UserID] = struct{}{}
		_ = s.tokens.RevokeTokensByUserID(ctx, *identity.UserID)
	}
}

func (s *directoryRuntimeService) setSyncing(syncing bool) {
	s.stateMu.Lock()
	if syncing {
		s.syncing++
	} else if s.syncing > 0 {
		s.syncing--
	}
	s.stateMu.Unlock()
}

func (s *directoryRuntimeService) setActiveServer(server string) {
	s.stateMu.Lock()
	s.activeServer = server
	s.stateMu.Unlock()
}

func (s *directoryRuntimeService) recordFailure() {
	s.stateMu.Lock()
	s.consecutiveFailures++
	s.stateMu.Unlock()
}

func (s *directoryRuntimeService) clearFailures() {
	s.stateMu.Lock()
	s.consecutiveFailures = 0
	s.stateMu.Unlock()
}

// Start runs a lightweight scheduler. It checks persisted cadence frequently
// enough to notice UI configuration changes without restarting the process.
func (s *directoryRuntimeService) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			// A newly enabled directory has no fresh snapshot yet; perform the
			// due check immediately so startup does not impose an artificial
			// access-paused window before the first 30-second supervisor tick.
			s.runScheduledIfDue(ctx)
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-s.stop:
					return
				case <-ticker.C:
					s.runScheduledIfDue(ctx)
				}
			}
		}()
	})
}

func (s *directoryRuntimeService) runScheduledIfDue(ctx context.Context) {
	directory, err := s.persistedDirectory(ctx)
	if err != nil || directory == nil || !directory.Enabled {
		return
	}
	interval := time.Duration(defaultInt(directory.SyncIntervalSeconds, 300)) * time.Second
	last := directory.LastSyncAttemptAt
	if last != nil && s.now().Before(last.Add(interval)) {
		return
	}
	_, _ = s.runSync(ctx, directoryScheduledTrigger)
}

func (s *directoryRuntimeService) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}
