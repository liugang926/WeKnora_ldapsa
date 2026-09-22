package directory

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
)

const maximumPageSize = 10_000

type ldapConnection interface {
	Bind(username, password string) error
	Search(searchRequest *ldap.SearchRequest) (*ldap.SearchResult, error)
	Close()
}

type dialConnection func(context.Context, Controller, *tls.Config, Config) (ldapConnection, error)

// Adapter provides the transport-independent directory boundary. It owns no
// persistence: callers apply a successful Snapshot in one database
// transaction and must discard it when Sync returns an error.
type Adapter struct {
	config Config
	dial   dialConnection
	now    func() time.Time
}

// NewAdapter validates configuration up front. Only encrypted LDAP transports
// are accepted, and server certificate verification cannot be disabled.
func NewAdapter(config Config) (*Adapter, error) {
	config = config.withDefaults()
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return &Adapter{config: config, dial: defaultDialConnection, now: time.Now}, nil
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.DirectoryID) == "" {
		return fmt.Errorf("directory ID is required")
	}
	if len(config.Controllers) == 0 {
		return fmt.Errorf("at least one directory controller is required")
	}
	if strings.TrimSpace(config.BindDN) == "" || config.BindPassword == "" {
		return fmt.Errorf("directory service bind DN and password are required")
	}
	if strings.TrimSpace(config.UserBaseDN) == "" || strings.TrimSpace(config.GroupBaseDN) == "" {
		return fmt.Errorf("directory user and group base DNs are required")
	}
	if config.ConnectTimeout <= 0 || config.QueryTimeout <= 0 {
		return fmt.Errorf("directory timeouts must be positive")
	}
	if config.PageSize == 0 || config.PageSize > maximumPageSize {
		return fmt.Errorf("directory page size must be between 1 and %d", maximumPageSize)
	}
	if config.MaxPages <= 0 || config.ResultLimit <= 0 {
		return fmt.Errorf("directory page and result limits must be positive")
	}
	if _, err := ldap.CompileFilter(config.UserFilter); err != nil {
		return fmt.Errorf("invalid directory user filter: %w", err)
	}
	if _, err := ldap.CompileFilter(config.GroupFilter); err != nil {
		return fmt.Errorf("invalid directory group filter: %w", err)
	}
	if !strings.Contains(config.LoginFilter, "{login}") {
		return fmt.Errorf("directory login filter must contain {login}")
	}
	probeLoginFilter := strings.ReplaceAll(config.LoginFilter, "{login}", ldap.EscapeFilter("login-probe"))
	if _, err := ldap.CompileFilter(probeLoginFilter); err != nil {
		return fmt.Errorf("invalid directory login filter: %w", err)
	}
	for _, controller := range config.Controllers {
		parsed, err := url.Parse(strings.TrimSpace(controller.URL))
		if err != nil || parsed.Hostname() == "" {
			return fmt.Errorf("invalid directory controller URL %q", controller.URL)
		}
		if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
			return fmt.Errorf("controller %q must not contain credentials, query, fragment, or base DN", controller.URL)
		}
		switch controller.TLSMode {
		case TLSModeLDAPS:
			if !strings.EqualFold(parsed.Scheme, "ldaps") {
				return fmt.Errorf("controller %q must use ldaps:// in LDAPS mode", controller.URL)
			}
		case TLSModeStartTLS:
			if !strings.EqualFold(parsed.Scheme, "ldap") {
				return fmt.Errorf("controller %q must use ldap:// in StartTLS mode", controller.URL)
			}
		default:
			return fmt.Errorf("controller %q has unsupported TLS mode %q", controller.URL, controller.TLSMode)
		}
		if _, err := buildTLSConfig(controller, config.TLS); err != nil {
			return fmt.Errorf("controller %q TLS configuration: %w", controller.URL, err)
		}
	}
	return nil
}

type productionConnection struct {
	conn *ldap.Conn
}

func (c *productionConnection) Bind(username, password string) error {
	return c.conn.Bind(username, password)
}

func (c *productionConnection) Search(request *ldap.SearchRequest) (*ldap.SearchResult, error) {
	return c.conn.Search(request)
}

func (c *productionConnection) Close() {
	c.conn.Close()
}

func defaultDialConnection(
	ctx context.Context,
	controller Controller,
	tlsConfig *tls.Config,
	config Config,
) (ldapConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: config.ConnectTimeout}
	options := []ldap.DialOpt{ldap.DialWithDialer(dialer)}
	if controller.TLSMode == TLSModeLDAPS {
		options = append(options, ldap.DialWithTLSConfig(tlsConfig))
	}
	conn, err := ldap.DialURL(controller.URL, options...)
	if err != nil {
		return nil, err
	}
	conn.SetTimeout(config.QueryTimeout)
	if controller.TLSMode == TLSModeStartTLS {
		// go-ldap's request timeout bounds the StartTLS extended operation but
		// not the subsequent TLS handshake itself. Run the whole upgrade behind
		// the configured connection timeout and close the socket to interrupt a
		// peer that accepts TCP then stalls during negotiation.
		result := make(chan error, 1)
		go func() { result <- conn.StartTLS(tlsConfig) }()
		timer := time.NewTimer(config.ConnectTimeout)
		defer timer.Stop()
		var startTLSErr error
		select {
		case startTLSErr = <-result:
		case <-ctx.Done():
			conn.Close()
			return nil, ctx.Err()
		case <-timer.C:
			conn.Close()
			return nil, fmt.Errorf("StartTLS negotiation exceeded connection timeout %s", config.ConnectTimeout)
		}
		if startTLSErr != nil {
			conn.Close()
			return nil, startTLSErr
		}
	}
	return &productionConnection{conn: conn}, nil
}

// Authenticate searches with the read-only service account, validates the
// password by binding as the resolved user DN, then re-binds the service
// account on that same controller to verify the user's complete selected-scope
// group closure. Invalid credentials are terminal and never fan out to another
// DC; operational failures after a successful user bind may retry the complete
// flow on the next controller.
func (a *Adapter) Authenticate(
	ctx context.Context,
	identifier string,
	password string,
) (*AuthenticationResult, error) {
	identifier = strings.TrimSpace(identifier)
	if password == "" {
		return nil, ErrEmptyPassword
	}
	if identifier == "" {
		return nil, ErrUserNotFound
	}

	attempts := make([]ControllerAttempt, 0, len(a.config.Controllers))
	for _, controller := range a.config.Controllers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := a.open(ctx, controller)
		if err != nil {
			attempts = append(attempts, ControllerAttempt{ControllerURL: controller.URL, Err: err})
			continue
		}

		result, terminal, err := a.authenticateOnConnection(ctx, conn, identifier, password)
		conn.Close()
		if err == nil {
			result.ControllerURL = controller.URL
			return result, nil
		}
		if terminal {
			return nil, err
		}
		attempts = append(attempts, ControllerAttempt{ControllerURL: controller.URL, Err: err})
	}
	return nil, &FailoverError{Operation: "authentication", Attempts: attempts}
}

func (a *Adapter) authenticateOnConnection(
	ctx context.Context,
	conn ldapConnection,
	identifier string,
	password string,
) (*AuthenticationResult, bool, error) {
	if err := conn.Bind(a.config.BindDN, a.config.BindPassword); err != nil {
		if isInvalidCredentials(err) {
			return nil, true, fmt.Errorf("%w: service account bind rejected", ErrInvalidServiceCredentials)
		}
		return nil, false, fmt.Errorf("service account bind: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}

	escaped := ldap.EscapeFilter(identifier)
	loginFilter := strings.ReplaceAll(a.config.LoginFilter, "{login}", escaped)
	filter := fmt.Sprintf("(&%s%s)", a.config.UserFilter, loginFilter)
	request := ldap.NewSearchRequest(
		a.config.UserBaseDN,
		ldap.ScopeWholeSubtree,
		ldap.NeverDerefAliases,
		2,
		queryTimeLimit(a.config.QueryTimeout),
		false,
		filter,
		userAttributes,
		nil,
	)
	searchResult, err := conn.Search(request)
	if err != nil {
		return nil, false, fmt.Errorf("search directory user: %w", err)
	}
	if searchResult == nil {
		return nil, false, fmt.Errorf("%w: user search returned no result object", ErrIncompleteResults)
	}
	if len(searchResult.Referrals) != 0 {
		return nil, false, fmt.Errorf("%w: user search returned referrals", ErrIncompleteResults)
	}
	switch len(searchResult.Entries) {
	case 0:
		return nil, true, ErrUserNotFound
	case 1:
	default:
		return nil, true, ErrAmbiguousUser
	}
	user, err := parseUserEntry(searchResult.Entries[0])
	if err != nil {
		return nil, false, err
	}
	if !user.Enabled {
		return nil, true, ErrUserDisabled
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if err := conn.Bind(user.DN, password); err != nil {
		if isInvalidCredentials(err) {
			return nil, true, ErrInvalidCredentials
		}
		return nil, false, fmt.Errorf("user bind: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if err := conn.Bind(a.config.BindDN, a.config.BindPassword); err != nil {
		if isInvalidCredentials(err) {
			return nil, true, fmt.Errorf("%w: service account re-bind rejected", ErrInvalidServiceCredentials)
		}
		return nil, false, fmt.Errorf("service account re-bind: %w", err)
	}
	effectiveGroupGUIDs, err := a.collectEffectiveGroupObjectGUIDs(ctx, conn, user)
	if err != nil {
		return nil, false, fmt.Errorf("verify live group memberships: %w", err)
	}
	return &AuthenticationResult{User: user, EffectiveGroupObjectGUIDs: effectiveGroupGUIDs}, false, nil
}

func (a *Adapter) open(ctx context.Context, controller Controller) (ldapConnection, error) {
	tlsConfig, err := buildTLSConfig(controller, a.config.TLS)
	if err != nil {
		return nil, err
	}
	return a.dial(ctx, controller, tlsConfig, a.config)
}

func isInvalidCredentials(err error) bool {
	return ldap.IsErrorWithCode(err, ldap.LDAPResultInvalidCredentials)
}

func queryTimeLimit(timeout time.Duration) int {
	seconds := int(timeout / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func contextOrError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	return err
}

func isTerminalSyncError(err error) bool {
	return errors.Is(err, ErrInvalidServiceCredentials)
}
