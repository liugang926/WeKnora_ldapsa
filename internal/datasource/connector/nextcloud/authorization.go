package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
)

const maxAuthorizationResponseBytes = 16 << 10

var (
	authorizationDirectoryIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	authorizationGUIDPattern        = regexp.MustCompile(
		`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]` +
			`{12}$`)
	// Source checks are on a read path. A slow or unreachable source must deny
	// promptly instead of holding an application request open for the sync timeout.
	authorizationHTTPClient = &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			TLSHandshakeTimeout:   3 * time.Second,
			ResponseHeaderTimeout: 4 * time.Second,
			IdleConnTimeout:       30 * time.Second,
			MaxIdleConns:          10,
		},
	}
)

// AuthorizationDecision is a fresh source decision. Callers must never cache
// an allow without a separately defined invalidation policy.
type AuthorizationDecision struct {
	Allow          bool
	Reason         string
	PolicyRevision string
	SourceETag     string
	CheckedAt      int64
}

// AuthorizeCurrentFile checks the live Nextcloud instance and the current
// mapped user's mount view. A capability probe binds the request to the same
// instance from which the knowledge document was imported. Every transport,
// protocol and response-shape error is returned to the caller for denial.
func AuthorizeCurrentFile(
	ctx context.Context, source *types.DataSourceConfig, expectedInstanceID, bindingID,
	directoryID, objectGUID string, fileID int64,
) (AuthorizationDecision, error) {
	if source == nil || source.Type != types.ConnectorTypeNextcloud {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud authorization requires a Nextcloud data source"),
			"Nextcloud authorization requires a Nextcloud data source")
	}
	if expectedInstanceID == "" || len(expectedInstanceID) > 256 ||
		!validBindingID(bindingID) || len(bindingID) > 128 ||
		!authorizationDirectoryIDPattern.MatchString(directoryID) ||
		!authorizationGUIDPattern.MatchString(objectGUID) || fileID < 1 {
		return AuthorizationDecision{}, fmt.Errorf("invalid Nextcloud authorization identity")
	}
	cfg, err := parseConfig(source)
	if err != nil {
		return AuthorizationDecision{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cli := &client{cfg: cfg, http: authorizationHTTPClient}
	capabilities, err := cli.capabilities(ctx)
	if err != nil {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"verify Nextcloud instance: %w", err), fmt.Sprintf("verify Nextcloud instance: %s",
			apperrors.PublicMessage(err)))
	}
	if capabilities.InstanceID != expectedInstanceID {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud instance changed after import"), "Nextcloud instance changed after import")
	}
	body, err := json.Marshal(map[string]any{
		"directory_id": directoryID,
		"object_guid":  strings.ToLower(objectGUID),
		"file_id":      fileID,
	})
	if err != nil {
		return AuthorizationDecision{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		cli.endpointURL("/bindings/"+bindingID+"/authorize", nil), bytes.NewReader(body))
	if err != nil {
		return AuthorizationDecision{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err := signMachineRequest(req, cfg, body); err != nil {
		return AuthorizationDecision{}, err
	}
	resp, err := cli.http.Do(req)
	if err != nil {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud authorization request: %w", err), fmt.Sprintf("Nextcloud authorization request: %s",
			apperrors.PublicMessage(err)))
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud authorization returned status %d", resp.StatusCode), fmt.Sprintf(
			"Nextcloud authorization returned status %d", resp.StatusCode))
	}
	if resp.ContentLength > maxAuthorizationResponseBytes {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud authorization response too large"), "Nextcloud authorization response too large")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAuthorizationResponseBytes+1))
	if err != nil {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"read Nextcloud authorization response: %w", err), fmt.Sprintf(
			"read Nextcloud authorization response: %s", apperrors.PublicMessage(err)))
	}
	if len(raw) > maxAuthorizationResponseBytes {
		return AuthorizationDecision{}, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud authorization response too large"), "Nextcloud authorization response too large")
	}
	var wire struct {
		Allow          *bool   `json:"allow"`
		Reason         *string `json:"reason"`
		PolicyRevision *string `json:"policy_revision"`
		SourceETag     *string `json:"source_etag"`
		CheckedAt      *int64  `json:"checked_at"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil || wire.Allow == nil ||
		wire.Reason == nil || strings.TrimSpace(*wire.Reason) == "" ||
		wire.CheckedAt == nil || *wire.CheckedAt <= 0 {
		return AuthorizationDecision{}, fmt.Errorf("invalid Nextcloud authorization response")
	}
	if *wire.Allow && (*wire.Reason != "authorized" || wire.PolicyRevision == nil ||
		strings.TrimSpace(*wire.PolicyRevision) == "" || wire.SourceETag == nil ||
		strings.TrimSpace(*wire.SourceETag) == "") {
		return AuthorizationDecision{}, fmt.Errorf("incomplete Nextcloud allow decision")
	}
	decision := AuthorizationDecision{Allow: *wire.Allow, Reason: *wire.Reason, CheckedAt: *wire.CheckedAt}
	if wire.PolicyRevision != nil {
		decision.PolicyRevision = *wire.PolicyRevision
	}
	if wire.SourceETag != nil {
		decision.SourceETag = *wire.SourceETag
	}
	return decision, nil
}
