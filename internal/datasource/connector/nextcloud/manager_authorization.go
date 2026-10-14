package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

var ErrManagerAuthorizationDenied = errors.New("Nextcloud manager authorization denied")
var ErrManagerAuthorizationUnavailable = errors.New("Nextcloud manager authorization unavailable")
var managerSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var managerControlIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,256}$`)

// ManagerAuthorizationScope is the exact immutable WK manager purpose scope.
// This endpoint proves management only; it never grants file READ or a WK role.
type ManagerAuthorizationScope struct {
	InstanceID           string `json:"instance_id"`
	OperationID          string `json:"operation_id"`
	TenantID             string `json:"tenant_id"`
	KnowledgeBaseID      string `json:"knowledge_base_id"`
	DataSourceID         string `json:"data_source_id"`
	ProcessingTemplateID string `json:"processing_template_id"`
	DirectoryID          string `json:"directory_id"`
	ObjectGUID           string `json:"object_guid"`
	PurposeSHA256        string `json:"purpose_sha256"`
}
type ManagerAuthorizationDecision struct {
	RootFileID       string
	RuleID           string
	RuleRevision     string
	PublicationEpoch string
	PolicyRevision   string
	CheckedAt        int64
}

func managerNonnegativeInteger(value string) bool {
	n, err := strconv.ParseUint(value, 10, 64)
	return err == nil && strconv.FormatUint(n, 10) == value
}
func managerPositiveInteger(value string) bool {
	return value != "0" && managerNonnegativeInteger(value)
}

// AuthorizeCurrentManager performs a new signed request on every invocation.
// Explicit, fully bound 403 is denied; transport, stale/malformed echoes and
// missing capabilities are unavailable, never an implicit allow or empty read.
func AuthorizeCurrentManager(ctx context.Context, source *types.DataSourceConfig, binding string, scope ManagerAuthorizationScope) (ManagerAuthorizationDecision, error) {
	unavailable := func() (ManagerAuthorizationDecision, error) {
		return ManagerAuthorizationDecision{}, ErrManagerAuthorizationUnavailable
	}
	if ctx == nil || source == nil || source.Type != types.ConnectorTypeNextcloud || !validBindingID(binding) || len(binding) > 128 || len(source.ResourceIDs) != 1 || source.ResourceIDs[0] != binding ||
		!managerControlIDPattern.MatchString(scope.InstanceID) || !managerControlIDPattern.MatchString(scope.OperationID) || !managerPositiveInteger(scope.TenantID) || !managerControlIDPattern.MatchString(scope.KnowledgeBaseID) || !managerControlIDPattern.MatchString(scope.DataSourceID) || !managerControlIDPattern.MatchString(scope.ProcessingTemplateID) ||
		!authorizationDirectoryIDPattern.MatchString(scope.DirectoryID) || !authorizationGUIDPattern.MatchString(scope.ObjectGUID) || scope.ObjectGUID != strings.ToLower(scope.ObjectGUID) || !managerSHA256Pattern.MatchString(scope.PurposeSHA256) {
		return unavailable()
	}
	cfg, err := parseConfig(source)
	if err != nil {
		return unavailable()
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cli := &client{cfg: cfg, http: authorizationHTTPClient}
	capabilities, err := cli.capabilities(bounded)
	if err != nil || capabilities.InstanceID != scope.InstanceID {
		return unavailable()
	}
	body, err := json.Marshal(scope)
	if err != nil {
		return unavailable()
	}
	req, err := http.NewRequestWithContext(bounded, http.MethodPost, cli.endpointURL("/bindings/"+binding+"/authorize-manager", nil), bytes.NewReader(body))
	if err != nil {
		return unavailable()
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err := signMachineRequest(req, cfg, body); err != nil {
		return unavailable()
	}
	response, err := cli.http.Do(req)
	if err != nil {
		return unavailable()
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusForbidden {
		return unavailable()
	}
	if response.ContentLength > maxAuthorizationResponseBytes {
		return unavailable()
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxAuthorizationResponseBytes+1))
	if err != nil || len(raw) > maxAuthorizationResponseBytes {
		return unavailable()
	}
	var wire struct {
		ManagerAuthorizationScope
		Allow            *bool  `json:"allow"`
		Reason           string `json:"reason"`
		BindingID        string `json:"binding_id"`
		RootFileID       string `json:"root_file_id"`
		RuleID           string `json:"rule_id"`
		RuleRevision     string `json:"rule_revision"`
		PublicationEpoch string `json:"publication_epoch"`
		PolicyRevision   string `json:"policy_revision"`
		CheckedAt        int64  `json:"checked_at"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.Allow == nil || wire.ManagerAuthorizationScope != scope || wire.BindingID != binding || wire.CheckedAt < time.Now().Add(-10*time.Second).Unix() || wire.CheckedAt > time.Now().Add(5*time.Second).Unix() {
		return unavailable()
	}
	if response.StatusCode == http.StatusForbidden {
		if *wire.Allow || wire.Reason != "manager_denied" {
			return unavailable()
		}
		return ManagerAuthorizationDecision{}, ErrManagerAuthorizationDenied
	}
	if !*wire.Allow || wire.Reason != "authorized" || !managerPositiveInteger(wire.RootFileID) || !managerControlIDPattern.MatchString(wire.RuleID) || !managerPositiveInteger(wire.RuleRevision) || !managerNonnegativeInteger(wire.PublicationEpoch) || !managerSHA256Pattern.MatchString(wire.PolicyRevision) {
		return unavailable()
	}
	if err := bounded.Err(); err != nil {
		return ManagerAuthorizationDecision{}, fmt.Errorf("%w: request expired", ErrManagerAuthorizationUnavailable)
	}
	return ManagerAuthorizationDecision{wire.RootFileID, wire.RuleID, wire.RuleRevision, wire.PublicationEpoch, wire.PolicyRevision, wire.CheckedAt}, nil
}
