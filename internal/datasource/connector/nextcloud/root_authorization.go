package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// AuthorizeCurrentRoot checks the signed current-root authorization response.
func AuthorizeCurrentRoot(
	ctx context.Context,
	source *types.DataSourceConfig,
	instance, binding, directory, guid string,
) error {
	if source == nil || source.Type != types.ConnectorTypeNextcloud || instance == "" || !validBindingID(binding) ||
		!authorizationDirectoryIDPattern.MatchString(directory) ||
		!authorizationGUIDPattern.MatchString(guid) ||
		len(source.ResourceIDs) != 1 ||
		source.ResourceIDs[0] != binding {
		return fmt.Errorf("invalid Nextcloud root authorization identity")
	}
	cfg, err := parseConfig(source)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cli := &client{cfg: cfg, http: authorizationHTTPClient}
	capabilities, err := cli.capabilities(ctx)
	if err != nil || capabilities.InstanceID != instance {
		return fmt.Errorf("nextcloud root instance is unavailable")
	}
	body, err := json.Marshal(map[string]string{"directory_id": directory, "object_guid": strings.ToLower(guid)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		cli.endpointURL("/bindings/"+binding+"/authorize-root", nil),
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := signMachineRequest(req, cfg, body); err != nil {
		return err
	}
	response, err := cli.http.Do(req)
	if err != nil {
		return fmt.Errorf("nextcloud root authorization unavailable")
	}
	defer closeNextcloudResponseBody(ctx, response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("nextcloud root authorization denied")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxAuthorizationResponseBytes+1))
	if err != nil || len(raw) > maxAuthorizationResponseBytes {
		return fmt.Errorf("nextcloud root authorization unavailable")
	}
	var wire struct {
		Allow            *bool  `json:"allow"`
		Reason           string `json:"reason"`
		BindingID        string `json:"binding_id"`
		DirectoryID      string `json:"directory_id"`
		ObjectGUID       string `json:"object_guid"`
		PublicationEpoch string `json:"publication_epoch"`
		PolicyRevision   string `json:"policy_revision"`
		CheckedAt        int64  `json:"checked_at"`
	}
	if json.Unmarshal(raw, &wire) != nil || wire.Allow == nil || !*wire.Allow || wire.Reason != "authorized" ||
		wire.BindingID != binding ||
		wire.DirectoryID != directory ||
		!strings.EqualFold(wire.ObjectGUID, guid) ||
		wire.PublicationEpoch == "" ||
		wire.PolicyRevision == "" ||
		wire.CheckedAt <= 0 ||
		wire.CheckedAt < time.Now().Add(-5*time.Minute).Unix() ||
		wire.CheckedAt > time.Now().Add(5*time.Minute).Unix() {
		return fmt.Errorf("nextcloud root authorization denied")
	}
	if n, err := strconv.ParseInt(
		wire.PublicationEpoch,
		10,
		64,
	); err != nil || n < 0 ||
		strconv.FormatInt(n, 10) != wire.PublicationEpoch {
		return fmt.Errorf("invalid Nextcloud root publication epoch")
	}
	return nil
}
