package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
)

// CheckCurrentPublication asks the paired Nextcloud binding whether the exact
// candidate version is still eligible. It uses only the configured instance
// origin and a fixed API path. The expected identity is in the signed body,
// so a successful response attests to the instance, binding, file, ETag and
// path together. Every non-204 response or transport failure denies publish.
func CheckCurrentPublication(ctx context.Context, source *types.DataSourceConfig,
	instanceID, bindingID string, fileID int64, etag, path string,
) error {
	if source == nil || source.Type != types.ConnectorTypeNextcloud ||
		len(source.ResourceIDs) != 1 || source.ResourceIDs[0] != bindingID ||
		instanceID == "" || len(instanceID) > 256 || !validBindingID(bindingID) ||
		len(bindingID) > 128 || fileID < 1 || strings.TrimSpace(etag) == "" ||
		len(etag) > 1024 || path == "" || len(path) > 4096 || strings.HasPrefix(path, "/") {
		return fmt.Errorf("invalid Nextcloud publication identity")
	}
	cfg, err := parseConfig(source)
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		InstanceID string `json:"instance_id"`
		ETag       string `json:"etag"`
		Path       string `json:"path"`
	}{instanceID, etag, path})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cli := &client{cfg: cfg, http: authorizationHTTPClient}
	endpoint := "/bindings/" + bindingID + "/files/" + strconv.FormatInt(fileID, 10) + "/publication-check"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cli.endpointURL(endpoint, nil), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := signMachineRequest(req, cfg, body); err != nil {
		return err
	}
	resp, err := cli.http.Do(req)
	if err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud publication check: %w", err), fmt.Sprintf(
			"Nextcloud publication check: %s", apperrors.PublicMessage(err)))
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud publication check returned status %d",
			resp.StatusCode), fmt.Sprintf("Nextcloud publication check returned status %d", resp.StatusCode))
	}
	return nil
}
