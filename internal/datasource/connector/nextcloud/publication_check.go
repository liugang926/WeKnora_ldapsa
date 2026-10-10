package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// ErrPublicationNotCurrent distinguishes a definitive source withdrawal from
// a transport/server outage for durable consumers.
var ErrPublicationNotCurrent = errors.New("Nextcloud publication is no longer current")

// CheckCurrentPublication asks the paired Nextcloud binding whether the exact
// candidate version is still eligible. It uses only the configured instance
// origin and a fixed API path. The expected identity is in the signed body,
// so a successful response attests to the instance, binding, file, ETag and
// path together. Every non-204 response or transport failure denies publish.
func CheckCurrentPublication(ctx context.Context, source *types.DataSourceConfig,
	instanceID, bindingID string, fileID int64, etag, path string) error {
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
	revision, revisionErr := types.NormalizeNextcloudContentRevision(types.NextcloudContentRevisionFromContext(ctx))
	if revisionErr != nil {
		return revisionErr
	}
	body, err := json.Marshal(struct {
		ContentRevision string `json:"content_revision"`
		InstanceID      string `json:"instance_id"`
		ETag            string `json:"etag"`
		Path            string `json:"path"`
	}{revision, instanceID, etag, path})
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
		return fmt.Errorf("Nextcloud publication check: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict, http.StatusGone, http.StatusLocked:
			return fmt.Errorf("%w: status %d", ErrPublicationNotCurrent, resp.StatusCode)
		}
		return fmt.Errorf("Nextcloud publication check returned status %d", resp.StatusCode)
	}
	return nil
}
