package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
)

// EmptyDecommissionInventorySHA256 identifies the canonical empty inventory digest.
const EmptyDecommissionInventorySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// SourceDecommission is the exact remote acknowledgement tuple for source retirement.
type SourceDecommission struct {
	OperationID      string `json:"operation_id"`
	PairOperationID  string `json:"pair_operation_id"`
	InstanceID       string `json:"instance_id"`
	BindingID        string `json:"binding_id"`
	TenantID         string `json:"tenant_id"`
	KnowledgeBaseID  string `json:"knowledge_base_id"`
	DataSourceID     string `json:"data_source_id"`
	KeyID            string `json:"key_id"`
	PublicationEpoch int64  `json:"publication_epoch"`
	State            string `json:"state"`
	InventorySHA256  string `json:"inventory_sha256"`
}

func (d SourceDecommission) sameIdentity(other SourceDecommission) bool {
	return d.OperationID == other.OperationID && d.PairOperationID == other.PairOperationID &&
		d.InstanceID == other.InstanceID && d.BindingID == other.BindingID &&
		d.TenantID == other.TenantID && d.KnowledgeBaseID == other.KnowledgeBaseID &&
		d.DataSourceID == other.DataSourceID && d.KeyID == other.KeyID &&
		d.PublicationEpoch == other.PublicationEpoch
}

// ReadSourceDecommission verifies a stopped Nextcloud intent through the
// exact source credential. A caller must compare every identity field.
func ReadSourceDecommission(ctx context.Context, source *types.DataSourceConfig,
	bindingID, operationID string,
) (SourceDecommission, error) {
	var empty SourceDecommission
	cfg, err := parseConfig(source)
	if err != nil {
		return empty, err
	}
	selected, err := selectedBindings(source.ResourceIDs)
	if err != nil || selected[0] != bindingID || !validBindingID(bindingID) || operationID == "" {
		return empty, apperrors.NewProtocolError(fmt.Errorf("nextcloud decommission binding mismatch"),
			"Nextcloud decommission binding mismatch")
	}
	cli := newClient(cfg)
	endpoint := "/bindings/" + bindingID + "/decommission/" + operationID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cli.endpointURL(endpoint, nil), nil)
	if err != nil {
		return empty, err
	}
	req.Header.Set("Accept", "application/json")
	if err := signMachineRequest(req, cfg, nil); err != nil {
		return empty, err
	}
	resp, err := cli.http.Do(req)
	if err != nil {
		return empty, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud decommission intent outcome uncertain: %w", err), fmt.Sprintf(
			"Nextcloud decommission intent outcome uncertain: %s", apperrors.PublicMessage(err)))
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return empty, &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	var answer struct {
		Decommission SourceDecommission `json:"decommission"`
	}
	if err := readDecommissionResponse(resp.Body, &answer); err != nil {
		return empty, err
	}
	if answer.Decommission.OperationID != operationID || answer.Decommission.BindingID != bindingID ||
		answer.Decommission.KeyID != cfg.keyID ||
		(answer.Decommission.State != "prepared" && answer.Decommission.State != "acknowledged") {
		return empty, apperrors.NewProtocolError(fmt.Errorf("nextcloud decommission intent tuple mismatch"),
			"Nextcloud decommission intent tuple mismatch")
	}
	return answer.Decommission, nil
}

// AcknowledgeEmptySourceDecommission sends an empty-inventory proof only after
// the repository has durably paused and fenced the never-touched source.
func AcknowledgeEmptySourceDecommission(ctx context.Context, source *types.DataSourceConfig,
	intent SourceDecommission,
) error {
	cfg, err := parseConfig(source)
	if err != nil {
		return err
	}
	selected, err := selectedBindings(source.ResourceIDs)
	if err != nil || selected[0] != intent.BindingID || cfg.keyID != intent.KeyID ||
		!validBindingID(intent.BindingID) || intent.OperationID == "" || intent.PairOperationID == "" {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud decommission credential mismatch"),
			"Nextcloud decommission credential mismatch")
	}
	payload := struct {
		SourceDecommission
		LogicalWithdrawn  bool `json:"logical_withdrawn"`
		InventoryComplete bool `json:"inventory_complete"`
		InventoryCount    int  `json:"inventory_count"`
		VisibleCount      int  `json:"visible_count"`
		RunningJobs       int  `json:"running_jobs"`
	}{SourceDecommission: intent, LogicalWithdrawn: true, InventoryComplete: true}
	payload.State = ""
	payload.InventorySHA256 = EmptyDecommissionInventorySHA256
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	cli := newClient(cfg)
	endpoint := "/bindings/" + intent.BindingID + "/decommission/" + intent.OperationID + "/ack"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cli.endpointURL(endpoint, nil), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err := signMachineRequest(req, cfg, body); err != nil {
		return err
	}
	resp, err := cli.http.Do(req)
	if err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud decommission ACK outcome uncertain: %w",
			err), fmt.Sprintf("Nextcloud decommission ACK outcome uncertain: %s", apperrors.PublicMessage(err)))
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	var answer struct {
		Decommission SourceDecommission `json:"decommission"`
	}
	if err := readDecommissionResponse(resp.Body, &answer); err != nil {
		return err
	}
	if !answer.Decommission.sameIdentity(intent) || answer.Decommission.State != "acknowledged" ||
		answer.Decommission.InventorySHA256 != EmptyDecommissionInventorySHA256 {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud decommission ACK tuple mismatch"),
			"Nextcloud decommission ACK tuple mismatch")
	}
	return nil
}

func readDecommissionResponse(body io.Reader, out any) error {
	data, err := io.ReadAll(io.LimitReader(body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return fmt.Errorf("invalid Nextcloud decommission response")
	}
	if err := json.Unmarshal(data, out); err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("invalid Nextcloud decommission response: %w", err),
			fmt.Sprintf("invalid Nextcloud decommission response: %s", apperrors.PublicMessage(err)))
	}
	return nil
}

// DecommissionTenantID parses the tenant identifier used by the retirement protocol.
func DecommissionTenantID(value uint64) string { return strconv.FormatUint(value, 10) }
