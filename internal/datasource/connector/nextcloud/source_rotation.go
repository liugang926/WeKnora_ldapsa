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

// SourceRotation pins the same source tuple as the initial pairing. Only the
// dedicated new key may commit or finalize this operation in Nextcloud.
type SourceRotation struct {
	OperationID     string
	PairOperationID string
	InstanceID      string
	BindingID       string
	TenantID        uint64
	KnowledgeBaseID string
	DataSourceID    string
}

// CommitSourceRotation commits the prepared remote source rotation.
func CommitSourceRotation(ctx context.Context, source *types.DataSourceConfig, rotation SourceRotation) error {
	return sourceRotationRequest(ctx, source, rotation, "commit", "committed")
}

// FinalizeSourceRotation finalizes the committed remote source rotation.
func FinalizeSourceRotation(ctx context.Context, source *types.DataSourceConfig, rotation SourceRotation) error {
	return sourceRotationRequest(ctx, source, rotation, "finalize", "finalized")
}

// AbortSourceRotation aborts the prepared operation using its old source identity.
func AbortSourceRotation(ctx context.Context, oldSource *types.DataSourceConfig, rotation SourceRotation) error {
	return sourceRotationRequest(ctx, oldSource, rotation, "abort", "aborted")
}

func sourceRotationRequest(ctx context.Context, source *types.DataSourceConfig, rotation SourceRotation,
	action, expectedState string,
) error {
	if rotation.OperationID == "" || rotation.PairOperationID == "" || rotation.InstanceID == "" ||
		!validBindingID(rotation.BindingID) || rotation.TenantID == 0 ||
		rotation.KnowledgeBaseID == "" || rotation.DataSourceID == "" {
		return fmt.Errorf("incomplete Nextcloud source rotation tuple")
	}
	cfg, err := parseConfig(source)
	if err != nil {
		return err
	}
	selected, err := selectedBindings(source.ResourceIDs)
	if err != nil || selected[0] != rotation.BindingID {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud source rotation binding mismatch"),
			"Nextcloud source rotation binding mismatch")
	}
	payload := struct {
		OperationID     string `json:"operation_id"`
		PairOperationID string `json:"pair_operation_id"`
		InstanceID      string `json:"instance_id"`
		TenantID        string `json:"tenant_id"`
		KnowledgeBaseID string `json:"knowledge_base_id"`
		DataSourceID    string `json:"data_source_id"`
	}{
		rotation.OperationID, rotation.PairOperationID, rotation.InstanceID,
		strconv.FormatUint(rotation.TenantID, 10), rotation.KnowledgeBaseID, rotation.DataSourceID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := "/bindings/" + rotation.BindingID + "/source-pairing/rotation/" + action
	cli := newClient(cfg)
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
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud source rotation outcome uncertain: %w",
			err), fmt.Sprintf("Nextcloud source rotation outcome uncertain: %s", apperrors.PublicMessage(err)))
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	if resp.ContentLength > 1<<20 {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud source rotation response too large"),
			"Nextcloud source rotation response too large")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return fmt.Errorf("invalid Nextcloud source rotation response")
	}
	var answer struct {
		Rotation struct {
			OperationID     string `json:"operation_id"`
			PairOperationID string `json:"pair_operation_id"`
			BindingID       string `json:"binding_id"`
			InstanceID      string `json:"instance_id"`
			TenantID        string `json:"tenant_id"`
			KnowledgeBaseID string `json:"knowledge_base_id"`
			DataSourceID    string `json:"data_source_id"`
			OldKeyID        string `json:"old_key_id"`
			NewKeyID        string `json:"new_key_id"`
			State           string `json:"state"`
		} `json:"rotation"`
	}
	keyID, _ := source.Credentials["key_id"].(string)
	if err := json.Unmarshal(data, &answer); err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("invalid Nextcloud source rotation response: %w", err),
			fmt.Sprintf("invalid Nextcloud source rotation response: %s", apperrors.PublicMessage(err)))
	}
	expectedKey := answer.Rotation.NewKeyID
	if action == "abort" {
		expectedKey = answer.Rotation.OldKeyID
	}
	if answer.Rotation.OperationID != rotation.OperationID ||
		answer.Rotation.PairOperationID != rotation.PairOperationID ||
		answer.Rotation.BindingID != rotation.BindingID ||
		answer.Rotation.InstanceID != rotation.InstanceID ||
		answer.Rotation.TenantID != payload.TenantID ||
		answer.Rotation.KnowledgeBaseID != rotation.KnowledgeBaseID ||
		answer.Rotation.DataSourceID != rotation.DataSourceID ||
		expectedKey != keyID ||
		(answer.Rotation.State != expectedState &&
			(action != "commit" || answer.Rotation.State != "finalized")) {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud source rotation response tuple mismatch"),
			"Nextcloud source rotation response tuple mismatch")
	}
	return nil
}
