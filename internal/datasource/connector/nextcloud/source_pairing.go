package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/Tencent/WeKnora/internal/types"
)

// SourcePairingCommit is the tuple prepared by a Nextcloud administrator and
// pinned to a paused WeKnora data source before the remote commit is called.
type SourcePairingCommit struct {
	OperationID     string
	InstanceID      string
	BindingID       string
	TenantID        uint64
	KnowledgeBaseID string
	DataSourceID    string
}

type SourcePairingRemoteError struct{ StatusCode int }

func (e *SourcePairingRemoteError) Error() string {
	return fmt.Sprintf("Nextcloud source pairing commit returned status %d", e.StatusCode)
}

// CommitSourcePairing is safe to retry after a lost response. The remote
// endpoint authenticates the exact pending one-time key and returns the active
// tuple both on the first commit and on an identical retry.
func CommitSourcePairing(ctx context.Context, source *types.DataSourceConfig, pair SourcePairingCommit) error {
	if pair.OperationID == "" || pair.InstanceID == "" || !validBindingID(pair.BindingID) ||
		pair.TenantID == 0 || pair.KnowledgeBaseID == "" || pair.DataSourceID == "" {
		return fmt.Errorf("incomplete Nextcloud source pairing tuple")
	}
	cfg, err := parseConfig(source)
	if err != nil {
		return err
	}
	selected, err := selectedBindings(source.ResourceIDs)
	if err != nil || selected[0] != pair.BindingID {
		return fmt.Errorf("Nextcloud source pairing binding mismatch")
	}
	cli := newClient(cfg)
	payload := struct {
		OperationID     string `json:"operation_id"`
		InstanceID      string `json:"instance_id"`
		TenantID        string `json:"tenant_id"`
		KnowledgeBaseID string `json:"knowledge_base_id"`
		DataSourceID    string `json:"data_source_id"`
	}{pair.OperationID, pair.InstanceID, strconv.FormatUint(pair.TenantID, 10), pair.KnowledgeBaseID, pair.DataSourceID}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := "/bindings/" + pair.BindingID + "/source-pairing/commit"
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
		return fmt.Errorf("Nextcloud pairing commit outcome uncertain: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	if resp.ContentLength > 1<<20 {
		return fmt.Errorf("Nextcloud pairing commit response too large")
	}
	limited := io.LimitReader(resp.Body, (1<<20)+1)
	data, err := io.ReadAll(limited)
	if err != nil || len(data) > 1<<20 {
		return fmt.Errorf("invalid Nextcloud pairing commit response")
	}
	var answer struct {
		Pairing struct {
			OperationID     string `json:"operation_id"`
			InstanceID      string `json:"instance_id"`
			BindingID       string `json:"binding_id"`
			TenantID        string `json:"tenant_id"`
			KnowledgeBaseID string `json:"knowledge_base_id"`
			DataSourceID    string `json:"data_source_id"`
			State           string `json:"state"`
		} `json:"pairing"`
	}
	if err := json.Unmarshal(data, &answer); err != nil ||
		answer.Pairing.OperationID != pair.OperationID || answer.Pairing.InstanceID != pair.InstanceID ||
		answer.Pairing.BindingID != pair.BindingID || answer.Pairing.TenantID != payload.TenantID ||
		answer.Pairing.KnowledgeBaseID != pair.KnowledgeBaseID ||
		answer.Pairing.DataSourceID != pair.DataSourceID || answer.Pairing.State != "active" {
		return fmt.Errorf("Nextcloud pairing commit response tuple mismatch")
	}
	return nil
}

// AbortSourcePairing asks Nextcloud to revoke only this pending operation's
// key. A lost response is retryable because Nextcloud retains an abort-only
// verifier that cannot authenticate ordinary source reads or commits.
func AbortSourcePairing(ctx context.Context, source *types.DataSourceConfig, pair SourcePairingCommit) error {
	if pair.OperationID == "" || pair.InstanceID == "" || !validBindingID(pair.BindingID) ||
		pair.TenantID == 0 || pair.KnowledgeBaseID == "" || pair.DataSourceID == "" {
		return fmt.Errorf("incomplete Nextcloud source pairing abort tuple")
	}
	cfg, err := parseConfig(source)
	if err != nil {
		return err
	}
	selected, err := selectedBindings(source.ResourceIDs)
	if err != nil || selected[0] != pair.BindingID {
		return fmt.Errorf("Nextcloud source pairing binding mismatch")
	}
	cli := newClient(cfg)
	payload := struct {
		OperationID     string `json:"operation_id"`
		InstanceID      string `json:"instance_id"`
		TenantID        string `json:"tenant_id"`
		KnowledgeBaseID string `json:"knowledge_base_id"`
	}{pair.OperationID, pair.InstanceID, strconv.FormatUint(pair.TenantID, 10), pair.KnowledgeBaseID}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := "/bindings/" + pair.BindingID + "/source-pairing/abort"
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
		return fmt.Errorf("Nextcloud pairing abort outcome uncertain: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	if resp.ContentLength > 1<<20 {
		return fmt.Errorf("Nextcloud pairing abort response too large")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return fmt.Errorf("invalid Nextcloud pairing abort response")
	}
	var answer struct {
		Pairing struct {
			OperationID     string  `json:"operation_id"`
			InstanceID      string  `json:"instance_id"`
			BindingID       string  `json:"binding_id"`
			TenantID        string  `json:"tenant_id"`
			KnowledgeBaseID string  `json:"knowledge_base_id"`
			DataSourceID    *string `json:"data_source_id"`
			KeyID           string  `json:"key_id"`
			State           string  `json:"state"`
		} `json:"pairing"`
	}
	if err := json.Unmarshal(data, &answer); err != nil ||
		answer.Pairing.OperationID != pair.OperationID || answer.Pairing.InstanceID != pair.InstanceID ||
		answer.Pairing.BindingID != pair.BindingID || answer.Pairing.TenantID != payload.TenantID ||
		answer.Pairing.KnowledgeBaseID != pair.KnowledgeBaseID ||
		answer.Pairing.KeyID != cfg.keyID || answer.Pairing.DataSourceID != nil ||
		answer.Pairing.State != "aborted" {
		return fmt.Errorf("Nextcloud pairing abort response tuple mismatch")
	}
	return nil
}
