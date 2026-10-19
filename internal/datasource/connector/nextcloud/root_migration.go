package nextcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Tencent/WeKnora/internal/types"
)

// RootMigration is signed/read using the exact existing source credential.
// Source IDs and content build generations are deliberately unchanged.
type RootMigration struct {
	OperationID        string `json:"operation_id"`
	PairOperationID    string `json:"pair_operation_id"`
	BindingID          string `json:"binding_id"`
	InstanceID         string `json:"instance_id"`
	TenantID           string `json:"tenant_id"`
	KnowledgeBaseID    string `json:"knowledge_base_id"`
	DataSourceID       string `json:"data_source_id"`
	RootFileID         string `json:"root_file_id"`
	PhysicalRootKey    string `json:"physical_root_key"`
	StorageIdentity    string `json:"storage_identity"`
	OldRootHash        string `json:"old_root_hash"`
	NewRootHash        string `json:"new_root_hash"`
	OldRootPath        string `json:"old_root_path"`
	NewRootPath        string `json:"new_root_path"`
	OldEpoch           string `json:"old_epoch"`
	PreparedEpoch      string `json:"prepared_epoch"`
	ResumeEpoch        string `json:"resume_epoch"`
	JournalStreamID    string `json:"journal_stream_id"`
	JournalSequence    string `json:"journal_sequence"`
	JournalChainSHA256 string `json:"journal_chain_sha256"`
	State              string `json:"state"`
}

func ReadRootMigration(
	ctx context.Context,
	source *types.DataSourceConfig,
	binding, operation string,
) (RootMigration, error) {
	var out RootMigration
	cfg, err := parseConfig(source)
	if err != nil {
		return out, err
	}
	selected, err := selectedBindings(source.ResourceIDs)
	if err != nil || selected[0] != binding || !validBindingID(binding) {
		return out, fmt.Errorf("root migration source mismatch")
	}
	cli := newClient(cfg)
	resp, err := cli.request(ctx, "/bindings/"+binding+"/root-migrations/"+operation, nil)
	if err != nil {
		return out, err
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return out, &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	var answer struct {
		Migration RootMigration `json:"migration"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&answer); err != nil {
		return out, err
	}
	out = answer.Migration
	if out.BindingID != binding || out.OperationID != operation ||
		(out.State != "observed" && out.State != "finalized") {
		return out, fmt.Errorf("root migration tuple mismatch")
	}
	return out, nil
}

func AcknowledgeRootMigration(
	ctx context.Context,
	source *types.DataSourceConfig,
	proof RootMigration,
) (RootMigration, error) {
	cfg, err := parseConfig(source)
	if err != nil {
		return RootMigration{}, err
	}
	cli := newClient(cfg)
	body, err := json.Marshal(proof)
	if err != nil {
		return RootMigration{}, err
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		cli.endpointURL("/bindings/"+proof.BindingID+"/root-migrations/"+proof.OperationID+"/ack", nil),
		bytes.NewReader(body),
	)
	if err != nil {
		return RootMigration{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err = signMachineRequest(req, cfg, body); err != nil {
		return RootMigration{}, err
	}
	resp, err := cli.http.Do(req)
	if err != nil {
		return RootMigration{}, err
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return RootMigration{}, &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	var answer struct {
		Migration RootMigration `json:"migration"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&answer); err != nil {
		return RootMigration{}, err
	}
	want, got := proof, answer.Migration
	want.State = ""
	got.State = ""
	if want != got || answer.Migration.State != "finalized" {
		return RootMigration{}, fmt.Errorf("root migration ACK mismatch")
	}
	return answer.Migration, nil
}
