package nextcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Tencent/WeKnora/internal/types"
)

// RecoveryControlState describes current source recovery controls.
// A current exact scoped machine observation, never an administrator-supplied boolean.
type RecoveryControlState struct {
	InstanceID       string `json:"instance_id"`
	BindingID        string `json:"binding_id"`
	PublicationEpoch int64  `json:"publication_epoch"`
	StreamID         string `json:"stream_id"`
	Sequence         string `json:"sequence"`
	ChainSHA256      string `json:"chain_sha256"`
	ControlsClosed   bool   `json:"controls_closed"`
	InventorySHA256  string `json:"control_inventory_sha256"`
}

// InspectRecoveryControls reads the current source recovery controls.
func InspectRecoveryControls(ctx context.Context, source *types.DataSourceConfig) (RecoveryControlState, error) {
	var out RecoveryControlState
	cfg, err := parseConfig(source)
	if err != nil {
		return out, err
	}
	selected, err := selectedBindings(source.ResourceIDs)
	if err != nil || len(selected) != 1 {
		return out, fmt.Errorf("recovery source mismatch")
	}
	resp, err := newClient(cfg).request(ctx, "/bindings/"+selected[0]+"/recovery-state", nil)
	if err != nil {
		return out, err
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return out, &SourcePairingRemoteError{StatusCode: resp.StatusCode}
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&out); err != nil {
		return out, err
	}
	if out.BindingID != selected[0] || out.InstanceID == "" || out.PublicationEpoch < 0 || len(out.ChainSHA256) != 64 ||
		len(out.InventorySHA256) != 64 ||
		out.ControlsClosed {
		return out, fmt.Errorf("recovery controls remain closed")
	}
	return out, nil
}
