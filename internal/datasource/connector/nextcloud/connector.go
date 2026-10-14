// Package nextcloud imports files exposed by the integration_weknora Nextcloud app.
package nextcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

var (
	_ datasource.Connector              = (*Connector)(nil)
	_ datasource.FullSyncWithCursor     = (*Connector)(nil)
	_ datasource.StreamingConnector     = (*Connector)(nil)
	_ datasource.FullStreamingConnector = (*Connector)(nil)
)

// A Nextcloud stream must report its live source identity before any item can
// be written. The service uses it to fence revoked event tasks and cloned
// instances. The ordinary batch Connector methods do not need this callback.
type identityStreamHandler interface {
	datasource.StreamHandler
	ObserveNextcloudIdentity(context.Context, string, string) error
}

// failedCandidateStreamHandler is optional for streaming syncs. The service
// supplies current failed file IDs after validating the live source identity.
type failedCandidateStreamHandler interface {
	FailedNextcloudCandidateETags() map[string]string
}

// A candidate retry owns one file. Filtering here, before content GET, keeps
// an unrelated changed file from failing or delaying that retry.
type candidateRetryTargetStreamHandler interface {
	NextcloudRetryExternalID() string
}

// Event dispatch needs proof that its own sync completed a full manifest.
// A settled changes feed alone cannot advance the reconciliation timestamp
// required for the event's applied acknowledgement.
type eventManifestStreamHandler interface {
	NextcloudRequireManifest() bool
}

// Connector implements the authenticated Nextcloud datasource adapter.
type Connector struct{}

// NewConnector constructs the Nextcloud datasource adapter.
func NewConnector() *Connector { return &Connector{} }

// Type identifies the Nextcloud connector provider.
func (*Connector) Type() string { return types.ConnectorTypeNextcloud }

// PairingIdentity is the live Nextcloud identity behind a configured source.
// It contains no machine credential and can be pinned to a committed data
// source when provisioning an inbound event connection.
type PairingIdentity struct {
	InstanceID       string
	BindingID        string
	BaseURL          string
	PublicationState string
	PublicationEpoch int64
}

// InspectPairing validates the selected binding against signed Nextcloud
// capabilities and binding registry responses. Callers must separately check
// the committed data source's tenant, knowledge base and config fingerprint.
func InspectPairing(ctx context.Context, ds *types.DataSourceConfig) (PairingIdentity, error) {
	if ds == nil || ds.Type != types.ConnectorTypeNextcloud {
		return PairingIdentity{}, datasource.ErrInvalidConfig
	}
	cfg, err := parseConfig(ds)
	if err != nil {
		return PairingIdentity{}, err
	}
	selected, err := selectedBindings(ds.ResourceIDs)
	if err != nil {
		return PairingIdentity{}, err
	}
	cli := newClient(cfg)
	capabilities, err := cli.capabilities(ctx)
	if err != nil {
		return PairingIdentity{}, err
	}
	bindings, err := cli.bindings(ctx)
	if err != nil {
		return PairingIdentity{}, err
	}
	for _, binding := range bindings {
		if binding.ID == selected[0] {
			baseURL := *cfg.baseURL
			baseURL.Host = strings.ToLower(baseURL.Host)
			return PairingIdentity{
				InstanceID:       capabilities.InstanceID,
				BindingID:        binding.ID,
				BaseURL:          baseURL.String(),
				PublicationState: binding.PublicationState,
				PublicationEpoch: binding.PublicationEpoch,
			}, nil
		}
	}
	return PairingIdentity{}, apperrors.NewProtocolError(fmt.Errorf(
		"%w: selected Nextcloud binding is unavailable", datasource.ErrInvalidConfig), fmt.Sprintf(
		"%s: selected Nextcloud binding is unavailable", apperrors.PublicMessage(datasource.ErrInvalidConfig)))
}

// Validate checks the selected binding and remote instance configuration.
func (*Connector) Validate(ctx context.Context, ds *types.DataSourceConfig) error {
	_, err := InspectPairing(ctx, ds)
	return err
}

// ListResources lists the remote binding resources exposed by the source instance.
func (*Connector) ListResources(ctx context.Context, ds *types.DataSourceConfig, parentID string) (
	[]types.Resource, error,
) {
	if parentID != "" {
		return []types.Resource{}, nil
	}
	cfg, err := parseConfig(ds)
	if err != nil {
		return nil, err
	}
	cli := newClient(cfg)
	if _, err := cli.capabilities(ctx); err != nil {
		return nil, err
	}
	bindings, err := cli.bindings(ctx)
	if err != nil {
		return nil, err
	}
	resources := make([]types.Resource, 0, len(bindings))
	for _, binding := range bindings {
		resources = append(resources, types.Resource{
			ExternalID: binding.ID,
			Name:       binding.Name,
			Type:       "folder",
			URL:        cfg.baseURL.String(),
			Metadata:   map[string]interface{}{"root_file_id": binding.RootFileID},
		})
	}
	return resources, nil
}

// ResolveResourceAncestors returns no ancestors for the flat binding resource list.
func (*Connector) ResolveResourceAncestors(context.Context, *types.DataSourceConfig, []string) ([]string, error) {
	return []string{}, nil
}

// FetchAll reads the complete selected remote inventory.
func (c *Connector) FetchAll(ctx context.Context, ds *types.DataSourceConfig, resourceIDs []string) (
	[]types.FetchedItem, error,
) {
	if len(resourceIDs) == 0 && ds != nil {
		resourceIDs = ds.ResourceIDs
	}
	items, _, err := c.walk(ctx, ds, resourceIDs, nil, true, false, nil)
	return items, err
}

// FetchAllFromCursor re-fetches every current file while retaining the prior
// inventory for the same two-scan deletion reconciliation as incremental sync.
func (c *Connector) FetchAllFromCursor(
	ctx context.Context, ds *types.DataSourceConfig, resourceIDs []string, old *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	return c.walk(ctx, ds, resourceIDs, old, true, true, nil)
}

// FetchIncremental reads changes while retaining the prior inventory evidence.
func (c *Connector) FetchIncremental(
	ctx context.Context, ds *types.DataSourceConfig, old *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	if ds == nil {
		return nil, nil, datasource.ErrInvalidConfig
	}
	return c.walk(ctx, ds, ds.ResourceIDs, old, false, true, nil)
}

// FetchStream bounds retained content to one fetched file. It deliberately
// makes no mid-scan checkpoint: the existing cursor describes only a completed
// inventory, and a partial inventory would corrupt two-scan deletion evidence.
func (c *Connector) FetchStream(
	ctx context.Context, ds *types.DataSourceConfig, old *types.SyncCursor, h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	identity, ok := h.(identityStreamHandler)
	if !ok {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud stream requires source identity observer"), "Nextcloud stream requires source identity observer")
	}
	if ds == nil {
		return nil, datasource.ErrInvalidConfig
	}
	_, next, err := c.walk(ctx, ds, ds.ResourceIDs, old, false, true, identity)
	return next, err
}

// FetchFullStream re-fetches content while retaining the old cursor as the
// deletion baseline for explicit full scans.
func (c *Connector) FetchFullStream(
	ctx context.Context, ds *types.DataSourceConfig, old *types.SyncCursor, h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	identity, ok := h.(identityStreamHandler)
	if !ok {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud stream requires source identity observer"), "Nextcloud stream requires source identity observer")
	}
	if ds == nil {
		return nil, datasource.ErrInvalidConfig
	}
	_, next, err := c.walk(ctx, ds, ds.ResourceIDs, old, true, true, identity)
	return next, err
}

func selectedBindings(resourceIDs []string) ([]string, error) {
	// V1 publication is one folder per dedicated knowledge base. Reject
	// multiple selections before any source reads so legacy configurations
	// cannot silently combine different audiences in one index.
	if len(resourceIDs) != 1 {
		return nil, apperrors.NewProtocolError(fmt.Errorf("%w: select exactly one Nextcloud binding",
			datasource.ErrInvalidConfig), fmt.Sprintf("%s: select exactly one Nextcloud binding",
			apperrors.PublicMessage(datasource.ErrInvalidConfig)))
	}
	seen := make(map[string]bool, len(resourceIDs))
	selected := make([]string, 0, len(resourceIDs))
	for _, id := range resourceIDs {
		if !validBindingID(id) {
			return nil, apperrors.NewProtocolError(fmt.Errorf("%w: invalid Nextcloud binding ID",
				datasource.ErrInvalidConfig), fmt.Sprintf("%s: invalid Nextcloud binding ID",
				apperrors.PublicMessage(datasource.ErrInvalidConfig)))
		}
		if !seen[id] {
			seen[id] = true
			selected = append(selected, id)
		}
	}
	return selected, nil
}

func (c *Connector) walk(
	ctx context.Context, ds *types.DataSourceConfig, resourceIDs []string,
	old *types.SyncCursor, full bool, reconcile bool, h identityStreamHandler,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	cfg, err := parseConfig(ds)
	if err != nil {
		return nil, nil, err
	}
	selected, err := selectedBindings(resourceIDs)
	if err != nil {
		return nil, nil, err
	}
	previous, err := decodeCursor(old)
	if err != nil {
		return nil, nil, err
	}
	cli := newClient(cfg)
	capabilities, err := cli.capabilities(ctx)
	if err != nil {
		return nil, nil, err
	}
	if old != nil && old.ConnectorCursor != nil && previous.InstanceID != capabilities.InstanceID {
		// A cloned instance may reuse file IDs. Dropping the old inventory
		// would also lose deletion evidence. A truncated cursor with no
		// instance ID is unsafe for the same reason.
		return nil, nil, apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud cursor instance differs from source; manual repair required"),
			"Nextcloud cursor instance differs from source; manual repair required")
	}
	if old != nil && old.ConnectorCursor != nil {
		for _, bindingID := range selected {
			if previous.Files[bindingID] == nil {
				return nil, nil, apperrors.NewProtocolError(fmt.Errorf(
					"nextcloud cursor inventory is incomplete; manual repair required"),
					"Nextcloud cursor inventory is incomplete; manual repair required")
			}
		}
	}
	if h != nil {
		// A selected binding that has been withdrawn must stop the stream
		// before it stages any source version or deletion.
		bindings, err := cli.bindings(ctx)
		if err != nil {
			return nil, nil, err
		}
		found := false
		for _, binding := range bindings {
			if binding.ID == selected[0] {
				found = true
				break
			}
		}
		if !found {
			return nil, nil, apperrors.NewProtocolError(fmt.Errorf(
				"%w: selected Nextcloud binding is unavailable", datasource.ErrInvalidConfig), fmt.Sprintf(
				"%s: selected Nextcloud binding is unavailable", apperrors.PublicMessage(
					datasource.ErrInvalidConfig)))
		}
		if err := h.ObserveNextcloudIdentity(ctx, capabilities.InstanceID, selected[0]); err != nil {
			return nil, nil, err
		}
	}
	var failedCandidates map[string]string
	if retry, ok := h.(failedCandidateStreamHandler); ok {
		failedCandidates = retry.FailedNextcloudCandidateETags()
	}
	retryExternalID := ""
	if retry, ok := h.(candidateRetryTargetStreamHandler); ok {
		retryExternalID = retry.NextcloudRetryExternalID()
	}
	requireManifest := false
	if event, ok := h.(eventManifestStreamHandler); ok {
		requireManifest = event.NextcloudRequireManifest()
	}
	policy := fmt.Sprintf("v1:multimodal=%t", ds.MultimodalEnabled)
	// Hints are read before the authoritative manifest. If a file changes
	// between these two reads, its hint is still pending on the next run.
	// No hint is ever used as evidence that a file is gone.
	changeCursors := make(map[string]string, len(selected))
	changedFiles := make(map[int64]bool)
	now := time.Now().UTC()
	forceContentAudit := full || previous.LastContentAuditAt <= 0 ||
		previous.LastContentAuditAt > now.Unix() ||
		now.Sub(time.Unix(previous.LastContentAuditAt, 0)) >= cfg.reconcileInterval
	forceAllContent := forceContentAudit
	forceManifest := requireManifest || len(failedCandidates) > 0 || forceContentAudit ||
		previous.LastReconcileAt <= 0 ||
		previous.LastReconcileAt > now.Unix() ||
		now.Sub(time.Unix(previous.LastReconcileAt, 0)) >= cfg.reconcileInterval ||
		previous.ImportPolicy != policy
	for _, bindingID := range selected {
		if previous.Files[bindingID] == nil || len(previous.Missing[bindingID]) > 0 ||
			len(previous.Tombstones[bindingID]) > 0 {
			forceManifest = true
		}
		if !reconcile {
			continue
		}
		hints, err := cli.changes(ctx, bindingID, previous.Changes[bindingID])
		if err != nil {
			return nil, nil, apperrors.NewProtocolError(fmt.Errorf("nextcloud binding %s changes: %w",
				bindingID, err), fmt.Sprintf("Nextcloud binding %s changes: %s", bindingID,
				apperrors.PublicMessage(err)))
		}
		if !hints.Supported || hints.NeedsReconcile || previous.Changes[bindingID] == "" {
			forceManifest = true
		}
		if hints.ForceAllContent || previous.Changes[bindingID] == "" {
			forceAllContent = true
		}
		for fileID := range hints.ChangedFiles {
			changedFiles[fileID] = true
		}
		if hints.Supported {
			changeCursors[bindingID] = hints.Cursor
		}
	}
	if reconcile && !forceManifest {
		// The bounded feed is empty and a complete reconciliation is recent.
		// Keep the confirmed inventory and its deletion evidence intact.
		previous.Changes = changeCursors
		cursor, err := encodeSyncCursor(previous)
		return nil, cursor, err
	}

	// Never compare or download from a partial scan. All selected bindings
	// must yield complete, internally consistent manifests first. A failed
	// page, permission response, or generation change returns no new cursor.
	manifests := make(map[string][]manifestItem, len(selected))
	generations := make(map[string]string, len(selected))
	globalFiles := make(map[int64]string)
	for _, bindingID := range selected {
		manifest, generation, err := cli.manifest(ctx, bindingID)
		if err != nil {
			return nil, nil, apperrors.NewProtocolError(fmt.Errorf("nextcloud binding %s manifest: %w",
				bindingID, err), fmt.Sprintf("Nextcloud binding %s manifest: %s", bindingID,
				apperrors.PublicMessage(err)))
		}
		for _, file := range manifest {
			if other, exists := globalFiles[file.FileID]; exists {
				return nil, nil, apperrors.NewProtocolError(fmt.Errorf(
					"nextcloud file %d appears in bindings %s and %s", file.FileID, other, bindingID),
					fmt.Sprintf("Nextcloud file %d appears in bindings %s and %s", file.FileID, other, bindingID))
			}
			globalFiles[file.FileID] = bindingID
		}
		manifests[bindingID] = manifest
		generations[bindingID] = generation
	}

	next := syncState{
		InstanceID:         capabilities.InstanceID,
		Files:              make(map[string]map[string]fileState, len(selected)),
		ImportPolicy:       policy,
		Changes:            changeCursors,
		LastReconcileAt:    now.Unix(),
		LastContentAuditAt: previous.LastContentAuditAt,
		Missing:            make(map[string]map[string]bool),
		Tombstones:         make(map[string]map[string]fileState),
	}
	if forceAllContent {
		next.LastContentAuditAt = now.Unix()
	}
	var out []types.FetchedItem
	emit := func(item types.FetchedItem) error {
		if retryExternalID != "" && item.ExternalID != retryExternalID {
			return nil
		}
		if h != nil {
			return h.Emit(ctx, item)
		}
		out = append(out, item)
		return nil
	}
	importableFiles := make(map[int64]bool)
	for _, bindingID := range selected {
		current := make(map[string]fileState)
		next.Files[bindingID] = current
		for _, file := range manifests[bindingID] {
			if !supportedFile(file.Name, ds.MultimodalEnabled) {
				continue
			}
			importableFiles[file.FileID] = true
			id := strconv.FormatInt(file.FileID, 10)
			state := file.state()
			oldState, hadOld := previous.Files[bindingID][id]
			_, wasTombstone := previous.Tombstones[bindingID][id]
			if retryExternalID != "" && retryExternalID != "nextcloud:"+capabilities.InstanceID+":"+id {
				// The complete manifest was validated above; only the claimed file
				// may incur a content read. The service retains the original cursor
				// so this neighbor is handled by a later ordinary sync.
				current[id] = state
				continue
			}
			_, retryFailed := failedCandidates[id]
			if !forceAllContent && !retryFailed && !changedFiles[file.FileID] && previous.ImportPolicy == policy &&
				hadOld && !previous.Missing[bindingID][id] && !wasTombstone &&
				oldState == state {
				current[id] = state
				continue
			}
			content, contentType, err := cli.content(ctx, bindingID, file)
			if err != nil {
				return nil, nil, err
			}
			if err := emit(fetchedItem(capabilities.InstanceID, bindingID, generations[bindingID], file,
				content, contentType)); err != nil {
				return nil, nil, err
			}
			current[id] = state
		}
	}

	if !reconcile {
		return out, nil, nil
	}
	// A manifest is only a complete traversal, not an atomic snapshot. A
	// missing file is therefore a deletion candidate on the first complete
	// scan and a tombstone only after a separate complete scan confirms it.
	// Keep emitting confirmed tombstones until the file reappears because the
	// importer has no per-item acknowledgement or deletion receipt.
	for _, bindingID := range selected {
		current := next.Files[bindingID]
		for _, id := range sortedStateIDs(previous.Files[bindingID]) {
			state := previous.Files[bindingID][id]
			fileID, err := strconv.ParseInt(id, 10, 64)
			if err != nil || fileID < 1 {
				return nil, nil, fmt.Errorf("invalid Nextcloud file ID in cursor")
			}
			if importableFiles[fileID] {
				// A move to another selected binding is an upsert there, not
				// a deletion of this stable source identity.
				continue
			}
			if _, alreadyConfirmed := previous.Tombstones[bindingID][id]; alreadyConfirmed {
				continue
			}
			if previous.Missing[bindingID][id] {
				rememberTombstone(&next, bindingID, id, state)
				if err := emit(deletedItem(capabilities.InstanceID, bindingID, id, state)); err != nil {
					return nil, nil, err
				}
			} else {
				current[id] = state
				if next.Missing[bindingID] == nil {
					next.Missing[bindingID] = make(map[string]bool)
				}
				next.Missing[bindingID][id] = true
			}
		}
		for _, id := range sortedStateIDs(previous.Tombstones[bindingID]) {
			state := previous.Tombstones[bindingID][id]
			fileID, err := strconv.ParseInt(id, 10, 64)
			if err != nil || fileID < 1 {
				return nil, nil, fmt.Errorf("invalid Nextcloud tombstone file ID in cursor")
			}
			if importableFiles[fileID] {
				continue
			}
			rememberTombstone(&next, bindingID, id, state)
			if err := emit(deletedItem(capabilities.InstanceID, bindingID, id, state)); err != nil {
				return nil, nil, err
			}
		}
	}
	cursor, err := encodeSyncCursor(next)
	return out, cursor, err
}

func encodeSyncCursor(state syncState) (*types.SyncCursor, error) {
	cursorMap := make(map[string]interface{})
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf("encode Nextcloud cursor: %w", err), fmt.Sprintf(
			"encode Nextcloud cursor: %s", apperrors.PublicMessage(err)))
	}
	if err := json.Unmarshal(raw, &cursorMap); err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf("decode Nextcloud cursor: %w", err), fmt.Sprintf(
			"decode Nextcloud cursor: %s", apperrors.PublicMessage(err)))
	}
	return &types.SyncCursor{LastSyncTime: time.Now().UTC(), ConnectorCursor: cursorMap}, nil
}

func sortedStateIDs(states map[string]fileState) []string {
	ids := make([]string, 0, len(states))
	for id := range states {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func rememberTombstone(next *syncState, bindingID, id string, state fileState) {
	if next.Tombstones[bindingID] == nil {
		next.Tombstones[bindingID] = make(map[string]fileState)
	}
	next.Tombstones[bindingID][id] = state
}

func deletedItem(instanceID, bindingID, id string, state fileState) types.FetchedItem {
	return types.FetchedItem{
		ExternalID:       "nextcloud:" + instanceID + ":" + id,
		SourceResourceID: bindingID,
		Title:            state.Name,
		FileName:         state.Name,
		IsDeleted:        true,
		Metadata: map[string]string{
			"channel":               types.ConnectorTypeNextcloud,
			"nextcloud_instance_id": instanceID,
			"nextcloud_binding_id":  bindingID,
			"nextcloud_file_id":     id,
		},
	}
}

func fetchedItem(instanceID, bindingID, generation string, file manifestItem, content []byte,
	contentType string,
) types.FetchedItem {
	updatedAt := time.Time{}
	if file.MTime > 0 {
		updatedAt = time.Unix(file.MTime, 0).UTC()
	}
	return types.FetchedItem{
		ExternalID:       "nextcloud:" + instanceID + ":" + strconv.FormatInt(file.FileID, 10),
		SourceResourceID: bindingID,
		Title:            file.Name,
		FileName:         file.Name,
		Content:          content,
		ContentType:      contentType,
		URL:              strings.TrimSpace(file.URL),
		UpdatedAt:        updatedAt,
		Metadata: map[string]string{
			"channel":               types.ConnectorTypeNextcloud,
			"nextcloud_instance_id": instanceID,
			"nextcloud_binding_id":  bindingID,
			"nextcloud_file_id":     strconv.FormatInt(file.FileID, 10),
			"nextcloud_etag":        file.ETag,
			"nextcloud_path":        file.Path,
			"nextcloud_human_url":   strings.TrimSpace(file.HumanURL),
			"nextcloud_generation":  generation,
		},
	}
}
