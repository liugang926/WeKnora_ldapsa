package nextcloudwebdav

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
)

const (
	sourceType = "nextcloud_webdav"
	maxEntries = 100000
)

var _ datasource.StreamingConnector = (*Connector)(nil)

// Connector imports the files visible in selected WebDAV folders.
type Connector struct{}

// NewConnector creates a connector whose requests use the credentials of each data source.
func NewConnector() *Connector { return &Connector{} }

// Type identifies the connector in the data source registry.
func (*Connector) Type() string { return sourceType }

func selectedRoots(ids []string) ([]string, error) {
	if len(ids) == 0 || len(ids) > 1000 {
		return nil, fmt.Errorf("%w: select one or more Nextcloud folders", datasource.ErrInvalidConfig)
	}
	selected := make(map[string]bool)
	for _, p := range ids {
		if !validPath(p) {
			return nil, fmt.Errorf("%w: invalid Nextcloud folder path", datasource.ErrInvalidConfig)
		}
		selected[p] = true
	}
	var roots []string
	for p := range selected {
		covered := false
		for a := p; a != "/"; {
			a = path.Dir(a)
			if selected[a] {
				covered = true
				break
			}
		}
		if !covered {
			roots = append(roots, p)
		}
	}
	slices.Sort(roots)
	return roots, nil
}

// Validate checks that the service account can list each selected folder.
func (*Connector) Validate(ctx context.Context, ds *types.DataSourceConfig) error {
	c, err := newClient(ds)
	if err != nil {
		return err
	}
	ids := ds.ResourceIDs
	if len(ids) == 0 {
		ids = []string{"/"}
	}
	roots, err := selectedRoots(ids)
	if err != nil {
		return err
	}
	for _, root := range roots {
		if _, err := c.list(ctx, root, "0"); err != nil {
			return err
		}
	}
	return nil
}

// ListResources returns the account root or the immediate child folders of parent.
func (*Connector) ListResources(
	ctx context.Context,
	ds *types.DataSourceConfig,
	parent string,
) ([]types.Resource, error) {
	c, err := newClient(ds)
	if err != nil {
		return nil, err
	}
	if parent == "" {
		if _, err := c.list(ctx, "/", "0"); err != nil {
			return nil, err
		}
		return []types.Resource{{ExternalID: "/", Name: "/", Type: "folder", HasChildren: true}}, nil
	}
	if !validPath(parent) {
		return nil, datasource.ErrInvalidConfig
	}
	entries, err := c.list(ctx, parent, "1")
	if err != nil {
		return nil, err
	}
	out := make([]types.Resource, 0)
	for _, e := range entries {
		if e.folder() {
			out = append(out, types.Resource{
				ExternalID:  e.path,
				Name:        path.Base(e.path),
				Type:        "folder",
				ParentID:    parent,
				HasChildren: true,
			})
		}
	}
	return out, nil
}

// ResolveResourceAncestors returns the parent folders needed to display a saved selection.
func (*Connector) ResolveResourceAncestors(
	_ context.Context,
	_ *types.DataSourceConfig,
	ids []string,
) ([]string, error) {
	set := make(map[string]bool)
	for _, p := range ids {
		if !validPath(p) {
			return nil, datasource.ErrInvalidConfig
		}
		for p != "/" {
			p = path.Dir(p)
			set[p] = true
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	slices.Sort(out)
	return out, nil
}

// The normal service streams one document at a time. Bound the aggregate only
// for callers of the older batch methods, which retain every document in memory.
type collector struct {
	items []types.FetchedItem
	bytes int64
}

func (h *collector) Emit(_ context.Context, item types.FetchedItem) error {
	h.bytes += int64(len(item.Content))
	if h.bytes > 256<<20 {
		return errors.New("batch of Nextcloud files exceeds memory limit; use streaming sync")
	}
	h.items = append(h.items, item)
	return nil
}
func (*collector) Checkpoint(context.Context, *types.SyncCursor) error { return nil }

// FetchAll collects all supported files in the specified folders.
func (c *Connector) FetchAll(
	ctx context.Context,
	ds *types.DataSourceConfig,
	ids []string,
) ([]types.FetchedItem, error) {
	if ds == nil {
		return nil, datasource.ErrInvalidConfig
	}
	config := *ds
	config.ResourceIDs = ids
	items, _, err := c.FetchIncremental(ctx, &config, nil)
	return items, err
}

// FetchIncremental collects files whose revision or path changed since the previous cursor.
func (c *Connector) FetchIncremental(
	ctx context.Context,
	ds *types.DataSourceConfig,
	old *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	h := new(collector)
	next, err := c.FetchStream(ctx, ds, old, h)
	return h.items, next, err
}

// FetchStream reads changed files one at a time and checkpoints successfully emitted revisions.
func (c *Connector) FetchStream(
	ctx context.Context,
	ds *types.DataSourceConfig,
	old *types.SyncCursor,
	h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	client, err := newClient(ds)
	if err != nil {
		return nil, err
	}
	roots, err := selectedRoots(ds.ResourceIDs)
	if err != nil {
		return nil, err
	}
	return client.sync(ctx, roots, old, h, utils.GetMaxFileSize())
}

func readVersions(old *types.SyncCursor, namespace string) (map[string]interface{}, error) {
	versions := make(map[string]interface{})
	if old == nil || len(old.ConnectorCursor) == 0 {
		return versions, nil
	}
	if old.ConnectorCursor["namespace"] != namespace {
		return versions, nil
	}
	files, ok := old.ConnectorCursor["files"].(map[string]interface{})
	if !ok || len(files) > maxEntries {
		return nil, errors.New("invalid Nextcloud sync cursor")
	}
	for id, v := range files {
		s, ok := v.(string)
		if !ok || len(s) != 64 || !fileID(id) {
			return nil, errors.New("invalid Nextcloud sync cursor")
		}
		versions[id] = s
	}
	return versions, nil
}

func (c *client) cursor(files map[string]interface{}) *types.SyncCursor {
	return &types.SyncCursor{
		LastSyncTime:    time.Now().UTC(),
		ConnectorCursor: map[string]interface{}{"namespace": c.namespace, "files": files},
	}
}

func (c *client) sync(
	ctx context.Context,
	roots []string,
	old *types.SyncCursor,
	h datasource.StreamHandler,
	limit int64,
) (*types.SyncCursor, error) {
	versions, err := readVersions(old, c.namespace)
	if err != nil {
		return nil, err
	}
	// A complete scan drops obsolete cursor records only. A missing DAV entry
	// could have become unreadable; its imported WeKnora document stays intact.
	current := make(map[string]interface{})
	seenFiles := make(map[string]bool)
	type directory struct{ root, path string }
	var queue []directory
	for _, root := range roots {
		queue = append(queue, directory{root, root})
	}
	total, changed := 0, 0
	for i := 0; i < len(queue); i++ {
		dir := queue[i]
		entries, err := c.list(ctx, dir.path, "1")
		if err != nil {
			return nil, err
		}
		total += len(entries)
		if total > maxEntries {
			return nil, errors.New("too many Nextcloud entries; select a smaller folder")
		}
		for _, e := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if e.folder() {
				queue = append(queue, directory{dir.root, e.path})
				continue
			}
			if !utils.IsSupportedImportExtension(path.Ext(e.path)) {
				continue
			}
			if !fileID(e.ID) || seenFiles[e.ID] {
				return nil, errors.New("missing or ambiguous Nextcloud file identity")
			}
			seenFiles[e.ID] = true
			fp := fmt.Sprintf("%x", sha256.Sum256([]byte(e.path+"\x00"+e.ETag)))
			if versions[e.ID] == fp {
				current[e.ID] = fp
				continue
			}
			item := c.item(e, dir.root)
			data, fetchErr := c.download(ctx, e, limit)
			if fetchErr != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if errors.Is(fetchErr, datasource.ErrInvalidCredentials) {
					return nil, fetchErr
				}
				item.Metadata["error"] = fetchErr.Error()
			} else if len(data) > 0 {
				item.Content = data
				item.ContentType = e.MIME
				if item.ContentType == "" {
					item.ContentType = http.DetectContentType(data)
				}
				// This browser link uses Nextcloud's human-facing file-ID route.
				// Download and retrieval of the imported copy use the KB's own ACL.
				u := c.base
				u.Path += "/f/" + e.ID
				item.URL = u.String()
			}
			if err := h.Emit(ctx, item); err != nil {
				return nil, err
			}
			if fetchErr != nil {
				continue
			}
			current[e.ID] = fp
			versions[e.ID] = fp
			changed++
			if changed%50 == 0 && len(versions) <= maxEntries {
				if err := h.Checkpoint(ctx, c.cursor(versions)); err != nil {
					return nil, err
				}
			}
		}
	}
	return c.cursor(current), nil
}

func (c *client) item(e entry, root string) types.FetchedItem {
	parts := strings.Split(strings.TrimPrefix(e.path, "/"), "/")
	last := len(parts) - 1
	ext := path.Ext(parts[last])
	parts[last] = strings.TrimSuffix(parts[last], ext)
	for i := range parts {
		parts[i] = datasource.SanitizeFileName(parts[i])
	}
	parts[last] += ext
	updated, _ := http.ParseTime(e.Modified)
	return types.FetchedItem{
		ExternalID:       sourceType + ":" + c.namespace + ":" + e.ID,
		Title:            path.Base(e.path),
		FileName:         strings.Join(parts, "/"),
		UpdatedAt:        updated,
		SourceResourceID: root,
		Metadata: map[string]string{
			"source_type":    sourceType,
			"webdav_file_id": e.ID,
			"webdav_path":    e.path,
			"webdav_etag":    e.ETag,
		},
	}
}
