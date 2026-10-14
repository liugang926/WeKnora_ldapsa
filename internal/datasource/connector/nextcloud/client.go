package nextcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
)

const (
	maxJSONResponseBytes   int64 = 16 << 20
	maxChangeResponseBytes int64 = 1 << 20
	maxContentBytes        int64 = 64 << 20
	maxManifestPages             = 1000
	maxManifestItems             = 100000
	maxChangePages               = 20
	maxChangesPerPage            = 200
)

type client struct {
	cfg  config
	http *http.Client
}

func closeNextcloudResponseBody(ctx context.Context, body io.Closer) {
	if err := body.Close(); err != nil {
		logger.Warnf(ctx, "close Nextcloud response body: %v", err)
	}
}

func newClient(cfg config) *client {
	return &client{
		cfg: cfg,
		http: &http.Client{
			Timeout: 60 * time.Second,
			// A configured private instance is intentional for local/enterprise
			// deployments. Requests are built only from our fixed API paths;
			// redirects (including redirects to an arbitrary content URL) are
			// never followed. Other connectors keep their global SSRF policy.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Transport: &http.Transport{
				DisableCompression: true,
				Proxy:              nil,
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 20 * time.Second,
				IdleConnTimeout:       30 * time.Second,
				MaxIdleConns:          10,
			},
		},
	}
}

func (c *client) endpointURL(endpoint string, query url.Values) string {
	u := *c.cfg.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + apiPath + endpoint
	u.RawPath = ""
	if len(query) != 0 {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func (c *client) request(ctx context.Context, endpoint string, query url.Values, ifMatch ...string) (
	*http.Response, error,
) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL(endpoint, query), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if len(ifMatch) != 0 {
		req.Header.Set("If-Match", `"`+normalizedETag(ifMatch[0])+`"`)
	}
	if err := signMachineRequest(req, c.cfg, nil); err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			return nil, apperrors.NewProtocolError(fmt.Errorf("%w: Nextcloud request %s: %w",
				datasource.ErrRetryableSource, endpoint, err), fmt.Sprintf("%s: Nextcloud request %s: %s",
				apperrors.PublicMessage(datasource.ErrRetryableSource), endpoint, apperrors.PublicMessage(err)))
		}
		return nil, apperrors.NewProtocolError(fmt.Errorf("nextcloud request %s: %w", endpoint, err),
			fmt.Sprintf("Nextcloud request %s: %s", endpoint, apperrors.PublicMessage(err)))
	}
	return resp, nil
}

func (c *client) get(ctx context.Context, endpoint string, query url.Values, ifMatch ...string) (
	*http.Response, error,
) {
	resp, err := c.request(ctx, endpoint, query, ifMatch...)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		closeNextcloudResponseBody(ctx, resp.Body)
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, apperrors.NewProtocolError(fmt.Errorf("%w: Nextcloud %s returned status %d",
				datasource.ErrInvalidCredentials, endpoint, resp.StatusCode), fmt.Sprintf(
				"%s: Nextcloud %s returned status %d", apperrors.PublicMessage(
					datasource.ErrInvalidCredentials), endpoint, resp.StatusCode))
		}
		if retryableNextcloudStatus(resp.StatusCode) ||
			(resp.StatusCode == http.StatusNotFound && len(ifMatch) != 0) {
			return nil, apperrors.NewProtocolError(fmt.Errorf("%w: Nextcloud %s returned status %d",
				datasource.ErrRetryableSource, endpoint, resp.StatusCode), fmt.Sprintf(
				"%s: Nextcloud %s returned status %d", apperrors.PublicMessage(
					datasource.ErrRetryableSource), endpoint, resp.StatusCode))
		}
		return nil, apperrors.NewProtocolError(fmt.Errorf("nextcloud %s returned status %d", endpoint,
			resp.StatusCode), fmt.Sprintf("Nextcloud %s returned status %d", endpoint, resp.StatusCode))
	}
	return resp, nil
}

func retryableNextcloudStatus(code int) bool {
	return code == http.StatusPreconditionFailed || code == http.StatusTooManyRequests ||
		(code >= http.StatusInternalServerError && code <= 599)
}

// changes polls at most maxChangePages per sync. The feed is only a hint:
// callers must reconcile a complete manifest before applying a deletion.
// A 404 supports older app versions by falling back to complete scans.
func (c *client) changes(ctx context.Context, bindingID, oldCursor string) (changeScan, error) {
	if !validBindingID(bindingID) {
		return changeScan{}, apperrors.NewProtocolError(fmt.Errorf("%w: invalid binding ID",
			datasource.ErrInvalidConfig), fmt.Sprintf("%s: invalid binding ID", apperrors.PublicMessage(
			datasource.ErrInvalidConfig)))
	}
	if oldCursor != "" && !validChangeCursor(oldCursor) {
		return changeScan{}, fmt.Errorf("invalid stored Nextcloud changes cursor")
	}
	endpoint := "/bindings/" + bindingID + "/changes"
	cursor := oldCursor
	seen := map[string]bool{}
	changedFiles := map[int64]bool{}
	forceAllContent := false
	var lastEventID uint64
	for pageNum := 0; pageNum < maxChangePages; pageNum++ {
		var query url.Values
		if cursor != "" {
			query = url.Values{"cursor": {cursor}}
		}
		resp, err := c.request(ctx, endpoint, query)
		if err != nil {
			return changeScan{}, err
		}
		if resp.StatusCode == http.StatusNotFound {
			closeNextcloudResponseBody(ctx, resp.Body)
			return changeScan{Supported: false, ForceAllContent: true}, nil
		}
		if resp.StatusCode == http.StatusConflict {
			var conflict struct {
				RescanRequired bool   `json:"rescan_required"`
				NextCursor     string `json:"next_cursor"`
			}
			if err := decodeChangeResponse(resp, endpoint, &conflict); err != nil {
				return changeScan{}, err
			}
			if !conflict.RescanRequired || (conflict.NextCursor != "" && !validChangeCursor(conflict.NextCursor)) {
				return changeScan{}, apperrors.NewProtocolError(fmt.Errorf(
					"nextcloud changes conflict lacks a valid rescan cursor"),
					"Nextcloud changes conflict lacks a valid rescan cursor")
			}
			// The recovery cursor is not authoritative until a complete manifest
			// has been processed and its downstream result committed.
			return changeScan{
				Supported: true, NeedsReconcile: true, ForceAllContent: true,
				Cursor: conflict.NextCursor,
			}, nil
		}
		if resp.StatusCode != http.StatusOK {
			closeNextcloudResponseBody(ctx, resp.Body)
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				return changeScan{}, apperrors.NewProtocolError(fmt.Errorf(
					"%w: Nextcloud %s returned status %d", datasource.ErrInvalidCredentials, endpoint,
					resp.StatusCode), fmt.Sprintf("%s: Nextcloud %s returned status %d",
					apperrors.PublicMessage(datasource.ErrInvalidCredentials), endpoint, resp.StatusCode))
			}
			if retryableNextcloudStatus(resp.StatusCode) {
				return changeScan{}, apperrors.NewProtocolError(fmt.Errorf(
					"%w: Nextcloud %s returned status %d", datasource.ErrRetryableSource, endpoint,
					resp.StatusCode), fmt.Sprintf("%s: Nextcloud %s returned status %d",
					apperrors.PublicMessage(datasource.ErrRetryableSource), endpoint, resp.StatusCode))
			}
			return changeScan{}, apperrors.NewProtocolError(fmt.Errorf("nextcloud %s returned status %d",
				endpoint, resp.StatusCode), fmt.Sprintf("Nextcloud %s returned status %d", endpoint, resp.StatusCode))
		}
		var page changesPage
		if err := decodeChangeResponse(resp, endpoint, &page); err != nil {
			return changeScan{}, err
		}
		if page.BindingID != bindingID || page.HintOnly == nil || !*page.HintOnly ||
			page.RescanRequired == nil || *page.RescanRequired || page.HasMore == nil ||
			!validChangeCursor(page.NextCursor) || len(page.Items) > maxChangesPerPage ||
			(*page.HasMore && len(page.Items) == 0) {
			return changeScan{}, fmt.Errorf("invalid Nextcloud changes page for binding %s", bindingID)
		}
		for _, hint := range page.Items {
			id, err := strconv.ParseUint(hint.EventID, 10, 64)
			if err != nil || id == 0 || id <= lastEventID || !validChangeType(hint.Type) ||
				(hint.FileID != nil && *hint.FileID < 1) {
				return changeScan{}, fmt.Errorf("invalid Nextcloud change hint for binding %s", bindingID)
			}
			lastEventID = id
			// Hints trigger a complete manifest comparison, never deletion.
			// A touched file must be re-read even when a backend reused its
			// opaque ETag and other metadata. Broad or unscoped hints fall
			// back to a full content refresh.
			switch hint.Type {
			case "upsert", "metadata", "delete", "reconcile":
				if hint.FileID == nil {
					forceAllContent = true
				} else {
					changedFiles[*hint.FileID] = true
				}
			case "subtree_scan", "subtree_moved", "subtree_deleted":
				forceAllContent = true
			}
		}
		if page.NextCursor == cursor || seen[page.NextCursor] {
			if *page.HasMore || len(page.Items) > 0 {
				return changeScan{}, apperrors.NewProtocolError(fmt.Errorf(
					"nextcloud changes cursor did not advance"), "Nextcloud changes cursor did not advance")
			}
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
		if !*page.HasMore {
			return changeScan{
				Supported: true, NeedsReconcile: len(page.Items) > 0 || pageNum > 0,
				Cursor: cursor, ChangedFiles: changedFiles, ForceAllContent: forceAllContent,
			}, nil
		}
	}
	return changeScan{
		Supported: true, NeedsReconcile: true, Cursor: cursor,
		ChangedFiles: changedFiles, ForceAllContent: true,
	}, nil
}

type changeScan struct {
	Supported       bool
	NeedsReconcile  bool
	Cursor          string
	ChangedFiles    map[int64]bool
	ForceAllContent bool
}

func decodeChangeResponse(resp *http.Response, endpoint string, target interface{}) error {
	ctx := context.Background()
	if resp.Request != nil {
		ctx = resp.Request.Context()
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.ContentLength > maxChangeResponseBytes {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud %s exceeds response size limit", endpoint),
			fmt.Sprintf("Nextcloud %s exceeds response size limit", endpoint))
	}
	data, err := readLimited(resp.Body, maxChangeResponseBytes)
	if err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud %s: %w", endpoint, err), fmt.Sprintf(
			"Nextcloud %s: %s", endpoint, apperrors.PublicMessage(err)))
	}
	if err := json.Unmarshal(data, target); err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("decode Nextcloud %s: %w", endpoint, err), fmt.Sprintf(
			"decode Nextcloud %s: %s", endpoint, apperrors.PublicMessage(err)))
	}
	return nil
}

func validChangeCursor(cursor string) bool {
	if len(cursor) == 0 || len(cursor) > 512 {
		return false
	}
	for _, c := range cursor {
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func validChangeType(kind string) bool {
	switch kind {
	case "upsert", "metadata", "delete", "subtree_scan", "subtree_moved", "subtree_deleted", "reconcile":
		return true
	default:
		return false
	}
}

func readLimited(body io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf("%w: read Nextcloud response: %w",
			datasource.ErrRetryableSource, err), fmt.Sprintf("%s: read Nextcloud response: %s",
			apperrors.PublicMessage(datasource.ErrRetryableSource), apperrors.PublicMessage(err)))
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("response exceeds %d MiB limit", limit>>20)
	}
	return data, nil
}

func (c *client) getJSON(ctx context.Context, endpoint string, query url.Values, target interface{}) error {
	resp, err := c.get(ctx, endpoint, query)
	if err != nil {
		return err
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.ContentLength > maxJSONResponseBytes {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud %s exceeds manifest response size limit",
			endpoint), fmt.Sprintf("Nextcloud %s exceeds manifest response size limit", endpoint))
	}
	data, err := readLimited(resp.Body, maxJSONResponseBytes)
	if err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("nextcloud %s: %w", endpoint, err), fmt.Sprintf(
			"Nextcloud %s: %s", endpoint, apperrors.PublicMessage(err)))
	}
	if err := json.Unmarshal(data, target); err != nil {
		return apperrors.NewProtocolError(fmt.Errorf("decode Nextcloud %s: %w", endpoint, err), fmt.Sprintf(
			"decode Nextcloud %s: %s", endpoint, apperrors.PublicMessage(err)))
	}
	return nil
}

func (c *client) capabilities(ctx context.Context) (capabilities, error) {
	var info capabilities
	if err := c.getJSON(ctx, "/capabilities", nil, &info); err != nil {
		return capabilities{}, err
	}
	if info.ProtocolVersion != "1" || strings.TrimSpace(info.InstanceID) == "" {
		return capabilities{}, fmt.Errorf("unsupported or incomplete Nextcloud integration protocol")
	}
	return info, nil
}

func (c *client) bindings(ctx context.Context) ([]binding, error) {
	var result bindingsResponse
	if err := c.getJSON(ctx, "/bindings", nil, &result); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(result.Bindings))
	for _, b := range result.Bindings {
		if !validBindingID(b.ID) || strings.TrimSpace(b.Name) == "" || b.RootFileID <= 0 || seen[b.ID] {
			return nil, fmt.Errorf("invalid or duplicate Nextcloud binding")
		}
		seen[b.ID] = true
	}
	return result.Bindings, nil
}

func (c *client) manifest(ctx context.Context, bindingID string) ([]manifestItem, string, error) {
	if !validBindingID(bindingID) {
		return nil, "", apperrors.NewProtocolError(fmt.Errorf("%w: invalid binding ID",
			datasource.ErrInvalidConfig), fmt.Sprintf("%s: invalid binding ID", apperrors.PublicMessage(
			datasource.ErrInvalidConfig)))
	}
	endpoint := "/bindings/" + bindingID + "/manifest"
	seenCursor := make(map[string]bool)
	seenFiles := make(map[int64]bool)
	var items []manifestItem
	var generation, cursor string
	for pageNum := 0; pageNum < maxManifestPages; pageNum++ {
		var query url.Values
		if cursor != "" {
			query = url.Values{"cursor": {cursor}}
		}
		var page manifestPage
		if err := c.getJSON(ctx, endpoint, query, &page); err != nil {
			return nil, "", err
		}
		if page.Complete == nil || strings.TrimSpace(page.Generation) == "" {
			return nil, "", apperrors.NewProtocolError(fmt.Errorf(
				"nextcloud manifest lacks completion or generation"),
				"Nextcloud manifest lacks completion or generation")
		}
		if generation == "" {
			generation = page.Generation
		} else if generation != page.Generation {
			return nil, "", apperrors.NewProtocolError(fmt.Errorf(
				"%w: Nextcloud manifest generation changed during pagination",
				datasource.ErrRetryableSource), fmt.Sprintf(
				"%s: Nextcloud manifest generation changed during pagination",
				apperrors.PublicMessage(datasource.ErrRetryableSource)))
		}
		for _, item := range page.Items {
			if err := item.validate(); err != nil {
				return nil, "", err
			}
			if seenFiles[item.FileID] {
				return nil, "", apperrors.NewProtocolError(fmt.Errorf(
					"nextcloud manifest contains duplicate file_id %d", item.FileID), fmt.Sprintf(
					"Nextcloud manifest contains duplicate file_id %d", item.FileID))
			}
			seenFiles[item.FileID] = true
			items = append(items, item)
			if len(items) > maxManifestItems {
				return nil, "", apperrors.NewProtocolError(fmt.Errorf(
					"nextcloud manifest exceeds %d item limit", maxManifestItems), fmt.Sprintf(
					"Nextcloud manifest exceeds %d item limit", maxManifestItems))
			}
		}
		if *page.Complete {
			if page.NextCursor != nil && *page.NextCursor != "" {
				return nil, "", apperrors.NewProtocolError(fmt.Errorf(
					"nextcloud manifest is complete but has next_cursor"),
					"Nextcloud manifest is complete but has next_cursor")
			}
			return items, generation, nil
		}
		if page.NextCursor == nil || *page.NextCursor == "" || seenCursor[*page.NextCursor] {
			return nil, "", apperrors.NewProtocolError(fmt.Errorf(
				"nextcloud manifest is incomplete without a new next_cursor"),
				"Nextcloud manifest is incomplete without a new next_cursor")
		}
		cursor = *page.NextCursor
		seenCursor[cursor] = true
	}
	return nil, "", apperrors.NewProtocolError(fmt.Errorf("nextcloud manifest exceeds %d pages",
		maxManifestPages), fmt.Sprintf("Nextcloud manifest exceeds %d pages", maxManifestPages))
}

func normalizedETag(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "W/")
	return strings.Trim(raw, `"`)
}

func (c *client) content(ctx context.Context, bindingID string, item manifestItem) ([]byte, string, error) {
	if !validBindingID(bindingID) || item.FileID <= 0 {
		return nil, "", apperrors.NewProtocolError(fmt.Errorf("%w: invalid content identifier",
			datasource.ErrInvalidConfig), fmt.Sprintf("%s: invalid content identifier",
			apperrors.PublicMessage(datasource.ErrInvalidConfig)))
	}
	endpoint := "/bindings/" + bindingID + "/files/" + strconv.FormatInt(item.FileID, 10) + "/content"
	resp, err := c.get(ctx, endpoint, nil, item.ETag)
	if err != nil {
		return nil, "", err
	}
	defer closeNextcloudResponseBody(ctx, resp.Body)
	if resp.ContentLength > maxContentBytes || item.Size > maxContentBytes {
		return nil, "", apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud file %d exceeds %d MiB download limit", item.FileID, maxContentBytes>>20),
			fmt.Sprintf("Nextcloud file %d exceeds %d MiB download limit", item.FileID, maxContentBytes>>20))
	}
	content, err := readLimited(resp.Body, maxContentBytes)
	if err != nil {
		return nil, "", apperrors.NewProtocolError(fmt.Errorf("download Nextcloud file %d: %w", item.FileID,
			err), fmt.Sprintf("download Nextcloud file %d: %s", item.FileID, apperrors.PublicMessage(err)))
	}
	if len(content) == 0 {
		return nil, "", apperrors.NewProtocolError(fmt.Errorf(
			"nextcloud file %d is empty; refusing to advance cursor without an import"+
				"able item", item.FileID), fmt.Sprintf(
			"Nextcloud file %d is empty; refusing to advance cursor without an import"+
				"able item", item.FileID))
	}
	if tag := normalizedETag(resp.Header.Get("ETag")); tag == "" || tag != normalizedETag(item.ETag) {
		return nil, "", apperrors.NewProtocolError(fmt.Errorf(
			"%w: Nextcloud file %d ETag changed during download", datasource.ErrRetryableSource,
			item.FileID), fmt.Sprintf("%s: Nextcloud file %d ETag changed during download",
			apperrors.PublicMessage(datasource.ErrRetryableSource), item.FileID))
	}
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = item.MimeType
	}
	return content, contentType, nil
}
