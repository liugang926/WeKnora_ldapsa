// Package nextcloudwebdav reads files through Nextcloud's standard WebDAV API.
package nextcloudwebdav

import (
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

const properties = `<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:prop>
    <d:resourcetype/><oc:fileid/><d:getetag/>
    <d:getcontentlength/><d:getcontenttype/><d:getlastmodified/>
  </d:prop>
</d:propfind>`

type client struct {
	http               *http.Client
	base               url.URL
	davRoot            string
	username, password string
	namespace          string
}

func newClient(ds *types.DataSourceConfig) (*client, error) {
	if ds == nil {
		return nil, datasource.ErrInvalidConfig
	}
	base, _ := ds.Credentials["base_url"].(string)
	user, _ := ds.Credentials["username"].(string)
	password, _ := ds.Credentials["app_password"].(string)
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		!validSegment(user) || strings.Contains(user, ":") || password == "" {
		return nil, fmt.Errorf("%w: HTTPS instance URL, username and app password are required",
			datasource.ErrInvalidConfig)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	if strings.Contains(strings.ToLower(u.EscapedPath()), "%2f") || (u.Path != "" && !validPath(u.Path)) {
		return nil, datasource.ErrInvalidConfig
	}
	u.RawPath = ""
	u.Host = strings.ToLower(u.Host)
	if err := datasource.ValidateConnectorBaseURL(u.String()); err != nil {
		return nil, fmt.Errorf("%w: Nextcloud server address is not permitted", datasource.ErrInvalidConfig)
	}
	h := datasource.NewConnectorHTTPClient(90 * time.Second)
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &client{
		http:      h,
		base:      *u,
		davRoot:   u.Path + "/remote.php/dav/files/" + user,
		username:  user,
		password:  password,
		namespace: fmt.Sprintf("%x", sha256.Sum256([]byte(u.String()+"\x00"+user))),
	}, nil
}

func validSegment(s string) bool {
	return s != "" && s != "." && s != ".." && utf8.ValidString(s) &&
		!strings.ContainsAny(s, "/\\") && !strings.ContainsFunc(s, unicode.IsControl)
}

func validPath(p string) bool {
	if p == "/" {
		return true
	}
	if !strings.HasPrefix(p, "/") || len(p) > 8192 {
		return false
	}
	for _, seg := range strings.Split(p[1:], "/") {
		if !validSegment(seg) {
			return false
		}
	}
	return true
}

// Send credentials only to a URL constructed from the configured instance.
// An upstream href is used as a DAV path only after checking the exact account
// prefix and its relation to the directory we just requested.
func (c *client) request(ctx context.Context, method, rel string, depth, etag string) (*http.Response, error) {
	u := c.base
	u.Path = c.davRoot + rel
	var body io.Reader
	if method == "PROPFIND" {
		if !strings.HasSuffix(u.Path, "/") {
			u.Path += "/"
		}
		body = strings.NewReader(properties)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, datasource.ErrInvalidConfig
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("Accept-Encoding", "identity")
	if method == "PROPFIND" {
		req.Header.Set("Depth", depth)
		req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	}
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	res, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: Nextcloud %s request failed", datasource.ErrFetchFailed, method)
	}
	want := http.StatusOK
	if method == "PROPFIND" {
		want = http.StatusMultiStatus
	}
	if res.StatusCode != want {
		_ = res.Body.Close()
		cause := datasource.ErrFetchFailed
		switch res.StatusCode {
		case http.StatusUnauthorized:
			cause = datasource.ErrInvalidCredentials
		case http.StatusNotFound:
			cause = datasource.ErrResourceNotFound
		}
		return nil, fmt.Errorf("%w: Nextcloud %s returned HTTP %d", cause, method, res.StatusCode)
	}
	return res, nil
}

func readBounded(body io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: incomplete Nextcloud response", datasource.ErrFetchFailed)
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: Nextcloud response exceeds size limit", datasource.ErrFetchFailed)
	}
	return data, nil
}

type props struct {
	Type *struct {
		Collection *struct{} `xml:"DAV: collection"`
	} `xml:"DAV: resourcetype"`
	ID       string `xml:"http://owncloud.org/ns fileid"`
	ETag     string `xml:"DAV: getetag"`
	Size     *int64 `xml:"DAV: getcontentlength"`
	MIME     string `xml:"DAV: getcontenttype"`
	Modified string `xml:"DAV: getlastmodified"`
}
type entry struct {
	path string
	props
}

func (e entry) folder() bool { return e.Type != nil && e.Type.Collection != nil }

func (c *client) relative(href string) (string, error) {
	u, err := url.Parse(href)
	if err != nil || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		strings.Contains(strings.ToLower(u.EscapedPath()), "%2f") {
		return "", errListing
	}
	if u.IsAbs() {
		if u.Scheme != c.base.Scheme || !strings.EqualFold(u.Host, c.base.Host) {
			return "", errListing
		}
	} else if u.Host != "" {
		return "", errListing
	}
	p := strings.TrimSuffix(u.Path, "/")
	if p == c.davRoot {
		return "/", nil
	}
	rel, ok := strings.CutPrefix(p, c.davRoot)
	if !ok || !validPath(rel) {
		return "", errListing
	}
	return rel, nil
}

var errListing = errors.New("invalid or incomplete Nextcloud directory listing")

func (c *client) list(ctx context.Context, dir, depth string) ([]entry, error) {
	res, err := c.request(ctx, "PROPFIND", dir, depth, "")
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := readBounded(res.Body, 8<<20)
	if err != nil {
		return nil, err
	}
	var multi struct {
		XMLName xml.Name `xml:"DAV: multistatus"`
		Rows    []struct {
			Href   string `xml:"DAV: href"`
			Status string `xml:"DAV: status"`
			Stats  []struct {
				Status string `xml:"DAV: status"`
				Props  props  `xml:"DAV: prop"`
			} `xml:"DAV: propstat"`
		} `xml:"DAV: response"`
	}
	if err = xml.Unmarshal(raw, &multi); err != nil {
		return nil, errListing
	}
	var entries []entry
	seen := make(map[string]bool)
	for _, row := range multi.Rows {
		p, err := c.relative(row.Href)
		if err != nil || seen[p] || (p != dir && (depth == "0" || path.Dir(p) != dir)) || row.Status != "" {
			return nil, errListing
		}
		seen[p] = true
		e := entry{path: p}
		for _, stat := range row.Stats {
			status := strings.Fields(stat.Status)
			if len(status) < 2 {
				return nil, errListing
			}
			if status[1] != "200" {
				continue
			}
			v := stat.Props
			if v.Type != nil {
				e.Type = v.Type
			}
			if v.ID != "" {
				e.ID = v.ID
			}
			if v.ETag != "" {
				e.ETag = v.ETag
			}
			if v.Size != nil {
				e.Size = v.Size
			}
			if v.Modified != "" {
				e.Modified = v.Modified
			}
			if v.MIME != "" {
				e.MIME = v.MIME
			}
		}
		if e.Type == nil || (p == dir && !e.folder()) {
			return nil, errListing
		}
		if p != dir {
			entries = append(entries, e)
		}
	}
	if !seen[dir] {
		return nil, errListing
	}
	slices.SortFunc(entries, func(a, b entry) int { return strings.Compare(a.path, b.path) })
	return entries, nil
}

func (c *client) download(ctx context.Context, e entry, limit int64) ([]byte, error) {
	if e.Size == nil || *e.Size < 0 || *e.Size > limit {
		return nil, errors.New("missing or excessive Nextcloud file size")
	}
	// Strong version precondition binds downloaded bytes to the DAV listing.
	if !validETag(e.ETag) {
		return nil, errors.New("missing strong ETag for Nextcloud file")
	}
	res, err := c.request(ctx, http.MethodGet, e.path, "", e.ETag)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if tag := res.Header.Get("ETag"); tag != "" && tag != e.ETag {
		return nil, errors.New("content of Nextcloud file changed during download")
	}
	body, err := readBounded(res.Body, *e.Size)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) != *e.Size {
		return nil, errors.New("size of Nextcloud file changed during download")
	}
	return body, nil
}

func validETag(s string) bool {
	if len(s) < 3 || len(s) > 512 || s[0] != '"' || s[len(s)-1] != '"' {
		return false
	}
	for _, c := range []byte(s[1 : len(s)-1]) {
		if c < 0x21 || c == '"' || c == 0x7f {
			return false
		}
	}
	return true
}

func fileID(s string) bool {
	n, err := strconv.ParseUint(s, 10, 64)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == s
}
