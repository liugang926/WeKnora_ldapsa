package nextcloud

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

const apiPath = "/index.php/apps/integration_weknora/api/v1"

var bindingIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type config struct {
	baseURL           *url.URL
	token             string
	keyID             string
	reconcileInterval time.Duration
}

func parseConfig(ds *types.DataSourceConfig) (config, error) {
	if ds == nil {
		return config{}, datasource.ErrInvalidConfig
	}
	base, _ := ds.Settings["base_url"].(string)
	token, _ := ds.Credentials["token"].(string)
	keyID, _ := ds.Credentials["key_id"].(string)
	base = strings.TrimSpace(base)
	token = strings.TrimSpace(token)
	if keyID == "" {
		keyID = "default"
	}
	if base == "" || token == "" {
		return config{}, apperrors.NewProtocolError(fmt.Errorf(
			"%w: settings.base_url and credentials.token are required", datasource.ErrInvalidCredentials),
			fmt.Sprintf("%s: settings.base_url and credentials.token are required",
				apperrors.PublicMessage(datasource.ErrInvalidCredentials)))
	}
	if !validMachineKeyID(keyID) {
		return config{}, apperrors.NewProtocolError(fmt.Errorf("%w: credentials.key_id is invalid",
			datasource.ErrInvalidCredentials), fmt.Sprintf("%s: credentials.key_id is invalid",
			apperrors.PublicMessage(datasource.ErrInvalidCredentials)))
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed == nil || parsed.Hostname() == "" || parsed.Opaque != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return config{}, apperrors.NewProtocolError(fmt.Errorf(
			"%w: base_url must be an HTTP(S) Nextcloud instance URL without credentia"+
				"ls, query or fragment", datasource.ErrInvalidConfig), fmt.Sprintf(
			"%s: base_url must be an HTTP(S) Nextcloud instance URL without credentia"+
				"ls, query or fragment", apperrors.PublicMessage(datasource.ErrInvalidConfig)))
	}
	// The configured URL is an instance root (which may be deployed below a
	// context path), never a caller-supplied API/content URL. Keep its path
	// simple so appending our fixed endpoint cannot escape that context.
	for _, part := range strings.Split(parsed.EscapedPath(), "/") {
		decoded, err := url.PathUnescape(part)
		if err != nil || decoded == "." || decoded == ".." || strings.ContainsAny(decoded, "/\\\r\n") {
			return config{}, apperrors.NewProtocolError(fmt.Errorf("%w: unsafe base_url path",
				datasource.ErrInvalidConfig), fmt.Sprintf("%s: unsafe base_url path",
				apperrors.PublicMessage(datasource.ErrInvalidConfig)))
		}
	}
	if strings.Contains(parsed.Path, "//") {
		return config{}, apperrors.NewProtocolError(fmt.Errorf("%w: unsafe base_url path",
			datasource.ErrInvalidConfig), fmt.Sprintf("%s: unsafe base_url path", apperrors.PublicMessage(
			datasource.ErrInvalidConfig)))
	}
	if !approvedNextcloudOrigin(parsed) {
		return config{}, apperrors.NewProtocolError(fmt.Errorf("%w: Nextcloud origin is not approved",
			datasource.ErrInvalidConfig), fmt.Sprintf("%s: Nextcloud origin is not approved",
			apperrors.PublicMessage(datasource.ErrInvalidConfig)))
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	interval := 24 * time.Hour
	if raw, exists := ds.Settings["full_reconcile_interval_seconds"]; exists {
		var seconds int64
		switch value := raw.(type) {
		case int:
			seconds = int64(value)
		case int64:
			seconds = value
		case float64:
			seconds = int64(value)
			if float64(seconds) != value {
				return config{}, apperrors.NewProtocolError(fmt.Errorf(
					"%w: full_reconcile_interval_seconds must be an integer", datasource.ErrInvalidConfig),
					fmt.Sprintf("%s: full_reconcile_interval_seconds must be an integer",
						apperrors.PublicMessage(datasource.ErrInvalidConfig)))
			}
		case string:
			seconds, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return config{}, apperrors.NewProtocolError(fmt.Errorf(
					"%w: full_reconcile_interval_seconds must be an integer", datasource.ErrInvalidConfig),
					fmt.Sprintf("%s: full_reconcile_interval_seconds must be an integer",
						apperrors.PublicMessage(datasource.ErrInvalidConfig)))
			}
		default:
			return config{}, apperrors.NewProtocolError(fmt.Errorf(
				"%w: full_reconcile_interval_seconds must be an integer", datasource.ErrInvalidConfig),
				fmt.Sprintf("%s: full_reconcile_interval_seconds must be an integer",
					apperrors.PublicMessage(datasource.ErrInvalidConfig)))
		}
		if seconds < 300 || seconds > 7*24*60*60 {
			return config{}, apperrors.NewProtocolError(fmt.Errorf(
				"%w: full_reconcile_interval_seconds must be between 300 and 604800",
				datasource.ErrInvalidConfig), fmt.Sprintf(
				"%s: full_reconcile_interval_seconds must be between 300 and 604800",
				apperrors.PublicMessage(datasource.ErrInvalidConfig)))
		}
		interval = time.Duration(seconds) * time.Second
	}
	return config{baseURL: parsed, token: token, keyID: keyID, reconcileInterval: interval}, nil
}

// Only a server operator may approve a production destination. A KB editor
// can configure a data source but must not be able to direct its Bearer token
// to an arbitrary host. HTTP is limited to the explicit local Docker test
// endpoint (or an explicitly listed loopback test server) behind a dev flag.
func approvedNextcloudOrigin(target *url.URL) bool {
	if target == nil {
		return false
	}
	if target.Scheme == "http" && os.Getenv("WEKNORA_NEXTCLOUD_DEV_HTTP") != "1" {
		return false
	}
	if target.Scheme == "http" && strings.EqualFold(target.Hostname(), "nextcloud") &&
		(target.Port() == "" || target.Port() == "80") &&
		(target.EscapedPath() == "" || target.EscapedPath() == "/") {
		return true
	}
	targetOrigin, ok := nextcloudOrigin(target)
	if !ok {
		return false
	}
	matched := false
	for _, raw := range strings.Split(os.Getenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS"), ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		allowed, err := url.Parse(raw)
		if err != nil || allowed == nil || allowed.User != nil || allowed.Opaque != "" ||
			allowed.RawQuery != "" || allowed.ForceQuery || allowed.Fragment != "" ||
			allowed.RawFragment != "" || allowed.Path != "" || allowed.RawPath != "" ||
			(allowed.Scheme != "https" && allowed.Scheme != "http") {
			return false
		}
		allowedOrigin, valid := nextcloudOrigin(allowed)
		if !valid || (allowed.Scheme == "http" && !isLoopbackHost(allowed.Hostname())) {
			return false
		}
		if allowedOrigin == targetOrigin {
			matched = true
		}
	}
	return matched
}

func nextcloudOrigin(u *url.URL) (string, bool) {
	if u == nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" {
		return "", false
	}
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	parsedPort, err := strconv.Atoi(port)
	if err != nil || parsedPort < 1 || parsedPort > 65535 {
		return "", false
	}
	return u.Scheme + "://" + strings.ToLower(u.Hostname()) + ":" + strconv.Itoa(parsedPort), true
}

func isLoopbackHost(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validBindingID(id string) bool {
	return bindingIDPattern.MatchString(id)
}

type capabilities struct {
	ProtocolVersion string `json:"protocol_version"`
	InstanceID      string `json:"instance_id"`
}

type binding struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	RootFileID       int64  `json:"root_file_id"`
	PublicationState string `json:"publication_state"`
	PublicationEpoch int64  `json:"publication_epoch"`
}

type bindingsResponse struct {
	Bindings []binding `json:"bindings"`
}

type manifestItem struct {
	FileID   int64  `json:"file_id"`
	ETag     string `json:"etag"`
	Name     string `json:"name"`
	Path     string `json:"path"`
	MimeType string `json:"mime_type"`
	Size     int64  `json:"size"`
	MTime    int64  `json:"mtime"`
	URL      string `json:"url"`
	HumanURL string `json:"human_url"`
}

type manifestPage struct {
	Generation string         `json:"generation"`
	Items      []manifestItem `json:"items"`
	Complete   *bool          `json:"complete"`
	NextCursor *string        `json:"next_cursor"`
}

type changeHint struct {
	EventID string `json:"event_id"`
	FileID  *int64 `json:"file_id"`
	Type    string `json:"type"`
}

type changesPage struct {
	BindingID      string       `json:"binding_id"`
	Items          []changeHint `json:"items"`
	NextCursor     string       `json:"next_cursor"`
	HasMore        *bool        `json:"has_more"`
	HintOnly       *bool        `json:"hint_only"`
	RescanRequired *bool        `json:"rescan_required"`
}

func (i manifestItem) validate() error {
	if i.FileID <= 0 || strings.TrimSpace(i.ETag) == "" || i.Name == "" ||
		i.Name == "." || i.Name == ".." || strings.ContainsAny(i.Name, "/\\\r\n") ||
		i.Size < 0 || i.MTime < 0 {
		return fmt.Errorf("invalid manifest item for file_id %d", i.FileID)
	}
	return nil
}

// fileState compares both the version token and the user-visible file fields.
// Nextcloud ETags are opaque change tokens, and a rename need not change one.
type fileState struct {
	ETag     string `json:"etag"`
	Name     string `json:"name"`
	Path     string `json:"path"`
	MimeType string `json:"mime_type"`
	Size     int64  `json:"size"`
	MTime    int64  `json:"mtime"`
}

func (i manifestItem) state() fileState {
	return fileState{ETag: i.ETag, Name: i.Name, Path: i.Path, MimeType: i.MimeType, Size: i.Size, MTime: i.MTime}
}

type syncState struct {
	InstanceID   string                          `json:"instance_id"`
	Files        map[string]map[string]fileState `json:"files"`
	ImportPolicy string                          `json:"import_policy,omitempty"`
	// Changes records the last durable hint cursor consumed for each binding.
	// It is committed together with the inventory only after downstream sync.
	Changes map[string]string `json:"changes,omitempty"`
	// LastReconcileAt bounds the time between complete manifest scans, which
	// recover Nextcloud storage changes that emitted no file event.
	LastReconcileAt int64 `json:"last_reconcile_at,omitempty"`
	// LastContentAuditAt records the last complete content refresh. A hint
	// manifest must not postpone this audit: ETags and metadata are opaque,
	// and a source write may have emitted no retained hint.
	LastContentAuditAt int64 `json:"last_content_audit_at,omitempty"`
	// Missing marks the first complete scan on which a previously imported
	// file was absent. Files retains its last known state until a second
	// complete scan confirms the absence.
	Missing map[string]map[string]bool `json:"missing,omitempty"`
	// Tombstones are retained and emitted on every later complete scan. The
	// importer has no per-item acknowledgement, so dropping them after one
	// attempt could lose a deletion with an uncertain downstream outcome.
	Tombstones map[string]map[string]fileState `json:"tombstones,omitempty"`
}

func decodeCursor(old *types.SyncCursor) (syncState, error) {
	state := syncState{Files: make(map[string]map[string]fileState)}
	if old == nil || old.ConnectorCursor == nil {
		return state, nil
	}
	raw, err := json.Marshal(old.ConnectorCursor)
	if err != nil {
		return syncState{}, apperrors.NewProtocolError(fmt.Errorf("encode Nextcloud cursor: %w", err),
			fmt.Sprintf("encode Nextcloud cursor: %s", apperrors.PublicMessage(err)))
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return syncState{}, apperrors.NewProtocolError(fmt.Errorf("decode Nextcloud cursor: %w", err),
			fmt.Sprintf("decode Nextcloud cursor: %s", apperrors.PublicMessage(err)))
	}
	if state.Files == nil {
		state.Files = make(map[string]map[string]fileState)
	}
	if state.Missing == nil {
		state.Missing = make(map[string]map[string]bool)
	}
	if state.Tombstones == nil {
		state.Tombstones = make(map[string]map[string]fileState)
	}
	if state.Changes == nil {
		state.Changes = make(map[string]string)
	}
	return state, nil
}

// The knowledge importer accepts these extensions. Unsupported files remain
// in Nextcloud and are omitted from this connector's import cursor.
var importExtensions = map[string]bool{
	"pdf": true, "txt": true, "docx": true, "doc": true, "epub": true,
	"html": true, "htm": true, "mhtml": true, "md": true, "markdown": true,
	"xmind": true, "png": true, "jpg": true, "jpeg": true, "gif": true,
	"csv": true, "xlsx": true, "xls": true, "pptx": true, "ppt": true,
	"json": true, "mp3": true, "wav": true, "m4a": true, "flac": true, "ogg": true,
}

func supportedFile(name string, multimodal bool) bool {
	ext := strings.TrimPrefix(strings.ToLower(path.Ext(name)), ".")
	if !importExtensions[ext] {
		return false
	}
	if !multimodal && (ext == "png" || ext == "jpg" || ext == "jpeg" || ext == "gif") {
		return false
	}
	return true
}

// ImportableFileName keeps event publication proof aligned with the manifest
// filter. The event path is a hint about whether an absent file is expected in
// the importable inventory; authorization still comes from the live source.
func ImportableFileName(name string, multimodal bool) bool {
	return supportedFile(name, multimodal)
}
