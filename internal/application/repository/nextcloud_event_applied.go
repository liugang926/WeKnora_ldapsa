package repository

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// A receipt can be acknowledged as applied only after the completed source
// inventory and every importable file in it agree with the durable publication
// rows. An in-flight parser is different from a broken or missing publication:
// the former should be polled without starting another scan.
type nextcloudApplyProof int

const (
	nextcloudApplyRetry nextcloudApplyProof = iota
	nextcloudApplyWaiting
	nextcloudApplyComplete
)

type nextcloudApplyFile struct {
	ETag string `json:"etag"`
	Name string `json:"name"`
	Path string `json:"path"`
}

type nextcloudApplyCursor struct {
	InstanceID string                                   `json:"instance_id"`
	Files      map[string]map[string]nextcloudApplyFile `json:"files"`
	Tombstones map[string]map[string]json.RawMessage    `json:"tombstones"`
	Changes    map[string]string                        `json:"changes"`
}

type nextcloudApplyKnowledge struct {
	ID           string     `gorm:"column:id"`
	ParseStatus  string     `gorm:"column:parse_status"`
	EnableStatus string     `gorm:"column:enable_status"`
	FileName     string     `gorm:"column:file_name"`
	Metadata     types.JSON `gorm:"column:metadata"`
	DeletedAt    *time.Time `gorm:"column:deleted_at"`
}

func nextcloudApplyMetadataString(metadata map[string]json.RawMessage, key string) string {
	var value string
	_ = json.Unmarshal(metadata[key], &value)
	return value
}

// Nextcloud's returned changes cursor is HMAC protected by Nextcloud and
// delivered over the signed machine API connection. We do not know its HMAC
// secret; parsing the binding and decimal position here must not be confused
// with independent authentication of arbitrary caller-supplied cursors.
func nextcloudChangeCursorID(raw, bindingID string) (int64, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) > 512 {
		return 0, false
	}
	var parts []json.RawMessage
	if json.Unmarshal(decoded, &parts) != nil || len(parts) != 4 {
		return 0, false
	}
	var version int
	var binding, decimal, signature string
	if json.Unmarshal(parts[0], &version) != nil || version != 1 ||
		json.Unmarshal(parts[1], &binding) != nil || binding != bindingID ||
		json.Unmarshal(parts[2], &decimal) != nil ||
		json.Unmarshal(parts[3], &signature) != nil {
		return 0, false
	}
	if decimal == "" || (len(decimal) > 1 && decimal[0] == '0') ||
		strings.Trim(decimal, "0123456789") != "" || len(signature) != 64 {
		return 0, false
	}
	if bytes, err := hex.DecodeString(signature); err != nil || len(bytes) != 32 {
		return 0, false
	}
	id, err := strconv.ParseInt(decimal, 10, 64)
	return id, err == nil && id >= 0
}

func nextcloudDispatchPublicationProof(
	tx *gorm.DB,
	connection nextcloudEventConnection,
	appliedEventID, targetEventID int64,
	syncStartedAt time.Time,
) (
	nextcloudApplyProof,
	error,
) {
	var source types.DataSource
	if err := tx.Select("last_sync_cursor").Where("id = ?", connection.DatasourceID).Take(&source).Error; err != nil {
		return nextcloudApplyRetry, err
	}
	cursor, err := source.ParseSyncCursor()
	if err != nil || cursor == nil || cursor.ConnectorCursor == nil || cursor.LastSyncTime.Before(syncStartedAt) {
		return nextcloudApplyRetry, err
	}
	raw, err := json.Marshal(cursor.ConnectorCursor)
	if err != nil {
		return nextcloudApplyRetry, err
	}
	var snapshot nextcloudApplyCursor
	if json.Unmarshal(raw, &snapshot) != nil || snapshot.InstanceID != connection.NextcloudInstanceID {
		return nextcloudApplyRetry, nil
	}
	observedID, valid := nextcloudChangeCursorID(snapshot.Changes[connection.BindingID], connection.BindingID)
	if !valid || observedID < targetEventID {
		return nextcloudApplyRetry, nil
	}
	files := snapshot.Files[connection.BindingID]
	if files == nil {
		return nextcloudApplyRetry, nil
	}
	tombstones := snapshot.Tombstones[connection.BindingID]
	versions := make([]nextcloudSourceVersion, 0)
	if err := tx.Table("nextcloud_source_versions").
		Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ?",
			connection.TenantID, connection.KnowledgeBaseID, connection.DatasourceID).
		Find(&versions).Error; err != nil {
		return nextcloudApplyRetry, err
	}
	prefix := "nextcloud:" + connection.NextcloudInstanceID + ":"
	byExternalID := make(map[string]nextcloudSourceVersion, len(versions))
	for _, version := range versions {
		if !strings.HasPrefix(version.ExternalID, prefix) {
			return nextcloudApplyRetry, nil
		}
		fileID := strings.TrimPrefix(version.ExternalID, prefix)
		if _, present := files[fileID]; !present {
			if len(tombstones[fileID]) == 0 || version.State != "tombstone" ||
				version.DesiredETag != "" || version.CandidateKnowledgeID != "" {
				return nextcloudApplyRetry, nil
			}
		} else if version.State != "published" && version.State != "staging" {
			return nextcloudApplyRetry, nil
		}
		byExternalID[version.ExternalID] = version
	}
	for fileID, file := range files {
		parsedID, parseErr := strconv.ParseInt(fileID, 10, 64)
		if parseErr != nil || parsedID < 1 || strconv.FormatInt(parsedID, 10) != fileID ||
			file.ETag == "" || file.Name == "" || file.Path == "" || len(tombstones[fileID]) > 0 {
			return nextcloudApplyRetry, nil
		}
		version, found := byExternalID[prefix+fileID]
		if !found || version.CandidateKnowledgeID == "" || version.DesiredETag != file.ETag {
			return nextcloudApplyRetry, nil
		}
	}
	for fileID := range tombstones {
		parsedID, parseErr := strconv.ParseInt(fileID, 10, 64)
		if parseErr != nil || parsedID < 1 || strconv.FormatInt(parsedID, 10) != fileID {
			return nextcloudApplyRetry, nil
		}
		if _, present := files[fileID]; present {
			return nextcloudApplyRetry, nil
		}
		if byExternalID[prefix+fileID].State != "tombstone" {
			return nextcloudApplyRetry, nil
		}
	}
	var knowledgeRows []nextcloudApplyKnowledge
	if err := tx.Table("knowledges").
		Select("id", "parse_status", "enable_status", "file_name", "metadata", "deleted_at").
		Where("tenant_id = ? AND knowledge_base_id = ? AND channel = ?",
			connection.TenantID, connection.KnowledgeBaseID, types.ConnectorTypeNextcloud).
		Find(&knowledgeRows).Error; err != nil {
		return nextcloudApplyRetry, err
	}
	byKnowledgeID := make(map[string]nextcloudApplyKnowledge, len(knowledgeRows))
	for _, row := range knowledgeRows {
		var metadata map[string]json.RawMessage
		if json.Unmarshal(row.Metadata, &metadata) != nil ||
			nextcloudApplyMetadataString(metadata, "datasource_id") != connection.DatasourceID {
			return nextcloudApplyRetry, nil
		}
		version, found := byExternalID[nextcloudApplyMetadataString(metadata, "external_id")]
		if !found {
			return nextcloudApplyRetry, nil
		}
		if nextcloudApplyMetadataString(metadata, "nextcloud_etag") != "" &&
			(row.DeletedAt != nil || version.State != "published" || version.CandidateKnowledgeID != row.ID ||
				nextcloudApplyMetadataString(metadata, "nextcloud_etag") != version.DesiredETag) {
			return nextcloudApplyRetry, nil
		}
		if version.State == "tombstone" && row.DeletedAt == nil {
			return nextcloudApplyRetry, nil
		}
		byKnowledgeID[row.ID] = row
	}
	for fileID, file := range files {
		version := byExternalID[prefix+fileID]
		candidate, found := byKnowledgeID[version.CandidateKnowledgeID]
		if !found {
			return nextcloudApplyRetry, nil
		}
		if version.State == "staging" && candidate.DeletedAt == nil &&
			(candidate.ParseStatus ==
				types.ParseStatusPending ||
				candidate.ParseStatus ==
					types.ParseStatusProcessing ||
				candidate.ParseStatus == types.ParseStatusFinalizing) {
			return nextcloudApplyWaiting, nil
		}
		var metadata map[string]json.RawMessage
		if json.Unmarshal(candidate.Metadata, &metadata) != nil ||
			version.State != "published" || candidate.DeletedAt != nil ||
			candidate.ParseStatus != types.ParseStatusCompleted || candidate.EnableStatus != "enabled" ||
			candidate.FileName != file.Name || nextcloudApplyMetadataString(metadata, "nextcloud_path") != file.Path ||
			nextcloudApplyMetadataString(metadata, "nextcloud_etag") != file.ETag ||
			nextcloudApplyMetadataString(metadata, "nextcloud_instance_id") != connection.NextcloudInstanceID ||
			nextcloudApplyMetadataString(metadata, "nextcloud_binding_id") != connection.BindingID ||
			nextcloudApplyMetadataString(metadata, "nextcloud_file_id") != fileID ||
			nextcloudApplyMetadataString(metadata, "source_resource_id") != connection.BindingID ||
			nextcloudApplyMetadataString(metadata, "external_id") != prefix+fileID {
			return nextcloudApplyRetry, nil
		}
	}
	barriersMatch, err := nextcloudLatestHintBarriers(tx, connection, appliedEventID,
		targetEventID, files, tombstones, byExternalID)
	if err != nil || !barriersMatch {
		return nextcloudApplyRetry, err
	}
	return nextcloudApplyComplete, nil
}

// Check the latest signed event for each file in the unapplied range. A
// changes cursor alone does not prove that a newly captured manifest saw the
// corresponding file version. Unsupported files have no source-version row;
// their ordinary manifest reconciliation remains the fallback.
func nextcloudLatestHintBarriers(tx *gorm.DB, connection nextcloudEventConnection,
	appliedEventID, targetEventID int64, files map[string]nextcloudApplyFile,
	tombstones map[string]json.RawMessage,
	versions map[string]nextcloudSourceVersion,
) (bool, error) {
	var unscoped int64
	if err := tx.Table("nextcloud_event_inbox").Where(
		"connection_id = ? AND event_id > ? AND event_id <= ? AND file_id IS NULL AND event_type NOT IN ?",
		connection.ConnectionID, appliedEventID, targetEventID,
		[]string{"subtree_scan", "subtree_moved", "subtree_deleted", "reconcile"}).Count(&unscoped).Error; err != nil {
		return false, err
	}
	if unscoped != 0 {
		// A broad hint has no file identity with which to bind a manifest
		// observation. Do not turn an advanced changes cursor into an ACK.
		return false, nil
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", connection.KnowledgeBaseID, connection.TenantID).
		Take(&kb).Error; err != nil {
		return false, err
	}
	var latest []struct {
		FileID       int64   `gorm:"column:file_id"`
		EventType    string  `gorm:"column:event_type"`
		ETag         *string `gorm:"column:etag"`
		Path         *string `gorm:"column:path"`
		RelativePath *string `gorm:"column:relative_path"`
	}
	err := tx.Raw(`SELECT i.file_id, i.event_type, i.etag, i.path, i.relative_path
		FROM nextcloud_event_inbox AS i
		JOIN (SELECT file_id, MAX(event_id) AS event_id
			FROM nextcloud_event_inbox
			WHERE connection_id = ? AND event_id > ? AND event_id <= ?
				AND file_id IS NOT NULL
			GROUP BY file_id) AS latest
			ON latest.file_id = i.file_id AND latest.event_id = i.event_id
		WHERE i.connection_id = ?`, connection.ConnectionID,
		appliedEventID, targetEventID, connection.ConnectionID).Scan(&latest).Error
	if err != nil {
		return false, err
	}
	prefix := "nextcloud:" + connection.NextcloudInstanceID + ":"
	for _, hint := range latest {
		fileID := strconv.FormatInt(hint.FileID, 10)
		file, present := files[fileID]
		version, indexed := versions[prefix+fileID]
		switch hint.EventType {
		case "upsert", "metadata":
			if hint.ETag == nil || strings.TrimSpace(*hint.ETag) == "" ||
				hint.RelativePath == nil || strings.TrimSpace(*hint.RelativePath) == "" {
				return false, nil
			}
			// The signed binding-relative path is directly comparable to the
			// manifest path. It also distinguishes an importable new file from
			// an unsupported extension omitted from the inventory.
			importable := nextcloud.ImportableFileName(path.Base(*hint.RelativePath), kb.IsMultimodalEnabled())
			if importable {
				if present && (file.ETag != *hint.ETag || file.Path != *hint.RelativePath) {
					return false, nil
				}
				if !present && indexed && (version.State != "tombstone" || len(tombstones[fileID]) == 0) {
					return false, nil
				}
			} else if present || (indexed && (version.State != "tombstone" || len(tombstones[fileID]) == 0)) {
				return false, nil
			}
		case "delete":
			if present || (indexed && (version.State != "tombstone" || len(tombstones[fileID]) == 0)) {
				return false, nil
			}
		case "subtree_scan", "subtree_moved", "subtree_deleted", "reconcile":
			// A second complete scan is required by the dispatch state machine.
		default:
			return false, nil
		}
	}
	return true, nil
}

// These hint shapes cannot be bound to a concrete manifest observation with
// the current signed payload. Report them for manual review after the short
// parser handoff window instead of retrying an impossible proof forever.
func nextcloudHintRangeUnprovable(tx *gorm.DB, connectionID string,
	appliedEventID, targetEventID int64,
) (bool, error) {
	var broad int64
	if err := tx.Table("nextcloud_event_inbox").Where(
		"connection_id = ? AND event_id > ? AND event_id <= ? AND file_id IS NULL AND event_type NOT IN ?",
		connectionID, appliedEventID, targetEventID,
		[]string{"subtree_scan", "subtree_moved", "subtree_deleted", "reconcile"}).Count(&broad).Error; err != nil {
		return false, err
	}
	if broad != 0 {
		return true, nil
	}
	var latest []struct {
		EventType    string  `gorm:"column:event_type"`
		ETag         *string `gorm:"column:etag"`
		Path         *string `gorm:"column:path"`
		RelativePath *string `gorm:"column:relative_path"`
	}
	if err := tx.Raw(`SELECT i.event_type, i.etag, i.path, i.relative_path
		FROM nextcloud_event_inbox AS i
		JOIN (SELECT file_id, MAX(event_id) AS event_id
			FROM nextcloud_event_inbox
			WHERE connection_id = ? AND event_id > ? AND event_id <= ?
				AND file_id IS NOT NULL
			GROUP BY file_id) AS latest
			ON latest.file_id = i.file_id AND latest.event_id = i.event_id
		WHERE i.connection_id = ?`, connectionID, appliedEventID,
		targetEventID, connectionID).Scan(&latest).Error; err != nil {
		return false, err
	}
	for _, hint := range latest {
		switch hint.EventType {
		case "upsert", "metadata":
			if hint.ETag == nil || strings.TrimSpace(*hint.ETag) == "" ||
				hint.RelativePath == nil || strings.TrimSpace(*hint.RelativePath) == "" {
				return true, nil
			}
		case "delete":
			// File identity and two-scan tombstone evidence can prove this.
		case "subtree_scan", "subtree_moved", "subtree_deleted", "reconcile":
			// Dispatch requires a second complete scan for this broad hint.
		default:
			return true, nil
		}
	}
	return false, nil
}

func nextcloudHintRangeNeedsTwoScans(tx *gorm.DB, connection nextcloudEventConnection,
	appliedEventID, targetEventID int64,
) (bool, error) {
	var count int64
	if err := tx.Table("nextcloud_event_inbox").Where(
		"connection_id = ? AND event_id > ? AND event_id <= ? AND event_type IN ?",
		connection.ConnectionID, appliedEventID, targetEventID,
		[]string{"subtree_scan", "subtree_moved", "subtree_deleted", "reconcile"}).Count(&count).Error; err != nil {
		return false, err
	}
	if count != 0 {
		return true, nil
	}
	var source types.DataSource
	if err := tx.Select("last_sync_cursor").Where("id = ?", connection.DatasourceID).
		Take(&source).Error; err != nil {
		return false, err
	}
	cursor, err := source.ParseSyncCursor()
	if err != nil || cursor == nil || cursor.ConnectorCursor == nil {
		return false, err
	}
	raw, err := json.Marshal(cursor.ConnectorCursor)
	if err != nil {
		return false, err
	}
	var snapshot nextcloudApplyCursor
	if json.Unmarshal(raw, &snapshot) != nil || snapshot.InstanceID != connection.NextcloudInstanceID ||
		snapshot.Files[connection.BindingID] == nil {
		return false, nil
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", connection.KnowledgeBaseID,
		connection.TenantID).Take(&kb).Error; err != nil {
		return false, err
	}
	var latest []struct {
		FileID       int64   `gorm:"column:file_id"`
		RelativePath *string `gorm:"column:relative_path"`
	}
	if err := tx.Raw(`SELECT i.file_id, i.relative_path
		FROM nextcloud_event_inbox AS i
		JOIN (SELECT file_id, MAX(event_id) AS event_id
			FROM nextcloud_event_inbox
			WHERE connection_id = ? AND event_id > ? AND event_id <= ?
				AND file_id IS NOT NULL GROUP BY file_id) AS latest
			ON latest.file_id = i.file_id AND latest.event_id = i.event_id
		WHERE i.connection_id = ? AND i.event_type IN ('upsert', 'metadata')`,
		connection.ConnectionID, appliedEventID, targetEventID,
		connection.ConnectionID).Scan(&latest).Error; err != nil {
		return false, err
	}
	for _, hint := range latest {
		if hint.RelativePath == nil ||
			!nextcloud.ImportableFileName(path.Base(*hint.RelativePath), kb.IsMultimodalEnabled()) {
			continue
		}
		if _, present := snapshot.Files[connection.BindingID][strconv.FormatInt(hint.FileID, 10)]; !present {
			// An explicit publication exclusion or a later move out of the
			// binding can make an event-time upsert absent from the current
			// authoritative manifest. Two complete scans bound this fallback.
			return true, nil
		}
	}
	return false, nil
}
