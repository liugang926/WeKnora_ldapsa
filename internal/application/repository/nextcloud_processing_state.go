package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type NextcloudStageState struct {
	State       string `json:"state"`
	StartedAt   *int64 `json:"started_at"`
	CompletedAt *int64 `json:"completed_at"`
}
type NextcloudGraphState struct {
	Enabled     bool   `json:"enabled"`
	Required    bool   `json:"required"`
	State       string `json:"state"`
	Expected    int    `json:"expected"`
	Succeeded   int    `json:"succeeded"`
	Failed      int    `json:"failed"`
	Skipped     int    `json:"skipped"`
	Cancelled   int    `json:"cancelled"`
	Pending     int    `json:"pending"`
	Persisted   bool   `json:"persisted"`
	ErrorCode   string `json:"error_code"`
	CompletedAt *int64 `json:"completed_at"`
}

// Revisions are decimal strings on the wire, avoiding JS precision loss.
type NextcloudFileStateV2 struct {
	SchemaVersion            int                            `json:"schema_version"`
	EventID                  string                         `json:"event_id"`
	ConnectionID             string                         `json:"connection_id"`
	InstanceID               string                         `json:"instance_id"`
	BindingID                string                         `json:"binding_id"`
	PairOperationID          string                         `json:"pair_operation_id"`
	TenantID                 string                         `json:"tenant_id"`
	KnowledgeBaseID          string                         `json:"knowledge_base_id"`
	DataSourceID             string                         `json:"data_source_id"`
	FileID                   string                         `json:"file_id"`
	SourceContentRevision    string                         `json:"source_content_revision"`
	SourceETag               string                         `json:"source_etag"`
	PublicationEpoch         string                         `json:"publication_epoch"`
	HeadRevision             string                         `json:"head_revision"`
	StatusRevision           string                         `json:"status_revision"`
	KnowledgeID              *string                        `json:"knowledge_id"`
	BuildAttempt             string                         `json:"build_attempt"`
	ProcessingConfigRevision string                         `json:"processing_config_revision"`
	SyncState                string                         `json:"sync_state"`
	Stages                   map[string]NextcloudStageState `json:"stages"`
	Graph                    NextcloudGraphState            `json:"graph"`
	PublicationState         string                         `json:"publication_state"`
	PublishedSourceETag      *string                        `json:"published_source_etag"`
	PermissionRevision       string                         `json:"permission_revision"`
	LastPermissionSyncAt     *int64                         `json:"last_permission_sync_at"`
	WithdrawalState          string                         `json:"withdrawal_state"`
	PermissionReclaimState   string                         `json:"permission_reclaim_state"`
	GCState                  string                         `json:"gc_state"`
	ErrorCode                string                         `json:"error_code"`
	OccurredAt               int64                          `json:"occurred_at"`
	LastVerifiedAt           *int64                         `json:"last_verified_at"`
	QAAvailable              bool                           `json:"qa_available"`
}
type nextcloudFileStateRow struct {
	ConnectionID                                                       string `gorm:"primaryKey"`
	FileID                                                             string `gorm:"primaryKey"`
	PublicationEpoch                                                   int64  `gorm:"primaryKey"`
	HeadRevision, StatusRevision, ConfigRevision                       int64
	HeadIdentity, ConfigDigest, PayloadJSON, BodyDigest, LatestEventID string
	LastVerifiedAt, OccurredAt                                         int64
}

func (nextcloudFileStateRow) TableName() string { return "nextcloud_file_states" }

type nextcloudStatusOutboxRow struct {
	EventID                                        string `gorm:"primaryKey"`
	ConnectionID, FileID                           string
	PublicationEpoch, HeadRevision, StatusRevision int64
	PayloadJSON, State                             string
	Attempts                                       int
	Priority                                       int
	NextAttemptAt                                  int64
	LastErrorCode                                  string
	CreatedAt                                      int64
	DeliveredAt                                    *int64
}

func (nextcloudStatusOutboxRow) TableName() string { return "nextcloud_status_outbox" }

type NextcloudProcessingStateStore struct {
	db                       *gorm.DB
	contentRevisionSupported bool
}

func NewNextcloudProcessingStateStore(db *gorm.DB) *NextcloudProcessingStateStore {
	return &NextcloudProcessingStateStore{
		db:                       db,
		contentRevisionSupported: db.Migrator().HasColumn("nextcloud_source_versions", "desired_content_revision"),
	}
}

func NextcloudStateSigningKey(secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("weknora-nextcloud-state-key-v2"))
	return mac.Sum(nil)
}

func unixPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	n := t.Unix()
	return &n
}
func decimal(n int64) string { return strconv.FormatInt(n, 10) }
func statusSpanState(s string) string {
	if s == types.SpanStatusDone {
		return "succeeded"
	}
	return s
}

// Capture is also used for one-minute compensation. It cannot manufacture an
// old worker's result: scope, pairing and authoritative current candidate are
// reloaded while the version/head lock is held.
func (s *NextcloudProcessingStateStore) Capture(
	ctx context.Context,
	tenant uint64,
	kb, ds, external string,
	verified bool,
) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return captureNextcloudStateInTx(ctx, tx, tenant, kb, ds, external, verified)
	})
}

func CaptureNextcloudKnowledgeStateInTx(ctx context.Context, tx *gorm.DB, knowledgeID string) error {
	if !tx.Migrator().HasTable("nextcloud_file_states") {
		return nil
	}
	var v nextcloudSourceVersion
	err := withNextcloudCandidateKnowledgeID(tx, knowledgeID).Take(&v).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return captureNextcloudStateInTx(ctx, tx, v.TenantID, v.KnowledgeBaseID, v.DataSourceID, v.ExternalID, false)
}

func captureNextcloudStateInTx(
	ctx context.Context,
	tx *gorm.DB,
	tenant uint64,
	kb, ds, external string,
	verified bool,
) error {
	if !tx.Migrator().HasTable("nextcloud_file_states") {
		return nil
	}
	var conn nextcloudEventConnection
	if err := tx.Table("nextcloud_event_connections").
		Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND status='active'", tenant, kb, ds).
		Take(&conn).
		Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	if err := checkNextcloudEventDataSource(tx, conn); err != nil {
		return err
	}
	var pair NextcloudSourcePairing
	if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND state='active'", tenant, kb, ds).
		Take(&pair).
		Error; err != nil {
		return err
	}
	publicationEpoch := pair.PublicationEpoch
	var epoch struct {
		PublicationEpoch int64
		VerifiedAt       int64
		PairOperationID  string
	}
	if err := tx.Table("nextcloud_source_status_epochs").
		Where("connection_id=?", conn.ConnectionID).
		Take(&epoch).
		Error; err == nil {
		if epoch.PairOperationID != pair.OperationID {
			return ErrNextcloudEventScope
		}
		publicationEpoch = epoch.PublicationEpoch
	} else if !errors.Is(
		err,
		gorm.ErrRecordNotFound,
	) {
		return err
	}
	var v nextcloudSourceVersion
	if err := nextcloudVersionQuery(
		tx,
		tenant,
		kb,
		ds,
		external,
	).Clauses(clause.Locking{Strength: "UPDATE"}).
		Take(&v).
		Error; err != nil {
		return err
	}
	prefix := "nextcloud:" + conn.NextcloudInstanceID + ":"
	if !strings.HasPrefix(external, prefix) {
		return ErrNextcloudEventScope
	}
	file := strings.TrimPrefix(external, prefix)
	id, err := strconv.ParseInt(file, 10, 64)
	if err != nil || id < 1 || decimal(id) != file {
		return ErrNextcloudEventScope
	}
	revision, err := types.NormalizeNextcloudContentRevision(v.DesiredContentRevision)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Unix()
	out := NextcloudFileStateV2{
		SchemaVersion:            2,
		ConnectionID:             conn.ConnectionID,
		InstanceID:               conn.NextcloudInstanceID,
		BindingID:                conn.BindingID,
		PairOperationID:          pair.OperationID,
		TenantID:                 strconv.FormatUint(tenant, 10),
		KnowledgeBaseID:          kb,
		DataSourceID:             ds,
		FileID:                   file,
		SourceETag:               v.DesiredETag,
		SourceContentRevision:    revision,
		PublicationEpoch:         decimal(publicationEpoch),
		BuildAttempt:             "0",
		PermissionRevision:       "0",
		ProcessingConfigRevision: "1",
		SyncState:                "discovered",
		PublicationState:         v.State,
		WithdrawalState:          "none",
		PermissionReclaimState:   "unverified",
		GCState:                  "cleanup_pending",
		Stages:                   map[string]NextcloudStageState{},
		Graph:                    NextcloudGraphState{State: "unverified"},
	}
	for _, stage := range []string{"parse", "chunk", "embedding", "index"} {
		out.Stages[stage] = NextcloudStageState{State: "pending"}
	}
	configDigest := ""
	attempt := 0
	if v.CandidateKnowledgeID != "" {
		var k types.Knowledge
		if err := tx.Unscoped().
			Where("id=? AND tenant_id=? AND knowledge_base_id=? AND channel=?", v.CandidateKnowledgeID, tenant, kb, types.ConnectorTypeNextcloud).
			Take(&k).
			Error; err != nil {
			return err
		}
		m, err := nextcloudMetadata(&k)
		if err != nil {
			return err
		}
		if nextcloudMetadataString(m, "datasource_id") != ds || nextcloudMetadataString(m, "external_id") != external ||
			nextcloudMetadataString(m, "nextcloud_binding_id") != conn.BindingID ||
			nextcloudMetadataString(m, "nextcloud_file_id") != file {
			return ErrNextcloudEventScope
		}
		candidateRevision, err := types.NormalizeNextcloudContentRevision(
			nextcloudMetadataString(m, "nextcloud_content_revision"),
		)
		if err != nil || candidateRevision != revision {
			return ErrKnowledgeInputChanged
		}
		var fences []nextcloudContentFence
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id IN ?", tenant, kb, []string{"", k.ID}).
			Find(&fences).
			Error; err != nil {
			return err
		}
		currentFence := len(fences) == 2
		for _, fence := range fences {
			if fence.State != "open" {
				currentFence = false
			}
		}
		out.KnowledgeID = &v.CandidateKnowledgeID
		out.SyncState = "ingested"
		var base types.KnowledgeBase
		if err := tx.Where("id=? AND tenant_id=?", kb, tenant).Take(&base).Error; err != nil {
			return err
		}
		configDigest, err = knowledgeConfigDigest(&base, &k)
		if err != nil {
			return err
		}
		if tx.Migrator().HasTable("knowledge_processing_spans") {
			if err := tx.Model(&types.KnowledgeProcessingSpan{}).
				Where("knowledge_id=?", k.ID).
				Select("COALESCE(MAX(attempt),0)").
				Scan(&attempt).
				Error; err != nil {
				return err
			}
			var spans []types.KnowledgeProcessingSpan
			if err := tx.Where("knowledge_id=? AND attempt=? AND kind=?", k.ID, attempt, types.SpanKindStage).
				Find(&spans).
				Error; err != nil {
				return err
			}
			mapping := map[string]string{
				types.StageDocReader: "parse",
				types.StageChunking:  "chunk",
				types.StageEmbedding: "embedding",
			}
			for _, span := range spans {
				if key, ok := mapping[span.Name]; ok {
					out.Stages[key] = NextcloudStageState{
						State:       statusSpanState(span.Status),
						StartedAt:   unixPtr(span.StartedAt),
						CompletedAt: unixPtr(span.FinishedAt),
					}
				}
			}
		}
		if k.ParseStatus == types.ParseStatusCompleted {
			for _, stage := range []string{"parse", "chunk", "embedding"} {
				if out.Stages[stage].State == "pending" {
					out.Stages[stage] = NextcloudStageState{State: "unverified"}
				}
			}
		}
		out.BuildAttempt = strconv.Itoa(attempt)
		if k.ParseStatus == types.ParseStatusFailed {
			out.SyncState = "failed"
			out.ErrorCode = "document_processing_failed"
		}
		// Index success is a separately verified publication fact. A done parse
		// never proves that a selected index provider persisted its output.
		if v.State == "published" && k.ParseStatus == types.ParseStatusCompleted &&
			nextcloudMetadataString(m, "nextcloud_etag") == v.DesiredETag {
			out.Stages["index"] = NextcloudStageState{State: "succeeded", CompletedAt: unixPtr(&v.UpdatedAt)}
			out.PublishedSourceETag = &v.DesiredETag
		} else if k.ParseStatus == types.ParseStatusFailed {
			out.Stages["index"] = NextcloudStageState{State: "failed"}
		} else {
			out.Stages["index"] = NextcloudStageState{State: "pending"}
		}
		out.Graph, err = readNextcloudGraphState(
			tx,
			&k,
			attempt,
			base.IsGraphEnabled(),
			base.IndexingStrategy.GraphRequired,
		)
		if err != nil {
			return err
		}
		oldEpoch := pair.PublicationEpoch
		if sourceEpoch := nextcloudMetadataString(m, "nextcloud_publication_epoch"); sourceEpoch != "" {
			parsed, err := strconv.ParseInt(sourceEpoch, 10, 64)
			if err != nil {
				return err
			}
			oldEpoch = parsed
		}
		if oldEpoch != publicationEpoch {
			authorized, err := nextcloudRootEpochAuthorized(
				ctx,
				tx,
				tenant,
				kb,
				ds,
				nextcloudMetadataString(m, "nextcloud_path"),
				oldEpoch,
				publicationEpoch,
			)
			if err != nil {
				return err
			}
			if !authorized {
				currentFence = false
			}
		}
		if !currentFence {
			out.PublicationState = "unverified"
			out.PublishedSourceETag = nil
			out.Graph.State = "cancelled"
			out.Graph.Persisted = false
			out.ErrorCode = "content_generation_closed"
		}
	}
	if out.PublicationState == "published" {
		valid := out.SyncState == "ingested" && out.BuildAttempt != "0" && out.Stages["parse"].State == "succeeded" &&
			out.Stages["chunk"].State == "succeeded" &&
			(out.Stages["embedding"].State == "succeeded" || out.Stages["embedding"].State == "skipped") &&
			out.Stages["index"].State == "succeeded" &&
			out.Stages["index"].CompletedAt != nil
		if out.Graph.Required {
			valid = valid && out.Graph.Persisted &&
				(out.Graph.State == "succeeded" || out.Graph.State == "succeeded_empty")
		}
		if !valid {
			out.PublicationState = "unverified"
			out.PublishedSourceETag = nil
			out.ErrorCode = "processing_stage_unverified"
		}
	}
	closed, err := nextcloudControlsClosed(tx, conn.NextcloudInstanceID)
	if err != nil {
		return err
	}
	if closed {
		out.PublicationState = "unverified"
		out.PublishedSourceETag = nil
		out.ErrorCode = "publication_recovery_closed"
	}
	if out.PublicationState == "staging" {
		out.PublicationState = "building"
	}
	if v.State == "tombstone" {
		out.SyncState = "withdrawn"
		out.WithdrawalState = "access_closed"
		out.PublicationState = "withdrawn"
		out.PublishedSourceETag = nil
		out.Graph.State = "cancelled"
		out.Graph.Persisted = false
	}
	// ACL projection and cleanup retain their independent source ledgers.
	if tx.Migrator().HasTable("nextcloud_onboarding_operations") {
		var op types.NextcloudOnboardingOperation
		if err := tx.Where("instance_id=? AND binding_id=? AND knowledge_base_id=?", conn.NextcloudInstanceID, conn.BindingID, kb).
			Take(&op).
			Error; err == nil {
			out.PermissionRevision = strconv.FormatUint(op.PermissionVersion, 10)
			if op.PermissionVersion > 0 {
				out.LastPermissionSyncAt = unixPtr(op.PermissionSyncedAt)
			}
		} else if !errors.Is(
			err,
			gorm.ErrRecordNotFound,
		) {
			return err
		}
	}
	if v.CandidateKnowledgeID != "" && tx.Migrator().HasTable("nextcloud_gc_jobs") {
		var job nextcloudGCJob
		if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND datasource_id=? AND external_id=? AND knowledge_id=?", tenant, kb, ds, external, v.CandidateKnowledgeID).
			Order("created_at DESC").
			Take(&job).
			Error; err == nil {
			switch job.State {
			case "blocked":
				out.GCState = "cleanup_blocked"
			case "running":
				out.GCState = "cleanup_partial"
			case "collected":
				out.GCState = "cleanup_verified"
			}
		} else if !errors.Is(
			err,
			gorm.ErrRecordNotFound,
		) {
			return err
		}
	}
	var old nextcloudFileStateRow
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("connection_id=? AND file_id=? AND publication_epoch=?", conn.ConnectionID, file, publicationEpoch).
		Take(&old).
		Error
	found := err == nil
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	identity := types.GraphDigest(struct {
		ETag            string
		ContentRevision string
		Knowledge       *string
		Attempt         int
		Config          string
	}{v.DesiredETag, revision, out.KnowledgeID, attempt, configDigest})
	row := old
	if !found {
		row = nextcloudFileStateRow{
			ConnectionID:     conn.ConnectionID,
			FileID:           file,
			PublicationEpoch: publicationEpoch,
			HeadRevision:     1,
			ConfigRevision:   1,
		}
	}
	if found && old.HeadIdentity != identity {
		row.HeadRevision++
		row.LastVerifiedAt = 0
	}
	if found && old.ConfigDigest != configDigest {
		row.ConfigRevision++
	}
	out.HeadRevision = decimal(row.HeadRevision)
	out.ProcessingConfigRevision = decimal(row.ConfigRevision)
	// Business digest excludes transport IDs and freshness. Identical signed
	// snapshots refresh only verification, preserving status revision.
	raw, err := json.Marshal(out)
	if err != nil {
		return err
	}
	digest := recoverySHA(raw)
	changed := !found || old.BodyDigest != digest
	if changed {
		row.StatusRevision++
		row.OccurredAt = now
		row.HeadIdentity = identity
		row.ConfigDigest = configDigest
		row.BodyDigest = digest
	}
	if verified {
		row.LastVerifiedAt = now
	}
	out.StatusRevision = decimal(row.StatusRevision)
	out.OccurredAt = row.OccurredAt
	if row.LastVerifiedAt > 0 {
		out.LastVerifiedAt = &row.LastVerifiedAt
	}
	if changed || verified {
		out.EventID = uuid.NewString()
	} else {
		var prior NextcloudFileStateV2
		if err := json.Unmarshal([]byte(old.PayloadJSON), &prior); err != nil {
			return err
		}
		out.EventID = prior.EventID
	}
	raw, err = json.Marshal(out)
	if err != nil {
		return err
	}
	row.PayloadJSON = string(raw)
	row.LatestEventID = out.EventID
	if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "connection_id"}, {Name: "file_id"}, {Name: "publication_epoch"}}, DoUpdates: clause.AssignmentColumns([]string{"head_revision", "status_revision", "config_revision", "head_identity", "config_digest", "payload_json", "body_digest", "last_verified_at", "occurred_at", "latest_event_id"})}).
		Create(&row).
		Error; err != nil {
		return err
	}
	if !changed && !verified {
		return nil
	}
	priority := 0
	if changed {
		priority = 1
	}
	return tx.Create(
		&nextcloudStatusOutboxRow{
			Priority:         priority,
			EventID:          out.EventID,
			ConnectionID:     conn.ConnectionID,
			FileID:           file,
			PublicationEpoch: publicationEpoch,
			HeadRevision:     row.HeadRevision,
			StatusRevision:   row.StatusRevision,
			PayloadJSON:      string(raw),
			State:            "pending",
			CreatedAt:        now,
		},
	).Error
}

func (s *NextcloudProcessingStateStore) Reconcile(ctx context.Context) error {
	// Durable least-recently-verified ordering rotates compensation across all
	// files. Bounded pages do not repeatedly choose the newest 200 documents.
	for batch := 0; batch < 100; batch++ {
		var versions []nextcloudSourceVersion
		query := s.db.WithContext(ctx).
			Table("nextcloud_source_versions v").
			Select("v.*").
			Joins("JOIN nextcloud_event_connections c ON c.tenant_id=v.tenant_id AND c.knowledge_base_id=v.knowledge_base_id AND c.datasource_id=v.datasource_id AND c.status='active'").
			Joins("JOIN nextcloud_source_pairings p ON p.tenant_id=v.tenant_id AND p.knowledge_base_id=v.knowledge_base_id AND p.datasource_id=v.datasource_id AND p.state='active'").
			Joins("LEFT JOIN nextcloud_source_status_epochs e ON e.connection_id=c.connection_id").
			Joins("LEFT JOIN nextcloud_file_states f ON f.connection_id=c.connection_id AND ('nextcloud:' || c.nextcloud_instance_id || ':' || f.file_id)=v.external_id AND f.publication_epoch=COALESCE(e.publication_epoch,p.publication_epoch)").
			Where("COALESCE(f.last_verified_at,0)<?", time.Now().UTC().Add(-55*time.Second).Unix()).
			Order("COALESCE(f.last_verified_at,0),v.tenant_id,v.knowledge_base_id,v.datasource_id,v.external_id").
			Limit(200)
		if err := query.Find(&versions).Error; err != nil {
			return err
		}
		if len(versions) == 0 {
			return nil
		}
		for _, v := range versions {
			if err := s.Capture(ctx, v.TenantID, v.KnowledgeBaseID, v.DataSourceID, v.ExternalID, true); err != nil {
				return fmt.Errorf("capture state: %w", err)
			}
		}
	}
	return nil
}
