package repository

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type nextcloudCandidateRetryJob struct {
	TenantID          uint64 `gorm:"primaryKey"`
	KnowledgeBaseID   string `gorm:"primaryKey"`
	DataSourceID      string `gorm:"column:datasource_id;primaryKey"`
	ExternalID        string `gorm:"primaryKey"`
	DesiredETag       string `gorm:"column:desired_etag"`
	FailedCandidateID string
	FirstStagedAt     time.Time
	AttemptCount      int
	NextAttemptAt     time.Time
	State             string
	LeaseToken        *string
	LeaseUntil        *time.Time
	SyncLogID         *string
	LastErrorCode     string
	UpdatedAt         time.Time
}

func (nextcloudCandidateRetryJob) TableName() string { return "nextcloud_candidate_retry_jobs" }

// NextcloudCandidateRetryClaim is a fenced, one-use queue intent. The task
// carries no source credential or content, only exact immutable selectors.
type NextcloudCandidateRetryClaim struct {
	TenantID          uint64
	KnowledgeBaseID   string
	DataSourceID      string
	ExternalID        string
	DesiredETag       string
	FailedCandidateID string
	InstanceID        string
	BindingID         string
	ConfigSHA         string
	PairOperationID   string
	PairingEpoch      int64
	LeaseToken        string
	SyncLogID         string
}

// NextcloudCandidateRetryStatus contains the persisted candidate-retry status.
type NextcloudCandidateRetryStatus struct {
	FileID        string    `json:"file_id"`
	SourceETag    string    `json:"source_etag"`
	CandidateID   string    `json:"candidate_id"`
	State         string    `json:"state"`
	AttemptCount  int       `json:"attempt_count"`
	FirstStagedAt time.Time `json:"first_staged_at"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	LastErrorCode string    `json:"last_error_code"`
}

// NextcloudFailedCandidateListItem contains the public failed-candidate list projection.
// The list contains only stable identifiers and static retry codes. Exact
// generation selectors are deliberately fetched by the per-file status API.
type NextcloudFailedCandidateListItem struct {
	FileID        string     `json:"file_id"`
	State         string     `json:"state"`
	AttemptCount  int        `json:"attempt_count"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
	LastErrorCode string     `json:"last_error_code"`
}

type nextcloudFailedCandidateListRow struct {
	ExternalID           string     `gorm:"column:external_id"`
	DesiredETag          string     `gorm:"column:desired_etag"`
	CandidateKnowledgeID string     `gorm:"column:candidate_knowledge_id"`
	JobDesiredETag       *string    `gorm:"column:job_desired_etag"`
	JobCandidateID       *string    `gorm:"column:job_candidate_id"`
	JobState             *string    `gorm:"column:job_state"`
	JobAttemptCount      *int       `gorm:"column:job_attempt_count"`
	JobNextAttemptAt     *time.Time `gorm:"column:job_next_attempt_at"`
	JobLastErrorCode     *string    `gorm:"column:job_last_error_code"`
}

// ListFailedCandidates uses a bounded keyset page over current failed staging
// generations in one active pairing. The caller must have checked KB edit access.
func (r *NextcloudSourcePairingRepository) ListFailedCandidates(ctx context.Context,
	pair NextcloudSourcePairing, limit int, cursor string,
) ([]NextcloudFailedCandidateListItem, string, error) {
	if pair.State != "active" || pair.TenantID == 0 || pair.KnowledgeBaseID == "" ||
		pair.DataSourceID == "" || pair.InstanceID == "" || limit < 1 || limit > 50 || len(cursor) > 512 {
		return nil, "", ErrNextcloudSourcePairingConflict
	}
	prefix := "nextcloud:" + pair.InstanceID + ":"
	after := ""
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(decoded) > 256 || !utf8.Valid(decoded) ||
			!strings.HasPrefix(string(decoded), prefix) || len(decoded) <= len(prefix) {
			return nil, "", ErrNextcloudSourcePairingConflict
		}
		after = string(decoded)
	}
	var rows []nextcloudFailedCandidateListRow
	query := r.db.WithContext(ctx).Table("nextcloud_source_versions AS v").
		Select(`v.external_id, v.desired_etag, v.candidate_knowledge_id,
			j.desired_etag AS job_desired_etag, j.failed_candidate_id AS job_candidate_id,
			j.state AS job_state, j.attempt_count AS job_attempt_count,
			j.next_attempt_at AS job_next_attempt_at, j.last_error_code AS job_last_error_code`).
		Joins(`JOIN knowledges AS k ON k.id = v.candidate_knowledge_id AND
			k.tenant_id = v.tenant_id AND k.knowledge_base_id = v.knowledge_base_id`).
		Joins(`JOIN data_sources AS d ON d.id = v.datasource_id AND d.tenant_id = v.tenant_id AND
			d.knowledge_base_id = v.knowledge_base_id AND d.type = ? AND d.status = ? AND d.deleted_at IS NULL`,
			types.ConnectorTypeNextcloud, types.DataSourceStatusActive).
		Joins(`JOIN nextcloud_source_pairings AS p ON p.tenant_id = v.tenant_id AND
			p.knowledge_base_id = v.knowledge_base_id AND p.datasource_id = v.datasource_id AND
			p.operation_id = ? AND p.state = 'active'`, pair.OperationID).
		Joins(("LEFT JOIN nextcloud_candidate_retry_jobs AS j ON j.tena" +
			"nt_id = v.tenant_id AND\n" +
			"\t\t\tj.knowledge_base_id = v.knowledge_base_id AND j.data" +
			"source_id = v.datasource_id AND j.external_id = v.exter" +
			"nal_id")).
		Where(`v.tenant_id = ? AND v.knowledge_base_id = ? AND v.datasource_id = ? AND
			v.state = 'staging' AND SUBSTR(v.external_id, 1, ?) = ? AND
			k.channel = ? AND k.parse_status = ? AND k.deleted_at IS NULL`,
			pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID, utf8.RuneCountInString(prefix), prefix,
			types.ConnectorTypeNextcloud, types.ParseStatusFailed)
	if after != "" {
		query = query.Where("v.external_id > ?", after)
	}
	err := query.Order("v.external_id ASC").Limit(limit + 1).Scan(&rows).Error
	if err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(rows) > limit {
		nextCursor = base64.RawURLEncoding.EncodeToString([]byte(rows[limit-1].ExternalID))
		rows = rows[:limit]
	}
	items := make([]NextcloudFailedCandidateListItem, 0, len(rows))
	for _, row := range rows {
		fileID := strings.TrimPrefix(row.ExternalID, prefix)
		parsed, parseErr := strconv.ParseInt(fileID, 10, 64)
		if parseErr != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != fileID {
			continue
		}
		item := NextcloudFailedCandidateListItem{FileID: fileID, State: "retry", LastErrorCode: "parse_failed"}
		if row.JobDesiredETag != nil && row.JobCandidateID != nil &&
			*row.JobDesiredETag == row.DesiredETag && *row.JobCandidateID == row.CandidateKnowledgeID {
			if row.JobState != nil {
				item.State = *row.JobState
			}
			if row.JobAttemptCount != nil {
				item.AttemptCount = *row.JobAttemptCount
			}
			item.NextAttemptAt = row.JobNextAttemptAt
			if row.JobLastErrorCode != nil && *row.JobLastErrorCode != "" {
				item.LastErrorCode = *row.JobLastErrorCode
			}
		}
		items = append(items, item)
	}
	return items, nextCursor, nil
}

type nextcloudCandidateFailure struct {
	TenantID             uint64    `gorm:"column:tenant_id"`
	KnowledgeBaseID      string    `gorm:"column:knowledge_base_id"`
	DataSourceID         string    `gorm:"column:datasource_id"`
	ExternalID           string    `gorm:"column:external_id"`
	DesiredETag          string    `gorm:"column:desired_etag"`
	CandidateKnowledgeID string    `gorm:"column:candidate_knowledge_id"`
	UpdatedAt            time.Time `gorm:"column:updated_at"`
	FailedAt             time.Time `gorm:"column:failed_at"`
}

func candidateRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<uint(attempt-1)) * time.Minute
}

// candidateRetryDelay adds bounded, stable jitter so multiple failed files do
// not retry together. The exact source file, failed generation, and attempt
// determine the delay across dispatcher restarts.
func candidateRetryDelay(externalID, candidateID string, attempt int) time.Duration {
	base := candidateRetryBackoff(attempt)
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(externalID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(candidateID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strconv.Itoa(attempt)))
	jitterSeconds := uint64((base / 5) / time.Second)
	return base + time.Duration(hash.Sum64()%(jitterSeconds+1))*time.Second
}

// CandidateRetryClaims claims eligible candidate-retry work.
func (r *NextcloudEventInboxRepository) CandidateRetryClaims(
	ctx context.Context,
	now time.Time,
) ([]NextcloudCandidateRetryClaim, error) {
	claims := make([]NextcloudCandidateRetryClaim, 0, 8)
	var after *nextcloudCandidateFailure
	for {
		var failures []nextcloudCandidateFailure
		query := r.db.WithContext(ctx).Table("nextcloud_source_versions AS v").
			Select("v.*, k.updated_at AS failed_at").
			Joins(("JOIN knowledges AS k ON k.id = v.candidate_knowledge_id"+
				" AND k.tenant_id = v.tenant_id AND k.knowledge_base_id "+
				"= v.knowledge_base_id")).
			Joins(("JOIN data_sources AS d ON d.id = v.datasource_id AND d."+
				"tenant_id = v.tenant_id AND d.knowledge_base_id = v.kno"+
				"wledge_base_id AND d.type = ? AND d.status = ? AND d.de"+
				"leted_at IS NULL"), types.ConnectorTypeNextcloud, types.DataSourceStatusActive).
			Joins(("JOIN nextcloud_source_pairings AS p ON p.tenant_id = v."+
				"tenant_id AND p.knowledge_base_id = v.knowledge_base_id"+
				" AND p.datasource_id = v.datasource_id AND p.state = 'a"+
				"ctive'")).
			Joins(("LEFT JOIN nextcloud_candidate_retry_jobs AS j ON j.tena"+
				"nt_id = v.tenant_id AND j.knowledge_base_id = v.knowled"+
				"ge_base_id AND j.datasource_id = v.datasource_id AND j."+
				"external_id = v.external_id")).
			Where("v.state = 'staging' AND k.channel = ? AND k.parse_status = ? AND k.deleted_at IS NULL",
				types.ConnectorTypeNextcloud, types.ParseStatusFailed).
			Where(`j.external_id IS NULL OR j.desired_etag <> v.desired_etag OR
				j.failed_candidate_id <> v.candidate_knowledge_id OR
				(j.state <> 'manual' AND (j.next_attempt_at <= ? OR j.lease_until <= ? OR j.first_staged_at <= ?))`,
				now, now, now.Add(-nextcloudAutomaticCandidateRetryWindow))
		if after != nil {
			query = query.Where("(v.tenant_id, v.knowledge_base_id, v.datasource_id, v.external_id) > (?, ?, ?, ?)",
				after.TenantID, after.KnowledgeBaseID, after.DataSourceID, after.ExternalID)
		}
		if err := query.Order("v.tenant_id ASC, v.knowledge_base_id ASC, v.datasource_id ASC, v.external_id ASC").
			Limit(64).Scan(&failures).Error; err != nil {
			return claims, fmt.Errorf("scan failed Nextcloud candidates: %w", err)
		}
		if len(failures) == 0 {
			return claims, nil
		}
		for _, failure := range failures {
			claim, err := r.claimCandidateRetry(ctx, failure, now)
			if errors.Is(err, ErrNextcloudSourcePairingConflict) {
				// A malformed or revoked row cannot delay another file's retry.
				continue
			}
			if err != nil {
				return claims, err
			}
			if claim != nil {
				claims = append(claims, *claim)
				if len(claims) == 8 {
					return claims, nil
				}
			}
		}
		last := failures[len(failures)-1]
		after = &last
		if len(failures) < 64 {
			return claims, nil
		}
	}
}

func (r *NextcloudEventInboxRepository) claimCandidateRetry(ctx context.Context,
	failure nextcloudCandidateFailure, now time.Time,
) (*NextcloudCandidateRetryClaim, error) {
	var claim *NextcloudCandidateRetryClaim
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}),
			failure.TenantID, failure.KnowledgeBaseID, failure.DataSourceID, failure.ExternalID).
			Take(&version).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		if version.State != "staging" || version.DesiredETag != failure.DesiredETag ||
			version.CandidateKnowledgeID != failure.CandidateKnowledgeID {
			return nil
		}
		var candidate types.Knowledge
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"channel = ? AND parse_status = ? AND deleted_at IS NULL"),
			version.CandidateKnowledgeID, version.TenantID, version.KnowledgeBaseID,
			types.ConnectorTypeNextcloud, types.ParseStatusFailed).Take(&candidate).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		var ds types.DataSource
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"type = ? AND status = ? AND deleted_at IS NULL"),
			version.DataSourceID, version.TenantID, version.KnowledgeBaseID,
			types.ConnectorTypeNextcloud, types.DataSourceStatusActive).Take(&ds).Error; err != nil {
			return nil // Paused or removed sources cannot enqueue retry work.
		}
		baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil {
			return nil
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = 'active'",
			version.TenantID, version.KnowledgeBaseID, version.DataSourceID).Take(&pair).Error; err != nil ||
			pair.BaseURL != baseURL || pair.ConfigSHA != configSHA || pair.BindingID != bindingID {
			return nil
		}
		fileID := strings.TrimPrefix(version.ExternalID, "nextcloud:"+pair.InstanceID+":")
		parsed, parseErr := strconv.ParseInt(fileID, 10, 64)
		metadata, metadataErr := nextcloudMetadata(&candidate)
		if !strings.HasPrefix(version.ExternalID, "nextcloud:"+pair.InstanceID+":") ||
			parseErr != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != fileID ||
			metadataErr != nil || nextcloudMetadataString(metadata, "datasource_id") != version.DataSourceID ||
			nextcloudMetadataString(metadata, "external_id") != version.ExternalID ||
			nextcloudMetadataString(metadata, "nextcloud_instance_id") != pair.InstanceID ||
			nextcloudMetadataString(metadata, "nextcloud_binding_id") != pair.BindingID ||
			nextcloudMetadataString(metadata, "nextcloud_file_id") != fileID ||
			nextcloudMetadataString(metadata, "nextcloud_target_etag") != version.DesiredETag {
			return ErrNextcloudSourcePairingConflict
		}
		var job nextcloudCandidateRetryJob
		query := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
			version.TenantID, version.KnowledgeBaseID, version.DataSourceID, version.ExternalID)
		jobErr := query.Take(&job).Error
		if errors.Is(jobErr, gorm.ErrRecordNotFound) {
			job = nextcloudCandidateRetryJob{
				TenantID:        version.TenantID,
				KnowledgeBaseID: version.KnowledgeBaseID, DataSourceID: version.DataSourceID,
				ExternalID: version.ExternalID, DesiredETag: version.DesiredETag,
				FailedCandidateID: version.CandidateKnowledgeID, FirstStagedAt: version.UpdatedAt,
				NextAttemptAt: candidate.UpdatedAt.Add(candidateRetryDelay(
					version.ExternalID,
					version.CandidateKnowledgeID,
					1,
				)), State: "retry", UpdatedAt: now,
			}
			insert := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&job)
			if insert.Error != nil {
				return insert.Error
			}
			if insert.RowsAffected != 1 {
				return nil
			}
		} else if jobErr != nil {
			return jobErr
		} else if job.DesiredETag != version.DesiredETag || job.FailedCandidateID != version.CandidateKnowledgeID {
			if job.DesiredETag != version.DesiredETag {
				job.FirstStagedAt = version.UpdatedAt
				job.AttemptCount = 0
			}
			job.DesiredETag = version.DesiredETag
			job.FailedCandidateID = version.CandidateKnowledgeID
			job.NextAttemptAt = candidate.UpdatedAt.Add(candidateRetryDelay(
				version.ExternalID,
				version.CandidateKnowledgeID,
				job.AttemptCount+1,
			))
			job.State, job.LeaseToken, job.LeaseUntil, job.SyncLogID = "retry", nil, nil, nil
			job.LastErrorCode = ""
			if err := tx.Save(&job).Error; err != nil {
				return err
			}
		}
		if now.Sub(job.FirstStagedAt) >= nextcloudAutomaticCandidateRetryWindow {
			return tx.Model(&job).Updates(map[string]any{
				"state": "manual", "lease_token": nil,
				"lease_until": nil, "last_error_code": "candidate_retry_exhausted", "updated_at": now,
			}).Error
		}
		if job.State == "manual" || now.Before(job.NextAttemptAt) {
			return nil
		}
		if job.State == "leased" && job.LeaseUntil != nil && now.Before(*job.LeaseUntil) {
			return nil
		}
		if job.State == "leased" && job.SyncLogID != nil {
			status, started, _, err := nextcloudDispatchSyncStatus(tx, job.SyncLogID)
			if err != nil {
				return err
			}
			if status == "running" {
				if now.Sub(started) >= 150*time.Minute {
					return tx.Model(&job).Updates(map[string]any{
						"state": "manual", "lease_token": nil,
						"lease_until": nil, "last_error_code": "sync_stale_manual_review", "updated_at": now,
					}).Error
				}
				return tx.Model(&job).Updates(map[string]any{"lease_until": now.Add(3 *
					time.Minute), "updated_at": now}).Error
			}
		}
		var running int64
		if err := tx.Table("sync_logs").Where("data_source_id = ? AND status = 'running'", version.DataSourceID).
			Count(&running).Error; err != nil {
			return err
		}
		if running > 0 {
			return nil
		}
		var otherLease int64
		if err := tx.Table("nextcloud_candidate_retry_jobs").Where(
			"datasource_id = ? AND external_id <> ? AND state = 'leased' AND lease_until > ?",
			version.DataSourceID, version.ExternalID, now).Count(&otherLease).Error; err != nil {
			return err
		}
		if otherLease > 0 {
			return nil
		}
		token, logID := uuid.NewString(), uuid.NewString()
		if err := tx.Model(&job).Updates(map[string]any{
			"state": "leased", "lease_token": token,
			"lease_until": now.Add(3 * time.Minute), "sync_log_id": logID,
			"attempt_count": job.AttemptCount + 1,
			"next_attempt_at": now.Add(candidateRetryDelay(
				version.ExternalID,
				version.CandidateKnowledgeID,
				job.AttemptCount+1,
			)),
			"last_error_code": "", "updated_at": now,
		}).Error; err != nil {
			return err
		}
		claim = &NextcloudCandidateRetryClaim{
			TenantID: version.TenantID, KnowledgeBaseID: version.KnowledgeBaseID,
			DataSourceID: version.DataSourceID, ExternalID: version.ExternalID,
			DesiredETag: version.DesiredETag, FailedCandidateID: version.CandidateKnowledgeID,
			InstanceID: pair.InstanceID, BindingID: pair.BindingID,
			ConfigSHA: pair.ConfigSHA, PairOperationID: pair.OperationID,
			PairingEpoch: pair.PublicationEpoch, LeaseToken: token, SyncLogID: logID,
		}
		return nil
	})
	return claim, err
}

// ValidateCandidateRetryTask rejects stale queued tasks before source reads.
// A running task may advance the candidate itself; the lease and source pair
// still have to match on every later checkpoint.
func (r *NextcloudEventInboxRepository) ValidateCandidateRetryTask(ctx context.Context,
	claim NextcloudCandidateRetryClaim, requireFailed bool,
) error {
	if claim.LeaseToken == "" || claim.SyncLogID == "" || claim.FailedCandidateID == "" ||
		claim.ConfigSHA == "" || claim.PairOperationID == "" || claim.PairingEpoch < 0 {
		return ErrNextcloudEventScope
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job nextcloudCandidateRetryJob
		if err := tx.Clauses(clause.Locking{
			Strength: "UPDATE",
		}).Where(("tenant_id = ? AND knowledge_base_id = ? AND datasource_" +
			"id = ? AND external_id = ?"),
			claim.TenantID, claim.KnowledgeBaseID, claim.DataSourceID, claim.ExternalID).Take(&job).Error; err != nil {
			return ErrNextcloudEventScope
		}
		now := time.Now().UTC()
		if job.State != "leased" || job.LeaseToken == nil || *job.LeaseToken != claim.LeaseToken ||
			job.LeaseUntil == nil || !now.Before(*job.LeaseUntil) ||
			!now.Before(job.FirstStagedAt.Add(nextcloudAutomaticCandidateRetryWindow)) ||
			job.SyncLogID == nil || *job.SyncLogID != claim.SyncLogID ||
			job.DesiredETag != claim.DesiredETag || job.FailedCandidateID != claim.FailedCandidateID {
			return ErrNextcloudEventScope
		}
		var ds types.DataSource
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"type = ? AND status = ? AND deleted_at IS NULL"),
			claim.DataSourceID, claim.TenantID, claim.KnowledgeBaseID,
			types.ConnectorTypeNextcloud, types.DataSourceStatusActive).Take(&ds).Error; err != nil {
			return ErrNextcloudEventScope
		}
		baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil || bindingID != claim.BindingID || configSHA != claim.ConfigSHA {
			return ErrNextcloudEventScope
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = 'active'",
			claim.TenantID, claim.KnowledgeBaseID, claim.DataSourceID).Take(&pair).Error; err != nil ||
			pair.InstanceID != claim.InstanceID || pair.BindingID != claim.BindingID ||
			pair.BaseURL != baseURL || pair.ConfigSHA != configSHA ||
			pair.OperationID != claim.PairOperationID || pair.PublicationEpoch != claim.PairingEpoch {
			return ErrNextcloudEventScope
		}
		if requireFailed {
			var version nextcloudSourceVersion
			if err := nextcloudVersionQuery(tx, claim.TenantID, claim.KnowledgeBaseID,
				claim.DataSourceID, claim.ExternalID).Take(&version).Error; err != nil ||
				version.State != "staging" || version.DesiredETag != claim.DesiredETag ||
				version.CandidateKnowledgeID != claim.FailedCandidateID {
				return ErrNextcloudEventScope
			}
			var count int64
			if err := tx.Model(&types.Knowledge{}).Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
				"parse_status = ?"),
				claim.FailedCandidateID, claim.TenantID, claim.KnowledgeBaseID,
				types.ParseStatusFailed).Count(&count).Error; err != nil || count != 1 {
				return ErrNextcloudEventScope
			}
		}
		return tx.Model(&job).Updates(map[string]any{
			"lease_until": now.Add(3 * time.Minute),
			"updated_at":  now,
		}).Error
	})
}

// RetryFailedCandidate is a compare-and-swap administrator restart of the
// automatic retry budget. The request names the current failed generation;
// it never reparses or reopens that knowledge ID. The scheduler fetches live
// source bytes and Stage replaces it with a distinct candidate.
func (r *NextcloudSourcePairingRepository) RetryFailedCandidate(ctx context.Context,
	pair NextcloudSourcePairing, fileID int64, etag, candidateID string, now time.Time,
) error {
	if pair.State != "active" || fileID < 1 || etag == "" || len(etag) > 256 ||
		candidateID == "" || len(candidateID) > 128 {
		return ErrNextcloudSourcePairingConflict
	}
	externalID := "nextcloud:" + pair.InstanceID + ":" + strconv.FormatInt(fileID, 10)
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ds types.DataSource
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"type = ? AND status = ? AND deleted_at IS NULL"),
			pair.DataSourceID, pair.TenantID, pair.KnowledgeBaseID,
			types.ConnectorTypeNextcloud, types.DataSourceStatusActive).Take(&ds).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil || baseURL != pair.BaseURL || configSHA != pair.ConfigSHA || bindingID != pair.BindingID {
			return ErrNextcloudSourcePairingConflict
		}
		var currentPair NextcloudSourcePairing
		if err := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'active'",
			pair.OperationID, pair.TenantID).Take(&currentPair).Error; err != nil ||
			currentPair.KnowledgeBaseID != pair.KnowledgeBaseID ||
			currentPair.DataSourceID != pair.DataSourceID || currentPair.InstanceID != pair.InstanceID ||
			currentPair.BindingID != pair.BindingID || currentPair.BaseURL != pair.BaseURL ||
			currentPair.ConfigSHA != pair.ConfigSHA {
			return ErrNextcloudSourcePairingConflict
		}
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}),
			pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID, externalID).
			Take(&version).Error; err != nil || version.State != "staging" ||
			version.DesiredETag != etag || version.CandidateKnowledgeID != candidateID {
			return ErrNextcloudSourcePairingConflict
		}
		var candidate types.Knowledge
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"channel = ? AND parse_status = ? AND deleted_at IS NULL"),
			candidateID, pair.TenantID, pair.KnowledgeBaseID,
			types.ConnectorTypeNextcloud, types.ParseStatusFailed).Take(&candidate).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		metadata, err := nextcloudMetadata(&candidate)
		if err != nil || nextcloudMetadataString(metadata, "datasource_id") != pair.DataSourceID ||
			nextcloudMetadataString(metadata, "external_id") != externalID ||
			nextcloudMetadataString(metadata, "nextcloud_instance_id") != pair.InstanceID ||
			nextcloudMetadataString(metadata, "nextcloud_binding_id") != pair.BindingID ||
			nextcloudMetadataString(metadata, "nextcloud_file_id") != strconv.FormatInt(fileID, 10) ||
			nextcloudMetadataString(metadata, "nextcloud_target_etag") != etag {
			return ErrNextcloudSourcePairingConflict
		}
		var running int64
		if err := tx.Table("sync_logs").Where("data_source_id = ? AND status = 'running'",
			pair.DataSourceID).Count(&running).Error; err != nil || running != 0 {
			return ErrNextcloudSourcePairingConflict
		}
		var job nextcloudCandidateRetryJob
		jobErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
			pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID, externalID).Take(&job).Error
		if jobErr != nil && !errors.Is(jobErr, gorm.ErrRecordNotFound) {
			return jobErr
		}
		if jobErr == nil && job.State == "leased" && job.LeaseUntil != nil && now.Before(*job.LeaseUntil) {
			return ErrNextcloudSourcePairingConflict
		}
		if err := nextcloudVersionQuery(tx, pair.TenantID, pair.KnowledgeBaseID,
			pair.DataSourceID, externalID).Update("updated_at", now).Error; err != nil {
			return err
		}
		job = nextcloudCandidateRetryJob{
			TenantID: pair.TenantID, KnowledgeBaseID: pair.KnowledgeBaseID,
			DataSourceID: pair.DataSourceID, ExternalID: externalID, DesiredETag: etag,
			FailedCandidateID: candidateID, FirstStagedAt: now, NextAttemptAt: now,
			State: "retry", UpdatedAt: now,
		}
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"},
				{Name: "knowledge_base_id"},
				{Name: "datasource_id"},
				{Name: "external_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"desired_etag", "failed_candidate_id", "first_staged_at",
				"attempt_count", "next_attempt_at", "state", "lease_token", "lease_until",
				"sync_log_id", "last_error_code", "updated_at",
			}),
		}).Create(&job).Error
	})
}

// FailedCandidateRetryStatus gives an administrator a static reason code and
// exact generation selectors without returning content or parser error text.
func (r *NextcloudSourcePairingRepository) FailedCandidateRetryStatus(ctx context.Context,
	pair NextcloudSourcePairing, fileID int64,
) (NextcloudCandidateRetryStatus, error) {
	var status NextcloudCandidateRetryStatus
	if pair.State != "active" || fileID < 1 {
		return status, ErrNextcloudSourcePairingConflict
	}
	status.FileID = strconv.FormatInt(fileID, 10)
	externalID := "nextcloud:" + pair.InstanceID + ":" + status.FileID
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var currentPair NextcloudSourcePairing
		if err := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'active'",
			pair.OperationID, pair.TenantID).Take(&currentPair).Error; err != nil ||
			currentPair.KnowledgeBaseID != pair.KnowledgeBaseID ||
			currentPair.DataSourceID != pair.DataSourceID || currentPair.InstanceID != pair.InstanceID ||
			currentPair.BindingID != pair.BindingID || currentPair.BaseURL != pair.BaseURL ||
			currentPair.ConfigSHA != pair.ConfigSHA {
			return ErrNextcloudSourcePairingConflict
		}
		var ds types.DataSource
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"type = ? AND status = ? AND deleted_at IS NULL"),
			pair.DataSourceID, pair.TenantID, pair.KnowledgeBaseID,
			types.ConnectorTypeNextcloud, types.DataSourceStatusActive).Take(&ds).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		base, hash, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil || base != currentPair.BaseURL || hash != currentPair.ConfigSHA ||
			binding != currentPair.BindingID {
			return ErrNextcloudSourcePairingConflict
		}
		var version nextcloudSourceVersion
		if err := nextcloudVersionQuery(tx, pair.TenantID, pair.KnowledgeBaseID,
			pair.DataSourceID, externalID).Take(&version).Error; err != nil || version.State != "staging" {
			return ErrNextcloudSourcePairingConflict
		}
		var candidate types.Knowledge
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"channel = ? AND parse_status = ? AND deleted_at IS NULL"),
			version.CandidateKnowledgeID, pair.TenantID, pair.KnowledgeBaseID,
			types.ConnectorTypeNextcloud, types.ParseStatusFailed).Take(&candidate).Error; err != nil {
			return ErrNextcloudSourcePairingConflict
		}
		metadata, err := nextcloudMetadata(&candidate)
		if err != nil || nextcloudMetadataString(metadata, "datasource_id") != pair.DataSourceID ||
			nextcloudMetadataString(metadata, "external_id") != externalID ||
			nextcloudMetadataString(metadata, "nextcloud_instance_id") != pair.InstanceID ||
			nextcloudMetadataString(metadata, "nextcloud_binding_id") != pair.BindingID ||
			nextcloudMetadataString(metadata, "nextcloud_file_id") != status.FileID ||
			nextcloudMetadataString(metadata, "nextcloud_target_etag") != version.DesiredETag {
			return ErrNextcloudSourcePairingConflict
		}
		status.SourceETag, status.CandidateID = version.DesiredETag, version.CandidateKnowledgeID
		status.FirstStagedAt = version.UpdatedAt
		status.NextAttemptAt = candidate.UpdatedAt.Add(candidateRetryDelay(externalID, version.CandidateKnowledgeID, 1))
		status.State, status.LastErrorCode = "retry", "parse_failed"
		var job nextcloudCandidateRetryJob
		err = tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
			pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID, externalID).Take(&job).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if job.DesiredETag == version.DesiredETag && job.FailedCandidateID == version.CandidateKnowledgeID {
			status.State, status.AttemptCount = job.State, job.AttemptCount
			status.FirstStagedAt, status.NextAttemptAt = job.FirstStagedAt, job.NextAttemptAt
			status.LastErrorCode = job.LastErrorCode
		}
		return nil
	})
	return status, err
}
