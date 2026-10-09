package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const nextcloudAutomaticCandidateRetryWindow = 24 * time.Hour

// ErrNextcloudCandidateRetryNotDue reports a candidate retry that is not yet eligible.
var ErrNextcloudCandidateRetryNotDue = apperrors.NewProtocolError(
	errors.New("nextcloud failed candidate retry is not due"), "Nextcloud failed candidate retry is not due",
)

type nextcloudCandidateRetryClaimKey struct{}

// WithNextcloudCandidateRetryClaim carries an exact validated queue claim
// through ingest. Stage independently checks it against locked source rows.
func WithNextcloudCandidateRetryClaim(ctx context.Context, claim NextcloudCandidateRetryClaim) context.Context {
	return context.WithValue(ctx, nextcloudCandidateRetryClaimKey{}, claim)
}

func nextcloudCandidateRetryClaimFromContext(ctx context.Context) (NextcloudCandidateRetryClaim, bool) {
	claim, ok := ctx.Value(nextcloudCandidateRetryClaimKey{}).(NextcloudCandidateRetryClaim)
	return claim, ok
}

// AdmitNextcloudCandidateRetry prevents an ordinary forced/full sync from
// creating an orphan parser job for a failed source file with unchanged ETag.
// Stage repeats this check under its source-version lock after file creation.
func (r *knowledgeRepository) AdmitNextcloudCandidateRetry(ctx context.Context,
	tenantID uint64, kbID, dsID, externalID, etag string,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		claim, claimed := nextcloudCandidateRetryClaimFromContext(ctx)
		var version nextcloudSourceVersion
		err := nextcloudVersionQuery(tx, tenantID, kbID, dsID, externalID).Take(&version).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if claimed {
				return ErrNextcloudCandidateRetryNotDue
			}
			return nil
		}
		if err != nil {
			return err
		}
		_, err = nextcloudFailedCandidateAdmission(tx, version, etag,
			claim, claimed, time.Now().UTC(), false)
		return err
	})
}

func nextcloudFailedCandidateAdmission(tx *gorm.DB, version nextcloudSourceVersion,
	etag string, claim NextcloudCandidateRetryClaim, claimed bool, now time.Time, lockSource bool,
) (bool, error) {
	if claimed {
		// A retry may discover a new live ETag, but its lease still belongs to
		// the exact failed generation that was claimed before the fetch.
		if version.State != "staging" || version.DesiredETag != claim.DesiredETag ||
			version.CandidateKnowledgeID != claim.FailedCandidateID {
			return true, ErrNextcloudCandidateRetryNotDue
		}
	} else if version.State != "staging" || version.DesiredETag != etag {
		return false, nil
	}
	var current types.Knowledge
	if err := tx.Select("parse_status").Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?",
		version.CandidateKnowledgeID, version.TenantID, version.KnowledgeBaseID).
		Take(&current).Error; err != nil {
		return false, err
	}
	if current.ParseStatus != types.ParseStatusFailed {
		if claimed {
			return true, ErrNextcloudCandidateRetryNotDue
		}
		return false, nil
	}
	if !now.Before(version.UpdatedAt.Add(nextcloudAutomaticCandidateRetryWindow)) {
		return true, ErrNextcloudCandidateRetryManual
	}
	var job nextcloudCandidateRetryJob
	err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
		version.TenantID, version.KnowledgeBaseID, version.DataSourceID, version.ExternalID).
		Take(&job).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return true, ErrNextcloudCandidateRetryNotDue
	}
	if err != nil {
		return true, err
	}
	if job.DesiredETag != version.DesiredETag || job.FailedCandidateID != version.CandidateKnowledgeID {
		return true, ErrNextcloudCandidateRetryNotDue
	}
	if job.State == "manual" || !now.Before(job.FirstStagedAt.Add(nextcloudAutomaticCandidateRetryWindow)) {
		return true, ErrNextcloudCandidateRetryManual
	}
	if claim.LeaseToken == "" || claim.ConfigSHA == "" || claim.PairOperationID == "" ||
		claim.TenantID != version.TenantID || claim.KnowledgeBaseID != version.KnowledgeBaseID ||
		claim.DataSourceID != version.DataSourceID || claim.ExternalID != version.ExternalID ||
		claim.DesiredETag != version.DesiredETag || claim.FailedCandidateID != version.CandidateKnowledgeID ||
		job.State != "leased" || job.LeaseToken == nil || *job.LeaseToken != claim.LeaseToken ||
		job.LeaseUntil == nil || !now.Before(*job.LeaseUntil) ||
		job.SyncLogID == nil || *job.SyncLogID != claim.SyncLogID {
		return true, ErrNextcloudCandidateRetryNotDue
	}
	var ds types.DataSource
	dsQuery := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
		"type = ? AND status = ? AND deleted_at IS NULL"),
		version.DataSourceID, version.TenantID, version.KnowledgeBaseID,
		types.ConnectorTypeNextcloud, types.DataSourceStatusActive)
	if lockSource {
		dsQuery = dsQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := dsQuery.Take(&ds).Error; err != nil {
		return true, ErrNextcloudCandidateRetryNotDue
	}
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || configSHA != claim.ConfigSHA || bindingID != claim.BindingID {
		return true, ErrNextcloudCandidateRetryNotDue
	}
	var pair NextcloudSourcePairing
	pairQuery := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = 'active'",
		version.TenantID, version.KnowledgeBaseID, version.DataSourceID)
	if lockSource {
		pairQuery = pairQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := pairQuery.Take(&pair).Error; err != nil || pair.OperationID != claim.PairOperationID ||
		pair.PublicationEpoch != claim.PairingEpoch || pair.ConfigSHA != configSHA ||
		pair.BaseURL != baseURL || pair.InstanceID != claim.InstanceID || pair.BindingID != bindingID {
		return true, ErrNextcloudCandidateRetryNotDue
	}
	return true, nil
}

// FailedNextcloudCandidateETags returns only current failed staging candidates
// for the exact active source pair. The connector uses these file IDs to force
// a fresh content fetch even when its inventory ETag has not changed.
func (r *knowledgeRepository) FailedNextcloudCandidateETags(ctx context.Context,
	tenantID uint64, kbID, dsID, instanceID, bindingID string, now time.Time, leaseToken string,
) (map[string]string, error) {
	result := make(map[string]string)
	if tenantID == 0 || kbID == "" || dsID == "" || instanceID == "" || bindingID == "" {
		return nil, ErrNextcloudSourcePairingConflict
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ds types.DataSource
		if err := tx.Where(("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND " +
			"type = ? AND status = ? AND deleted_at IS NULL"),
			dsID, tenantID, kbID, types.ConnectorTypeNextcloud, types.DataSourceStatusActive).Take(&ds).Error; err !=
			nil {
			return ErrNextcloudSourcePairingConflict
		}
		baseURL, configSHA, currentBinding, err := NextcloudEventDataSourceIdentity(ds.Config)
		if err != nil || currentBinding != bindingID {
			return ErrNextcloudSourcePairingConflict
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = ?",
			tenantID, kbID, dsID, "active").Take(&pair).Error; err != nil ||
			pair.InstanceID != instanceID || pair.BindingID != bindingID ||
			pair.BaseURL != baseURL || pair.ConfigSHA != configSHA {
			return ErrNextcloudSourcePairingConflict
		}
		var versions []nextcloudSourceVersion
		// Only the durable per-file retry task may bypass an unchanged ETag.
		// Ordinary source events must not consume uncounted attempts or break
		// exponential backoff by opportunistically rebuilding this candidate.
		if leaseToken == "" {
			return nil
		}
		if err := tx.Table("nextcloud_source_versions AS v").Select("v.*").
			Joins(("JOIN knowledges AS k ON k.id = v.candidate_knowledge_id"+
				" AND k.tenant_id = v.tenant_id AND k.knowledge_base_id "+
				"= v.knowledge_base_id")).
			Where("v.tenant_id = ? AND v.knowledge_base_id = ? AND v.datasource_id = ? AND v.state = 'staging'",
				tenantID, kbID, dsID).
			Where("k.channel = ? AND k.parse_status = ? AND k.deleted_at IS NULL",
				types.ConnectorTypeNextcloud, types.ParseStatusFailed).
			Find(&versions).Error; err != nil {
			return fmt.Errorf("read failed Nextcloud candidates: %w", err)
		}
		prefix := "nextcloud:" + instanceID + ":"
		for _, version := range versions {
			if now.Sub(version.UpdatedAt) >= nextcloudAutomaticCandidateRetryWindow {
				continue
			}
			var candidate types.Knowledge
			if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND id = ?",
				tenantID, kbID, version.CandidateKnowledgeID).Take(&candidate).Error; err != nil {
				return ErrNextcloudSourcePairingConflict
			}
			var job nextcloudCandidateRetryJob
			jobErr := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
				tenantID, kbID, dsID, version.ExternalID).Take(&job).Error
			if jobErr != nil && !errors.Is(jobErr, gorm.ErrRecordNotFound) {
				return jobErr
			}
			if errors.Is(jobErr, gorm.ErrRecordNotFound) {
				continue
			}
			if job.DesiredETag != version.DesiredETag ||
				job.FailedCandidateID != version.CandidateKnowledgeID ||
				job.State != "leased" || job.LeaseToken == nil || *job.LeaseToken != leaseToken ||
				job.LeaseUntil == nil || !now.Before(*job.LeaseUntil) {
				continue
			}
			fileID := strings.TrimPrefix(version.ExternalID, prefix)
			id, err := strconv.ParseInt(fileID, 10, 64)
			if !strings.HasPrefix(version.ExternalID, prefix) || err != nil || id < 1 ||
				strconv.FormatInt(id, 10) != fileID || version.DesiredETag == "" {
				return ErrNextcloudSourcePairingConflict
			}
			metadata, err := nextcloudMetadata(&candidate)
			if err != nil || nextcloudMetadataString(metadata, "datasource_id") != dsID ||
				nextcloudMetadataString(metadata, "external_id") != version.ExternalID ||
				nextcloudMetadataString(metadata, "nextcloud_instance_id") != instanceID ||
				nextcloudMetadataString(metadata, "nextcloud_binding_id") != bindingID ||
				nextcloudMetadataString(metadata, "nextcloud_file_id") != fileID ||
				nextcloudMetadataString(metadata, "nextcloud_target_etag") != version.DesiredETag {
				return ErrNextcloudSourcePairingConflict
			}
			result[fileID] = version.DesiredETag
		}
		return nil
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNextcloudSourcePairingConflict
	}
	return result, err
}
