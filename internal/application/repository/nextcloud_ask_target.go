package repository

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

var (
	// ErrNextcloudAskTargetNotFound reports that no published source ask target is available.
	ErrNextcloudAskTargetNotFound = apperrors.NewProtocolError(
		errors.New("nextcloud ask target not found"), "Nextcloud ask target not found",
	)
	// ErrNextcloudAskTargetUnavailable reports an unavailable published source ask target.
	ErrNextcloudAskTargetUnavailable = apperrors.NewProtocolError(errors.New(
		"nextcloud ask target unavailable",
	), "Nextcloud ask target unavailable")
	askSourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	askETagPattern     = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,256}$`)
)

// NextcloudAskTargetRepository resolves an untrusted browser deep link to the
// one currently published candidate. It grants no access; the human handler
// must independently check the KB and live Nextcloud source before returning.
type NextcloudAskTargetRepository struct{ db *gorm.DB }

// NewNextcloudAskTargetRepository returns the source ask-target repository.
func NewNextcloudAskTargetRepository(db *gorm.DB) *NextcloudAskTargetRepository {
	return &NextcloudAskTargetRepository{db: db}
}

// FindPublished loads a published source target in the requested tenant scope.
func (r *NextcloudAskTargetRepository) FindPublished(
	ctx context.Context, instanceID, bindingID string, fileID int64, sourceETag string,
) (*types.Knowledge, error) {
	if r == nil || r.db == nil {
		return nil, ErrNextcloudAskTargetUnavailable
	}
	if !askSourceIDPattern.MatchString(instanceID) || !askSourceIDPattern.MatchString(bindingID) ||
		fileID < 1 || !askETagPattern.MatchString(sourceETag) {
		return nil, ErrNextcloudAskTargetNotFound
	}
	db := r.db.WithContext(ctx)
	var pairs []NextcloudSourcePairing
	if err := db.Where("nextcloud_instance_id = ? AND binding_id = ? AND state = ?",
		instanceID, bindingID, "active").Limit(2).Find(&pairs).Error; err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: read source pair: %v",
			ErrNextcloudAskTargetUnavailable,
			err,
		), fmt.Sprintf("%s: read source pair: %v", apperrors.PublicMessage(
			ErrNextcloudAskTargetUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	if len(pairs) == 0 {
		return nil, ErrNextcloudAskTargetNotFound
	}
	if len(pairs) != 1 {
		return nil, ErrNextcloudAskTargetUnavailable
	}
	pair := pairs[0]
	externalID := "nextcloud:" + instanceID + ":" + strconv.FormatInt(fileID, 10)
	var version nextcloudSourceVersion
	err := db.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
		pair.TenantID, pair.KnowledgeBaseID, pair.DataSourceID, externalID).Take(&version).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNextcloudAskTargetNotFound
	}
	if err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: read source version: %v",
			ErrNextcloudAskTargetUnavailable,
			err,
		), fmt.Sprintf("%s: read source version: %v", apperrors.PublicMessage(
			ErrNextcloudAskTargetUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	if version.State != "published" || version.DesiredETag != sourceETag ||
		version.CandidateKnowledgeID == "" {
		return nil, ErrNextcloudAskTargetNotFound
	}
	checked, ready, err := (&knowledgeRepository{db: r.db}).publicationCandidate(db,
		version.CandidateKnowledgeID, false)
	if err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: verify candidate: %v",
			ErrNextcloudAskTargetUnavailable,
			err,
		), fmt.Sprintf("%s: verify candidate: %v", apperrors.PublicMessage(
			ErrNextcloudAskTargetUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	if !ready || checked.version.TenantID != pair.TenantID ||
		checked.version.KnowledgeBaseID != pair.KnowledgeBaseID ||
		checked.version.DataSourceID != pair.DataSourceID ||
		checked.version.ExternalID != externalID || checked.version.DesiredETag != sourceETag ||
		checked.version.State != "published" || checked.instance != instanceID ||
		checked.binding != bindingID || checked.fileID != fileID {
		return nil, ErrNextcloudAskTargetNotFound
	}
	var knowledge types.Knowledge
	err = db.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND channel = ?",
		version.CandidateKnowledgeID, pair.TenantID, pair.KnowledgeBaseID,
		types.ConnectorTypeNextcloud).Take(&knowledge).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrNextcloudAskTargetNotFound
	}
	if err != nil {
		return nil, apperrors.NewProtocolError(fmt.Errorf(
			"%w: read candidate: %v",
			ErrNextcloudAskTargetUnavailable,
			err,
		), fmt.Sprintf("%s: read candidate: %v", apperrors.PublicMessage(
			ErrNextcloudAskTargetUnavailable,
		), func() any {
			if err == nil {
				return nil
			}
			return apperrors.PublicMessage(err)
		}()))
	}
	metadata, err := nextcloudMetadata(&knowledge)
	if err != nil || nextcloudMetadataString(metadata, "nextcloud_etag") != sourceETag ||
		nextcloudMetadataString(metadata, "nextcloud_target_etag") != sourceETag {
		return nil, ErrNextcloudAskTargetNotFound
	}
	return &knowledge, nil
}
