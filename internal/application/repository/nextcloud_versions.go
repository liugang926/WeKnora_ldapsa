package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// nextcloudSourceVersion is the durable publication intent for one source file.
// Its key deliberately includes the data source: external IDs are not global.
type nextcloudSourceVersion struct {
	TenantID             uint64 `gorm:"primaryKey"`
	KnowledgeBaseID      string `gorm:"primaryKey"`
	DataSourceID         string `gorm:"column:datasource_id;primaryKey"`
	ExternalID           string `gorm:"primaryKey"`
	DesiredETag          string `gorm:"column:desired_etag"`
	CandidateKnowledgeID string
	State                string
	UpdatedAt            time.Time
}

func (nextcloudSourceVersion) TableName() string { return "nextcloud_source_versions" }

type nextcloudPublicationCheck func(context.Context, *types.DataSourceConfig,
	string, string, int64, string, string) error

type nextcloudPublicationCandidate struct {
	version   nextcloudSourceVersion
	config    *types.DataSourceConfig
	configSHA string
	instance  string
	binding   string
	fileID    int64
	etag      string
	path      string
}

func (r *knowledgeRepository) publicationCandidate(
	tx *gorm.DB, candidateID string, lock bool,
) (nextcloudPublicationCandidate, bool, error) {
	var result nextcloudPublicationCandidate
	versionQuery := tx.Where("candidate_knowledge_id = ? AND state IN ?", candidateID,
		[]string{"staging", "published"})
	if lock {
		versionQuery = versionQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	err := versionQuery.Take(&result.version).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	var candidate types.Knowledge
	candidateQuery := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND id = ? AND channel = ?",
		result.version.TenantID, result.version.KnowledgeBaseID, candidateID,
		types.ConnectorTypeNextcloud)
	if lock {
		candidateQuery = candidateQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	err = candidateQuery.Take(&candidate).Error
	if err != nil {
		return result, false, err
	}
	if candidate.ParseStatus != types.ParseStatusCompleted || candidate.EnableStatus != "enabled" {
		return result, false, nil
	}
	if NextcloudTextChunkRequired(candidate.FileType) {
		hasText, err := nextcloudHasRetrievableTextChunk(tx, &candidate)
		if err != nil {
			return result, false, fmt.Errorf("read Nextcloud candidate text chunks: %w", err)
		}
		if !hasText {
			return result, false, nil
		}
	}
	metadata, err := nextcloudMetadata(&candidate)
	if err != nil {
		return result, false, err
	}
	result.instance = nextcloudMetadataString(metadata, "nextcloud_instance_id")
	result.binding = nextcloudMetadataString(metadata, "nextcloud_binding_id")
	fileIDRaw := nextcloudMetadataString(metadata, "nextcloud_file_id")
	result.etag = nextcloudMetadataString(metadata, "nextcloud_target_etag")
	result.path = nextcloudMetadataString(metadata, "nextcloud_path")
	result.fileID, err = strconv.ParseInt(fileIDRaw, 10, 64)
	if err != nil || result.fileID < 1 || strconv.FormatInt(result.fileID, 10) != fileIDRaw ||
		result.instance == "" || result.binding == "" || result.path == "" ||
		result.etag != result.version.DesiredETag ||
		nextcloudMetadataString(metadata, "datasource_id") != result.version.DataSourceID ||
		nextcloudMetadataString(metadata, "external_id") != result.version.ExternalID ||
		nextcloudMetadataString(metadata, "source_resource_id") != result.binding ||
		result.version.ExternalID != "nextcloud:"+result.instance+":"+fileIDRaw {
		return result, false, apperrors.NewProtocolError(errors.New(
			"nextcloud candidate source identity mismatch",
		), "Nextcloud candidate source identity mismatch")
	}
	currentETag := nextcloudMetadataString(metadata, "nextcloud_etag")
	if currentETag != "" &&
		(result.version.State != "published" || currentETag != result.version.DesiredETag) {
		return result, false, apperrors.NewProtocolError(errors.New(
			"nextcloud candidate publication identity mismatch",
		), "Nextcloud candidate publication identity mismatch")
	}
	var ds types.DataSource
	sourceQuery := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND deleted_at IS NULL",
		result.version.DataSourceID, result.version.TenantID, result.version.KnowledgeBaseID).
		Where("type = ? AND status = ?", types.ConnectorTypeNextcloud, types.DataSourceStatusActive)
	if lock {
		sourceQuery = sourceQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := sourceQuery.Take(&ds).Error; err != nil {
		return result, false, fmt.Errorf("load paired Nextcloud source: %w", err)
	}
	baseURL, configSHA, bindingID, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || bindingID != result.binding {
		return result, false, apperrors.NewProtocolError(errors.New(
			"nextcloud source configuration changed",
		), "Nextcloud source configuration changed")
	}
	pairQuery := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = ?",
		ds.TenantID, ds.KnowledgeBaseID, ds.ID, "active")
	if lock {
		pairQuery = pairQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var pairs []NextcloudSourcePairing
	if err := pairQuery.Limit(2).Find(&pairs).Error; err != nil || len(pairs) != 1 {
		return result, false, apperrors.NewProtocolError(errors.New(
			"nextcloud source pairing is unavailable",
		), "Nextcloud source pairing is unavailable")
	}
	pair := pairs[0]
	if pair.InstanceID != result.instance || pair.BindingID != result.binding ||
		pair.BaseURL != baseURL || pair.ConfigSHA != configSHA {
		return result, false, apperrors.NewProtocolError(errors.New(
			"nextcloud source pairing is unavailable",
		), "Nextcloud source pairing is unavailable")
	}
	result.config, err = ds.ParseConfig()
	if err != nil || result.config == nil {
		return result, false, apperrors.NewProtocolError(errors.New(
			"nextcloud source credential is unavailable",
		), "Nextcloud source credential is unavailable")
	}
	result.config.Type = ds.Type
	result.configSHA = configSHA
	return result, true, nil
}

func samePublicationCandidate(a, b nextcloudPublicationCandidate) bool {
	return a.version.TenantID == b.version.TenantID &&
		a.version.KnowledgeBaseID == b.version.KnowledgeBaseID &&
		a.version.DataSourceID == b.version.DataSourceID &&
		a.version.ExternalID == b.version.ExternalID &&
		a.version.CandidateKnowledgeID == b.version.CandidateKnowledgeID &&
		a.version.DesiredETag == b.version.DesiredETag &&
		a.instance == b.instance && a.binding == b.binding && a.fileID == b.fileID &&
		a.etag == b.etag && a.path == b.path && a.configSHA == b.configSHA
}

// A failed source recheck after a successful local commit must close that
// exact publication without hiding a newer candidate staged concurrently.
func (r *knowledgeRepository) unpublishIfCurrent(ctx context.Context, checked nextcloudPublicationCandidate) error {
	// A parser task may cancel its context when the second source probe fails.
	// Use a bounded cleanup context so cancellation cannot preserve a marker
	// that the failed probe has just called into question.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return r.db.WithContext(cleanupCtx).Transaction(func(tx *gorm.DB) error {
		var current nextcloudSourceVersion
		err := nextcloudVersionQuery(tx, checked.version.TenantID, checked.version.KnowledgeBaseID,
			checked.version.DataSourceID, checked.version.ExternalID).
			Clauses(clause.Locking{Strength: "UPDATE"}).Take(&current).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if current.State != "published" || current.CandidateKnowledgeID != checked.version.CandidateKnowledgeID ||
			current.DesiredETag != checked.version.DesiredETag {
			return nil
		}
		var candidate types.Knowledge
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND id = ?",
			current.TenantID, current.KnowledgeBaseID, current.CandidateKnowledgeID).
			Clauses(clause.Locking{Strength: "UPDATE"}).Take(&candidate).Error; err != nil {
			return err
		}
		metadata, err := nextcloudMetadata(&candidate)
		if err != nil {
			return err
		}
		if (checked.path != "" && nextcloudMetadataString(metadata, "nextcloud_path") != checked.path) ||
			(checked.etag != "" && nextcloudMetadataString(metadata, "nextcloud_target_etag") != checked.etag) {
			return nil
		}
		if err := r.hideNextcloudSourceRows(tx, current.TenantID, current.KnowledgeBaseID,
			current.DataSourceID, current.ExternalID); err != nil {
			return err
		}
		return nextcloudVersionQuery(tx, current.TenantID, current.KnowledgeBaseID,
			current.DataSourceID, current.ExternalID).
			Update("state", "staging").Error
	})
}

func nextcloudMetadata(k *types.Knowledge) (map[string]any, error) {
	var metadata map[string]any
	if err := json.Unmarshal(k.Metadata, &metadata); err != nil {
		return nil, fmt.Errorf("invalid Nextcloud knowledge metadata: %w", err)
	}
	if metadata == nil {
		return nil, errors.New("missing Nextcloud knowledge metadata")
	}
	return metadata, nil
}

func nextcloudMetadataString(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

// A parser may have loaded a candidate before the source moved or was
// renamed. Its later whole-row Save must keep the newer source-owned fields
// written by StageNextcloudVersionWithSource.
func (r *knowledgeRepository) preserveNextcloudSourceFields(tx *gorm.DB, k *types.Knowledge) error {
	var version nextcloudSourceVersion
	err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND candidate_knowledge_id = ?",
		k.TenantID, k.KnowledgeBaseID, k.ID).Take(&version).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	var current types.Knowledge
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "tenant_id", "knowledge_base_id", "file_name", "title", "metadata").
		Where("tenant_id = ? AND knowledge_base_id = ? AND id = ?",
			k.TenantID, k.KnowledgeBaseID, k.ID).Take(&current).Error; err != nil {
		return err
	}
	currentMetadata, err := nextcloudMetadata(&current)
	if err != nil {
		return err
	}
	incomingMetadata, err := nextcloudMetadata(k)
	if err != nil {
		return err
	}
	for _, key := range []string{
		"datasource_id", "external_id", "source_resource_id", "channel",
		"nextcloud_instance_id", "nextcloud_binding_id", "nextcloud_file_id", "nextcloud_path",
		"nextcloud_generation", "source_updated_at", "source_created_at", "nextcloud_target_etag", "nextcloud_etag",
	} {
		if value, ok := currentMetadata[key]; ok {
			incomingMetadata[key] = value
		} else {
			delete(incomingMetadata, key)
		}
	}
	encoded, err := json.Marshal(incomingMetadata)
	if err != nil {
		return err
	}
	k.Metadata = types.JSON(encoded)
	k.FileName = current.FileName
	k.Title = current.Title
	return nil
}

func writeNextcloudETag(tx *gorm.DB, k *types.Knowledge, etag string) error {
	metadata, err := nextcloudMetadata(k)
	if err != nil {
		return err
	}
	metadata["nextcloud_etag"] = etag
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if err := tx.Unscoped().Model(&types.Knowledge{}).
		Where("tenant_id = ? AND id = ?", k.TenantID, k.ID).
		Update("metadata", types.JSON(encoded)).Error; err != nil {
		return err
	}
	k.Metadata = types.JSON(encoded)
	return nil
}

func nextcloudVersionQuery(tx *gorm.DB, tenantID uint64, kbID, dsID, externalID string) *gorm.DB {
	return tx.Model(&nextcloudSourceVersion{}).
		Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND external_id = ?",
			tenantID, kbID, dsID, externalID)
}

func (r *knowledgeRepository) nextcloudRowsForSource(
	tx *gorm.DB, tenantID uint64, kbID, dsID, externalID string,
) ([]types.Knowledge, error) {
	var rows []types.Knowledge
	err := tx.Unscoped().Model(&types.Knowledge{}).
		Where("tenant_id = ? AND knowledge_base_id = ? AND channel = ?", tenantID, kbID, types.ConnectorTypeNextcloud).
		Where("metadata->>'datasource_id' = ? AND metadata->>'external_id' = ?", dsID, externalID).
		Find(&rows).Error
	return rows, err
}

func (r *knowledgeRepository) hideNextcloudSourceRows(
	tx *gorm.DB, tenantID uint64, kbID, dsID, externalID string,
) error {
	rows, err := r.nextcloudRowsForSource(tx, tenantID, kbID, dsID, externalID)
	if err != nil {
		return err
	}
	for i := range rows {
		if err := writeNextcloudETag(tx, &rows[i], ""); err != nil {
			return err
		}
	}
	return nil
}

// lockNextcloudSourceRowsInTx serializes source transitions with vector writes.
// The KB sentinel must be held before enumerating rows, including for an empty
// source. All exact fences, including the candidate, are then locked in ID
// order before a source-version or knowledge row is locked.
func (r *knowledgeRepository) lockNextcloudSourceRowsInTx(tx *gorm.DB,
	tenantID uint64, kbID, dsID, externalID, candidateID string,
) ([]types.Knowledge, error) {
	nowMS, err := nextcloudLeaseNowMS(tx)
	if err != nil {
		return nil, err
	}
	kbFence, err := nextcloudLockKBFence(tx, tenantID, kbID, nowMS)
	if err != nil {
		return nil, err
	}
	if kbFence.State != "open" {
		return nil, ErrNextcloudContentLeaseDenied
	}
	rows, err := r.nextcloudRowsForSource(tx, tenantID, kbID, dsID, externalID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows)+1)
	seen := make(map[string]bool, len(rows)+1)
	for _, row := range rows {
		if !seen[row.ID] {
			ids = append(ids, row.ID)
			seen[row.ID] = true
		}
	}
	if candidateID != "" && !seen[candidateID] {
		ids = append(ids, candidateID)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if _, err := nextcloudLockExactFence(tx, NextcloudContentScope{
			TenantID: tenantID, KnowledgeBaseID: kbID, KnowledgeID: id,
			DataSourceID: dsID, ExternalID: externalID,
		}, nowMS, true); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// retireNextcloudSourceRowsLockedInTx closes build/read admission only after
// the caller has locked the whole source and decided on its new candidate.
func (r *knowledgeRepository) retireNextcloudSourceRowsLockedInTx(ctx context.Context,
	tx *gorm.DB, tenantID uint64, kbID, dsID, externalID, keepKnowledgeID string,
	rows []types.Knowledge,
) error {
	leases := NewNextcloudContentLeaseStore(r.db)
	for _, row := range rows {
		if row.ID == keepKnowledgeID {
			continue
		}
		if err := leases.RetireKnowledgeInTx(ctx, tx, NextcloudContentScope{
			TenantID: tenantID, KnowledgeBaseID: kbID, KnowledgeID: row.ID,
			DataSourceID: dsID, ExternalID: externalID,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *knowledgeRepository) retireNextcloudSourceRowsInTx(ctx context.Context,
	tx *gorm.DB, tenantID uint64, kbID, dsID, externalID, keepKnowledgeID string,
) error {
	rows, err := r.lockNextcloudSourceRowsInTx(tx, tenantID, kbID, dsID, externalID, keepKnowledgeID)
	if err != nil {
		return err
	}
	return r.retireNextcloudSourceRowsLockedInTx(ctx, tx, tenantID, kbID,
		dsID, externalID, keepKnowledgeID, rows)
}

// ErrNextcloudCandidateRetryManual reports a candidate that requires a manual retry.
var ErrNextcloudCandidateRetryManual = apperrors.NewProtocolError(
	errors.New("nextcloud failed candidate requires administrator retry"),
	"Nextcloud failed candidate requires administrator retry",
)

// StageNextcloudVersion records the newest fetched candidate and clears the
// publication ETag on every older row in one transaction. A candidate is
// created with an empty publication ETag before this call, so a failed stage
// leaves it hidden and leaves the previous knowledge and file intact.
func (r *knowledgeRepository) StageNextcloudVersion(
	ctx context.Context, tenantID uint64, kbID, dsID, externalID, etag, candidateID string,
) error {
	return r.stageNextcloudVersion(ctx, tenantID, kbID, dsID, externalID, etag, candidateID, "", nil)
}

// StageNextcloudVersionWithSource updates source-owned display metadata in the
// same transaction as the visibility barrier. Reusing parsed bytes after a
// rename must not leave an old citation path on the newly published version.
func (r *knowledgeRepository) StageNextcloudVersionWithSource(
	ctx context.Context, tenantID uint64, kbID, dsID, externalID, etag, candidateID, fileName string,
	sourceMetadata map[string]string,
) error {
	if fileName == "" || sourceMetadata["datasource_id"] != dsID ||
		sourceMetadata["external_id"] != externalID || sourceMetadata["nextcloud_target_etag"] != etag ||
		sourceMetadata["nextcloud_etag"] != "" || sourceMetadata["nextcloud_path"] == "" {
		return errors.New("incomplete Nextcloud source metadata")
	}
	return r.stageNextcloudVersion(ctx, tenantID, kbID, dsID, externalID, etag, candidateID,
		fileName, sourceMetadata)
}

func (r *knowledgeRepository) stageNextcloudVersion(
	ctx context.Context, tenantID uint64, kbID, dsID, externalID, etag, candidateID, fileName string,
	sourceMetadata map[string]string,
) error {
	if tenantID == 0 || kbID == "" || dsID == "" || externalID == "" ||
		strings.TrimSpace(etag) == "" || candidateID == "" {
		return errors.New("incomplete Nextcloud source version")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		nowMS, err := nextcloudLeaseNowMS(tx)
		if err != nil {
			return err
		}
		kbFence, err := nextcloudLockKBFence(tx, tenantID, kbID, nowMS)
		if err != nil {
			return err
		}
		if kbFence.State != "open" {
			return ErrNextcloudContentLeaseDenied
		}
		if err := r.retireNextcloudSourceRowsInTx(ctx, tx, tenantID, kbID, dsID, externalID, candidateID); err != nil {
			return err
		}
		candidateScope := NextcloudContentScope{
			TenantID: tenantID, KnowledgeBaseID: kbID,
			KnowledgeID: candidateID, DataSourceID: dsID, ExternalID: externalID,
		}
		candidateFence, err := nextcloudLockExactFence(tx, candidateScope, nowMS, true)
		if err != nil {
			return err
		}
		if candidateFence.State != "open" {
			// A retired knowledge ID can never be reused for a new version.
			return ErrNextcloudContentLeaseDenied
		}
		var previous nextcloudSourceVersion
		err = nextcloudVersionQuery(tx.Clauses(clause.Locking{Strength: "UPDATE"}),
			tenantID, kbID, dsID, externalID).Take(&previous).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		claim, claimed := nextcloudCandidateRetryClaimFromContext(ctx)
		if claimed && errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNextcloudCandidateRetryNotDue
		}
		if err == nil {
			failed, admissionErr := nextcloudFailedCandidateAdmission(tx, previous, etag,
				claim, claimed, time.Now().UTC(), true)
			if admissionErr != nil {
				return admissionErr
			}
			if failed && previous.CandidateKnowledgeID == candidateID {
				return ErrNextcloudCandidateRetryNotDue // A retry always needs a new generation.
			}
		}
		if err == nil && previous.CandidateKnowledgeID == candidateID && previous.DesiredETag != etag {
			// The same bytes may be reused after a rename. Advance the open
			// generation so an older task cannot index under the new ETag.
			if err := tx.Exec(`UPDATE nextcloud_content_fences
				SET epoch = epoch + 1, updated_at_ms = ?
				WHERE tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND state = 'open'`,
				nowMS, tenantID, kbID, candidateID).Error; err != nil {
				return err
			}
		}
		version := nextcloudSourceVersion{
			TenantID: tenantID, KnowledgeBaseID: kbID, DataSourceID: dsID,
			ExternalID: externalID, DesiredETag: etag,
			CandidateKnowledgeID: candidateID, State: "staging", UpdatedAt: time.Now().UTC(),
		}
		if err == nil && previous.State == "staging" && previous.DesiredETag == etag {
			// Preserve the first staging time across same-ETag generations. It
			// bounds automatic retries even when every new parser fails quickly.
			version.UpdatedAt = previous.UpdatedAt
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"},
				{Name: "knowledge_base_id"},
				{Name: "datasource_id"},
				{Name: "external_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"desired_etag",
				"candidate_knowledge_id",
				"state",
				"updated_at",
			}),
		}).Create(&version).Error; err != nil {
			return err
		}
		var candidate types.Knowledge
		if err := tx.Clauses(clause.Locking{
			Strength: "UPDATE",
		}).Where(("tenant_id = ? AND knowledge_base_id = ? AND id = ? AND " +
			"channel = ?"),
			tenantID, kbID, candidateID, types.ConnectorTypeNextcloud).Take(&candidate).Error; err != nil {
			return err
		}
		metadata, err := nextcloudMetadata(&candidate)
		if err != nil {
			return err
		}
		if nextcloudMetadataString(metadata, "datasource_id") != dsID ||
			nextcloudMetadataString(metadata, "external_id") != externalID {
			return apperrors.NewProtocolError(errors.New(
				"nextcloud candidate source identity mismatch",
			), "Nextcloud candidate source identity mismatch")
		}
		if err := r.hideNextcloudSourceRows(tx, tenantID, kbID, dsID, externalID); err != nil {
			return err
		}
		if sourceMetadata == nil {
			// The direct staging entry point still records the exact desired
			// version on the candidate. The parser may finish asynchronously.
			if err := tx.Where("tenant_id = ? AND id = ?", tenantID, candidateID).
				Take(&candidate).Error; err != nil {
				return err
			}
			metadata, err = nextcloudMetadata(&candidate)
			if err != nil {
				return err
			}
			metadata["nextcloud_target_etag"] = etag
			encoded, err := json.Marshal(metadata)
			if err != nil {
				return err
			}
			return tx.Model(&types.Knowledge{}).Where("tenant_id = ? AND id = ?", tenantID, candidateID).
				Update("metadata", types.JSON(encoded)).Error
		}
		// The hide operation has already rewritten the candidate's ETag. Reload
		// before merging so parser-owned metadata is not lost.
		if err := tx.Where("tenant_id = ? AND id = ?", tenantID, candidateID).
			Take(&candidate).Error; err != nil {
			return err
		}
		metadata, err = nextcloudMetadata(&candidate)
		if err != nil {
			return err
		}
		for _, key := range []string{
			"datasource_id", "external_id", "source_resource_id",
			"nextcloud_instance_id", "nextcloud_binding_id", "nextcloud_file_id",
		} {
			if sourceMetadata[key] == "" || nextcloudMetadataString(metadata, key) != sourceMetadata[key] {
				return apperrors.NewProtocolError(errors.New(
					"nextcloud candidate source identity mismatch",
				), "Nextcloud candidate source identity mismatch")
			}
		}
		for _, key := range []string{
			"nextcloud_path",
			"nextcloud_generation",
			"source_updated_at",
			"source_created_at",
		} {
			if value, ok := sourceMetadata[key]; ok {
				metadata[key] = value
			}
		}
		metadata["nextcloud_target_etag"] = etag
		metadata["nextcloud_etag"] = ""
		encoded, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		return tx.Model(&types.Knowledge{}).Where("tenant_id = ? AND id = ?", tenantID, candidateID).
			Updates(map[string]any{"file_name": fileName, "title": fileName, "metadata": types.JSON(encoded)}).Error
	})
}

// PublishNextcloudVersion publishes only the currently desired candidate after
// processing has completed and the exact source version is rechecked. Network
// calls are outside the SQL transaction; the transaction compares the source
// identity again before making the candidate visible. A second source check
// catches changes during that local commit and closes this exact publication.
func (r *knowledgeRepository) PublishNextcloudVersion(ctx context.Context, candidateID string) (bool, error) {
	if candidateID == "" {
		return false, nil
	}
	if r.nextcloudPublicationCheck == nil {
		return false, apperrors.NewProtocolError(errors.New(
			"nextcloud publication verifier unavailable",
		), "Nextcloud publication verifier unavailable")
	}
	checked, ready, err := r.publicationCandidate(r.db.WithContext(ctx), candidateID, false)
	if err != nil {
		if checked.version.CandidateKnowledgeID == candidateID {
			if hideErr := r.unpublishIfCurrent(ctx, checked); hideErr != nil {
				return false, apperrors.NewProtocolError(fmt.Errorf(
					"load publication candidate: %v; hide failed: %w",
					err,
					hideErr,
				), fmt.Sprintf("load publication candidate: %v; hide failed: %s", func() any {
					if err == nil {
						return nil
					}
					return apperrors.PublicMessage(err)
				}(), apperrors.PublicMessage(hideErr)))
			}
		}
		return false, err
	}
	if !ready {
		return false, nil
	}
	verify := func() error {
		return r.nextcloudPublicationCheck(ctx, checked.config, checked.instance,
			checked.binding, checked.fileID, checked.etag, checked.path)
	}
	if err := verify(); err != nil {
		if hideErr := r.unpublishIfCurrent(ctx, checked); hideErr != nil {
			return false, apperrors.NewProtocolError(fmt.Errorf(
				"source publication check failed: %v; hide failed: %w",
				err,
				hideErr,
			), fmt.Sprintf("source publication check failed: %v; hide failed: %s", func() any {
				if err == nil {
					return nil
				}
				return apperrors.PublicMessage(err)
			}(), apperrors.PublicMessage(hideErr)))
		}
		return false, apperrors.NewProtocolError(fmt.Errorf(
			"source publication check failed: %w",
			err,
		), fmt.Sprintf("source publication check failed: %s", apperrors.PublicMessage(
			err,
		)))
	}
	published := false
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		rows, err := r.lockNextcloudSourceRowsInTx(tx,
			checked.version.TenantID, checked.version.KnowledgeBaseID,
			checked.version.DataSourceID, checked.version.ExternalID, candidateID)
		if err != nil {
			return err
		}
		current, ready, err := r.publicationCandidate(tx, candidateID, true)
		if err != nil {
			return err
		}
		if !ready || !samePublicationCandidate(checked, current) {
			return nil
		}
		if err := ValidateNextcloudBuildWrite(ctx, tx,
			checked.version.TenantID, checked.version.KnowledgeBaseID, candidateID); err != nil {
			return err
		}
		if err := r.retireNextcloudSourceRowsLockedInTx(ctx, tx,
			checked.version.TenantID, checked.version.KnowledgeBaseID,
			checked.version.DataSourceID, checked.version.ExternalID, candidateID, rows); err != nil {
			return err
		}
		if err := r.hideNextcloudSourceRows(tx, current.version.TenantID, current.version.KnowledgeBaseID,
			current.version.DataSourceID, current.version.ExternalID); err != nil {
			return err
		}
		// hideNextcloudSourceRows changed this row too. Reload to preserve other
		// metadata fields written by parsing since the original read.
		var candidate types.Knowledge
		if err := tx.Where("id = ?", candidateID).Take(&candidate).Error; err != nil {
			return err
		}
		if err := writeNextcloudETag(tx, &candidate, current.version.DesiredETag); err != nil {
			return err
		}
		if err := nextcloudVersionQuery(tx, current.version.TenantID, current.version.KnowledgeBaseID,
			current.version.DataSourceID, current.version.ExternalID).
			Updates(map[string]any{"state": "published", "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		published = true
		return nil
	})
	if err != nil || !published {
		if err != nil {
			if hideErr := r.unpublishIfCurrent(ctx, checked); hideErr != nil {
				return false, apperrors.NewProtocolError(fmt.Errorf(
					"publish candidate: %v; hide failed: %w",
					err,
					hideErr,
				), fmt.Sprintf("publish candidate: %v; hide failed: %s", func() any {
					if err == nil {
						return nil
					}
					return apperrors.PublicMessage(err)
				}(), apperrors.PublicMessage(hideErr)))
			}
			return false, err
		}
		return false, nil
	}
	if err := verify(); err != nil {
		if hideErr := r.unpublishIfCurrent(ctx, checked); hideErr != nil {
			return false, apperrors.NewProtocolError(
				fmt.Errorf(
					("source publication changed after commit: %v; hide faile"+
						"d: %w"), err, hideErr), fmt.Sprintf(("source publication changed after commit: %v; hide faile"+
					"d: %s"), func() any {
					if err == nil {
						return nil
					}
					return apperrors.PublicMessage(err)
				}(), apperrors.PublicMessage(hideErr)))
		}
		return false, apperrors.NewProtocolError(fmt.Errorf(
			"source publication changed after commit: %w",
			err,
		), fmt.Sprintf("source publication changed after commit: %s", apperrors.PublicMessage(
			err,
		)))
	}
	final, ready, err := r.publicationCandidate(r.db.WithContext(ctx), candidateID, false)
	if err != nil {
		if hideErr := r.unpublishIfCurrent(ctx, checked); hideErr != nil {
			return false, apperrors.NewProtocolError(fmt.Errorf(
				"final publication check failed: %v; hide failed: %w",
				err,
				hideErr,
			), fmt.Sprintf("final publication check failed: %v; hide failed: %s", func() any {
				if err == nil {
					return nil
				}
				return apperrors.PublicMessage(err)
			}(), apperrors.PublicMessage(hideErr)))
		}
		return false, err
	}
	if !ready || final.version.State != "published" || !samePublicationCandidate(checked, final) {
		if hideErr := r.unpublishIfCurrent(ctx, checked); hideErr != nil {
			return false, fmt.Errorf("final publication identity changed; hide failed: %w", hideErr)
		}
		return false, nil
	}
	return true, nil
}

// TombstoneNextcloudVersion hides and soft-deletes all retained versions before
// the sync cursor can pass a source deletion. Physical rows/files remain for
// later recovery policy and GC.
func (r *knowledgeRepository) TombstoneNextcloudVersion(
	ctx context.Context, tenantID uint64, kbID, dsID, externalID string,
) error {
	if tenantID == 0 || kbID == "" || dsID == "" || externalID == "" {
		return errors.New("incomplete Nextcloud tombstone identity")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := r.retireNextcloudSourceRowsInTx(ctx, tx, tenantID, kbID, dsID, externalID, ""); err != nil {
			return err
		}
		version := nextcloudSourceVersion{
			TenantID: tenantID, KnowledgeBaseID: kbID, DataSourceID: dsID,
			ExternalID: externalID, State: "tombstone", UpdatedAt: time.Now().UTC(),
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"},
				{Name: "knowledge_base_id"},
				{Name: "datasource_id"},
				{Name: "external_id"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"desired_etag",
				"candidate_knowledge_id",
				"state",
				"updated_at",
			}),
		}).Create(&version).Error; err != nil {
			return err
		}
		if err := r.hideNextcloudSourceRows(tx, tenantID, kbID, dsID, externalID); err != nil {
			return err
		}
		return tx.Where("tenant_id = ? AND knowledge_base_id = ? AND channel = ?",
			tenantID, kbID, types.ConnectorTypeNextcloud).
			Where("metadata->>'datasource_id' = ? AND metadata->>'external_id' = ?", dsID, externalID).
			Delete(&types.Knowledge{}).Error
	})
}

// enforceNextcloudVersionMetadata runs after generic knowledge saves, inside
// the same transaction. Late processing writes cannot restore the ETag of an
// older candidate after a newer stage or tombstone has committed.
func (r *knowledgeRepository) enforceNextcloudVersionMetadata(tx *gorm.DB, k *types.Knowledge) error {
	if k.Channel != types.ConnectorTypeNextcloud {
		return nil
	}
	metadata, err := nextcloudMetadata(k)
	if err != nil {
		return err
	}
	dsID := nextcloudMetadataString(metadata, "datasource_id")
	externalID := nextcloudMetadataString(metadata, "external_id")
	if dsID == "" || externalID == "" {
		return nil // publication guard rejects incomplete identities
	}
	var version nextcloudSourceVersion
	err = nextcloudVersionQuery(tx, k.TenantID, k.KnowledgeBaseID, dsID, externalID).Take(&version).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil // pre-migration legacy document, not yet staged
	}
	if err != nil {
		return err
	}
	etag := ""
	if version.State == "published" && version.CandidateKnowledgeID == k.ID {
		etag = version.DesiredETag
	}
	if nextcloudMetadataString(metadata, "nextcloud_etag") != etag {
		return writeNextcloudETag(tx, k, etag)
	}
	return nil
}

func (r *knowledgeRepository) maybePublishNextcloudVersion(ctx context.Context, id string) error {
	var knowledge types.Knowledge
	if err := r.db.WithContext(ctx).Select("id", "channel").Where("id = ?", id).Take(&knowledge).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	if knowledge.Channel != types.ConnectorTypeNextcloud {
		return nil
	}
	_, err := r.PublishNextcloudVersion(ctx, id)
	return err
}
