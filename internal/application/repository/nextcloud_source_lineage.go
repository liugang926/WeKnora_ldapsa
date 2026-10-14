package repository

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	apperrors "github.com/Tencent/WeKnora/internal/errors"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

var (
	// ErrNextcloudLineageSnapshotDenied reports an unavailable authorized source-lineage snapshot.
	ErrNextcloudLineageSnapshotDenied = apperrors.NewProtocolError(
		errors.New("nextcloud lineage snapshot is unavailable or changed"),
		"Nextcloud lineage snapshot is unavailable or changed",
	)
	// ErrNextcloudLineageSnapshotStore reports unavailable durable source-lineage storage.
	ErrNextcloudLineageSnapshotStore = apperrors.NewProtocolError(errors.New(
		"nextcloud lineage snapshot storage unavailable",
	), "Nextcloud lineage snapshot storage unavailable")
)

// ResolveNextcloudSourceLineage is an unused producer boundary. Arguments are
// the server-resolved document scope, not citation/model metadata. Every part
// of the returned identity is reloaded from persisted rows in one snapshot.
// It neither authorizes the caller nor accounts for other influencing inputs.
func (r *knowledgeRepository) ResolveNextcloudSourceLineage(
	ctx context.Context, tenantID uint64, kbID, knowledgeID string,
) (*types.Knowledge, types.SourceIdentity, error) {
	var knowledge *types.Knowledge
	var identity types.SourceIdentity
	if r == nil || r.db == nil || tenantID == 0 || tenantID > math.MaxInt64 || kbID == "" || knowledgeID == "" {
		return nil, identity, ErrNextcloudLineageSnapshotDenied
	}
	if r.db.Name() != "postgres" && r.db.Name() != "sqlite" {
		return nil, identity, ErrNextcloudLineageSnapshotStore
	}
	// PostgreSQL READ COMMITTED would permit a mixed version/pair/fence across
	// queries. SQLite's read transaction supplies a coherent database snapshot.
	options := &sql.TxOptions{ReadOnly: true}
	if r.db.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		knowledge, identity, err = r.resolveNextcloudSourceLineageInTx(tx, tenantID, kbID, knowledgeID)
		return err
	}, options)
	if err != nil {
		return nil, types.SourceIdentity{}, err
	}
	return knowledge, identity, nil
}

func (
	r *knowledgeRepository,
) resolveNextcloudSourceLineageInTx(tx *gorm.DB, tenantID uint64, kbID, knowledgeID string) (
	*types.Knowledge,
	types.SourceIdentity,
	error,
) {
	var knowledge *types.Knowledge
	var identity types.SourceIdentity
	err := func() error {
		var current types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", knowledgeID, tenantID, kbID).
			Take(&current).Error; err != nil {
			return lineageSnapshotReadError(err)
		}
		if current.Channel !=
			types.ConnectorTypeNextcloud ||
			current.ParseStatus !=
				types.ParseStatusCompleted ||
			current.EnableStatus !=
				"enabled" {
			return ErrNextcloudLineageSnapshotDenied
		}
		metadata, err := strictLineageKnowledgeMetadata(current.Metadata)
		if err != nil {
			return err
		}
		var kb types.KnowledgeBase
		if err := tx.Select("id", "tenant_id", "deleted_at").Where("id = ? AND tenant_id = ?", kbID, tenantID).
			Take(&kb).Error; err != nil {
			return lineageSnapshotReadError(err)
		}
		candidate, ready, err := r.publicationCandidate(tx, knowledgeID, false)
		if err != nil {
			return apperrors.NewProtocolError(fmt.Errorf(
				"%w: publication snapshot",
				ErrNextcloudLineageSnapshotStore,
			), fmt.Sprintf("%s: publication snapshot", apperrors.PublicMessage(
				ErrNextcloudLineageSnapshotStore,
			)))
		}
		if !ready || candidate.version.State != "published" || candidate.version.TenantID != tenantID ||
			candidate.version.KnowledgeBaseID != kbID || candidate.version.CandidateKnowledgeID != knowledgeID {
			return ErrNextcloudLineageSnapshotDenied
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND datasource_id = ? AND state = 'active'",
			tenantID, kbID, candidate.version.DataSourceID).Take(&pair).Error; err != nil {
			return lineageSnapshotReadError(err)
		}
		var observed struct {
			Revision             int64
			DesiredETag          string `gorm:"column:desired_etag"`
			CandidateKnowledgeID string
			State                string
			SourceUpdatedAt      time.Time
		}
		if err := tx.Table(
			"nextcloud_source_revisions",
		).Where(("tenant_id = ? AND knowledge_base_id = ? AND datasource_" +
			"id = ? AND external_id = ?"),
			tenantID, kbID, candidate.version.DataSourceID, candidate.version.ExternalID).
			Order("revision DESC").Take(&observed).Error; err != nil {
			return lineageSnapshotReadError(err)
		}
		if observed.Revision <= 0 || observed.DesiredETag != candidate.version.DesiredETag ||
			observed.CandidateKnowledgeID !=
				knowledgeID ||
			observed.State !=
				"published" ||
			!observed.SourceUpdatedAt.Equal(candidate.version.UpdatedAt) {
			return ErrNextcloudLineageSnapshotDenied
		}
		var fence nextcloudContentFence
		var kbFence nextcloudContentFence
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ''", tenantID, kbID).
			Take(&kbFence).Error; err != nil {
			return lineageSnapshotReadError(err)
		}
		if kbFence.State != "open" || kbFence.Epoch <= 0 {
			return ErrNextcloudLineageSnapshotDenied
		}
		if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", tenantID, kbID, knowledgeID).
			Take(&fence).Error; err != nil {
			return lineageSnapshotReadError(err)
		}
		if fence.Epoch <=
			0 ||
			fence.State !=
				"open" ||
			fence.DataSourceID !=
				candidate.version.DataSourceID ||
			fence.ExternalID !=
				candidate.version.ExternalID {
			return ErrNextcloudLineageSnapshotDenied
		}
		// The observation sequence is not a publication/build generation. The
		// exact open fence epoch is the durable build-write token; paired with
		// immutable knowledge ID, it survives same-content path/metadata edits.
		identity = types.SourceIdentity{
			Provider: types.ConnectorTypeNextcloud, TenantID: current.TenantID,
			KnowledgeBaseID: current.KnowledgeBaseID, DataSourceID: candidate.version.DataSourceID,
			PairOperationID: pair.OperationID, InstanceID: candidate.instance, BindingID: candidate.binding,
			FileID: strconv.FormatInt(candidate.fileID, 10), ExternalID: candidate.version.ExternalID,
			KnowledgeID: current.ID, Revision: uint64(fence.Epoch), ETag: metadata["nextcloud_etag"],
		}
		if identity.ETag !=
			candidate.etag ||
			identity.ETag !=
				candidate.version.DesiredETag ||
			identity.Validate() !=
				nil {
			return ErrNextcloudLineageSnapshotDenied
		}
		knowledge = &current
		return nil
	}()
	if err != nil {
		return nil, types.SourceIdentity{}, err
	}
	return knowledge, identity, nil
}

// RecheckNextcloudSourceLineage compares the entire dependency set in one
// coherent local snapshot after every remote/grant check has completed. A
// source checked early cannot be silently changed during a later remote call.
func (r *knowledgeRepository) RecheckNextcloudSourceLineage(ctx context.Context, lineage *types.SourceLineage) (
	bool,
	error,
) {
	if lineage.RequireComplete() != nil {
		return false, nil
	}
	if r == nil || r.db == nil {
		return false, ErrNextcloudLineageSnapshotStore
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch r.db.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return false, ErrNextcloudLineageSnapshotStore
	}
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, saved := range lineage.Sources {
			_, current, err := r.resolveNextcloudSourceLineageInTx(
				tx,
				saved.TenantID,
				saved.KnowledgeBaseID,
				saved.KnowledgeID,
			)
			if err != nil {
				return err
			}
			if current != saved {
				return ErrNextcloudLineageSnapshotDenied
			}
		}
		return nil
	}, options)
	if errors.Is(err, ErrNextcloudLineageSnapshotDenied) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func lineageSnapshotReadError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNextcloudLineageSnapshotDenied
	}
	return apperrors.NewProtocolError(fmt.Errorf(
		"%w: snapshot read",
		ErrNextcloudLineageSnapshotStore,
	), fmt.Sprintf("%s: snapshot read", apperrors.PublicMessage(
		ErrNextcloudLineageSnapshotStore,
	)))
}

// Metadata may contain unrelated parser fields, but identity keys must be
// canonical strings and unique. No source identity is recovered from text.
func strictLineageKnowledgeMetadata(data []byte) (map[string]string, error) {
	if !utf8.Valid(data) {
		return nil, ErrNextcloudLineageSnapshotDenied
	}
	d := json.NewDecoder(bytes.NewReader(data))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, ErrNextcloudLineageSnapshotDenied
	}
	seen := map[string]bool{}
	out := map[string]string{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return nil, ErrNextcloudLineageSnapshotDenied
		}
		seen[key] = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return nil, ErrNextcloudLineageSnapshotDenied
		}
		switch key {
		case
			"datasource_id",
			"external_id",
			"source_resource_id",
			"nextcloud_instance_id",
			"nextcloud_binding_id",
			"nextcloud_file_id",
			"nextcloud_etag",
			"nextcloud_target_etag":
			var value *string
			if err := json.Unmarshal(raw, &value); err != nil || value == nil {
				return nil, ErrNextcloudLineageSnapshotDenied
			}
			out[key] = *value
		}
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') {
		return nil, ErrNextcloudLineageSnapshotDenied
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, ErrNextcloudLineageSnapshotDenied
	}
	return out, nil
}
