package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var (
	// ErrNextcloudOnboardingDenied reports a nextcloud onboarding denied condition.
	ErrNextcloudOnboardingDenied = errors.New("nextcloud_onboarding_denied")
	// ErrNextcloudOnboardingNotFound reports a missing onboarding control row.
	ErrNextcloudOnboardingNotFound = errors.New("nextcloud_onboarding_not_found")
	// ErrNextcloudOnboardingConflict reports a conflicting onboarding binding.
	ErrNextcloudOnboardingConflict = errors.New("nextcloud_onboarding_conflict")
	// ErrNextcloudKBBlankUnverified reports that an empty knowledge base has not been proven.
	ErrNextcloudKBBlankUnverified = errors.New("knowledge_base_blank_unverified")
	// ErrNextcloudKBUnavailable reports that an existing knowledge base cannot
	// be safely attached to a Nextcloud source.
	ErrNextcloudKBUnavailable = errors.New("knowledge_base_unavailable")
)

// NextcloudOnboardingStore represents nextcloud onboarding store repository data.
type NextcloudOnboardingStore struct{ db *gorm.DB }

// NewNextcloudOnboardingStore constructs nextcloud onboarding store.
func NewNextcloudOnboardingStore(db *gorm.DB) *NextcloudOnboardingStore {
	return &NextcloudOnboardingStore{db: db}
}

func onboardingDigest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Credential returns the credential result.
func (s *NextcloudOnboardingStore) Credential(
	ctx context.Context,
	token string,
) (types.NextcloudOnboardingCredential, error) {
	var c types.NextcloudOnboardingCredential
	if len(token) < 32 || len(token) > 4096 {
		return c, ErrNextcloudOnboardingDenied
	}
	h := sha256.Sum256([]byte(token))
	err := s.db.WithContext(ctx).
		Where("token_sha256 = ? AND enabled = ?", hex.EncodeToString(h[:]), true).
		Take(&c).
		Error
	if err != nil {
		return c, ErrNextcloudOnboardingDenied
	}
	if closed, err := nextcloudControlsClosed(s.db.WithContext(ctx), c.InstanceID); err != nil || closed {
		return c, ErrNextcloudOnboardingDenied
	}
	return c, nil
}

func directoryUsable(tx *gorm.DB, directoryID string, now time.Time) error {
	var d types.Directory
	if err := tx.Where("id = ?", directoryID).Take(&d).Error; err != nil {
		return ErrNextcloudOnboardingDenied
	}
	if !d.Enabled || d.LastSuccessfulSyncAt == nil || d.LastSyncError != "" || d.LastSuccessfulSyncAt.After(now) ||
		now.Sub(*d.LastSuccessfulSyncAt) > 300*time.Second {
		return ErrNextcloudOnboardingDenied
	}
	return nil
}

// Actor returns the actor result.
func (s *NextcloudOnboardingStore) Actor(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	dir, guid string,
) (string, error) {
	tx := s.db.WithContext(ctx)
	if err := requireNextcloudTargetTenantLiveInTx(tx, c.TenantID); err != nil {
		return "", err
	}
	if dir != c.DirectoryID || directoryUsable(tx, dir, time.Now().UTC()) != nil {
		return "", ErrNextcloudOnboardingDenied
	}
	var i types.DirectoryIdentity
	if err := tx.Where(("directory_id = ? AND object_guid = ? AND status = ? AND snapshot_version =" +
		" (SELECT snapshot_version FROM directories WHERE id=?)"), dir, guid, types.DirectoryObjectActive, dir).
		Take(&i).
		Error; err != nil {
		return "", ErrNextcloudOnboardingDenied
	}
	// A connection-issued credential is held only by the Nextcloud server. The
	// server verifies the current folder manager before sending this actor; the
	// directory row and live AD identity above remain mandatory. This is the
	// multi-workspace equivalent of an explicit per-user delegation.
	if strings.HasPrefix(c.ID, "nc-conn-") {
		if i.UserID != nil && *i.UserID != "" {
			return *i.UserID, nil
		}
		return "", nil
	}
	if i.UserID == nil || *i.UserID == "" {
		var delegated int64
		if err := tx.Model(&types.NextcloudOnboardingDelegation{}).
			Where("tenant_id=? AND directory_id=? AND object_guid=?", c.TenantID, dir, guid).
			Count(&delegated).
			Error; err != nil ||
			delegated != 1 {
			return "", ErrNextcloudOnboardingDenied
		}
		// A verified GUID delegation works before first WeKnora login. Do not
		// manufacture an ordinary account, tenant member or a synthetic user.
		return "", nil
	}
	var member types.TenantMember
	if err := tx.Where(
		"tenant_id = ? AND user_id = ? AND status = ?",
		c.TenantID,
		*i.UserID,
		types.TenantMemberStatusActive,
	).
		Take(&member).
		Error; err == nil &&
		member.Role.HasPermission(types.TenantRoleAdmin) {
		return *i.UserID, nil
	}
	var n int64
	if err := tx.Model(&types.NextcloudOnboardingDelegation{}).
		Where("tenant_id = ? AND directory_id = ? AND object_guid = ?", c.TenantID, dir, guid).
		Count(&n).
		Error; err != nil ||
		n != 1 {
		return "", ErrNextcloudOnboardingDenied
	}
	return *i.UserID, nil
}

// onboardingExistingTarget admits an existing document knowledge base without
// requiring it to be empty. The owning knowledge base may already contain
// content; attaching a Nextcloud source leaves that content in place while the
// source guard prevents a second source from taking over the same KB.
func onboardingExistingTarget(tx *gorm.DB, tenant uint64, id string) (*types.KnowledgeBase, int64, error) {
	var kb types.KnowledgeBase
	if err := tx.Where("tenant_id=? AND id=? AND deleted_at IS NULL", tenant, id).Take(&kb).Error; err != nil {
		return nil, 0, ErrNextcloudKBUnavailable
	}
	if kb.Type != types.KnowledgeBaseTypeDocument || kb.IsTemporary || kb.EverHadNextcloudSource {
		return nil, 0, ErrNextcloudKBUnavailable
	}
	var n int64
	if err := tx.Model(&types.NextcloudOnboardingOperation{}).
		Where("tenant_id=? AND knowledge_base_id=?", tenant, id).
		Count(&n).
		Error; err != nil || n != 0 {
		return nil, 0, ErrNextcloudKBUnavailable
	}
	if _, _, err := onboardingConfigurationMetadata(tx, tenant, id); err != nil {
		return nil, 0, ErrNextcloudKBUnavailable
	}
	if err := tx.Table("knowledges").
		Where("tenant_id=? AND knowledge_base_id=? AND deleted_at IS NULL", tenant, id).
		Count(&n).
		Error; err != nil {
		return nil, 0, ErrNextcloudKBUnavailable
	}
	return &kb, n, nil
}

// Candidates returns the candidates result.
func (s *NextcloudOnboardingStore) Candidates(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	dir, guid string,
) ([]map[string]string, error) {
	if _, err := s.Actor(ctx, c, dir, guid); err != nil {
		return nil, err
	}
	var rows []types.KnowledgeBase
	if err := s.db.WithContext(ctx).
		Where("tenant_id = ? AND deleted_at IS NULL", c.TenantID).
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	out := []map[string]string{}
	for _, kb := range rows {
		if kb.ID == c.TemplateKBID {
			// The processing template must remain available for creating new
			// dedicated knowledge bases.
			continue
		}
		actual, contentCount, err := onboardingExistingTarget(s.db.WithContext(ctx), c.TenantID, kb.ID)
		if err != nil {
			continue
		}
		if err := validateOnboardingTemplate(s.db.WithContext(ctx), actual); err != nil {
			continue
		}
		out = append(out, map[string]string{
			"id":                     kb.ID,
			"name":                   kb.Name,
			"processing_template_id": c.TemplateKBID,
			"content_count":          strconv.FormatInt(contentCount, 10),
			"existing":               "true",
		})
	}
	return out, nil
}

func validateOnboardingTemplate(tx *gorm.DB, kb *types.KnowledgeBase) error {
	if err := kb.IndexingStrategy.ValidateGraphRequirement(); err != nil {
		return ErrNextcloudOnboardingConflict
	}
	if kb.Type != "document" || kb.IsTemporary || !kb.IndexingStrategy.HasAnyIndexing() ||
		kb.IndexingStrategy.WikiEnabled {
		return ErrNextcloudOnboardingConflict
	}
	if kb.IndexingStrategy.NeedsEmbedding() {
		var model types.Model
		if err := tx.Where(
			("id = ? AND (tenant_id = ? OR is_builtin = ?) AND type = ? AND status = ? A" +
				"ND deleted_at IS NULL"),
			kb.EmbeddingModelID,
			kb.TenantID,
			true,
			types.ModelTypeEmbedding,
			types.ModelStatusActive,
		).
			Take(&model).
			Error; err != nil {
			return ErrNextcloudOnboardingConflict
		}
	}
	if kb.IndexingStrategy.GraphEnabled {
		if kb.ExtractConfig == nil || !kb.ExtractConfig.Enabled {
			return ErrNextcloudOnboardingConflict
		}
		var model types.Model
		if err := tx.Where(("id=? AND (tenant_id=? OR is_builtin=?) AND type=? AND status=? AND deleted" +
			"_at IS NULL"), kb.SummaryModelID, kb.TenantID, true, types.ModelTypeKnowledgeQA, types.ModelStatusActive).
			Take(&model).
			Error; err != nil {
			return ErrNextcloudOnboardingConflict
		}
	}
	// Explicit supported backend required; absence/unknown inventory is not blank proof.
	if kb.StorageBackendID == nil || *kb.StorageBackendID == "" {
		return ErrNextcloudOnboardingConflict
	}
	var n int64
	if err := tx.Table("storage_backends").
		Where(
			"id = ? AND tenant_id = ? AND status = 'active' AND deleted_at IS NULL",
			*kb.StorageBackendID,
			kb.TenantID,
		).
		Count(&n).
		Error; err != nil ||
		n != 1 {
		return ErrNextcloudOnboardingConflict
	}
	return nil
}

// Reserve returns the reserve result.
func (s *NextcloudOnboardingStore) Reserve(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	req types.NextcloudOnboardingRequest,
) (types.NextcloudOnboardingOperation, error) {
	var out types.NextcloudOnboardingOperation
	if _, err := uuid.Parse(
		req.OperationID,
	); err != nil || req.TenantID != c.TenantID || req.InstanceID != c.InstanceID || req.BindingID == "" || len(
		req.BindingID,
	) > 128 || strings.TrimSpace(req.Name) == "" || len(
		req.Name,
	) > 255 || (req.Mode != "create" && req.Mode != "existing") ||
		req.ProcessingTemplateID != c.TemplateKBID {
		return out, ErrNextcloudOnboardingDenied
	}
	if _, err := currentOnboardingCredential(s.db.WithContext(ctx), c); err != nil {
		return out, err
	}
	if err := currentOnboardingActor(s.db.WithContext(ctx), c, req.ActorDirectoryID, req.ActorObjectGUID); err != nil {
		return out, err
	}
	// A durable exact replay does not consume old configuration a second time.
	// Recheck its credential, actor and permanent target controls in the writer.
	var existing types.NextcloudOnboardingOperation
	replay := s.db.WithContext(ctx).Where("operation_id=?", req.OperationID).Take(&existing).Error
	if replay != nil && !errors.Is(replay, gorm.ErrRecordNotFound) {
		return out, replay
	}
	var read *NextcloudOnboardingConfigurationRead
	if errors.Is(replay, gorm.ErrRecordNotFound) {
		var err error
		if req.Mode == "existing" {
			read, err = s.beginOnboardingExistingConfigurationRead(ctx, c, req)
		} else {
			read, err = s.beginOnboardingTemplateRead(ctx, c, req)
		}
		if err != nil {
			return out, err
		}
		defer func() { _ = read.Close() }()
	}
	var actor string
	writerCtx := ctx
	if read != nil {
		writerCtx = read.Context(ctx)
	}
	err := s.db.WithContext(writerCtx).Transaction(func(tx *gorm.DB) error {
		// Tenant -> credential -> KB is the admission lock order.
		if err := lockNextcloudTargetTenantLiveInTx(tx, c.TenantID); err != nil {
			return err
		}
		// Credential lock serializes quota and operation uniqueness before KB mutation.
		if q := tx.Exec(
			"UPDATE nextcloud_onboarding_credentials SET enabled = enabled WHERE id = ? AND enabled = ?",
			c.ID,
			true,
		); q.Error != nil ||
			q.RowsAffected != 1 {
			return ErrNextcloudOnboardingDenied
		}
		actualCredential, err := currentOnboardingCredential(tx, c)
		if err != nil {
			return err
		}
		if err := currentOnboardingActor(tx, actualCredential, req.ActorDirectoryID, req.ActorObjectGUID); err != nil {
			return err
		}
		actor, err = NewNextcloudOnboardingStore(
			tx,
		).Actor(ctx, actualCredential, req.ActorDirectoryID, req.ActorObjectGUID)
		if err != nil {
			return err
		}
		e := tx.Where("operation_id = ?", req.OperationID).Take(&out).Error
		if e == nil {
			if err := requireNextcloudTargetOpenInTx(tx, out.TenantID, out.KnowledgeBaseID); err != nil {
				return err
			}
			if out.CredentialID != c.ID || out.RequestSHA256 != onboardingDigest(req) {
				return ErrNextcloudOnboardingConflict
			}
			return nil
		}
		if !errors.Is(e, gorm.ErrRecordNotFound) {
			return e
		}
		var n int64
		if err := tx.Model(&types.NextcloudOnboardingOperation{}).
			Where("credential_id = ?", c.ID).
			Count(&n).
			Error; err != nil {
			return err
		}
		if c.MaxKnowledgeBases <= 0 || n >= int64(c.MaxKnowledgeBases) {
			return ErrNextcloudOnboardingConflict
		}
		if read == nil {
			return ErrNextcloudOnboardingConflict
		}
		kb, err := writeOnboardingConfigurationClone(writerCtx, tx, c, req, actor, read)
		if err != nil {
			return err
		}
		out = types.NextcloudOnboardingOperation{
			OperationID:          req.OperationID,
			CredentialID:         c.ID,
			TenantID:             c.TenantID,
			InstanceID:           c.InstanceID,
			BindingID:            req.BindingID,
			KnowledgeBaseID:      kb.ID,
			ActorDirectoryID:     req.ActorDirectoryID,
			ActorObjectGUID:      req.ActorObjectGUID,
			ActorUserID:          actor,
			Mode:                 req.Mode,
			Name:                 req.Name,
			ProcessingTemplateID: req.ProcessingTemplateID,
			RequestSHA256:        onboardingDigest(req),
			State:                "reserved",
			Revision:             1,
			CreatedAt:            time.Now().UTC(),
			UpdatedAt:            time.Now().UTC(),
		}
		if err := tx.Create(&out).Error; err != nil {
			return err
		}
		_, err = NewGroupAccessRepository(
			tx,
		).UpsertResourceAccessPolicy(ctx, &types.ResourceAccessPolicy{
			TenantID:     c.TenantID,
			ResourceType: types.GroupResourceTypeKnowledgeBase,
			ResourceID:   kb.ID,
			Mode:         types.ResourceAccessRestricted,
			UpdatedBy:    actor,
		})
		return err
	})
	return out, err
}

// Operation returns the operation result.
func (s *NextcloudOnboardingStore) Operation(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	id string,
) (types.NextcloudOnboardingOperation, error) {
	var out types.NextcloudOnboardingOperation
	if err := s.db.WithContext(ctx).
		Where(("operation_id = ? AND credential_id = ? AND tenant_id = ? AND instance_id =" +
			" ?"), id, c.ID, c.TenantID, c.InstanceID).
		Take(&out).
		Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Unknown and out-of-scope IDs have the same opaque response. The
			// trusted coordinator can then reserve its durable new operation.
			return out, ErrNextcloudOnboardingNotFound
		}
		return out, err
	}
	return out, nil
}

// MarkActive returns the mark active result.
func (s *NextcloudOnboardingStore) MarkActive(ctx context.Context, op types.NextcloudOnboardingOperation) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireNextcloudTargetOpenInTx(tx, op.TenantID, op.KnowledgeBaseID); err != nil {
			return err
		}
		var pair NextcloudSourcePairing
		if err := tx.Where(
			("operation_id=? AND tenant_id=? AND knowledge_base_id=? AND nextcloud_insta" +
				"nce_id=? AND binding_id=? AND state='active'"),
			op.OperationID,
			op.TenantID,
			op.KnowledgeBaseID,
			op.InstanceID,
			op.BindingID,
		).
			Take(&pair).
			Error; err != nil {
			return ErrNextcloudOnboardingConflict
		}
		return tx.Model(&types.NextcloudOnboardingOperation{}).
			Where("operation_id = ? AND state = 'reserved'", op.OperationID).
			Updates(map[string]any{"state": "active", "updated_at": time.Now().UTC()}).
			Error
	})
}

// ApplyGrantSnapshot applies grant snapshot.
func (s *NextcloudOnboardingStore) ApplyGrantSnapshot(
	ctx context.Context,
	op types.NextcloudOnboardingOperation,
	snapshot types.NextcloudGrantSnapshot,
) error {
	if !snapshot.Complete || snapshot.PermissionVersion == 0 || len(snapshot.Subjects) > 10000 {
		return ErrNextcloudOnboardingConflict
	}
	sort.Slice(snapshot.Subjects, func(i, j int) bool {
		return onboardingDigest(snapshot.Subjects[i]) < onboardingDigest(snapshot.Subjects[j])
	})
	digest := onboardingDigest(snapshot)
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireNextcloudTargetOpenInTx(tx, op.TenantID, op.KnowledgeBaseID); err != nil {
			return err
		}
		if q := tx.Exec(
			"UPDATE nextcloud_onboarding_operations SET revision = revision WHERE operation_id = ? AND tenant_id = ?",
			op.OperationID,
			op.TenantID,
		); q.Error != nil ||
			q.RowsAffected != 1 {
			return ErrNextcloudOnboardingConflict
		}
		var current types.NextcloudOnboardingOperation
		if err := tx.Where("operation_id = ?", op.OperationID).Take(&current).Error; err != nil {
			return err
		}
		if current.State != "active" {
			return ErrNextcloudOnboardingConflict
		}
		var pair NextcloudSourcePairing
		if err := tx.Where("operation_id = ? AND tenant_id = ? AND state = 'active' ", op.OperationID, op.TenantID).
			Take(&pair).
			Error; err != nil ||
			pair.KnowledgeBaseID != op.KnowledgeBaseID {
			return ErrNextcloudOnboardingConflict
		}
		currentEpoch := pair.PublicationEpoch
		if tx.Migrator().HasTable("nextcloud_source_status_epochs") {
			var epoch struct {
				PublicationEpoch int64
				PairOperationID  string
				VerifiedAt       int64
			}
			err := tx.Table("nextcloud_source_status_epochs e").
				Select("e.*").
				Joins("JOIN nextcloud_event_connections c ON c.connection_id=e.connection_id AND c.status='active'").
				Where(("c.tenant_id=? AND c.knowledge_base_id=? AND c.datasource_id=? AND e.pair_o" +
					"peration_id=?"), op.TenantID, op.KnowledgeBaseID, pair.DataSourceID, op.OperationID).
				Take(&epoch).
				Error
			if err != nil || epoch.PairOperationID != op.OperationID || time.Now().Unix()-epoch.VerifiedAt > 300 {
				return ErrNextcloudOnboardingConflict
			}
			currentEpoch = epoch.PublicationEpoch
		}
		if snapshot.PublicationEpoch != currentEpoch {
			return ErrNextcloudOnboardingConflict
		}
		if current.PermissionVersion > snapshot.PermissionVersion {
			return ErrNextcloudOnboardingConflict
		}
		isRetry := current.PermissionVersion == snapshot.PermissionVersion
		if isRetry && current.PermissionSHA256 != digest {
			return ErrNextcloudOnboardingConflict
		}
		grants := []types.NextcloudSourceGrant{}
		for _, subject := range snapshot.Subjects {
			if subject.DirectoryID != op.ActorDirectoryID || subject.SourceRelationID == "" ||
				len(subject.SourceRelationID) > 255 ||
				directoryUsable(tx, subject.DirectoryID, time.Now().UTC()) != nil {
				return ErrNextcloudOnboardingDenied
			}
			principal := ""
			switch subject.Kind {
			case "user":
				var i types.DirectoryIdentity
				if err := tx.Where(
					("directory_id = ? AND object_guid = ? AND status = ? AND snapshot_version =" +
						" (SELECT snapshot_version FROM directories WHERE id=?)"),
					subject.DirectoryID,
					subject.ObjectGUID,
					types.DirectoryObjectActive,
					subject.DirectoryID,
				).
					Take(&i).
					Error; err != nil {
					return ErrNextcloudOnboardingDenied
				}
				principal = i.ID
			case "group":
				var g types.DirectoryGroup
				if err := tx.Where(
					("directory_id = ? AND object_guid = ? AND status = ? AND snapshot_version =" +
						" (SELECT snapshot_version FROM directories WHERE id=?)"),
					subject.DirectoryID,
					subject.ObjectGUID,
					types.DirectoryObjectActive,
					subject.DirectoryID,
				).
					Take(&g).
					Error; err != nil {
					return ErrNextcloudOnboardingDenied
				}
				principal = g.ID
			default:
				return ErrNextcloudOnboardingDenied
			}
			grants = append(
				grants,
				types.NextcloudSourceGrant{
					OperationID:       op.OperationID,
					SourceRelationID:  subject.SourceRelationID,
					Kind:              subject.Kind,
					DirectoryID:       subject.DirectoryID,
					ObjectGUID:        subject.ObjectGUID,
					TenantID:          op.TenantID,
					KnowledgeBaseID:   op.KnowledgeBaseID,
					PrincipalID:       principal,
					PermissionVersion: snapshot.PermissionVersion,
					PublicationEpoch:  snapshot.PublicationEpoch,
					Origin:            "nextcloud",
				},
			)
		}
		if isRetry {
			return tx.Model(&current).Update("permission_synced_at", time.Now().UTC()).Error
		}
		if err := tx.Where("operation_id = ?", op.OperationID).Delete(&types.NextcloudSourceGrant{}).Error; err != nil {
			return err
		}
		if len(grants) > 0 {
			if err := tx.Create(&grants).Error; err != nil {
				return err
			}
		}
		if err := tx.Model(&current).
			Updates(map[string]any{
				"permission_version":   snapshot.PermissionVersion,
				"permission_sha256":    digest,
				"permission_synced_at": time.Now().UTC(),
				"updated_at":           time.Now().UTC(),
			}).
			Error; err != nil {
			return err
		}
		_, err := bumpPermissionVersion(tx, op.TenantID)
		return err
	})
}

// ScopedMatches returns the scoped matches result.
func (s *NextcloudOnboardingStore) ScopedMatches(
	ctx context.Context,
	user string,
	tenant uint64,
	kb string,
	now time.Time,
) ([]types.NextcloudScopedGrantMatch, error) {
	tx := s.db.WithContext(ctx)
	if (!tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) || !tx.Migrator().HasTable(
		&NextcloudTargetClosure{},
	) || !tx.Migrator().HasTable(
		&types.Tenant{},
	)) &&
		nextcloudCurrentControlModule(tx) {
		return []types.NextcloudScopedGrantMatch{}, nil
	}
	var rows []struct {
		types.NextcloudScopedGrantMatch
		LastSuccessfulSyncAt *time.Time
		LastSyncError        string
		Enabled              bool
	}
	query := ("SELECT DISTINCT sg.tenant_id, sg.knowledge_base_id, sg.directory_id,d.last" +
		"_successful_sync_at,d.last_sync_error,d.enabled\n FROM nextcloud_source_gr" +
		"ants sg JOIN nextcloud_onboarding_operations o ON o.operation_id=sg.operat" +
		"ion_id AND o.state='active'\n JOIN nextcloud_source_pairings p ON p.operat" +
		"ion_id=o.operation_id AND p.state='active'\n JOIN data_sources ds ON ds.id" +
		"=p.datasource_id AND ds.status='active' AND ds.deleted_at IS NULL\n JOIN d" +
		"irectories d ON d.id=sg.directory_id\n JOIN directory_identities i ON i.di" +
		"rectory_id=d.id AND i.user_id=? AND i.status='active' AND i.snapshot_versi" +
		"on=d.snapshot_version\n WHERE ( (sg.kind='user' AND sg.principal_id=i.id) " +
		"OR (sg.kind='group' AND EXISTS(SELECT 1 FROM directory_group_memberships g" +
		"m JOIN directory_groups g ON g.id=gm.group_id AND g.status='active' AND g." +
		"snapshot_version=d.snapshot_version WHERE gm.identity_id=i.id AND gm.group" +
		"_id=sg.principal_id AND gm.snapshot_version=d.snapshot_version)))")
	if tx.Migrator().HasTable("nextcloud_source_status_epochs") {
		query += (" AND sg.publication_epoch=COALESCE((SELECT MAX(e.publication_epoch) FROM n" +
			"extcloud_source_status_epochs e JOIN nextcloud_event_connections c ON c.co" +
			"nnection_id=e.connection_id AND c.status='active' WHERE e.pair_operation_i" +
			"d=p.operation_id AND c.datasource_id=ds.id),p.publication_epoch)")
	} else {
		query += " AND sg.publication_epoch=p.publication_epoch"
	}

	if tx.Migrator().HasTable(&nextcloudControlRecoveryGate{}) {
		query += (" AND NOT EXISTS(SELECT 1 FROM nextcloud_control_recovery_gates cg WHERE cg" +
			".instance_id=o.instance_id AND cg.closed=TRUE)")
	}
	query += (" AND NOT EXISTS(SELECT 1 FROM nextcloud_onboarding_target_closures tc WHER" +
		"E (tc.operation_id=o.operation_id OR (tc.tenant_id=o.tenant_id AND tc.reas" +
		"on='tenant_deleted'))) AND EXISTS(SELECT 1 FROM tenants tt WHERE tt.id=o.t" +
		"enant_id AND tt.deleted_at IS NULL) AND EXISTS(SELECT 1 FROM knowledge_bas" +
		"es kk WHERE kk.id=o.knowledge_base_id AND kk.tenant_id=o.tenant_id AND kk." +
		"deleted_at IS NULL)")
	args := []any{user}
	if tenant != 0 {
		query += " AND sg.tenant_id=?"
		args = append(args, tenant)
	}
	if kb != "" {
		query += " AND sg.knowledge_base_id=?"
		args = append(args, kb)
	}
	if err := tx.Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := []types.NextcloudScopedGrantMatch{}
	for _, r := range rows {
		if r.Enabled && r.LastSyncError == "" && r.LastSuccessfulSyncAt != nil && !r.LastSuccessfulSyncAt.After(now) &&
			now.Sub(*r.LastSuccessfulSyncAt) <= 300*time.Second {
			out = append(out, r.NextcloudScopedGrantMatch)
		}
	}
	return out, nil
}

// EventCredential returns the event credential result.
func (s *NextcloudOnboardingStore) EventCredential(
	ctx context.Context,
	op types.NextcloudOnboardingOperation,
) (NextcloudEventConnectionCredential, error) {
	if err := s.RequireTargetOpen(ctx, op); err != nil {
		return NextcloudEventConnectionCredential{}, err
	}
	var row nextcloudEventConnection
	err := s.db.WithContext(ctx).
		Table("nextcloud_event_connections").
		Where(("tenant_id = ? AND knowledge_base_id = ? AND nextcloud_instance_id = ? AND " +
			"binding_id = ? AND status = 'active' AND datasource_id IN (SELECT datasour" +
			"ce_id FROM nextcloud_source_pairings WHERE operation_id=? AND state='activ" +
			"e')"), op.TenantID, op.KnowledgeBaseID, op.InstanceID, op.BindingID, op.OperationID).
		Take(&row).
		Error
	if err != nil {
		return NextcloudEventConnectionCredential{}, err
	}
	secret, err := utils.DecryptAESGCM(row.CurrentSecretCiphertext, utils.GetAESKey())
	if err != nil {
		return NextcloudEventConnectionCredential{}, err
	}
	return NextcloudEventConnectionCredential{
		ConnectionID:        row.ConnectionID,
		NextcloudInstanceID: row.NextcloudInstanceID,
		BindingID:           row.BindingID,
		KeyID:               row.CurrentKeyID,
		Secret:              secret,
	}, nil
}

// SyncEnabled returns the sync enabled result.
func (s *NextcloudOnboardingStore) SyncEnabled(
	ctx context.Context,
	op types.NextcloudOnboardingOperation,
) (bool, error) {
	if err := s.RequireTargetOpen(ctx, op); err != nil {
		return false, err
	}
	var row struct{ SyncEnabled bool }
	q := s.db.WithContext(ctx).
		Table("data_sources").
		Select("sync_enabled").
		Where(("id IN (SELECT datasource_id FROM nextcloud_source_pairings WHERE operation" +
			"_id = ? AND state='active')"), op.OperationID).
		Take(&row)
	return row.SyncEnabled, q.Error
}

// SetSyncEnabled sets sync enabled.
func (s *NextcloudOnboardingStore) SetSyncEnabled(
	ctx context.Context,
	op types.NextcloudOnboardingOperation,
	enabled bool,
	revision uint64,
) (types.NextcloudOnboardingOperation, error) {
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := requireNextcloudTargetOpenInTx(tx, op.TenantID, op.KnowledgeBaseID); err != nil {
			return err
		}
		q := tx.Exec(
			"UPDATE nextcloud_onboarding_operations SET revision=revision WHERE operation_id=? AND state='active'",
			op.OperationID,
		)
		if q.Error != nil || q.RowsAffected != 1 {
			return ErrNextcloudOnboardingConflict
		}
		var current types.NextcloudOnboardingOperation
		if err := tx.Where("operation_id=?", op.OperationID).Take(&current).Error; err != nil {
			return err
		}
		if current.Revision != revision {
			if current.Revision == revision+1 {
				state, err := NewNextcloudOnboardingStore(tx).SyncEnabled(ctx, current)
				if err == nil && state == enabled {
					op = current
					return nil
				}
			}
			return ErrNextcloudOnboardingConflict
		}
		q = tx.Model(&current).Updates(map[string]any{"revision": revision + 1, "updated_at": time.Now().UTC()})
		if q.Error != nil {
			return q.Error
		}
		q = tx.Table("data_sources").
			Where(("id IN (SELECT datasource_id FROM nextcloud_source_pairings WHERE operation"+
				"_id = ? AND state='active')"), op.OperationID).
			Update("sync_enabled", enabled)
		if q.Error != nil || q.RowsAffected != 1 {
			return ErrNextcloudOnboardingConflict
		}
		op = current
		op.Revision = revision + 1
		return nil
	})
	return op, err
}

// TenantScoped prevents the pre-RBAC legacy membership fallback from widening
// a resource-scoped entrant after the last share has been revoked.
func (s *NextcloudOnboardingStore) TenantScoped(ctx context.Context, tenant uint64) (bool, error) {
	var n int64
	err := s.db.WithContext(ctx).
		Model(&types.NextcloudOnboardingOperation{}).
		Where("tenant_id = ?", tenant).
		Count(&n).
		Error
	return n > 0, err
}

// ScopedSession returns the scoped session result.
func (s *NextcloudOnboardingStore) ScopedSession(ctx context.Context, tenant uint64, user, id string) (bool, error) {
	var n int64
	err := s.db.WithContext(ctx).
		Table("sessions").
		Where(("tenant_id = ? AND user_id = ? AND id = ? AND nextcloud_scoped = ? AND dele" +
			"ted_at IS NULL"), tenant, user, id, true).
		Count(&n).
		Error
	return n == 1, err
}

// ManagedKB returns the managed kb result.
func (s *NextcloudOnboardingStore) ManagedKB(ctx context.Context, tenant uint64, kb string) (bool, error) {
	var n int64
	err := s.db.WithContext(ctx).
		Model(&types.NextcloudOnboardingOperation{}).
		Where("tenant_id=? AND knowledge_base_id=?", tenant, kb).
		Count(&n).
		Error
	return n > 0, err
}

// RefreshGrantEpoch returns the refresh grant epoch result.
func (s *NextcloudOnboardingStore) RefreshGrantEpoch(ctx context.Context, op types.NextcloudOnboardingOperation) error {
	if err := s.RequireTargetOpen(ctx, op); err != nil {
		return err
	}
	var connection nextcloudEventConnection
	if err := s.db.WithContext(ctx).
		Table("nextcloud_event_connections").
		Where(("tenant_id=? AND knowledge_base_id=? AND nextcloud_instance_id=? AND bindin" +
			"g_id=? AND status='active'"), op.TenantID, op.KnowledgeBaseID, op.InstanceID, op.BindingID).
		Take(&connection).
		Error; err != nil {
		return err
	}
	return NewNextcloudProcessingStateStore(s.db).RefreshSourceEpoch(ctx, connection.ConnectionID)
}

// TemplateDescriptor returns the template descriptor result.
func (s *NextcloudOnboardingStore) TemplateDescriptor(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
) (map[string]string, error) {
	var kb types.KnowledgeBase
	if err := s.db.WithContext(ctx).
		Where("tenant_id=? AND id=? AND deleted_at IS NULL", c.TenantID, c.TemplateKBID).
		Take(&kb).
		Error; err != nil {
		return nil, err
	}
	if err := validateOnboardingTemplate(s.db.WithContext(ctx), &kb); err != nil {
		return nil, err
	}
	mode := "off"
	if kb.IndexingStrategy.GraphEnabled {
		mode = "optional"
		if kb.IndexingStrategy.GraphRequired {
			mode = "required"
		}
	}
	return map[string]string{"id": kb.ID, "name": kb.Name, "graph_mode": mode}, nil
}
