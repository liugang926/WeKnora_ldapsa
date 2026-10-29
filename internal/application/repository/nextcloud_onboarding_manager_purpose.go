package repository

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// NextcloudOnboardingManagerPurpose represents nextcloud onboarding manager purpose repository data.
// A successful explicit pairing action records its actual manager separately
// from the immutable original onboarding actor. These metadata facts contain
// no ordinary role, membership, browser user, token or model/read permission.
type NextcloudOnboardingManagerPurpose struct {
	PurposeID            string `gorm:"primaryKey"`
	OperationID          string
	Version              uint64
	PreviousPurposeID    string
	CredentialID         string
	CredentialDigest     string
	RequestSHA256        string
	TenantID             uint64
	ProcessingTemplateID string
	KnowledgeBaseID      string
	DataSourceID         string `gorm:"column:datasource_id"`
	InstanceID           string
	BindingID            string
	ConfigSHA256         string
	ActorDirectoryID     string
	ActorObjectGUID      string
	Action               string
	ActionDigest         string
	CreatedAtMS          int64
}

// TableName returns the persisted table name.
func (NextcloudOnboardingManagerPurpose) TableName() string {
	return "nextcloud_onboarding_manager_purposes"
}

type nextcloudOnboardingManagerHead struct {
	OperationID string `gorm:"primaryKey"`
	PurposeID   string
	Version     uint64
}

func (nextcloudOnboardingManagerHead) TableName() string { return "nextcloud_onboarding_manager_heads" }

func onboardingCredentialPurposeDigest(c types.NextcloudOnboardingCredential) string {
	// The ordinary JSON projection omits the token hash. Include it here, only
	// inside this one-way snapshot digest, so revocation/rotation changes scope.
	return onboardingDigest(struct {
		Credential  types.NextcloudOnboardingCredential
		TokenSHA256 string
	}{c, c.TokenSHA256})
}

func nextcloudAutomaticReadableStatuses() []string {
	return []string{types.DataSourceStatusActive, types.DataSourceStatusError}
}

func onboardingManagerPairMetadata(
	tx *gorm.DB,
	op types.NextcloudOnboardingOperation,
	c types.NextcloudOnboardingCredential,
) (NextcloudSourcePairing, error) {
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
		return pair, types.ErrSourceLineageUnknown
	}
	if pair.BaseURL != c.BaseURL {
		return pair, ErrKnowledgeInputChanged
	}
	var ds types.DataSource
	if err := tx.Select("id,tenant_id,knowledge_base_id,type,status,config").
		Where(
			("id=? AND tenant_id=? AND knowledge_base_id=? AND type=? AND status IN ? AND d" +
				"eleted_at IS NULL"),
			pair.DataSourceID,
			op.TenantID,
			op.KnowledgeBaseID,
			types.ConnectorTypeNextcloud,
			nextcloudAutomaticReadableStatuses(),
		).
		Take(&ds).
		Error; err != nil {
		return pair, err
	}
	base, sha, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || base != pair.BaseURL || sha != pair.ConfigSHA || binding != pair.BindingID {
		return pair, ErrKnowledgeInputChanged
	}
	config, err := ds.ParseConfig()
	if err != nil || config == nil || config.Settings["onboarding_operation_id"] != op.OperationID {
		return pair, ErrKnowledgeInputChanged
	}
	return pair, nil
}

func managerPurposeSameScope(
	p NextcloudOnboardingManagerPurpose,
	c types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
	pair NextcloudSourcePairing,
) bool {
	return p.CredentialID == c.ID && p.CredentialDigest == onboardingCredentialPurposeDigest(c) &&
		p.OperationID == op.OperationID &&
		p.RequestSHA256 == op.RequestSHA256 &&
		p.ProcessingTemplateID == op.ProcessingTemplateID &&
		p.TenantID == op.TenantID &&
		p.KnowledgeBaseID == op.KnowledgeBaseID &&
		p.DataSourceID == pair.DataSourceID &&
		p.InstanceID == op.InstanceID &&
		p.BindingID == op.BindingID &&
		p.ConfigSHA256 == pair.ConfigSHA &&
		p.ActorDirectoryID == c.DirectoryID &&
		p.Action == "pair" &&
		p.PurposeID != "" &&
		p.Version > 0
}

// A stored head is not a continuing NC delegation. Recheck the real paired
// authority on every capture/checkpoint, with the complete immutable purpose
// digest. This does not authorize any source READ or ordinary WK privilege.
func authorizeOnboardingManagerPurpose(
	ctx context.Context,
	tx *gorm.DB,
	c types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
	p *NextcloudOnboardingManagerPurpose,
) error {
	if p == nil {
		return types.ErrSourceLineageUnknown
	}
	pair, err := onboardingManagerPairMetadata(tx, op, c)
	if err != nil {
		return err
	}
	if !managerPurposeSameScope(*p, c, op, pair) {
		return ErrKnowledgeInputChanged
	}
	var actual types.DataSource
	if err := tx.Select("id,tenant_id,knowledge_base_id,type,status,config").
		Where(("id=? AND tenant_id=? AND knowledge_base_id=? AND status IN ? AND deleted_at I" +
			"S NULL"), p.DataSourceID, p.TenantID, p.KnowledgeBaseID,
			nextcloudAutomaticReadableStatuses()).
		Take(&actual).
		Error; err != nil {
		return err
	}
	config, err := actual.ParseConfig()
	if err != nil {
		return types.ErrSourceLineageUnknown
	}
	scope := nextcloud.ManagerAuthorizationScope{
		InstanceID:           p.InstanceID,
		OperationID:          p.OperationID,
		TenantID:             strconv.FormatUint(p.TenantID, 10),
		KnowledgeBaseID:      p.KnowledgeBaseID,
		DataSourceID:         p.DataSourceID,
		ProcessingTemplateID: p.ProcessingTemplateID,
		DirectoryID:          p.ActorDirectoryID,
		ObjectGUID:           strings.ToLower(p.ActorObjectGUID),
		PurposeSHA256:        onboardingDigest(*p),
	}
	_, err = nextcloud.AuthorizeCurrentManager(ctx, config, p.BindingID, scope)
	if errors.Is(err, nextcloud.ErrManagerAuthorizationDenied) {
		return errors.Join(ErrNextcloudOnboardingDenied, access.ErrNextcloudPublicationDenied)
	}
	if err != nil {
		return errors.Join(access.ErrNextcloudPublicationUnavailable, err)
	}
	return nil
}

func currentOnboardingManagerPurpose(
	tx *gorm.DB,
	c types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
) (*NextcloudOnboardingManagerPurpose, error) {
	if !tx.Migrator().HasTable(&NextcloudOnboardingManagerPurpose{}) ||
		!tx.Migrator().HasTable(&nextcloudOnboardingManagerHead{}) {
		return nil, types.ErrSourceLineageUnknown
	}
	pair, err := onboardingManagerPairMetadata(tx, op, c)
	if err != nil {
		return nil, err
	}
	var head nextcloudOnboardingManagerHead
	if err := tx.Where("operation_id=?", op.OperationID).Take(&head).Error; err != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var purpose NextcloudOnboardingManagerPurpose
	if err := tx.Where("purpose_id=? AND operation_id=? AND version=?", head.PurposeID, op.OperationID, head.Version).
		Take(&purpose).
		Error; err != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if !managerPurposeSameScope(purpose, c, op, pair) {
		return nil, ErrKnowledgeInputChanged
	}
	if err := currentOnboardingActor(tx, c, purpose.ActorDirectoryID, purpose.ActorObjectGUID); err != nil {
		return nil, err
	}
	if err := authorizeOnboardingManagerPurpose(tx.Statement.Context, tx, c, op, &purpose); err != nil {
		// The immutable pairing purpose and every local scope fact have already
		// been checked above. A transient manager endpoint failure must not turn
		// an otherwise valid machine source into a permanently unusable source.
		// An explicit manager denial still fails closed, and the source fetch
		// remains protected by the paired scoped credential.
		if canUseDurableManagerPurpose(err) {
			return &purpose, nil
		}
		return nil, err
	}
	return &purpose, nil
}

func canUseDurableManagerPurpose(err error) bool {
	return errors.Is(err, access.ErrNextcloudPublicationUnavailable)
}

// RecordPairManagerPurpose accepts only the private, freshly checked capability
// created at the authenticated Pair boundary. GET/status and arbitrary UUIDs
// cannot update the head. Remote uncertain/pending outcomes cannot record it.
func (s *NextcloudOnboardingStore) RecordPairManagerPurpose(
	ctx context.Context,
) (*NextcloudOnboardingManagerPurpose, error) {
	permit, ok := ctx.Value(nextcloudOnboardingProducerKey{}).(nextcloudOnboardingProducer)
	if !ok {
		return nil, types.ErrSourceLineageUnknown
	}
	var out *NextcloudOnboardingManagerPurpose
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if !tx.Migrator().HasTable(&NextcloudOnboardingManagerPurpose{}) ||
			!tx.Migrator().HasTable(&nextcloudOnboardingManagerHead{}) {
			return types.ErrSourceLineageUnknown
		}
		if err := lockNextcloudTargetTenantLiveInTx(tx, permit.Credential.TenantID); err != nil {
			return err
		}
		if q := tx.Exec(
			"UPDATE nextcloud_onboarding_credentials SET enabled=enabled WHERE id=? AND enabled=?",
			permit.Credential.ID,
			true,
		); q.Error != nil {
			return q.Error
		} else if q.RowsAffected != 1 {
			return ErrNextcloudOnboardingDenied
		}
		if q := tx.Exec(
			"UPDATE nextcloud_onboarding_operations SET revision=revision WHERE operation_id=? AND state='active'",
			permit.Operation.OperationID,
		); q.Error != nil {
			return q.Error
		} else if q.RowsAffected != 1 {
			return ErrNextcloudOnboardingDenied
		}
		if err := permit.check(tx); err != nil {
			return err
		}
		var op types.NextcloudOnboardingOperation
		if err := tx.Select(("operation_id,credential_id,tenant_id,instance_id,binding_id,knowledge_base" +
			"_id,processing_template_id,actor_directory_id,request_sha256,state")).
			Where("operation_id=? AND state='active'", permit.Operation.OperationID).
			Take(&op).
			Error; err != nil {
			return err
		}
		pair, err := onboardingManagerPairMetadata(tx, op, permit.Credential)
		if err != nil {
			return err
		}
		actionDigest := onboardingDigest(
			struct{ OperationID, RequestSHA256, Directory, GUID, Datasource, ConfigSHA string }{
				op.OperationID,
				op.RequestSHA256,
				permit.ActorDirectoryID,
				permit.ActorObjectGUID,
				pair.DataSourceID,
				pair.ConfigSHA,
			},
		)
		var head nextcloudOnboardingManagerHead
		lookup := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("operation_id=?", op.OperationID).
			Take(&head).
			Error
		if lookup != nil && !errors.Is(lookup, gorm.ErrRecordNotFound) {
			return lookup
		}
		if lookup == nil {
			var prior NextcloudOnboardingManagerPurpose
			if err := tx.Where(
				"purpose_id=? AND operation_id=? AND version=?",
				head.PurposeID,
				op.OperationID,
				head.Version,
			).
				Take(&prior).
				Error; err != nil {
				return types.ErrSourceLineageUnknown
			}
			// Exact success retries remain idempotent. A -> B -> A is a new version,
			// since only the current head may satisfy this match.
			if managerPurposeSameScope(prior, permit.Credential, op, pair) &&
				prior.ActorDirectoryID == permit.ActorDirectoryID &&
				prior.ActorObjectGUID == permit.ActorObjectGUID &&
				prior.ActionDigest == actionDigest {
				if err := authorizeOnboardingManagerPurpose(ctx, tx, permit.Credential, op, &prior); err != nil {
					return err
				}
				out = &prior
				return nil
			}
		}
		purpose := NextcloudOnboardingManagerPurpose{
			PurposeID:            uuid.NewString(),
			OperationID:          op.OperationID,
			Version:              head.Version + 1,
			PreviousPurposeID:    head.PurposeID,
			CredentialID:         permit.Credential.ID,
			CredentialDigest:     onboardingCredentialPurposeDigest(permit.Credential),
			RequestSHA256:        op.RequestSHA256,
			ProcessingTemplateID: op.ProcessingTemplateID,
			TenantID:             op.TenantID,
			KnowledgeBaseID:      op.KnowledgeBaseID,
			DataSourceID:         pair.DataSourceID,
			InstanceID:           op.InstanceID,
			BindingID:            op.BindingID,
			ConfigSHA256:         pair.ConfigSHA,
			ActorDirectoryID:     permit.ActorDirectoryID,
			ActorObjectGUID:      permit.ActorObjectGUID,
			Action:               "pair",
			ActionDigest:         actionDigest,
			CreatedAtMS:          time.Now().UTC().UnixMilli(),
		}
		if purpose.Version == 0 {
			return ErrKnowledgeInputChanged
		}
		if err := authorizeOnboardingManagerPurpose(ctx, tx, permit.Credential, op, &purpose); err != nil {
			return err
		}
		if err := tx.Create(&purpose).Error; err != nil {
			return err
		}
		if lookup == nil {
			q := tx.Model(&nextcloudOnboardingManagerHead{}).
				Where("operation_id=? AND purpose_id=? AND version=?", head.OperationID, head.PurposeID, head.Version).
				Updates(map[string]any{"purpose_id": purpose.PurposeID, "version": purpose.Version})
			if q.Error != nil {
				return q.Error
			}
			if q.RowsAffected != 1 {
				return ErrKnowledgeInputChanged
			}
		} else if err := tx.Create(&nextcloudOnboardingManagerHead{
			OperationID: op.OperationID,
			PurposeID:   purpose.PurposeID,
			Version:     purpose.Version,
		}).Error; err != nil {
			return err
		}
		if err := permit.check(tx); err != nil {
			return err
		}
		if err := authorizeOnboardingManagerPurpose(ctx, tx, permit.Credential, op, &purpose); err != nil {
			return err
		}
		out = &purpose
		return nil
	})
	return out, err
}

func sameOnboardingManagerPurpose(a, b *NextcloudOnboardingManagerPurpose) bool {
	return a != nil && b != nil && reflect.DeepEqual(*a, *b)
}

// ValidateNextcloudOnboardingManagerPurposeCoverageInTx is a metadata-only
// recovery prerequisite. It proves every currently active onboarding operation
// has an exact committed monotonic head and immutable scope fact. It neither
// authorizes reading nor contacts AD/NC, hydrates names/config text, or invents
// a missing historical fact. Retired operations cannot consume configuration.
// Detecting rollback of the whole ledger still requires the external retained
// control inventory/floor; current table presence alone is not rollback proof.
func ValidateNextcloudOnboardingManagerPurposeCoverageInTx(tx *gorm.DB, instance string) error {
	if tx == nil || instance == "" || !tx.Migrator().HasTable(&NextcloudOnboardingManagerPurpose{}) ||
		!tx.Migrator().HasTable(&nextcloudOnboardingManagerHead{}) {
		return types.ErrSourceLineageUnknown
	}
	var operations []types.NextcloudOnboardingOperation
	if err := tx.Select(("operation_id,credential_id,tenant_id,instance_id,binding_id,knowledge_base" +
		"_id,processing_template_id,actor_directory_id,request_sha256,state")).
		Where("instance_id=? AND state='active'", instance).
		Find(&operations).
		Error; err != nil {
		return err
	}
	for _, op := range operations {
		var c types.NextcloudOnboardingCredential
		if err := tx.Where("id=?", op.CredentialID).Take(&c).Error; err != nil {
			return types.ErrSourceLineageUnknown
		}
		if c.InstanceID != op.InstanceID || c.TenantID != op.TenantID || c.TemplateKBID != op.ProcessingTemplateID ||
			c.DirectoryID != op.ActorDirectoryID {
			return ErrKnowledgeInputChanged
		}
		pair, err := onboardingManagerPairMetadata(tx, op, c)
		if err != nil {
			return err
		}
		var head nextcloudOnboardingManagerHead
		if err := tx.Where("operation_id=?", op.OperationID).Take(&head).Error; err != nil {
			return types.ErrSourceLineageUnknown
		}
		var p NextcloudOnboardingManagerPurpose
		if err := tx.Where(
			"purpose_id=? AND operation_id=? AND version=?",
			head.PurposeID,
			op.OperationID,
			head.Version,
		).
			Take(&p).
			Error; err != nil {
			return types.ErrSourceLineageUnknown
		}
		// Recovery closes credentials before reopening controls. Validate the
		// immutable original scope with its admitted enabled bit; this metadata
		// check grants no runtime use and does not re-enable the credential.
		admittedCredential := c
		admittedCredential.Enabled = true
		if !managerPurposeSameScope(p, admittedCredential, op, pair) {
			return ErrKnowledgeInputChanged
		}
		var maxVersion uint64
		if err := tx.Model(&NextcloudOnboardingManagerPurpose{}).
			Select("MAX(version)").
			Where("operation_id=?", op.OperationID).
			Scan(&maxVersion).
			Error; err != nil {
			return err
		}
		if maxVersion != head.Version {
			return ErrKnowledgeInputChanged
		}
		// A finite metadata budget returns unavailable rather than certifying a
		// truncated chain. Normal management actions need only a few generations.
		var facts []NextcloudOnboardingManagerPurpose
		if err := tx.Select("purpose_id,version,previous_purpose_id").
			Where("operation_id=?", op.OperationID).
			Order("version ASC").
			Limit(16385).
			Find(&facts).
			Error; err != nil {
			return err
		}
		if len(facts) > 16384 || uint64(len(facts)) != head.Version {
			return types.ErrSourceLineageUnknown
		}
		priorID := ""
		for i, fact := range facts {
			if fact.Version != uint64(i+1) || fact.PurposeID == "" || fact.PreviousPurposeID != priorID {
				return ErrKnowledgeInputChanged
			}
			priorID = fact.PurposeID
		}
		if priorID != head.PurposeID {
			return ErrKnowledgeInputChanged
		}
	}
	return nil
}
