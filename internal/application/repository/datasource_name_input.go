package repository

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type datasourceNameInput struct {
	ID                                                string `gorm:"primaryKey"`
	DataSourceID                                      string `gorm:"column:datasource_id"`
	TenantID                                          uint64
	KnowledgeBaseID                                   string
	Revision                                          uint64
	BodyDigest, OriginKind, ActualInputs, InputDigest string
}

func (datasourceNameInput) TableName() string { return "datasource_name_inputs" }

type (
	datasourceNameEditKey struct{}
	datasourceNameEdit    struct {
		Name                     string
		Kind                     string
		PairOperation, BindingID string
		Producer                 *nextcloudOnboardingProducerReceipt `json:"onboarding_producer,omitempty"`
	}
)

// WithIndependentDataSourceNameInput belongs only at an actual edit boundary.
// Updating credentials/sync state without this marker cannot certify old text.
func WithIndependentDataSourceNameInput(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, datasourceNameEditKey{}, datasourceNameEdit{Name: name, Kind: "human"})
}

// ConfigureDataSourceNameInputWriters configures data source name input writers.
func ConfigureDataSourceNameInputWriters(
	ds interfaces.DataSourceRepository,
	pairs *NextcloudSourcePairingRepository,
	factory KnowledgeDisplayAuthorityFactory,
) {
	if r, ok := ds.(*DataSourceRepository); ok {
		r.nameAuthorityFactory = factory
	}
	if pairs != nil {
		pairs.nameAuthorityFactory = factory
	}
}

func datasourceNameBody(ds *types.DataSource) any {
	return struct {
		ID, KB, Type, Name string
		Tenant             uint64
	}{ds.ID, ds.KnowledgeBaseID, ds.Type, ds.Name, ds.TenantID}
}

func datasourceNameRevision(tx *gorm.DB, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	err := tx.Table("datasource_name_versions").Where("datasource_id = ?", id).Take(&row).Error
	if err == nil && row.Revision == 0 {
		err = ErrKnowledgeInputChanged
	}
	return row.Revision, err
}

func admitDataSourceNameInput(
	ctx context.Context,
	tx *gorm.DB,
	ds *types.DataSource,
	factory KnowledgeDisplayAuthorityFactory,
) error {
	edit, marked := ctx.Value(datasourceNameEditKey{}).(datasourceNameEdit)
	if !marked {
		return nil
	}
	if factory == nil || strings.TrimSpace(edit.Name) == "" || edit.Name != ds.Name {
		return types.ErrSourceLineageUnknown
	}
	authority := factory(tx)
	if authority == nil || authority.db != tx {
		return types.ErrSourceLineageUnknown
	}
	// A machine coordinator carries only this exact private producer purpose.
	// The ordinary authority factory must still be real and transaction-bound;
	// no caller, workspace role or model/read capability is synthesized.
	producer, scoped, err := scopedOnboardingDataSourceName(ctx, tx, ds, edit)
	if err != nil {
		return err
	}
	if scoped {
		edit.Producer = producer
	} else {
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", ds.KnowledgeBaseID, ds.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		fresh, err := authority.AuthorizeCurrentKnowledgeInputWrite(ctx, &kb)
		if err != nil {
			return err
		}
		caller := types.CallerFromContext(fresh)
		if caller.TenantID != ds.TenantID || !caller.Role.HasPermission(types.TenantRoleAdmin) {
			return access.ErrForbidden
		}
	}
	if edit.Kind != "human" &&
		(edit.Kind != "pairing_configuration" ||
			edit.BindingID == "" || edit.PairOperation == "" || edit.Name != "Nextcloud · "+edit.BindingID) {
		return ErrKnowledgeInputChanged
	}
	actual, err := json.Marshal(edit)
	if err != nil {
		return err
	}
	digest, err := KnowledgeInputDigest(edit)
	if err != nil {
		return err
	}
	body, err := KnowledgeInputDigest(datasourceNameBody(ds))
	if err != nil {
		return err
	}
	revision, err := datasourceNameRevision(tx, ds.ID)
	if err != nil {
		return err
	}
	return tx.Create(
		&datasourceNameInput{
			ID:              uuid.NewString(),
			DataSourceID:    ds.ID,
			TenantID:        ds.TenantID,
			KnowledgeBaseID: ds.KnowledgeBaseID,
			Revision:        revision,
			BodyDigest:      body,
			OriginKind:      edit.Kind,
			ActualInputs:    string(actual),
			InputDigest:     digest,
		},
	).Error
}

func readDataSourceNameInput(tx *gorm.DB, ds *types.DataSource) (*types.DataSourceNameInputProof, error) {
	if ds == nil || ds.DeletedAt.Valid || ds.ID == "" || ds.TenantID == 0 || ds.KnowledgeBaseID == "" {
		return nil, ErrKnowledgeInputChanged
	}
	revision, err := datasourceNameRevision(tx, ds.ID)
	if err != nil {
		return nil, err
	}
	body, err := KnowledgeInputDigest(datasourceNameBody(ds))
	if err != nil {
		return nil, err
	}
	var receipt datasourceNameInput
	if err := tx.Where(("datasource_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND revision" +
		" = ? AND body_digest = ?"), ds.ID, ds.TenantID, ds.KnowledgeBaseID, revision, body).
		Order("id").
		Take(&receipt).
		Error; err != nil {
		return nil, err
	}
	var actual datasourceNameEdit
	if json.Unmarshal([]byte(receipt.ActualInputs), &actual) != nil || actual.Name != ds.Name ||
		actual.Kind != receipt.OriginKind {
		return nil, ErrKnowledgeInputChanged
	}
	digest, err := KnowledgeInputDigest(actual)
	if err != nil || digest != receipt.InputDigest {
		return nil, ErrKnowledgeInputChanged
	}
	return &types.DataSourceNameInputProof{
		ID:              receipt.ID,
		DataSourceID:    ds.ID,
		TenantID:        ds.TenantID,
		KnowledgeBaseID: ds.KnowledgeBaseID,
		Revision:        revision,
		BodyDigest:      body,
		InputDigest:     digest,
		Name:            actual.Name,
	}, nil
}

// ReadDataSourceNameInput reads data source name input.
func (s *KnowledgeInputProvenanceStore) ReadDataSourceNameInput(
	ctx context.Context,
	tenant uint64,
	kb, id string,
) (*types.DataSourceNameInputProof, error) {
	var ds types.DataSource
	if err := s.db.WithContext(ctx).
		Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", id, tenant, kb).
		Take(&ds).
		Error; err != nil {
		return nil, err
	}
	return readDataSourceNameInput(s.db.WithContext(ctx), &ds)
}

func (s *KnowledgeInputProvenanceStore) recheckDataSourceNameInput(
	tx *gorm.DB,
	p *types.DataSourceNameInputProof,
) error {
	if p == nil {
		return ErrKnowledgeInputChanged
	}
	var ds types.DataSource
	if err := tx.Where(
		"id = ? AND tenant_id = ? AND knowledge_base_id = ?",
		p.DataSourceID,
		p.TenantID,
		p.KnowledgeBaseID,
	).
		Take(&ds).
		Error; err != nil {
		return err
	}
	actual, err := readDataSourceNameInput(tx, &ds)
	if err != nil {
		return err
	}
	a, err := KnowledgeInputDigest(actual)
	if err != nil {
		return err
	}
	b, err := KnowledgeInputDigest(p)
	if err != nil || a != b {
		return ErrKnowledgeInputChanged
	}
	return nil
}
