package repository

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

// The capability is private server state. It carries no ordinary role, user,
// membership, API key or model purpose. Only the exact pairing name consumes it.
type (
	nextcloudOnboardingProducerKey struct{}
	nextcloudOnboardingProducer    struct {
		Credential                        types.NextcloudOnboardingCredential
		Operation                         types.NextcloudOnboardingOperation
		ActorDirectoryID, ActorObjectGUID string
	}
)

type nextcloudOnboardingProducerReceipt struct {
	CredentialID, OperationID, RequestSHA256, InstanceID, BindingID  string
	TenantID                                                         uint64
	KnowledgeBaseID, TemplateKBID, ActorDirectoryID, ActorObjectGUID string
}

func (p nextcloudOnboardingProducer) receipt() nextcloudOnboardingProducerReceipt {
	return nextcloudOnboardingProducerReceipt{
		p.Credential.ID,
		p.Operation.OperationID,
		p.Operation.RequestSHA256,
		p.Operation.InstanceID,
		p.Operation.BindingID,
		p.Operation.TenantID,
		p.Operation.KnowledgeBaseID,
		p.Operation.ProcessingTemplateID,
		p.ActorDirectoryID,
		p.ActorObjectGUID,
	}
}

func currentOnboardingCredential(
	tx *gorm.DB,
	want types.NextcloudOnboardingCredential,
) (types.NextcloudOnboardingCredential, error) {
	var actual types.NextcloudOnboardingCredential
	if err := tx.Where("id=? AND enabled=?", want.ID, true).Take(&actual).Error; err != nil {
		return actual, ErrNextcloudOnboardingDenied
	}
	// Include the token hash, not only its JSON projection (which omits it).
	if actual != want || actual.TenantID == 0 || actual.DirectoryID == "" || actual.TemplateKBID == "" {
		return actual, ErrNextcloudOnboardingDenied
	}
	closed, err := nextcloudControlsClosed(tx, actual.InstanceID)
	if err != nil {
		return actual, err
	}
	if closed {
		return actual, ErrNextcloudOnboardingDenied
	}
	if err := requireNextcloudTargetTenantLiveInTx(tx, actual.TenantID); err != nil {
		return actual, err
	}
	if err := requireNextcloudTargetOpenInTx(tx, actual.TenantID, actual.TemplateKBID); err != nil {
		return actual, err
	}
	return actual, nil
}

func currentOnboardingActor(tx *gorm.DB, c types.NextcloudOnboardingCredential, directory, guid string) error {
	if _, err := NewNextcloudOnboardingStore(tx).Actor(tx.Statement.Context, c, directory, guid); err != nil {
		return err
	}
	var d types.Directory
	if err := tx.Select("id,protocol,enabled,last_successful_sync_at,last_sync_error,snapshot_version").
		Where("id=?", directory).
		Take(&d).
		Error; err != nil {
		return err
	}
	if d.Protocol != types.DirectoryProtocolAD || d.SnapshotVersion == 0 || !d.Enabled ||
		d.LastSuccessfulSyncAt == nil ||
		d.LastSyncError != "" ||
		time.Since(*d.LastSuccessfulSyncAt) > 300*time.Second ||
		time.Until(*d.LastSuccessfulSyncAt) > 5*time.Second {
		return ErrNextcloudOnboardingDenied
	}
	return nil
}

func sameOnboardingOperation(a, b types.NextcloudOnboardingOperation) bool {
	return a.OperationID == b.OperationID && a.CredentialID == b.CredentialID && a.TenantID == b.TenantID &&
		a.InstanceID == b.InstanceID &&
		a.BindingID == b.BindingID &&
		a.KnowledgeBaseID == b.KnowledgeBaseID &&
		a.ActorDirectoryID == b.ActorDirectoryID &&
		a.ActorObjectGUID == b.ActorObjectGUID &&
		a.ActorUserID == b.ActorUserID &&
		a.Mode == b.Mode &&
		a.Name == b.Name &&
		a.ProcessingTemplateID == b.ProcessingTemplateID &&
		a.RequestSHA256 == b.RequestSHA256
}

// BeginPairProducer begins pair producer.
func (s *NextcloudOnboardingStore) BeginPairProducer(
	ctx context.Context,
	c types.NextcloudOnboardingCredential,
	op types.NextcloudOnboardingOperation,
	directory, guid string,
) (context.Context, error) {
	p := nextcloudOnboardingProducer{c, op, directory, guid}
	if err := p.check(s.db.WithContext(ctx)); err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, nextcloudOnboardingProducerKey{}, p), nil
}

func (p nextcloudOnboardingProducer) check(tx *gorm.DB) error {
	c, err := currentOnboardingCredential(tx, p.Credential)
	if err != nil {
		return err
	}
	var op types.NextcloudOnboardingOperation
	if err := tx.Where("operation_id=? AND credential_id=?", p.Operation.OperationID, c.ID).
		Take(&op).
		Error; err != nil {
		return err
	}
	if !sameOnboardingOperation(op, p.Operation) || (op.State != "reserved" && op.State != "active") ||
		op.TenantID != c.TenantID ||
		op.InstanceID != c.InstanceID ||
		op.ProcessingTemplateID != c.TemplateKBID ||
		op.ActorDirectoryID != c.DirectoryID ||
		len(op.RequestSHA256) != 64 {
		return ErrNextcloudOnboardingDenied
	}
	if err := requireNextcloudTargetOpenInTx(tx, op.TenantID, op.KnowledgeBaseID); err != nil {
		return err
	}
	return currentOnboardingActor(tx, c, p.ActorDirectoryID, p.ActorObjectGUID)
}

func scopedOnboardingDataSourceName(
	ctx context.Context,
	tx *gorm.DB,
	ds *types.DataSource,
	edit datasourceNameEdit,
) (*nextcloudOnboardingProducerReceipt, bool, error) {
	p, ok := ctx.Value(nextcloudOnboardingProducerKey{}).(nextcloudOnboardingProducer)
	if !ok {
		return nil, false, nil
	}
	if err := p.check(tx.WithContext(ctx)); err != nil {
		return nil, true, err
	}
	if ds == nil || ds.TenantID != p.Operation.TenantID || ds.KnowledgeBaseID != p.Operation.KnowledgeBaseID ||
		ds.Type != types.ConnectorTypeNextcloud ||
		edit.Kind != "pairing_configuration" ||
		edit.PairOperation != p.Operation.OperationID ||
		edit.BindingID != p.Operation.BindingID ||
		ds.Name != "Nextcloud · "+p.Operation.BindingID ||
		edit.Name != ds.Name {
		return nil, true, ErrNextcloudOnboardingDenied
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return nil, true, err
	}
	base, _, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil || cfg == nil || len(cfg.ResourceIDs) != 1 || cfg.ResourceIDs[0] != p.Operation.BindingID ||
		base != p.Credential.BaseURL ||
		binding != p.Operation.BindingID ||
		cfg.Settings["onboarding_operation_id"] != p.Operation.OperationID {
		return nil, true, ErrNextcloudOnboardingDenied
	}
	r := p.receipt()
	return &r, true, nil
}

// The name is the actual authenticated NC manager input, not retained template
// text. Its independent envelope does not certify any copied configuration.
func admitOnboardingKnowledgeBaseName(
	tx *gorm.DB,
	req types.NextcloudOnboardingRequest,
	c types.NextcloudOnboardingCredential,
	kb *types.KnowledgeBase,
) error {
	if req.Mode != "create" || kb == nil || kb.Name != req.Name || kb.TenantID != c.TenantID {
		return ErrNextcloudOnboardingDenied
	}
	if err := currentOnboardingActor(tx, c, req.ActorDirectoryID, req.ActorObjectGUID); err != nil {
		return err
	}
	lineage, err := types.NewCompleteSourceLineage()
	if err != nil {
		return err
	}
	input := struct {
		Request      types.NextcloudOnboardingRequest
		CredentialID string
	}{req, c.ID}
	return admitKnowledgeDisplayOrigin(tx.Statement.Context, tx, "kb-name", kb.ID, kb, kb.Name, input, lineage, "human")
}

func onboardingConfigurationMetadata(
	tx *gorm.DB,
	tenant uint64,
	kb string,
) (knowledgeConfigurationInputOrigin, *types.SourceLineage, error) {
	var origin knowledgeConfigurationInputOrigin
	revision, err := knowledgeConfigurationControlRevision(tx, kb)
	if err != nil {
		return origin, nil, err
	}
	if err := tx.Where("tenant_id=? AND knowledge_base_id=? AND config_revision=?", tenant, kb, revision).
		Take(&origin).
		Error; err != nil {
		return origin, nil, err
	}
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(origin.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil ||
		origin.InputDigest == "" {
		return origin, nil, types.ErrSourceLineageUnknown
	}
	return origin, &lineage, nil
}

func sameOnboardingConfigurationMetadata(a, b knowledgeConfigurationInputOrigin) bool {
	return reflect.DeepEqual(a, b)
}
