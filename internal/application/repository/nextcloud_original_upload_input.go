package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
)

type nextcloudOriginalUploadInput struct {
	KnowledgeID                                                         string `gorm:"primaryKey"`
	TenantID                                                            uint64
	KnowledgeBaseID                                                     string
	DataSourceID                                                        string `gorm:"column:datasource_id"`
	SourceRevision, ConfigRevision, RootRevision                        uint64
	RoutingRevision, TenantRevision                                     uint64
	RoutingDigest, TenantDigest                                         string
	ConfigDigest, RootDigest, ArtifactDigest, ActualInputs, InputDigest string
}

func (nextcloudOriginalUploadInput) TableName() string { return "nextcloud_original_upload_inputs" }

type nextcloudFetchedArtifactKey struct{}
type nextcloudOriginalUploadBuildKey struct{}
type nextcloudOriginalFetchKey struct{}
type nextcloudOriginalFetch struct {
	TenantID                                                        uint64
	KB, DataSourceID                                                string
	SourceRevision, ConfigRevision, RoutingRevision, TenantRevision uint64
	ConfigDigest, RoutingDigest, TenantDigest                       string
	Name                                                            *types.DataSourceNameInputProof
}
type nextcloudOriginalUploadActual struct {
	Artifact                          nextcloud.OriginalFetchedArtifactInput
	PairOperation, ConfigSHA, BaseURL string
	InitialTagIDs                     []string
	Name                              *types.DataSourceNameInputProof
	TaskControls                      *nextcloudOriginalDocumentTaskControls
}
type nextcloudOriginalDocumentTaskControls struct {
	Language, FileName, FileType string
	Multimodal, Questions        bool
	QuestionCount                int
}
type nextcloudOriginalDocumentControlsKey struct{}

func WithNextcloudOriginalDocumentControls(ctx context.Context, language, fileName, fileType string, multimodal, questions bool, count int) context.Context {
	return context.WithValue(ctx, nextcloudOriginalDocumentControlsKey{}, nextcloudOriginalDocumentTaskControls{language, fileName, fileType, multimodal, questions, count})
}

type nextcloudFetchedArtifactContext struct {
	Actual        nextcloud.OriginalFetchedArtifactInput
	DataSourceID  string
	InitialTagIDs []string
}

func WithNextcloudOriginalFetchedArtifact(ctx context.Context, dsID string, item *types.FetchedItem, tagIDs []string) (context.Context, error) {
	actual, err := nextcloud.VerifyOriginalFetchedArtifact(item)
	if err != nil {
		return ctx, err
	}
	if dsID == "" {
		return ctx, ErrKnowledgeInputChanged
	}
	return context.WithValue(ctx, nextcloudFetchedArtifactKey{}, nextcloudFetchedArtifactContext{actual, dsID, normalizedKnowledgeDisplayTagIDs(tagIDs)}), nil
}

// BindNextcloudOriginalFetch starts before connector transport. Its monotonic
// controls remain fixed while actual signed content bytes are fetched.
func (s *KnowledgeInputProvenanceStore) BindNextcloudOriginalFetch(ctx context.Context, kb *types.KnowledgeBase, ds *types.DataSource) (context.Context, error) {
	if kb == nil || ds == nil || kb.ID != ds.KnowledgeBaseID || kb.TenantID != ds.TenantID || ds.Type != types.ConnectorTypeNextcloud {
		return ctx, ErrKnowledgeInputChanged
	}
	if err := CheckScopedOnboardingConfigurationReads(ctx); err != nil {
		return ctx, err
	}
	var actual types.DataSource
	if err := s.db.WithContext(ctx).Select("id", "tenant_id", "knowledge_base_id", "type", "status", "config", "name").Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND status = ?", ds.ID, ds.TenantID, ds.KnowledgeBaseID, types.DataSourceStatusActive).Take(&actual).Error; err != nil {
		return ctx, err
	}
	a, err := KnowledgeInputDigest(ds.Config)
	if err != nil {
		return ctx, err
	}
	b, err := KnowledgeInputDigest(actual.Config)
	if err != nil || a != b {
		return ctx, ErrKnowledgeInputChanged
	}
	current, err := loadKnowledgeBuildConfiguration(ctx, s.db, kb.TenantID, kb.ID)
	if err != nil {
		return ctx, err
	}
	empty := &types.Knowledge{}
	digest, err := knowledgeConfigDigest(kb, empty)
	if err != nil {
		return ctx, err
	}
	currentDigest, err := knowledgeConfigDigest(current, empty)
	if err != nil || digest != currentDigest {
		return ctx, ErrKnowledgeInputChanged
	}
	lineage, err := readKnowledgeConfigurationOrigin(s.db.WithContext(ctx), current)
	if err != nil {
		return ctx, err
	}
	if len(lineage.Sources) > 0 {
		v, ok := s.originalInputSources.(originalUploadConfigurationSourceVerifier)
		if !ok {
			return ctx, types.ErrSourceLineageUnknown
		}
		if err := v.CheckKnowledgeConfigurationSources(ctx, kb.TenantID, kb.ID, lineage); err != nil {
			return ctx, err
		}
	}
	sourceRev, err := nextcloudUploadSourceRevision(s.db.WithContext(ctx), ds.ID)
	if err != nil {
		return ctx, err
	}
	configRev, err := knowledgeConfigurationControlRevision(s.db.WithContext(ctx), kb.ID)
	if err != nil {
		return ctx, err
	}
	routingRev, err := knowledgeEnrichmentRevision(s.db.WithContext(ctx), "routing", kb.ID)
	if err != nil {
		return ctx, err
	}
	routingDigest, err := knowledgeEnrichmentRoutingDigest(kb)
	if err != nil {
		return ctx, err
	}
	actualRouting, err := knowledgeEnrichmentRoutingDigest(current)
	if err != nil || actualRouting != routingDigest {
		return ctx, ErrKnowledgeInputChanged
	}
	var tenant types.Tenant
	if err := s.db.WithContext(ctx).Where("id = ?", kb.TenantID).Take(&tenant).Error; err != nil {
		return ctx, err
	}
	tenantDigest, err := knowledgeProducerTenantDigest(&tenant)
	if err != nil {
		return ctx, err
	}
	tenantRev, err := knowledgeEnrichmentRevision(s.db.WithContext(ctx), "tenant", strconv.FormatUint(kb.TenantID, 10))
	if err != nil {
		return ctx, err
	}
	value := nextcloudOriginalFetch{TenantID: kb.TenantID, KB: kb.ID, DataSourceID: ds.ID, SourceRevision: sourceRev, ConfigRevision: configRev, RoutingRevision: routingRev, TenantRevision: tenantRev, ConfigDigest: digest, RoutingDigest: routingDigest, TenantDigest: tenantDigest}
	value.Name, err = readDataSourceNameInput(s.db.WithContext(ctx), &actual)
	if err != nil {
		return ctx, err
	}
	ctx = context.WithValue(ctx, types.TenantInfoContextKey, &tenant)
	return context.WithValue(ctx, nextcloudOriginalFetchKey{}, value), nil
}
func (s *KnowledgeInputProvenanceStore) verifyNextcloudOriginalFetchInTx(tx *gorm.DB, ctx context.Context, kb *types.KnowledgeBase, ds string) error {
	original, ok := ctx.Value(nextcloudOriginalFetchKey{}).(nextcloudOriginalFetch)
	if !ok || original.TenantID != kb.TenantID || original.KB != kb.ID || original.DataSourceID != ds {
		return ErrKnowledgeInputChanged
	}
	if err := s.recheckDataSourceNameInput(tx, original.Name); err != nil {
		return err
	}
	source, err := nextcloudUploadSourceRevision(tx, ds)
	if err != nil || source != original.SourceRevision {
		return ErrKnowledgeInputChanged
	}
	config, err := knowledgeConfigDigest(kb, &types.Knowledge{})
	if err != nil || config != original.ConfigDigest {
		return ErrKnowledgeInputChanged
	}
	configRev, err := knowledgeConfigurationControlRevision(tx, kb.ID)
	if err != nil || configRev != original.ConfigRevision {
		return ErrKnowledgeInputChanged
	}
	routing, err := knowledgeEnrichmentRoutingDigest(kb)
	if err != nil || routing != original.RoutingDigest {
		return ErrKnowledgeInputChanged
	}
	routingRev, err := knowledgeEnrichmentRevision(tx, "routing", kb.ID)
	if err != nil || routingRev != original.RoutingRevision {
		return ErrKnowledgeInputChanged
	}
	var tenant types.Tenant
	if err := tx.Where("id = ?", kb.TenantID).Take(&tenant).Error; err != nil {
		return err
	}
	digest, err := knowledgeProducerTenantDigest(&tenant)
	if err != nil || digest != original.TenantDigest {
		return ErrKnowledgeInputChanged
	}
	rev, err := knowledgeEnrichmentRevision(tx, "tenant", strconv.FormatUint(kb.TenantID, 10))
	if err != nil || rev != original.TenantRevision {
		return ErrKnowledgeInputChanged
	}
	return nil
}
func nextcloudUploadSourceRevision(tx *gorm.DB, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	err := tx.Table("nextcloud_upload_source_versions").Where("datasource_id = ?", id).Take(&row).Error
	if err == nil && row.Revision == 0 {
		err = ErrKnowledgeInputChanged
	}
	return row.Revision, err
}
func originalUploadRootDigest(k *types.Knowledge) (string, error) {
	root, err := knowledgeRootDigest(k)
	if err != nil {
		return "", err
	}
	return KnowledgeInputDigest(struct{ Root, Title, Folder string }{root, k.Title, k.FolderPath})
}
func knowledgeEnrichmentRoutingDigest(kb *types.KnowledgeBase) (string, error) {
	return KnowledgeInputDigest(struct {
		Vector, Storage *string
		Provider        *types.StorageProviderConfig
		Legacy          types.StorageConfig
	}{kb.VectorStoreID, kb.StorageBackendID, kb.StorageProviderConfig, kb.StorageConfig})
}
func originalUploadSourceInTx(tx *gorm.DB, k *types.Knowledge, input nextcloudFetchedArtifactContext) (*types.DataSource, *NextcloudSourcePairing, error) {
	meta := k.GetMetadata()
	a := input.Actual
	currentRevision, err := types.NormalizeNextcloudContentRevision(meta["nextcloud_content_revision"])
	if err != nil {
		return nil, nil, ErrKnowledgeInputChanged
	}
	artifactRevision, err := types.NormalizeNextcloudContentRevision(a.ContentRevision)
	if err != nil || currentRevision != artifactRevision {
		return nil, nil, ErrKnowledgeInputChanged
	}
	if k.Channel != types.ConnectorTypeNextcloud || k.Type != "file" || k.ID == "" || k.FileName != a.FileName || meta["datasource_id"] != input.DataSourceID || meta["external_id"] != a.ExternalID || meta["source_resource_id"] != a.BindingID || meta["nextcloud_instance_id"] != a.InstanceID || meta["nextcloud_binding_id"] != a.BindingID || meta["nextcloud_file_id"] != a.FileID || meta["nextcloud_target_etag"] != a.ETag || meta["nextcloud_path"] != a.Path {
		return nil, nil, ErrKnowledgeInputChanged
	}
	var ds types.DataSource
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ? AND status = ?", input.DataSourceID, k.TenantID, k.KnowledgeBaseID, types.ConnectorTypeNextcloud, types.DataSourceStatusActive).Take(&ds).Error; err != nil {
		return nil, nil, err
	}
	base, hash, binding, err := NextcloudEventDataSourceIdentity(ds.Config)
	if err != nil {
		return nil, nil, err
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return nil, nil, err
	}
	cfg.Type = ds.Type
	if err := nextcloud.VerifyOriginalArtifactConfiguration(cfg, a); err != nil {
		return nil, nil, err
	}
	var pair NextcloudSourcePairing
	if err := tx.Where("datasource_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND state = 'active'", ds.ID, ds.TenantID, ds.KnowledgeBaseID).Take(&pair).Error; err != nil {
		return nil, nil, err
	}
	if pair.InstanceID != a.InstanceID || pair.BindingID != a.BindingID || binding != a.BindingID || pair.ConfigSHA != hash || pair.BaseURL != base {
		return nil, nil, ErrKnowledgeInputChanged
	}
	return &ds, &pair, nil
}

type originalUploadConfigurationSourceVerifier interface {
	CheckKnowledgeConfigurationSources(context.Context, uint64, string, *types.SourceLineage) error
}

// CheckNextcloudOriginalUploadInput requires the opaque actual connector read
// and independently checks its signed current source before physical storage.
func (s *KnowledgeInputProvenanceStore) CheckNextcloudOriginalUploadInput(ctx context.Context, kb *types.KnowledgeBase, k *types.Knowledge, input []byte) error {
	if err := CheckScopedOnboardingConfigurationReads(ctx); err != nil {
		return err
	}
	original, ok := ctx.Value(nextcloudFetchedArtifactKey{}).(nextcloudFetchedArtifactContext)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	hash := sha256.Sum256(input)
	if len(input) == 0 || hex.EncodeToString(hash[:]) != original.Actual.ArtifactSHA256 {
		return ErrKnowledgeInputChanged
	}
	ds, _, err := originalUploadSourceInTx(s.db.WithContext(ctx), k, original)
	if err != nil {
		return err
	}
	current, err := loadKnowledgeBuildConfiguration(ctx, s.db, k.TenantID, k.KnowledgeBaseID)
	if err != nil {
		return err
	}
	if err := s.verifyNextcloudOriginalFetchInTx(s.db.WithContext(ctx), ctx, current, original.DataSourceID); err != nil {
		return err
	}
	wanted, err := knowledgeConfigDigest(kb, k)
	if err != nil {
		return err
	}
	actual, err := knowledgeConfigDigest(current, k)
	if err != nil || actual != wanted {
		return ErrKnowledgeInputChanged
	}
	lineage, err := readKnowledgeConfigurationOrigin(s.db.WithContext(ctx), current)
	if err != nil {
		return err
	}
	if len(lineage.Sources) > 0 {
		verifier, ok := s.originalInputSources.(originalUploadConfigurationSourceVerifier)
		if !ok {
			return types.ErrSourceLineageUnknown
		}
		if err := verifier.CheckKnowledgeConfigurationSources(ctx, k.TenantID, k.KnowledgeBaseID, lineage); err != nil {
			return err
		}
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return err
	}
	cfg.Type = ds.Type
	fileID, err := strconv.ParseInt(original.Actual.FileID, 10, 64)
	if err != nil {
		return err
	}
	if err := nextcloud.CheckCurrentPublication(types.WithNextcloudContentRevision(ctx, original.Actual.ContentRevision), cfg, original.Actual.InstanceID, original.Actual.BindingID, fileID, original.Actual.ETag, original.Actual.Path); err != nil {
		return err
	}
	return s.verifyNextcloudOriginalFetchInTx(s.db.WithContext(ctx), ctx, current, original.DataSourceID)
}
func (s *KnowledgeInputProvenanceStore) CreateNextcloudKnowledgeWithOriginalUpload(ctx context.Context, kb *types.KnowledgeBase, k *types.Knowledge, input []byte) error {
	if err := s.CheckNextcloudOriginalUploadInput(ctx, kb, k, input); err != nil {
		return err
	}
	original := ctx.Value(nextcloudFetchedArtifactKey{}).(nextcloudFetchedArtifactContext)
	before, err := nextcloudUploadSourceRevision(s.db.WithContext(ctx), original.DataSourceID)
	if err != nil {
		return err
	}
	configRevision, err := knowledgeConfigurationControlRevision(s.db.WithContext(ctx), kb.ID)
	if err != nil {
		return err
	}
	configDigest, err := knowledgeConfigDigest(kb, k)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		_, pair, err := originalUploadSourceInTx(tx, k, original)
		if err != nil {
			return err
		}
		revision, err := nextcloudUploadSourceRevision(tx, original.DataSourceID)
		if err != nil || revision != before {
			return ErrKnowledgeInputChanged
		}
		current, err := loadKnowledgeBuildConfigurationInTx(tx, kb.TenantID, kb.ID)
		if err != nil {
			return err
		}
		if err := s.verifyNextcloudOriginalFetchInTx(tx, ctx, current, original.DataSourceID); err != nil {
			return err
		}
		nowConfig, err := knowledgeConfigDigest(current, k)
		if err != nil || nowConfig != configDigest {
			return ErrKnowledgeInputChanged
		}
		nowRevision, err := knowledgeConfigurationControlRevision(tx, kb.ID)
		if err != nil || nowRevision != configRevision {
			return ErrKnowledgeInputChanged
		}
		lineage, err := readKnowledgeConfigurationOrigin(tx, current)
		if err != nil {
			return err
		}
		if err := verifyKnowledgeInheritedSources(tx, lineage); err != nil {
			return err
		}
		if err := tx.Create(k).Error; err != nil {
			return err
		}
		root, err := originalUploadRootDigest(k)
		if err != nil {
			return err
		}
		rootRevision, err := knowledgeEnrichmentRevision(tx, "knowledge", k.ID)
		if err != nil {
			return err
		}
		fetch := ctx.Value(nextcloudOriginalFetchKey{}).(nextcloudOriginalFetch)
		actual := nextcloudOriginalUploadActual{Artifact: original.Actual, PairOperation: pair.OperationID, ConfigSHA: pair.ConfigSHA, BaseURL: pair.BaseURL, InitialTagIDs: original.InitialTagIDs, Name: fetch.Name}
		controls, ok := ctx.Value(nextcloudOriginalDocumentControlsKey{}).(nextcloudOriginalDocumentTaskControls)
		if !ok {
			return types.ErrSourceLineageUnknown
		}
		actual.TaskControls = &controls
		encoded, err := json.Marshal(actual)
		if err != nil {
			return err
		}
		digest, err := KnowledgeInputDigest(actual)
		if err != nil {
			return err
		}
		return tx.Create(&nextcloudOriginalUploadInput{KnowledgeID: k.ID, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, DataSourceID: original.DataSourceID, SourceRevision: revision, ConfigRevision: configRevision, ConfigDigest: configDigest, RootDigest: root, RootRevision: rootRevision, ArtifactDigest: original.Actual.ArtifactSHA256, ActualInputs: string(encoded), InputDigest: digest, RoutingRevision: fetch.RoutingRevision, RoutingDigest: fetch.RoutingDigest, TenantRevision: fetch.TenantRevision, TenantDigest: fetch.TenantDigest}).Error
	}, knowledgeDisplayWriteOptions(s.db))
}

func (s *KnowledgeInputProvenanceStore) VerifyNextcloudOriginalDocumentTask(ctx context.Context, p types.DocumentProcessPayload) error {
	original := &types.KnowledgeBuildInputProof{TenantID: p.TenantID, KnowledgeBaseID: p.KnowledgeBaseID, KnowledgeID: p.KnowledgeID}
	actual, err := readOriginalUploadActual(s.db.WithContext(ctx), original)
	if err != nil {
		return err
	}
	if actual.TaskControls == nil || p.URL != "" || len(p.Passages) > 0 {
		return ErrKnowledgeInputChanged
	}
	controls := nextcloudOriginalDocumentTaskControls{p.Language, p.FileName, p.FileType, p.EnableMultimodel, p.EnableQuestionGeneration, p.QuestionCount}
	wanted, err := KnowledgeInputDigest(actual.TaskControls)
	if err != nil {
		return err
	}
	got, err := KnowledgeInputDigest(controls)
	if err != nil || wanted != got {
		return ErrKnowledgeInputChanged
	}
	var k types.Knowledge
	if err := s.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", p.KnowledgeID, p.TenantID, p.KnowledgeBaseID).Take(&k).Error; err != nil {
		return err
	}
	if p.FilePath != k.FilePath || p.FilePath == "" {
		return ErrKnowledgeInputChanged
	}
	return nil
}
func (s *KnowledgeInputProvenanceStore) verifyOriginalUploadInTx(tx *gorm.DB, k *types.Knowledge, input []byte, path string, initial bool) (*nextcloudOriginalUploadInput, error) {
	var receipt nextcloudOriginalUploadInput
	if err := tx.Where("knowledge_id = ? AND tenant_id = ? AND knowledge_base_id = ?", k.ID, k.TenantID, k.KnowledgeBaseID).Take(&receipt).Error; err != nil {
		return nil, err
	}
	var actual nextcloudOriginalUploadActual
	if json.Unmarshal([]byte(receipt.ActualInputs), &actual) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	digest, err := KnowledgeInputDigest(actual)
	if err != nil || digest != receipt.InputDigest {
		return nil, ErrKnowledgeInputChanged
	}
	_, pair, err := originalUploadSourceInTx(tx, k, nextcloudFetchedArtifactContext{Actual: actual.Artifact, DataSourceID: receipt.DataSourceID})
	if err != nil {
		return nil, err
	}
	if actual.PairOperation != pair.OperationID || actual.BaseURL != pair.BaseURL || actual.ConfigSHA != pair.ConfigSHA {
		return nil, ErrKnowledgeInputChanged
	}
	revision, err := nextcloudUploadSourceRevision(tx, receipt.DataSourceID)
	if err != nil || revision != receipt.SourceRevision {
		return nil, ErrKnowledgeInputChanged
	}
	if initial {
		if err := s.recheckDataSourceNameInput(tx, actual.Name); err != nil {
			return nil, err
		}
		hash := sha256.Sum256(input)
		if path == "" || path != k.FilePath || receipt.ArtifactDigest != hex.EncodeToString(hash[:]) {
			return nil, ErrKnowledgeInputChanged
		}
		root, err := originalUploadRootDigest(k)
		if err != nil || root != receipt.RootDigest {
			return nil, ErrKnowledgeInputChanged
		}
		revision, err := knowledgeEnrichmentRevision(tx, "knowledge", k.ID)
		if err != nil || revision != receipt.RootRevision {
			return nil, ErrKnowledgeInputChanged
		}
		kb, err := loadKnowledgeBuildConfigurationInTx(tx, k.TenantID, k.KnowledgeBaseID)
		if err != nil {
			return nil, err
		}
		digest, err := knowledgeConfigDigest(kb, k)
		if err != nil || digest != receipt.ConfigDigest {
			return nil, ErrKnowledgeInputChanged
		}
		revision, err = knowledgeConfigurationControlRevision(tx, kb.ID)
		if err != nil || revision != receipt.ConfigRevision {
			return nil, ErrKnowledgeInputChanged
		}
		routing, err := knowledgeEnrichmentRoutingDigest(kb)
		if err != nil || routing != receipt.RoutingDigest {
			return nil, ErrKnowledgeInputChanged
		}
		routingRevision, err := knowledgeEnrichmentRevision(tx, "routing", kb.ID)
		if err != nil || routingRevision != receipt.RoutingRevision {
			return nil, ErrKnowledgeInputChanged
		}
		var tenant types.Tenant
		if err := tx.Where("id = ?", k.TenantID).Take(&tenant).Error; err != nil {
			return nil, err
		}
		digest, err = knowledgeProducerTenantDigest(&tenant)
		if err != nil || digest != receipt.TenantDigest {
			return nil, ErrKnowledgeInputChanged
		}
		revision, err = knowledgeEnrichmentRevision(tx, "tenant", strconv.FormatUint(k.TenantID, 10))
		if err != nil || revision != receipt.TenantRevision {
			return nil, ErrKnowledgeInputChanged
		}
	}
	return &receipt, nil
}
func (s *KnowledgeInputProvenanceStore) AdmitNextcloudBuildInputsFromOriginalUpload(ctx context.Context, input []byte, path string) (*types.KnowledgeBuildInputProof, error) {
	_, scope, err := NextcloudBuildInputLease(ctx)
	if err != nil {
		return nil, err
	}
	var k types.Knowledge
	if err := s.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", scope.KnowledgeID, scope.TenantID, scope.KnowledgeBaseID).Take(&k).Error; err != nil {
		return nil, err
	}
	receipt, err := s.verifyOriginalUploadInTx(s.db.WithContext(ctx), &k, input, path, true)
	if err != nil {
		return nil, err
	}
	var actual nextcloudOriginalUploadActual
	if json.Unmarshal([]byte(receipt.ActualInputs), &actual) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	ds, _, err := originalUploadSourceInTx(s.db.WithContext(ctx), &k, nextcloudFetchedArtifactContext{Actual: actual.Artifact, DataSourceID: receipt.DataSourceID})
	if err != nil {
		return nil, err
	}
	cfg, err := ds.ParseConfig()
	if err != nil {
		return nil, err
	}
	cfg.Type = ds.Type
	id, err := strconv.ParseInt(actual.Artifact.FileID, 10, 64)
	if err != nil {
		return nil, err
	}
	if err := nextcloud.CheckCurrentPublication(types.WithNextcloudContentRevision(ctx, actual.Artifact.ContentRevision), cfg, actual.Artifact.InstanceID, actual.Artifact.BindingID, id, actual.Artifact.ETag, actual.Artifact.Path); err != nil {
		return nil, err
	}
	return s.AdmitNextcloudBuildInputs(context.WithValue(ctx, nextcloudOriginalUploadBuildKey{}, path), input)
}
