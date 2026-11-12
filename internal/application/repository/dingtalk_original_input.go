package repository

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/Tencent/WeKnora/internal/datasource/connector/dingtalk"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const dingTalkOriginalKind = "dingtalk_original"

type (
	dingTalkFetchKey    struct{}
	dingTalkArtifactKey struct{}
	dingTalkFetch       struct {
		ExecutionToken  string
		Invocation      *dingtalk.OriginalFetchInvocation
		Tenant          uint64
		KB, Source, Run string
		Revision        uint64
		Config          types.JSON
	}
)

type dingTalkArtifact struct {
	Fetch                           dingTalkFetch
	Fact                            dingtalk.OriginalFetchedArtifactInput
	InputDigest, FileName, FileHash string
	Metadata                        map[string]string
	Size                            int64
}
type dingTalkOriginalReceipt struct {
	AdmissionID                                           string `gorm:"primaryKey"`
	TenantID                                              uint64
	KnowledgeBaseID, KnowledgeID, DataSourceID, SyncLogID string
	SourceRevision                                        uint64
	Facts, FactsDigest, InputDigest, RootDigest           string
}

func (dingTalkOriginalReceipt) TableName() string { return "dingtalk_original_input_receipts" }

// BeginDingTalkOriginalFetch captures control facts before network I/O. It does
// not authorize a task: the caller must retain all existing admission checks.
func (s *KnowledgeInputProvenanceStore) BeginDingTalkOriginalFetch(
	ctx context.Context,
	ds *types.DataSource,
	run string) (context.Context,
	error,
) {
	if ds == nil || ds.Type != types.ConnectorTypeDingTalk || run == "" {
		return ctx, ErrKnowledgeInputChanged
	}
	execution, _ := ctx.Value(dingTalkExecutionKey{}).(dingTalkRunClaim)
	f := dingTalkFetch{
		ExecutionToken: execution.Token,
		Tenant:         ds.TenantID,
		KB:             ds.KnowledgeBaseID,
		Source:         ds.ID,
		Run:            run,
		Config: append(types.JSON(nil),
			ds.Config...),
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var actual types.DataSource
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where(
			"id = ? AND tenant_id = ? AND knowledge_base_id = ?",
			f.Source,
			f.Tenant,
			f.KB).Take(&actual).Error; err != nil {
			return err
		}
		if actual.Type != types.ConnectorTypeDingTalk || !reflect.DeepEqual(actual.Config, ds.Config) {
			return ErrKnowledgeInputChanged
		}
		if err := tx.Table("dingtalk_source_input_revisions").Select(
			"revision").Where("datasource_id = ?",
			f.Source).Scan(&f.Revision).Error; err != nil {
			return err
		}
		if f.Revision == 0 {
			return ErrKnowledgeInputChanged
		}
		return checkDingTalkRun(tx, f)
	}); err != nil {
		return ctx, err
	}
	ctx, f.Invocation = dingtalk.BeginOriginalFetch(ctx)
	return context.WithValue(ctx, dingTalkFetchKey{}, f), nil
}

func checkDingTalkRun(tx *gorm.DB, f dingTalkFetch) error {
	var ds types.DataSource
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where(
		"id = ? AND tenant_id = ? AND knowledge_base_id = ?",
		f.Source,
		f.Tenant,
		f.KB).Take(&ds).Error; err != nil {
		return err
	}
	if ds.Type != types.ConnectorTypeDingTalk ||
		ds.Status == types.DataSourceStatusPaused ||
		ds.Status == types.DataSourceStatusDeleted ||
		!reflect.DeepEqual(ds.Config,
			f.Config) {
		return ErrKnowledgeInputChanged
	}
	var revision uint64
	if err := tx.Table("dingtalk_source_input_revisions").Select(
		"revision").Where("datasource_id = ?",
		f.Source).Scan(&revision).Error; err != nil {
		return err
	}
	if revision != f.Revision {
		return ErrKnowledgeInputChanged
	}
	var run types.SyncLog
	if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where(
		"id = ? AND data_source_id = ? AND tenant_id = ?",
		f.Run,
		f.Source,
		f.Tenant).Take(&run).Error; err != nil {
		return err
	}
	if err := checkDingTalkExecution(tx, f); err != nil {
		return err
	}
	if run.Status != types.SyncLogStatusRunning {
		return ErrKnowledgeInputChanged
	}
	return nil
}

// BindDingTalkOriginalArtifact consumes only a genuine, untouched connector
// result. Descriptors or request metadata cannot manufacture this capability.
func (s *KnowledgeInputProvenanceStore) BindDingTalkOriginalArtifact(
	ctx context.Context,
	item *types.FetchedItem) (context.Context,
	error,
) {
	f, ok := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !ok {
		return ctx, types.ErrSourceLineageUnknown
	}
	fact, err := dingtalk.VerifyOriginalFetchedArtifactForInvocation(f.Invocation, item)
	if err != nil {
		return ctx, err
	}
	source := types.DataSource{Type: types.ConnectorTypeDingTalk, Config: f.Config}
	cfg, err := source.ParseConfig()
	if err != nil {
		return ctx, err
	}
	if err = dingtalk.VerifyOriginalArtifactConfiguration(cfg, fact); err != nil {
		return ctx, err
	}
	digest, err := KnowledgeInputDigest(item.Content)
	if err != nil {
		return ctx, err
	}
	meta := map[string]string{}
	for k, v := range item.Metadata {
		meta[k] = v
	}
	safeName, valid := utils.ValidateInput(item.FileName)
	if !valid || safeName == "" {
		return ctx, ErrKnowledgeInputChanged
	}
	// Rendered ALIDOC documents arrive as markdown; binary wiki FILE nodes
	// (pdf/office/text) arrive with their real extension and MIME type.
	extension := strings.ToLower(strings.TrimPrefix(filepath.Ext(safeName), "."))
	if extension == "md" {
		if item.ContentType != "text/markdown" {
			return ctx, ErrKnowledgeInputChanged
		}
	} else if dingtalk.BinarySyncContentType(extension) == "" ||
		item.ContentType != dingtalk.BinarySyncContentType(
			extension) {
		return ctx, ErrKnowledgeInputChanged
	}
	hash := md5.Sum(item.Content) // Matches the existing file producer's deduplication digest.
	a := dingTalkArtifact{
		FileHash: hex.EncodeToString(
			hash[:]),
		Fetch:       f,
		Fact:        fact,
		InputDigest: digest,
		FileName:    safeName,
		Metadata:    meta,
		Size:        int64(len(item.Content)),
	}
	lineage, err := types.NewCompleteSourceLineage()
	if err != nil {
		return ctx, err
	}
	ctx = context.WithValue(ctx, dingTalkArtifactKey{}, a)
	return context.WithValue(ctx, knowledgeInputOriginKey{}, knowledgeInputOrigin{lineage, dingTalkOriginalKind}), nil
}

func (s *KnowledgeInputProvenanceStore) checkDingTalkOriginalCreation(
	ctx context.Context,
	tx *gorm.DB,
	k *types.Knowledge,
	input any) (dingTalkArtifact,
	error,
) {
	a, ok := ctx.Value(dingTalkArtifactKey{}).(dingTalkArtifact)
	if !ok {
		return a, types.ErrSourceLineageUnknown
	}
	current, present := ctx.Value(dingTalkFetchKey{}).(dingTalkFetch)
	if !present || current.Invocation != a.Fetch.Invocation {
		return a, ErrKnowledgeInputChanged
	}
	bytes, ok := input.([]byte)
	if !ok {
		return a, ErrKnowledgeInputChanged
	}
	digest, err := KnowledgeInputDigest(bytes)
	if err != nil {
		return a, err
	}
	expectedType := "md"
	if expected := strings.ToLower(strings.TrimPrefix(filepath.Ext(a.FileName), ".")); expected != "md" {
		expectedType = expected
	}
	if digest != a.InputDigest ||
		k.TenantID != a.Fetch.Tenant ||
		k.KnowledgeBaseID != a.Fetch.KB ||
		k.Channel != types.ConnectorTypeDingTalk ||
		k.Type != "file" ||
		k.FileName != a.FileName ||
		k.Title != a.FileName ||
		k.FileType != expectedType ||
		k.FileHash != a.FileHash ||
		k.FileSize != a.Size {
		return a, ErrKnowledgeInputChanged
	}
	meta := k.GetMetadata()
	if meta["datasource_id"] != a.Fetch.Source ||
		meta["external_id"] != a.Fact.DocumentID ||
		meta["source_resource_id"] != a.Fact.ResourceID {
		return a, ErrKnowledgeInputChanged
	}
	for key, value := range a.Metadata {
		if meta[key] != value {
			return a, ErrKnowledgeInputChanged
		}
	}
	return a, checkDingTalkRun(tx, a.Fetch)
}

func saveDingTalkOriginalReceipt(tx *gorm.DB,
	k *types.Knowledge,
	proof *types.KnowledgeBuildInputProof,
	a dingTalkArtifact,
) error {
	facts, err := json.Marshal(a.Fact)
	if err != nil {
		return err
	}
	fd, err := KnowledgeInputDigest(a.Fact)
	if err != nil {
		return err
	}
	root, err := knowledgeRootDigest(k)
	if err != nil {
		return err
	}
	return tx.Create(&dingTalkOriginalReceipt{
		AdmissionID:     proof.AdmissionID,
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID,
		KnowledgeID:     k.ID,
		DataSourceID:    a.Fetch.Source,
		SyncLogID:       a.Fetch.Run,
		SourceRevision:  a.Fetch.Revision,
		Facts:           string(facts),
		FactsDigest:     fd,
		InputDigest:     proof.InputDigest,
		RootDigest:      root,
	}).Error
}

func verifyDingTalkOriginalReceipt(tx *gorm.DB, k *types.Knowledge, a *knowledgeInputAdmission) error {
	var r dingTalkOriginalReceipt
	if err := tx.Where("admission_id = ?", a.AdmissionID).Take(&r).Error; err != nil {
		return types.ErrSourceLineageUnknown
	}
	var fact dingtalk.OriginalFetchedArtifactInput
	if json.Unmarshal([]byte(r.Facts), &fact) != nil {
		return ErrKnowledgeInputChanged
	}
	digest, err := KnowledgeInputDigest(fact)
	if err != nil {
		return err
	}
	meta := k.GetMetadata()
	if r.TenantID != k.TenantID ||
		r.KnowledgeBaseID != k.KnowledgeBaseID ||
		r.KnowledgeID != k.ID ||
		r.RootDigest != a.RootDigest ||
		r.InputDigest != a.InputDigest ||
		digest != r.FactsDigest ||
		r.SourceRevision == 0 ||
		r.SyncLogID == "" ||
		r.DataSourceID != meta["datasource_id"] ||
		fact.DocumentID != meta["external_id"] ||
		fact.ResourceID != meta["source_resource_id"] ||
		fact.SourceConfigDigest == "" ||
		fact.ArtifactSHA256 == "" ||
		fact.ItemDigest == "" ||
		k.Channel != types.ConnectorTypeDingTalk {
		return ErrKnowledgeInputChanged
	}
	return nil
}
