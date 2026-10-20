package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type templateParentInput struct {
	Knowledge *types.Knowledge
	Chunks    []*types.Chunk
	Proof     *types.KnowledgeBuildInputProof
	AllChunks bool
}
type knowledgeTemplateInput struct {
	ID                                            string `gorm:"primaryKey"`
	TenantID                                      uint64
	KnowledgeBaseID                               string
	Purpose                                       string
	ModelID                                       string
	ModelDigest                                   string
	ConfigDigest                                  string
	TenantDigest                                  string
	ActorSelector                                 string
	Parents                                       string
	SourceLineage                                 string
	InputDigest                                   string
	ModelRevision, ConfigRevision, TenantRevision uint64
}

func (knowledgeTemplateInput) TableName() string { return "knowledge_enrichment_template_inputs" }

type knowledgeTemplateResult struct {
	ID                    string `gorm:"primaryKey"`
	InputID, OutputDigest string
}

func (knowledgeTemplateResult) TableName() string { return "knowledge_enrichment_template_results" }

// KnowledgeEnrichmentTemplateInputSnapshot represents knowledge enrichment template input snapshot repository data.
type KnowledgeEnrichmentTemplateInputSnapshot struct {
	ID, ModelID, Purpose string
	Tenant               *types.Tenant
	Parents              []*types.ChunkOutputInputSnapshot
	Lineage              *types.SourceLineage
}

// KnowledgeEnrichmentTemplateModelResult represents knowledge enrichment template model result repository data.
type KnowledgeEnrichmentTemplateModelResult struct{ inputID, resultID, content string }

func validKnowledgeTemplate(purpose, content string) error {
	if (purpose != "summary" && purpose != "question") || strings.TrimSpace(content) == "" ||
		!utf8.ValidString(content) ||
		len(content) > 32768 {
		return ErrKnowledgeInputChanged
	}
	return nil
}

func (s *KnowledgeInputProvenanceStore) templateAuthority(tx *gorm.DB) *KnowledgeInputProvenanceStore {
	if s.displayAuthorityFactory != nil {
		return s.displayAuthorityFactory(tx)
	}
	return &KnowledgeInputProvenanceStore{
		db:              tx,
		actorGroups:     s.actorGroups,
		actorShares:     s.actorShares,
		messageVerifier: s.messageVerifier,
	}
}

func (s *KnowledgeInputProvenanceStore) authorizeKnowledgeTemplateWrite(
	ctx context.Context,
	kb *types.KnowledgeBase,
) (context.Context, error) {
	fresh, err := s.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	if err != nil {
		return ctx, err
	}
	caller := types.CallerFromContext(fresh)
	if caller.TenantID != kb.TenantID || !caller.Role.HasPermission(types.TenantRoleAdmin) {
		return ctx, access.ErrForbidden
	}
	return fresh, nil
}

// ReadKnowledgeTemplateConfiguration reads knowledge template configuration.
func (s *KnowledgeInputProvenanceStore) ReadKnowledgeTemplateConfiguration(
	ctx context.Context,
	tenant uint64,
	kbID string,
) (*types.KnowledgeBase, *types.SourceLineage, error) {
	var kb types.KnowledgeBase
	if err := s.db.WithContext(ctx).Where("id = ? AND tenant_id = ?", kbID, tenant).Take(&kb).Error; err != nil {
		return nil, nil, err
	}
	lineage, err := readKnowledgeConfigurationOrigin(s.db.WithContext(ctx), &kb)
	return &kb, lineage, err
}

// WriteIndependentKnowledgeEnrichmentTemplate writes independent knowledge enrichment template.
func (s *KnowledgeInputProvenanceStore) WriteIndependentKnowledgeEnrichmentTemplate(
	ctx context.Context,
	tenant uint64,
	kbID, purpose, content string,
) (*types.KnowledgeBase, error) {
	if err := validKnowledgeTemplate(purpose, content); err != nil {
		return nil, err
	}
	var result *types.KnowledgeBase
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", kbID, tenant).Take(&kb).Error; err != nil {
			return err
		}
		authority := s.templateAuthority(tx)
		if authority == nil {
			return types.ErrSourceLineageUnknown
		}
		if _, err := authority.authorizeKnowledgeTemplateWrite(ctx, &kb); err != nil {
			return err
		}
		previous, err := readKnowledgeConfigurationOrigin(tx, &kb)
		if err != nil {
			return err
		}
		if kb.EnrichmentTemplates == nil {
			kb.EnrichmentTemplates = &types.KnowledgeEnrichmentTemplates{}
		}
		if err := kb.EnrichmentTemplates.Set(purpose, content); err != nil {
			return err
		}
		if err := tx.Model(&types.KnowledgeBase{}).
			Where("id = ? AND tenant_id = ?", kbID, tenant).
			Update("enrichment_templates", kb.EnrichmentTemplates).
			Error; err != nil {
			return err
		}
		if err := tx.Where("id = ? AND tenant_id = ?", kbID, tenant).Take(&kb).Error; err != nil {
			return err
		}
		if err := admitKnowledgeConfigurationOrigin(tx, &kb, knowledgeInputOrigin{previous, "human"}); err != nil {
			return err
		}
		result = &kb
		return nil
	}, knowledgeDisplayWriteOptions(s.db))
	return result, err
}

// WriteCodeKnowledgeEnrichmentTemplate writes code knowledge enrichment template.
// The code boundary accepts an exact new literal and a known configuration;
// it cannot adopt old unknown configuration fields or client origin markers.
func (s *KnowledgeInputProvenanceStore) WriteCodeKnowledgeEnrichmentTemplate(
	ctx context.Context,
	tenant uint64,
	kbID, purpose, literal string,
) (*types.KnowledgeBase, error) {
	if err := validKnowledgeTemplate(purpose, literal); err != nil {
		return nil, err
	}
	var result *types.KnowledgeBase
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", kbID, tenant).Take(&kb).Error; err != nil {
			return err
		}
		lineage, err := readKnowledgeConfigurationOrigin(tx, &kb)
		if err != nil {
			return err
		}
		if kb.EnrichmentTemplates == nil {
			kb.EnrichmentTemplates = &types.KnowledgeEnrichmentTemplates{}
		}
		if err := kb.EnrichmentTemplates.Set(purpose, literal); err != nil {
			return err
		}
		if err := tx.Model(&types.KnowledgeBase{}).
			Where("id = ? AND tenant_id = ?", kbID, tenant).
			Update("enrichment_templates", kb.EnrichmentTemplates).
			Error; err != nil {
			return err
		}
		if err := tx.Where("id = ? AND tenant_id = ?", kbID, tenant).Take(&kb).Error; err != nil {
			return err
		}
		if err := admitKnowledgeConfigurationOrigin(tx, &kb, knowledgeInputOrigin{lineage, "human"}); err != nil {
			return err
		}
		result = &kb
		return nil
	}, knowledgeDisplayWriteOptions(s.db))
	return result, err
}

// CaptureKnowledgeEnrichmentTemplateInputs captures knowledge enrichment template inputs.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeEnrichmentTemplateInputs(
	ctx context.Context,
	tenant uint64,
	kbID, purpose string,
	parents []*types.ChunkOutputInputSnapshot,
) (*KnowledgeEnrichmentTemplateInputSnapshot, error) {
	if len(parents) == 0 || len(parents) > 32 || (purpose != "summary" && purpose != "question") {
		return nil, types.ErrSourceLineageUnknown
	}
	kb, configLineage, err := s.ReadKnowledgeTemplateConfiguration(ctx, tenant, kbID)
	if err != nil {
		return nil, err
	}
	ctx, err = s.authorizeKnowledgeTemplateWrite(ctx, kb)
	if err != nil {
		return nil, err
	}
	actor, err := captureKnowledgeActorSelector(ctx, "")
	if err != nil {
		return nil, err
	}
	lineage, err := types.UnionSourceLineage(configLineage)
	if err != nil {
		return nil, err
	}
	actual := []templateParentInput{}
	for _, parent := range parents {
		if err := s.RecheckChunkOutputInputs(ctx, parent); err != nil {
			return nil, err
		}
		if err := s.RecheckKnowledgeBuildInputs(ctx, parent.Proof); err != nil {
			return nil, err
		}
		lineage, err = types.UnionSourceLineage(lineage, parent.Proof.SourceLineage)
		if err != nil {
			return nil, err
		}
		actual = append(actual, templateParentInput{parent.Knowledge, parent.Chunks, parent.Proof, parent.AllChunks})
	}
	var model types.Model
	if err := s.db.WithContext(ctx).
		Where("id = ? AND (tenant_id = ? OR is_builtin = ?)", kb.SummaryModelID, tenant, true).
		Take(&model).
		Error; err != nil {
		return nil, err
	}
	modelDigest, err := KnowledgeInputDigest(&model)
	if err != nil {
		return nil, err
	}
	modelRevision, err := knowledgeEnrichmentRevision(s.db, "model", model.ID)
	if err != nil {
		return nil, err
	}
	configDigest, err := knowledgeConfigurationOriginDigest(kb)
	if err != nil {
		return nil, err
	}
	configRevision, err := knowledgeConfigurationControlRevision(s.db, kbID)
	if err != nil {
		return nil, err
	}
	var tenantRow types.Tenant
	if err := s.db.WithContext(ctx).Where("id = ?", tenant).Take(&tenantRow).Error; err != nil {
		return nil, err
	}
	tenantDigest, err := knowledgeProducerTenantDigest(&tenantRow)
	if err != nil {
		return nil, err
	}
	tenantRevision, err := knowledgeEnrichmentRevision(s.db, "tenant", fmt.Sprint(tenant))
	if err != nil {
		return nil, err
	}
	parentJSON, err := json.Marshal(actual)
	if err != nil {
		return nil, err
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return nil, err
	}
	lineageJSON, err := json.Marshal(lineage)
	if err != nil {
		return nil, err
	}
	inputDigest, err := KnowledgeInputDigest(actual)
	if err != nil {
		return nil, err
	}
	row := knowledgeTemplateInput{
		ID:              uuid.NewString(),
		TenantID:        tenant,
		KnowledgeBaseID: kbID,
		Purpose:         purpose,
		ModelID:         model.ID,
		ModelDigest:     modelDigest,
		ModelRevision:   modelRevision,
		ConfigDigest:    configDigest,
		ConfigRevision:  configRevision,
		TenantDigest:    tenantDigest,
		TenantRevision:  tenantRevision,
		ActorSelector:   string(actorJSON),
		Parents:         string(parentJSON),
		SourceLineage:   string(lineageJSON),
		InputDigest:     inputDigest,
	}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil {
		return nil, err
	}
	return &KnowledgeEnrichmentTemplateInputSnapshot{row.ID, row.ModelID, purpose, &tenantRow, parents, lineage}, nil
}

func (s *KnowledgeInputProvenanceStore) recheckTemplateInputInTx(
	ctx context.Context,
	tx *gorm.DB,
	id string,
) (*knowledgeTemplateInput, error) {
	var row knowledgeTemplateInput
	if err := tx.Where("id = ?", id).Take(&row).Error; err != nil {
		return nil, err
	}
	var actor knowledgeBuildActorSelector
	if json.Unmarshal([]byte(row.ActorSelector), &actor) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	authority := s.templateAuthority(tx)
	if authority == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	fresh, err := authority.restoreCurrentKnowledgeActor(ctx, actor, row.TenantID)
	if err != nil {
		return nil, err
	}
	a, _ := types.PrincipalFromContext(ctx)
	b, _ := types.PrincipalFromContext(fresh)
	if a != b || types.CallerFromContext(ctx) != types.CallerFromContext(fresh) {
		return nil, ErrKnowledgeInputChanged
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", row.KnowledgeBaseID, row.TenantID).Take(&kb).Error; err != nil {
		return nil, err
	}
	if _, err := authority.authorizeKnowledgeTemplateWrite(fresh, &kb); err != nil {
		return nil, err
	}
	digest, err := knowledgeConfigurationOriginDigest(&kb)
	if err != nil {
		return nil, err
	}
	rev, err := knowledgeConfigurationControlRevision(tx, kb.ID)
	if err != nil {
		return nil, err
	}
	if digest != row.ConfigDigest || rev != row.ConfigRevision {
		return nil, ErrKnowledgeInputChanged
	}
	if _, err := readKnowledgeConfigurationOrigin(tx, &kb); err != nil {
		return nil, err
	}
	var model types.Model
	if err := tx.Where("id = ? AND (tenant_id = ? OR is_builtin = ?)", row.ModelID, row.TenantID, true).
		Take(&model).
		Error; err != nil {
		return nil, err
	}
	digest, err = KnowledgeInputDigest(&model)
	if err != nil {
		return nil, err
	}
	rev, err = knowledgeEnrichmentRevision(tx, "model", model.ID)
	if err != nil {
		return nil, err
	}
	if digest != row.ModelDigest || rev != row.ModelRevision {
		return nil, ErrKnowledgeInputChanged
	}
	var tenant types.Tenant
	if err := tx.Where("id = ?", row.TenantID).Take(&tenant).Error; err != nil {
		return nil, err
	}
	digest, err = knowledgeProducerTenantDigest(&tenant)
	if err != nil {
		return nil, err
	}
	rev, err = knowledgeEnrichmentRevision(tx, "tenant", fmt.Sprint(tenant.ID))
	if err != nil {
		return nil, err
	}
	if digest != row.TenantDigest || rev != row.TenantRevision {
		return nil, ErrKnowledgeInputChanged
	}
	var parents []templateParentInput
	if json.Unmarshal([]byte(row.Parents), &parents) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	for _, parent := range parents {
		p := parent.Proof
		if p == nil || parent.Knowledge == nil {
			return nil, ErrKnowledgeInputChanged
		}
		_, err := s.captureKnowledgeBuildSnapshotInTx(
			tx,
			types.KnowledgeBuildInputCaptureRequest{
				TenantID:        p.TenantID,
				KnowledgeBaseID: p.KnowledgeBaseID,
				KnowledgeID:     p.KnowledgeID,
				ChunkIDs:        p.ChunkIDs,
				Expected:        p,
			},
			true,
		)
		if err != nil {
			return nil, err
		}
		if parent.AllChunks {
			var ids []string
			if err := tx.Model(&types.Chunk{}).
				Where(
					"tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?",
					p.TenantID,
					p.KnowledgeBaseID,
					p.KnowledgeID,
				).
				Order("id ASC").
				Pluck("id", &ids).
				Error; err != nil {
				return nil, err
			}
			if !reflect.DeepEqual(ids, p.ChunkIDs) {
				return nil, ErrKnowledgeInputChanged
			}
		}
	}
	return &row, ctx.Err()
}

// RecheckKnowledgeEnrichmentTemplateInputs rechecks knowledge enrichment template inputs.
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeEnrichmentTemplateInputs(ctx context.Context, id string) error {
	var row knowledgeTemplateInput
	if err := s.db.WithContext(ctx).Where("id = ?", id).Take(&row).Error; err != nil {
		return err
	}
	var parents []templateParentInput
	if json.Unmarshal([]byte(row.Parents), &parents) != nil {
		return ErrKnowledgeInputChanged
	}
	for _, p := range parents {
		if err := s.RecheckChunkOutputInputs(
			ctx,
			&types.ChunkOutputInputSnapshot{
				Knowledge: p.Knowledge,
				Chunks:    p.Chunks,
				Proof:     p.Proof,
				AllChunks: p.AllChunks,
			},
		); err != nil {
			return err
		}
		if err := s.RecheckKnowledgeBuildInputs(ctx, p.Proof); err != nil {
			return err
		}
	}
	return s.db.WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			_, err := s.recheckTemplateInputInTx(
				ctx,
				tx,
				id,
			)
			return err
		}, knowledgeDisplayWriteOptions(
			s.db,
		))
}

// CaptureKnowledgeTemplateModelResult captures knowledge template model result.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeTemplateModelResult(
	ctx context.Context,
	id, content string,
) (*KnowledgeEnrichmentTemplateModelResult, error) {
	if err := s.RecheckKnowledgeEnrichmentTemplateInputs(ctx, id); err != nil {
		return nil, err
	}
	var input knowledgeTemplateInput
	if err := s.db.WithContext(ctx).Where("id = ?", id).Take(&input).Error; err != nil {
		return nil, err
	}
	if err := validKnowledgeTemplate(input.Purpose, content); err != nil {
		return nil, err
	}
	digest, err := KnowledgeInputDigest(content)
	if err != nil {
		return nil, err
	}
	result := knowledgeTemplateResult{uuid.NewString(), id, digest}
	if err := s.db.WithContext(ctx).Create(&result).Error; err != nil {
		return nil, err
	}
	return &KnowledgeEnrichmentTemplateModelResult{id, result.ID, content}, nil
}

// CommitDerivedKnowledgeEnrichmentTemplate commits derived knowledge enrichment template.
func (s *KnowledgeInputProvenanceStore) CommitDerivedKnowledgeEnrichmentTemplate(
	ctx context.Context,
	result *KnowledgeEnrichmentTemplateModelResult,
) (*types.KnowledgeBase, error) {
	if result == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := s.RecheckKnowledgeEnrichmentTemplateInputs(ctx, result.inputID); err != nil {
		return nil, err
	}
	var kbResult *types.KnowledgeBase
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, err := s.recheckTemplateInputInTx(ctx, tx, result.inputID)
		if err != nil {
			return err
		}
		var output knowledgeTemplateResult
		if err := tx.Where("id = ? AND input_id = ?", result.resultID, result.inputID).Take(&output).Error; err != nil {
			return err
		}
		digest, err := KnowledgeInputDigest(result.content)
		if err != nil {
			return err
		}
		if digest != output.OutputDigest {
			return ErrKnowledgeInputChanged
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", row.KnowledgeBaseID, row.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		previous, err := readKnowledgeConfigurationOrigin(tx, &kb)
		if err != nil {
			return err
		}
		var inputs types.SourceLineage
		if json.Unmarshal([]byte(row.SourceLineage), &inputs) != nil {
			return ErrKnowledgeInputChanged
		}
		lineage, err := types.UnionSourceLineage(previous, &inputs)
		if err != nil {
			return err
		}
		if kb.EnrichmentTemplates == nil {
			kb.EnrichmentTemplates = &types.KnowledgeEnrichmentTemplates{}
		}
		if err := kb.EnrichmentTemplates.Set(row.Purpose, result.content); err != nil {
			return err
		}
		if err := tx.Model(&types.KnowledgeBase{}).
			Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).
			Update("enrichment_templates", kb.EnrichmentTemplates).
			Error; err != nil {
			return err
		}
		if err := tx.Where("id = ?", kb.ID).Take(&kb).Error; err != nil {
			return err
		}
		if err := admitKnowledgeConfigurationOrigin(tx, &kb, knowledgeInputOrigin{lineage, "derived"}); err != nil {
			return err
		}
		kbResult = &kb
		return nil
	}, knowledgeDisplayWriteOptions(s.db))
	return kbResult, err
}

// AuthorizeKnowledgeTemplateConfigurationEdit authorizes knowledge template configuration edit.
func (s *KnowledgeInputProvenanceStore) AuthorizeKnowledgeTemplateConfigurationEdit(
	ctx context.Context,
	kb *types.KnowledgeBase,
) (context.Context, error) {
	return s.authorizeKnowledgeTemplateWrite(ctx, kb)
}

// ReadKnowledgeEnrichmentTemplateInputs reads knowledge enrichment template inputs.
func (s *KnowledgeInputProvenanceStore) ReadKnowledgeEnrichmentTemplateInputs(
	ctx context.Context,
	id string,
) (*KnowledgeEnrichmentTemplateInputSnapshot, error) {
	if err := s.RecheckKnowledgeEnrichmentTemplateInputs(ctx, id); err != nil {
		return nil, err
	}
	var row knowledgeTemplateInput
	if err := s.db.WithContext(ctx).Where("id = ?", id).Take(&row).Error; err != nil {
		return nil, err
	}
	var saved []templateParentInput
	if json.Unmarshal([]byte(row.Parents), &saved) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	parents := []*types.ChunkOutputInputSnapshot{}
	for _, p := range saved {
		parents = append(
			parents,
			&types.ChunkOutputInputSnapshot{
				Knowledge: p.Knowledge,
				Chunks:    p.Chunks,
				Proof:     p.Proof,
				AllChunks: p.AllChunks,
			},
		)
	}
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(row.SourceLineage), &lineage) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	var tenant types.Tenant
	if err := s.db.WithContext(ctx).Where("id = ?", row.TenantID).Take(&tenant).Error; err != nil {
		return nil, err
	}
	return &KnowledgeEnrichmentTemplateInputSnapshot{row.ID, row.ModelID, row.Purpose, &tenant, parents, &lineage}, nil
}

// KnowledgeTemplateConfigurationStamp returns the knowledge template configuration stamp result.
func (s *KnowledgeInputProvenanceStore) KnowledgeTemplateConfigurationStamp(
	ctx context.Context,
	tenant uint64,
	id string,
) (uint64, string, error) {
	var kb types.KnowledgeBase
	if err := s.db.WithContext(ctx).Where("id = ? AND tenant_id = ?", id, tenant).Take(&kb).Error; err != nil {
		return 0, "", err
	}
	revision, err := knowledgeConfigurationControlRevision(s.db.WithContext(ctx), id)
	if err != nil {
		return 0, "", err
	}
	digest, err := knowledgeConfigurationOriginDigest(&kb)
	return revision, digest, err
}
