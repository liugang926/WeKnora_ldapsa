package repository

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func readOriginalUploadActual(
	tx *gorm.DB,
	original *types.KnowledgeBuildInputProof,
) (*nextcloudOriginalUploadActual, error) {
	if original == nil {
		return nil, ErrKnowledgeInputChanged
	}
	var receipt nextcloudOriginalUploadInput
	if err := tx.Where("knowledge_id = ? AND tenant_id = ? AND knowledge_base_id = ?", original.KnowledgeID, original.TenantID, original.KnowledgeBaseID).
		Take(&receipt).
		Error; err != nil {
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
	return &actual, nil
}

func groupingTagProofs(tx *gorm.DB, tag *types.KnowledgeTag) (*types.SourceLineage, error) {
	lineage, _ := types.NewCompleteSourceLineage()
	for _, field := range []string{"name", "color", "sort_order"} {
		proof, err := readKnowledgeDisplayOrigin(
			tx,
			"tag-"+field,
			tag.ID,
			tag.TenantID,
			tag.KnowledgeBaseID,
			tagDisplayFieldValue(tag, field),
		)
		if err != nil {
			return nil, err
		}
		lineage, err = types.UnionSourceLineage(lineage, proof.SourceLineage)
		if err != nil {
			return nil, err
		}
	}
	return lineage, nil
}

func (s *KnowledgeInputProvenanceStore) verifyNextcloudGroupingDisplayInput(
	tx *gorm.DB,
	request knowledgeDisplayRequest,
) (*types.SourceLineage, error) {
	if request.Original == nil || request.OriginalUpload != request.Original.KnowledgeID ||
		request.DataSourceName == nil {
		return nil, ErrKnowledgeInputChanged
	}
	actual, err := readOriginalUploadActual(tx, request.Original)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(actual.Name, request.DataSourceName) {
		return nil, ErrKnowledgeInputChanged
	}
	if request.Kind == "datasource-name-tag" {
		if request.KB != request.Original.KnowledgeBaseID || request.Fields["name"] != actual.Name.Name ||
			request.Fields["color"] != "" ||
			request.Fields["sort_order"] != 0 {
			return nil, ErrKnowledgeInputChanged
		}
		// No source document body is read to compose these fields: exact original
		// configuration name + two code literals. The private source lease is a
		// scoped write purpose, and cannot supply extra tag-source read authority.
		return types.NewCompleteSourceLineage()
	}
	if request.Kind != "original-upload-tags" {
		return nil, ErrKnowledgeInputChanged
	}
	groupID, ok := request.Fields["grouping_tag_id"].(string)
	if !ok || groupID == "" {
		return nil, ErrKnowledgeInputChanged
	}
	wanted := normalizedKnowledgeDisplayTagIDs(append(append([]string{}, actual.InitialTagIDs...), groupID))
	if !reflect.DeepEqual(wanted, request.Assignments[request.Original.KnowledgeID]) {
		return nil, ErrKnowledgeInputChanged
	}
	lineage, _ := types.NewCompleteSourceLineage()
	for _, id := range wanted {
		var tag types.KnowledgeTag
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", id, request.Original.TenantID, request.Original.KnowledgeBaseID).
			Take(&tag).
			Error; err != nil {
			return nil, err
		}
		if id == groupID && tag.Name != actual.Name.Name {
			return nil, ErrKnowledgeInputChanged
		}
		proof, err := groupingTagProofs(tx, &tag)
		if err != nil {
			return nil, err
		}
		lineage, err = types.UnionSourceLineage(lineage, proof)
		if err != nil {
			return nil, err
		}
	}
	return lineage, nil
}

// AttachNextcloudOriginalUploadTags runs after Stage and actual original bytes
// admission. It consumes immutable initial selectors and the original name
// receipt, never a current data-source text or a client actor/lineage envelope.
func (s *KnowledgeInputProvenanceStore) AttachNextcloudOriginalUploadTags(
	ctx context.Context,
	original *types.KnowledgeBuildInputProof,
) error {
	if err := s.RecheckKnowledgeBuildInputs(ctx, original); err != nil {
		return err
	}
	actual, err := readOriginalUploadActual(s.db.WithContext(ctx), original)
	if err != nil {
		return err
	}
	if err := s.recheckDataSourceNameInput(s.db.WithContext(ctx), actual.Name); err != nil {
		return err
	}
	var kb types.KnowledgeBase
	if err := s.db.WithContext(ctx).
		Where("id = ? AND tenant_id = ?", original.KnowledgeBaseID, original.TenantID).
		Take(&kb).
		Error; err != nil {
		return err
	}
	if _, err := s.authorizeOriginalKnowledgeProducerWrite(ctx, &kb, original); err != nil {
		return err
	}
	var group types.KnowledgeTag
	err = s.db.WithContext(ctx).
		Where("tenant_id = ? AND knowledge_base_id = ? AND name = ?", kb.TenantID, kb.ID, actual.Name.Name).
		Take(&group).
		Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		group = types.KnowledgeTag{
			ID:              uuid.NewString(),
			TenantID:        kb.TenantID,
			KnowledgeBaseID: kb.ID,
			Name:            actual.Name.Name,
			Color:           "",
			SortOrder:       0,
			CreatedAt:       time.Now().UTC(),
			UpdatedAt:       time.Now().UTC(),
		}
		request := knowledgeDisplayRequest{
			Kind:           "datasource-name-tag",
			KB:             kb.ID,
			Fields:         map[string]any{"name": group.Name, "color": "", "sort_order": 0},
			Original:       cloneKnowledgeInputProof(original),
			OriginalUpload: original.KnowledgeID,
			DataSourceName: actual.Name,
		}
		writeCtx := context.WithValue(ctx, knowledgeDisplayRequestKey{}, request)
		if err := writeKnowledgeTagWithDisplayInput(
			writeCtx,
			s.db,
			&group,
			true,
			s.displayAuthorityFactory,
		); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	ids := normalizedKnowledgeDisplayTagIDs(append(append([]string{}, actual.InitialTagIDs...), group.ID))
	request := knowledgeDisplayRequest{
		Kind:           "original-upload-tags",
		KB:             kb.ID,
		Fields:         map[string]any{"grouping_tag_id": group.ID},
		Assignments:    map[string][]string{original.KnowledgeID: ids},
		Original:       cloneKnowledgeInputProof(original),
		OriginalUpload: original.KnowledgeID,
		DataSourceName: actual.Name,
	}
	lineage, err := s.verifyNextcloudGroupingDisplayInput(s.db.WithContext(ctx), request)
	if err != nil {
		return err
	}
	if len(lineage.Sources) > 0 {
		v, ok := s.originalInputSources.(originalUploadConfigurationSourceVerifier)
		if !ok {
			return types.ErrSourceLineageUnknown
		}
		if err := v.CheckKnowledgeConfigurationSources(
			ctx,
			original.TenantID,
			original.KnowledgeBaseID,
			lineage,
		); err != nil {
			return err
		}
	}
	if err := s.RecheckKnowledgeBuildInputs(ctx, original); err != nil {
		return err
	}
	repo := &knowledgeRepository{db: s.db, displayAuthorityFactory: s.displayAuthorityFactory}
	return repo.SetKnowledgeTags(
		context.WithValue(ctx, knowledgeDisplayRequestKey{}, request),
		original.KnowledgeID,
		ids,
	)
}

func (s *KnowledgeInputProvenanceStore) originalUploadAutoTagNameInTx(
	tx *gorm.DB,
	original *types.KnowledgeBuildInputProof,
) (*types.DataSourceNameInputProof, error) {
	if !tx.Migrator().HasTable("nextcloud_original_upload_inputs") {
		return nil, nil
	}
	var count int64
	if err := tx.Table("nextcloud_original_upload_inputs").
		Where("knowledge_id = ?", original.KnowledgeID).
		Count(&count).
		Error; err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	actual, err := readOriginalUploadActual(tx, original)
	if err != nil {
		return nil, err
	}
	if err := s.recheckDataSourceNameInput(tx, actual.Name); err != nil {
		return nil, err
	}
	return actual.Name, nil
}
