package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrKnowledgeInputChanged = errors.New("original knowledge input changed or is unknown")

type knowledgeInputAdmission struct {
	AdmissionID     string `gorm:"primaryKey"`
	TenantID        uint64
	KnowledgeBaseID string
	KnowledgeID     string
	BuildGeneration uint64
	OriginKind      string
	InputDigest     string
	RootDigest      string
	ConfigDigest    string
	ConfigRevision  uint64
	SourceLineage   string
}

func (knowledgeInputAdmission) TableName() string { return "knowledge_input_admissions" }

type knowledgeInputMaterialReceipt struct {
	RowKind     string `gorm:"primaryKey"`
	RowID       string `gorm:"primaryKey"`
	AdmissionID string
	RowRevision uint64
	BodyDigest  string
}

func (knowledgeInputMaterialReceipt) TableName() string { return "knowledge_input_material_receipts" }

// KnowledgeInputProvenanceStore owns private, original admission and material
// receipts. Missing old/derived facts are unknown; there is no inferred origin.
type KnowledgeInputProvenanceStore struct{ db *gorm.DB }

func NewKnowledgeInputProvenanceStore(db *gorm.DB) *KnowledgeInputProvenanceStore {
	return &KnowledgeInputProvenanceStore{db}
}

func KnowledgeInputArtifactMatches(proof *types.KnowledgeBuildInputProof, input any) error {
	if proof == nil || proof.Version != 1 || proof.SourceLineage.RequireComplete() != nil {
		return types.ErrSourceLineageUnknown
	}
	digest, err := KnowledgeInputDigest(input)
	if err != nil {
		return err
	}
	if digest != proof.InputDigest {
		return ErrKnowledgeInputChanged
	}
	return nil
}

// UpdateKnowledgeWithInput admits an explicit new human/derived revision and
// updates its document atomically. A generic Save cannot call this capability.
func (s *KnowledgeInputProvenanceStore) UpdateKnowledgeWithInput(ctx context.Context, k *types.Knowledge, input any) (*types.KnowledgeBuildInputProof, error) {
	origin, ok := ctx.Value(knowledgeInputOriginKey{}).(knowledgeInputOrigin)
	if !ok || origin.lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var proof *types.KnowledgeBuildInputProof
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var old types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", k.ID, k.TenantID, k.KnowledgeBaseID).Take(&old).Error; err != nil {
			return err
		}
		controlled, err := ragKnowledgeIsControlled(tx, &old)
		if err != nil {
			return err
		}
		if controlled {
			return ErrKnowledgeInputChanged
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", k.KnowledgeBaseID, k.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		if err := tx.Omit(omitFieldsOnUpdate...).Save(k).Error; err != nil {
			return err
		}
		proof, err = s.admitInTx(ctx, tx, k, &kb, input, origin)
		return err
	})
	return proof, err
}

func KnowledgeInputDigest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var canonical any
	if err := decoder.Decode(&canonical); err != nil {
		return "", err
	}
	data, err = json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

// Only original document inputs belong here. Generated display fields are
// bound separately to a material receipt, so model outputs inherit the root.
func knowledgeRootDigest(k *types.Knowledge) (string, error) {
	metadata := k.Metadata
	if k.Channel == types.ConnectorTypeNextcloud {
		var fields map[string]json.RawMessage
		if json.Unmarshal(metadata, &fields) != nil {
			return "", ErrKnowledgeInputChanged
		}
		// Publication bookkeeping does not alter the original build input. The
		// immutable desired ETag and full source tuple remain in the digest.
		delete(fields, "nextcloud_etag")
		encoded, err := json.Marshal(fields)
		if err != nil {
			return "", err
		}
		metadata = encoded
	}
	return KnowledgeInputDigest(struct {
		Tenant                                                                uint64
		KB, ID, Type, Source, Channel, FileName, FileType, FileHash, FilePath string
		Metadata, CustomMetadata                                              types.JSON
	}{
		k.TenantID, k.KnowledgeBaseID, k.ID, k.Type, k.Source, k.Channel, k.FileName, k.FileType, k.FileHash, k.FilePath, metadata, k.CustomMetadata})
}
func knowledgeMaterialDigest(k *types.Knowledge) (string, error) {
	return KnowledgeInputDigest(struct {
		Root                       string
		Title, Description, Folder string
		Profile                    *types.KnowledgeProfile
	}{mustKnowledgeRootDigest(k), k.Title, k.Description, k.FolderPath, k.Profile})
}
func mustKnowledgeRootDigest(k *types.Knowledge) string {
	digest, _ := knowledgeRootDigest(k)
	return digest
}

func knowledgeConfigDigest(kb *types.KnowledgeBase, k *types.Knowledge) (string, error) {
	overrides, err := k.ProcessOverrides()
	if err != nil {
		return "", err
	}
	// The actual typed parser/model controls, plus their document overrides.
	// Computed ranking scores and generated KB descriptions are not authorities.
	return KnowledgeInputDigest(struct {
		Chunking           types.ChunkingConfig
		Image              types.ImageProcessingConfig
		Embedding, Summary string
		VLM                types.VLMConfig
		ASR                types.ASRConfig
		Extract            *types.ExtractConfig
		FAQ                *types.FAQConfig
		Questions          *types.QuestionGenerationConfig
		AutoTag            *types.AutoTagConfig
		Profile            *types.KnowledgeBaseProfileConfig
		Wiki               *types.WikiConfig
		Indexing           types.IndexingStrategy
		Overrides          *types.KnowledgeProcessOverrides
	}{
		kb.ChunkingConfig, kb.ImageProcessingConfig, kb.EmbeddingModelID, kb.SummaryModelID, kb.VLMConfig, kb.ASRConfig, kb.ExtractConfig, kb.FAQConfig, kb.QuestionGenerationConfig, kb.AutoTagConfig, kb.ProfileConfig, kb.WikiConfig, kb.IndexingStrategy, overrides})
}
func knowledgeChunkDigest(c *types.Chunk) (string, error) {
	// Persistence timestamps, vector scores and indexing bookkeeping are not
	// model material. Text, metadata, image bodies and relationship controls are.
	return KnowledgeInputDigest(struct {
		Tenant                                                                   uint64
		KB, KG, ID, Content, Source, Header, Type, Parent, Pre, Next, Image, Tag string
		Metadata, Relations, Indirect                                            types.JSON
		Revision, Index, Start, End                                              int
	}{
		c.TenantID, c.KnowledgeBaseID, c.KnowledgeID, c.ID, c.Content, c.SourceContent, c.ContextHeader, c.ChunkType, c.ParentChunkID, c.PreChunkID, c.NextChunkID, c.ImageInfo, c.TagID, c.Metadata, c.RelationChunks, c.IndirectRelationChunks, c.ContentRevision, c.ChunkIndex, c.StartAt, c.EndAt})
}
func inputRowRevision(tx *gorm.DB, kind, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table("knowledge_input_row_versions").Where("row_kind = ? AND row_id = ?", kind, id).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

// CreateKnowledgeWithInput inserts the document and its origin atomically.
// No origin is recorded unless a trusted entry point explicitly admitted it.
func (s *KnowledgeInputProvenanceStore) CreateKnowledgeWithInput(ctx context.Context, k *types.Knowledge, input any) (*types.KnowledgeBuildInputProof, error) {
	if s == nil || s.db == nil || k == nil || k.TenantID == 0 || k.KnowledgeBaseID == "" {
		return nil, ErrKnowledgeInputChanged
	}
	origin, ok := ctx.Value(knowledgeInputOriginKey{}).(knowledgeInputOrigin)
	if !ok || origin.lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if k.ID == "" {
		k.ID = uuid.NewString()
	}
	var proof *types.KnowledgeBuildInputProof
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", k.KnowledgeBaseID, k.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		controlled, err := ragKnowledgeIsControlled(tx, k)
		if err != nil {
			return err
		}
		if controlled {
			return ErrKnowledgeInputChanged
		}
		if err := tx.Create(k).Error; err != nil {
			return err
		}
		proof, err = s.admitInTx(ctx, tx, k, &kb, input, origin)
		return err
	})
	return proof, err
}
func (s *KnowledgeInputProvenanceStore) admitInTx(ctx context.Context, tx *gorm.DB, k *types.Knowledge, kb *types.KnowledgeBase, input any, origin knowledgeInputOrigin) (*types.KnowledgeBuildInputProof, error) {
	inputDigest, err := KnowledgeInputDigest(input)
	if err != nil {
		return nil, err
	}
	rootDigest, err := knowledgeRootDigest(k)
	if err != nil {
		return nil, err
	}
	configDigest, err := knowledgeConfigDigest(kb, k)
	if err != nil {
		return nil, err
	}
	generation, err := inputRowRevision(tx, "knowledge", k.ID)
	if err != nil {
		return nil, err
	}
	if origin.kind == "nextcloud" {
		lease, _, err := NextcloudBuildInputLease(ctx)
		if err != nil {
			return nil, err
		}
		generation = uint64(lease.Epoch)
	}
	configRevision, err := inputRowRevision(tx, "config", kb.ID)
	if err != nil {
		return nil, err
	}
	lineage, err := types.UnionSourceLineage(origin.lineage)
	if err != nil {
		return nil, err
	}
	if origin.kind != "nextcloud" {
		if err := verifyKnowledgeInheritedSources(tx, lineage); err != nil {
			return nil, err
		}
	}
	encoded, err := json.Marshal(lineage)
	if err != nil {
		return nil, err
	}
	a := knowledgeInputAdmission{uuid.NewString(), k.TenantID, k.KnowledgeBaseID, k.ID, generation, origin.kind, inputDigest, rootDigest, configDigest, configRevision, string(encoded)}
	if err := tx.Create(&a).Error; err != nil {
		return nil, err
	}
	if err := sealKnowledgeInputMaterial(tx, a.AdmissionID, k, nil); err != nil {
		return nil, err
	}
	return &types.KnowledgeBuildInputProof{Version: 1, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, AdmissionID: a.AdmissionID, BuildGeneration: generation, InputDigest: inputDigest, ConfigDigest: configDigest, SourceLineage: lineage}, nil
}
func sealKnowledgeInputMaterial(tx *gorm.DB, admissionID string, k *types.Knowledge, c *types.Chunk) error {
	kind, id := "knowledge", k.ID
	var digest string
	var err error
	if c != nil {
		kind, id = "chunk", c.ID
		digest, err = knowledgeChunkDigest(c)
	} else {
		digest, err = knowledgeMaterialDigest(k)
	}
	if err != nil {
		return err
	}
	revision, err := inputRowRevision(tx, kind, id)
	if err != nil {
		return err
	}
	receipt := knowledgeInputMaterialReceipt{kind, id, admissionID, revision, digest}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "row_kind"}, {Name: "row_id"}}, DoUpdates: clause.AssignmentColumns([]string{"admission_id", "row_revision", "body_digest"})}).Create(&receipt).Error
}
func verifyKnowledgeInputMaterial(tx *gorm.DB, a *knowledgeInputAdmission, k *types.Knowledge, c *types.Chunk) error {
	kind, id := "knowledge", k.ID
	var digest string
	var err error
	if c != nil {
		kind, id = "chunk", c.ID
		digest, err = knowledgeChunkDigest(c)
	} else {
		digest, err = knowledgeMaterialDigest(k)
	}
	if err != nil {
		return err
	}
	revision, err := inputRowRevision(tx, kind, id)
	if err != nil {
		return err
	}
	var receipt knowledgeInputMaterialReceipt
	if err := tx.Where("row_kind = ? AND row_id = ?", kind, id).Take(&receipt).Error; err != nil {
		return err
	}
	if receipt.AdmissionID != a.AdmissionID || receipt.RowRevision != revision || receipt.BodyDigest != digest {
		return ErrKnowledgeInputChanged
	}
	return nil
}
func (s *KnowledgeInputProvenanceStore) admissionInTx(tx *gorm.DB, k *types.Knowledge, expected *types.KnowledgeBuildInputProof) (*knowledgeInputAdmission, *types.KnowledgeBase, *types.SourceLineage, error) {
	var a knowledgeInputAdmission
	query := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", k.TenantID, k.KnowledgeBaseID, k.ID)
	if expected != nil {
		query = query.Where("admission_id = ?", expected.AdmissionID)
	} else {
		query = query.Order("build_generation DESC")
	}
	if err := query.Take(&a).Error; err != nil {
		return nil, nil, nil, err
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id = ? AND tenant_id = ?", k.KnowledgeBaseID, k.TenantID).Take(&kb).Error; err != nil {
		return nil, nil, nil, err
	}
	revision, err := inputRowRevision(tx, "config", kb.ID)
	if err != nil {
		return nil, nil, nil, err
	}
	rootDigest, err := knowledgeRootDigest(k)
	if err != nil {
		return nil, nil, nil, err
	}
	configDigest, err := knowledgeConfigDigest(&kb, k)
	if err != nil {
		return nil, nil, nil, err
	}
	if a.RootDigest != rootDigest || a.ConfigDigest != configDigest || a.ConfigRevision != revision {
		return nil, nil, nil, ErrKnowledgeInputChanged
	}
	var lineage types.SourceLineage
	if err := json.Unmarshal([]byte(a.SourceLineage), &lineage); err != nil {
		return nil, nil, nil, err
	}
	if err := lineage.RequireComplete(); err != nil {
		return nil, nil, nil, err
	}
	if a.OriginKind == "nextcloud" {
		identity, err := knowledgeBuildSourceIdentity(tx, k, a.BuildGeneration)
		if err != nil {
			return nil, nil, nil, err
		}
		if len(lineage.Sources) != 1 || lineage.Sources[0] != identity {
			return nil, nil, nil, ErrKnowledgeInputChanged
		}
	} else if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
		return nil, nil, nil, err
	}
	if expected != nil && (expected.Version != 1 || expected.TenantID != k.TenantID || expected.KnowledgeBaseID != k.KnowledgeBaseID || expected.KnowledgeID != k.ID || expected.BuildGeneration != a.BuildGeneration || expected.InputDigest != a.InputDigest || expected.ConfigDigest != a.ConfigDigest || !reflect.DeepEqual(expected.SourceLineage, &lineage)) {
		return nil, nil, nil, ErrKnowledgeInputChanged
	}
	if err := verifyKnowledgeInputMaterial(tx, &a, k, nil); err != nil {
		return nil, nil, nil, err
	}
	return &a, &kb, &lineage, nil
}

func (s *KnowledgeInputProvenanceStore) ReadOrdinaryRAGInputLineage(ctx context.Context, tx *gorm.DB, k *types.Knowledge, c *types.Chunk) (*types.SourceLineage, error) {
	a, _, lineage, err := s.admissionInTx(tx, k, nil)
	if err == nil && c != nil {
		err = verifyKnowledgeInputMaterial(tx, a, k, c)
	}
	if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, ErrKnowledgeInputChanged) || errors.Is(err, ErrNextcloudLineageSnapshotDenied) {
		return types.UnknownSourceLineage(), nil
	}
	if err != nil {
		return nil, err
	}
	return types.UnionSourceLineage(lineage)
}

func verifyKnowledgeInheritedSources(tx *gorm.DB, lineage *types.SourceLineage) error {
	for _, saved := range lineage.Sources {
		_, current, err := (&knowledgeRepository{db: tx}).resolveNextcloudSourceLineageInTx(tx, saved.TenantID, saved.KnowledgeBaseID, saved.KnowledgeID)
		if err != nil {
			return err
		}
		if saved != current {
			return ErrKnowledgeInputChanged
		}
	}
	return nil
}

// ReadControlledRAGInputLineage binds each actual source chunk/header to its
// original producer generation. A new publication cannot certify old SQL text.
func (s *KnowledgeInputProvenanceStore) ReadControlledRAGInputLineage(ctx context.Context, tx *gorm.DB, k *types.Knowledge, c *types.Chunk, current *types.SourceLineage) (*types.SourceLineage, error) {
	a, _, lineage, err := s.admissionInTx(tx, k, nil)
	if err == nil && (a.OriginKind != "nextcloud" || !reflect.DeepEqual(lineage, current)) {
		err = ErrKnowledgeInputChanged
	}
	if err == nil && c != nil {
		err = verifyKnowledgeInputMaterial(tx, a, k, c)
	}
	if errors.Is(err, gorm.ErrRecordNotFound) || errors.Is(err, ErrKnowledgeInputChanged) || errors.Is(err, ErrNextcloudLineageSnapshotDenied) {
		return types.UnknownSourceLineage(), nil
	}
	if err != nil {
		return nil, err
	}
	return types.UnionSourceLineage(lineage)
}

func (s *KnowledgeInputProvenanceStore) snapshot(ctx context.Context, request types.KnowledgeBuildInputCaptureRequest, recheck bool) (*types.KnowledgeBuildInputSnapshot, error) {
	if s == nil || s.db == nil || request.Expected == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if len(request.ChunkIDs) > 4096 {
		return nil, types.ErrSourceLineageLimit
	}
	options := &sql.TxOptions{ReadOnly: true}
	switch s.db.Dialector.Name() {
	case "postgres":
		options.Isolation = sql.LevelRepeatableRead
	case "sqlite":
	default:
		return nil, ErrKnowledgeInputChanged
	}
	if _, nested := s.db.Statement.ConnPool.(gorm.TxCommitter); nested {
		return nil, ErrKnowledgeInputChanged
	}
	var result *types.KnowledgeBuildInputSnapshot
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var k types.Knowledge
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", request.KnowledgeID, request.TenantID, request.KnowledgeBaseID).Take(&k).Error; err != nil {
			return err
		}
		a, kb, lineage, err := s.admissionInTx(tx, &k, request.Expected)
		if err != nil {
			return err
		}
		chunks := make([]*types.Chunk, 0, len(request.ChunkIDs))
		seen := map[string]bool{}
		for _, id := range request.ChunkIDs {
			if id == "" || seen[id] {
				return ErrKnowledgeInputChanged
			}
			seen[id] = true
			c := &types.Chunk{}
			if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", id, k.TenantID, k.KnowledgeBaseID, k.ID).Take(c).Error; err != nil {
				return err
			}
			if err := verifyKnowledgeInputMaterial(tx, a, &k, c); err != nil {
				return err
			}
			chunks = append(chunks, c)
		}
		ids := append([]string{}, request.ChunkIDs...)
		sort.Strings(ids)
		material := []string{a.AdmissionID, a.RootDigest, a.ConfigDigest}
		kgDigest, _ := knowledgeMaterialDigest(&k)
		material = append(material, kgDigest)
		ordered := append([]*types.Chunk{}, chunks...)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
		for _, c := range ordered {
			digest, err := knowledgeChunkDigest(c)
			if err != nil {
				return err
			}
			revision, err := inputRowRevision(tx, "chunk", c.ID)
			if err != nil {
				return err
			}
			material = append(material, fmt.Sprintf("%s:%d:%s", c.ID, revision, digest))
		}
		kgRevision, err := inputRowRevision(tx, "knowledge", k.ID)
		if err != nil {
			return err
		}
		material = append(material, fmt.Sprint(kgRevision))
		bodyDigest, err := KnowledgeInputDigest(material)
		if err != nil {
			return err
		}
		proof := &types.KnowledgeBuildInputProof{Version: 1, TenantID: k.TenantID, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, AdmissionID: a.AdmissionID, BuildGeneration: a.BuildGeneration, InputDigest: a.InputDigest, ConfigDigest: a.ConfigDigest, BodyDigest: bodyDigest, ChunkIDs: ids, SourceLineage: lineage}
		if (recheck || request.Expected.BodyDigest != "") && (proof.BodyDigest != request.Expected.BodyDigest || !reflect.DeepEqual(proof.ChunkIDs, request.Expected.ChunkIDs)) {
			return ErrKnowledgeInputChanged
		}
		result = &types.KnowledgeBuildInputSnapshot{Knowledge: &k, KnowledgeBase: kb, Chunks: chunks, Proof: proof}
		return nil
	}, options)
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeBuildInputs(ctx context.Context, request types.KnowledgeBuildInputCaptureRequest) (*types.KnowledgeBuildInputSnapshot, error) {
	if request.Expected == nil {
		request.Expected = KnowledgeBuildInputProofFromContext(ctx)
	}
	return s.snapshot(ctx, request, false)
}
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeBuildInputs(ctx context.Context, proof *types.KnowledgeBuildInputProof) error {
	if proof == nil {
		return types.ErrSourceLineageUnknown
	}
	_, err := s.snapshot(ctx, types.KnowledgeBuildInputCaptureRequest{TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID, KnowledgeID: proof.KnowledgeID, ChunkIDs: proof.ChunkIDs, Expected: proof}, proof.BodyDigest != "")
	return err
}

// These writer hooks execute inside the actual SQL mutation transaction. An
// unrelated update cannot re-seal old material without the original admission.
func validateKnowledgeInputWrite(ctx context.Context, tx *gorm.DB, tenant uint64, kb, id string) (*knowledgeInputAdmission, error) {
	guard, ok := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	if !ok {
		return nil, nil
	}
	if guard.admission.Err() != nil {
		return nil, guard.admission.Err()
	}
	if guard.proof == nil || guard.proof.TenantID != tenant || guard.proof.KnowledgeBaseID != kb || guard.proof.KnowledgeID != id {
		return nil, ErrKnowledgeInputChanged
	}
	var k types.Knowledge
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", id, tenant, kb).Take(&k).Error; err != nil {
		return nil, err
	}
	a, _, _, err := (&KnowledgeInputProvenanceStore{tx}).admissionInTx(tx, &k, guard.proof)
	return a, err
}

func sealKnowledgeInputChunks(ctx context.Context, tx *gorm.DB, chunks []*types.Chunk) error {
	for _, c := range chunks {
		a, err := validateKnowledgeInputWrite(ctx, tx, c.TenantID, c.KnowledgeBaseID, c.KnowledgeID)
		if err != nil {
			return err
		}
		if a == nil {
			continue
		}
		var actual types.Chunk
		if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", c.ID, c.TenantID, c.KnowledgeBaseID, c.KnowledgeID).Take(&actual).Error; err != nil {
			return err
		}
		if err := sealKnowledgeInputMaterial(tx, a.AdmissionID, &types.Knowledge{ID: c.KnowledgeID}, &actual); err != nil {
			return err
		}
	}
	return nil
}
