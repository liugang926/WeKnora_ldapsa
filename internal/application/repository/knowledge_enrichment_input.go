package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type knowledgeProducerInput struct {
	ID                                                                           string `gorm:"primaryKey"`
	AdmissionID, Purpose, InputDigest, OriginalProof, ModelDigest, ActorSelector string
}

func (knowledgeProducerInput) TableName() string { return "knowledge_producer_inputs" }

func knowledgeEnrichmentRevision(tx *gorm.DB, kind, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	err := tx.Table("knowledge_enrichment_row_versions").Where("row_kind = ? AND row_id = ?", kind, id).Take(&row).Error
	if err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}
func knowledgeEnrichmentDocumentDigest(k *types.Knowledge) (string, error) {
	return KnowledgeInputDigest(struct{ Root, Title, Folder string }{mustKnowledgeRootDigest(k), k.Title, k.FolderPath})
}
func knowledgeEnrichmentChunkDigest(c *types.Chunk) (string, error) {
	return KnowledgeInputDigest(struct {
		Tenant                                                              uint64
		KB, KG, ID, Content, Source, Header, Type, Parent, Pre, Next, Image string
		Revision, Index, Start, End                                         int
		Enabled                                                             bool
	}{c.TenantID, c.KnowledgeBaseID, c.KnowledgeID, c.ID, c.Content, c.SourceContent, c.ContextHeader, c.ChunkType, c.ParentChunkID, c.PreChunkID, c.NextChunkID, c.ImageInfo, c.ContentRevision, c.ChunkIndex, c.StartAt, c.EndAt, c.IsEnabled})
}

// CaptureKnowledgeEnrichmentInputs starts from the original private material
// receipt. It cannot admit text from a caller-supplied cached document.
func (s *KnowledgeInputProvenanceStore) CaptureKnowledgeEnrichmentInputs(ctx context.Context, request types.KnowledgeBuildInputCaptureRequest) (*types.KnowledgeBuildInputSnapshot, error) {
	if request.Expected == nil {
		actual, err := s.CaptureKnowledgeOutputInputs(ctx, request)
		if err != nil {
			return nil, err
		}
		request.Expected = cloneKnowledgeInputProof(actual.Proof)
	}
	// A complete full manifest must be checked before projecting its inputs.
	if request.Expected.BodyDigest != "" && request.Expected.Projection != types.KnowledgeBuildProjectionEnrichment {
		if err := s.RecheckKnowledgeBuildInputs(ctx, request.Expected); err != nil {
			return nil, err
		}
		request.Expected = cloneKnowledgeInputProof(request.Expected)
		request.Expected.BodyDigest = ""
		request.Expected.ChunkIDs = nil
	}
	request.Projection = types.KnowledgeBuildProjectionEnrichment
	return s.CaptureKnowledgeBuildInputs(ctx, request)
}

func knowledgeProducerModelDigest(tx *gorm.DB, snapshot *types.KnowledgeBuildInputSnapshot, purpose string) (string, error) {
	kb, k := snapshot.KnowledgeBase, snapshot.Knowledge
	routingRevision, err := knowledgeEnrichmentRevision(tx, "routing", kb.ID)
	if err != nil {
		return "", err
	}
	routingDigest, err := KnowledgeInputDigest(struct {
		Vector, Storage *string
		Provider        *types.StorageProviderConfig
		Legacy          types.StorageConfig
	}{kb.VectorStoreID, kb.StorageBackendID, kb.StorageProviderConfig, kb.StorageConfig})
	if err != nil {
		return "", err
	}
	var tenant types.Tenant
	if err := tx.Where("id = ?", k.TenantID).Take(&tenant).Error; err != nil {
		return "", err
	}
	tenantRevision, err := knowledgeEnrichmentRevision(tx, "tenant", fmt.Sprint(tenant.ID))
	if err != nil {
		return "", err
	}
	tenantDigest, err := knowledgeProducerTenantDigest(&tenant)
	if err != nil {
		return "", err
	}
	ids := []string{kb.SummaryModelID}
	if purpose == "image" {
		cfg := kb.VLMConfig
		overrides, err := k.ProcessOverrides()
		if err != nil {
			return "", err
		}
		if overrides != nil && overrides.VLMConfig != nil {
			cfg = *overrides.VLMConfig
		}
		ids = []string{cfg.ModelID}
	}
	if kb.NeedsEmbeddingModel() {
		ids = append(ids, kb.EmbeddingModelID)
	}
	sort.Strings(ids)
	rows := []string{fmt.Sprintf("routing:%d:%s", routingRevision, routingDigest), fmt.Sprintf("tenant:%d:%s", tenantRevision, tenantDigest)}
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		var model types.Model
		if err := tx.Where("id = ? AND (tenant_id = ? OR is_builtin = ?)", id, k.TenantID, true).Take(&model).Error; err != nil {
			return "", err
		}
		if model.DeletedAt.Valid {
			return "", ErrKnowledgeInputChanged
		}
		rev, err := knowledgeEnrichmentRevision(tx, "model", id)
		if err != nil {
			return "", err
		}
		digest, err := KnowledgeInputDigest(model)
		if err != nil {
			return "", err
		}
		rows = append(rows, fmt.Sprintf("%s:%d:%s", id, rev, digest))
	}
	return KnowledgeInputDigest(rows)
}

func knowledgeProducerTenantDigest(tenant *types.Tenant) (string, error) {
	return KnowledgeInputDigest(struct {
		ID          uint64
		Status      string
		Engines     types.RetrieverEngines
		Credentials *types.CredentialsConfig
		Storage     *types.StorageEngineConfig
		Default     *string
	}{tenant.ID, tenant.Status, tenant.RetrieverEngines, tenant.Credentials, tenant.StorageEngineConfig, tenant.DefaultStorageBackendID})
}

// The actual model/factory context is loaded after private receipt validation;
// an old middleware TenantInfo pointer cannot certify a newly captured row.
func (s *KnowledgeInputProvenanceStore) ReadKnowledgeProducerTenant(ctx context.Context, proof *types.KnowledgeBuildInputProof) (*types.Tenant, error) {
	if proof == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	var tenant types.Tenant
	if err := s.db.WithContext(ctx).Where("id = ?", proof.TenantID).Take(&tenant).Error; err != nil {
		return nil, err
	}
	return &tenant, nil
}

// AdmitKnowledgeProducerInput persists the actual selected rows and exact
// trusted runtime/task controls before enqueue or a synchronous model call.
func (s *KnowledgeInputProvenanceStore) AdmitKnowledgeProducerInput(ctx context.Context, proof *types.KnowledgeBuildInputProof, purpose string, input any) (*types.KnowledgeProducerInputProof, error) {
	if proof == nil || proof.Projection != types.KnowledgeBuildProjectionEnrichment || (purpose != "summary" && purpose != "question" && purpose != "image") {
		return nil, types.ErrSourceLineageUnknown
	}
	if ctx.Value(types.CallerContextKey) != nil {
		var kb types.KnowledgeBase
		if err := s.db.WithContext(ctx).Where("id = ? AND tenant_id = ?", proof.KnowledgeBaseID, proof.TenantID).Take(&kb).Error; err != nil {
			return nil, err
		}
		var err error
		ctx, err = s.AuthorizeKnowledgeCurrentProducerWrite(ctx, &kb)
		if err != nil {
			return nil, err
		}
	} else if !NextcloudBuildLeasePresent(ctx, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID) {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := s.RecheckKnowledgeBuildInputs(ctx, proof); err != nil {
		return nil, err
	}
	digest, err := KnowledgeInputDigest(input)
	if err != nil {
		return nil, err
	}
	actorJSON := "null"
	if ctx.Value(types.CallerContextKey) != nil {
		actor, e := captureKnowledgeActorSelector(ctx, proof.AdmissionID)
		if e != nil {
			return nil, e
		}
		data, e := json.Marshal(actor)
		if e != nil {
			return nil, e
		}
		actorJSON = string(data)
	}
	var result *types.KnowledgeProducerInputProof
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		snapshot, err := s.captureKnowledgeBuildSnapshotInTx(tx, types.KnowledgeBuildInputCaptureRequest{Projection: proof.Projection, TenantID: proof.TenantID, KnowledgeBaseID: proof.KnowledgeBaseID, KnowledgeID: proof.KnowledgeID, ChunkIDs: proof.ChunkIDs, Expected: proof}, true)
		if err != nil {
			return err
		}
		modelDigest, err := knowledgeProducerModelDigest(tx, snapshot, purpose)
		if err != nil {
			return err
		}
		data, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		row := knowledgeProducerInput{ID: uuid.NewString(), AdmissionID: proof.AdmissionID, Purpose: purpose, InputDigest: digest, OriginalProof: string(data), ModelDigest: modelDigest, ActorSelector: actorJSON}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		result = &types.KnowledgeProducerInputProof{ID: row.ID, Purpose: purpose, Digest: digest}
		return nil
	})
	return result, err
}

func (s *KnowledgeInputProvenanceStore) recheckKnowledgeProducerInputInTx(ctx context.Context, tx *gorm.DB, original *types.KnowledgeBuildInputProof, proof *types.KnowledgeProducerInputProof, input any) error {
	if proof == nil || original == nil || proof.ID == "" || original.Projection != types.KnowledgeBuildProjectionEnrichment {
		return types.ErrSourceLineageUnknown
	}
	digest, err := KnowledgeInputDigest(input)
	if err != nil {
		return err
	}
	var saved knowledgeProducerInput
	if err := tx.Where("id = ? AND admission_id = ?", proof.ID, original.AdmissionID).Take(&saved).Error; err != nil {
		return err
	}
	var admitted types.KnowledgeBuildInputProof
	if json.Unmarshal([]byte(saved.OriginalProof), &admitted) != nil || !reflect.DeepEqual(&admitted, original) || saved.Purpose != proof.Purpose || digest != saved.InputDigest || digest != proof.Digest {
		return ErrKnowledgeInputChanged
	}
	snapshot, err := s.captureKnowledgeBuildSnapshotInTx(tx, types.KnowledgeBuildInputCaptureRequest{Projection: original.Projection, TenantID: original.TenantID, KnowledgeBaseID: original.KnowledgeBaseID, KnowledgeID: original.KnowledgeID, ChunkIDs: original.ChunkIDs, Expected: original}, true)
	if err != nil {
		return err
	}
	modelDigest, err := knowledgeProducerModelDigest(tx, snapshot, proof.Purpose)
	if err != nil {
		return err
	}
	if modelDigest != saved.ModelDigest {
		return ErrKnowledgeInputChanged
	}
	if actual, ok := types.TenantInfoFromContext(ctx); ok {
		var current types.Tenant
		if err := tx.Where("id = ?", original.TenantID).Take(&current).Error; err != nil {
			return err
		}
		actualDigest, err := knowledgeProducerTenantDigest(actual)
		if err != nil {
			return err
		}
		currentDigest, err := knowledgeProducerTenantDigest(&current)
		if err != nil {
			return err
		}
		if actualDigest != currentDigest {
			return ErrKnowledgeInputChanged
		}
	}
	return ctx.Err()
}
func (s *KnowledgeInputProvenanceStore) RecheckKnowledgeProducerInput(ctx context.Context, original *types.KnowledgeBuildInputProof, proof *types.KnowledgeProducerInputProof, input any) error {
	if err := s.RecheckKnowledgeBuildInputs(ctx, original); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return s.recheckKnowledgeProducerInputInTx(ctx, tx, original, proof, input) })
}

type knowledgeProducerInputContextKey struct{}
type knowledgeProducerInputContext struct {
	proof *types.KnowledgeProducerInputProof
	input func() any
}

func WithKnowledgeProducerInput(ctx context.Context, proof *types.KnowledgeProducerInputProof, input func() any) context.Context {
	var copy *types.KnowledgeProducerInputProof
	if proof != nil {
		v := *proof
		copy = &v
	}
	return context.WithValue(ctx, knowledgeProducerInputContextKey{}, knowledgeProducerInputContext{copy, input})
}
func CheckKnowledgeProducerInput(ctx context.Context) error {
	guard, ok := ctx.Value(knowledgeProducerInputContextKey{}).(knowledgeProducerInputContext)
	if !ok {
		return ctx.Err()
	}
	build, ok := ctx.Value(knowledgeBuildInputKey{}).(knowledgeBuildInputContext)
	if !ok || guard.input == nil {
		return types.ErrSourceLineageUnknown
	}
	store, ok := build.reader.(*KnowledgeInputProvenanceStore)
	if !ok {
		return types.ErrSourceLineageUnknown
	}
	return store.RecheckKnowledgeProducerInput(ctx, build.proof, guard.proof, guard.input())
}

type knowledgeProducerActorContextKey struct{}

type knowledgeCurrentProducerWriteKey struct{}
type knowledgeCurrentProducerWrite struct {
	actor  knowledgeBuildActorSelector
	tenant uint64
	kb     string
}

// AuthorizeKnowledgeCurrentProducerWrite is only called by the actual current
// generation boundary. Its private marker cannot be acquired by a public read.
func (s *KnowledgeInputProvenanceStore) AuthorizeKnowledgeCurrentProducerWrite(ctx context.Context, kb *types.KnowledgeBase) (context.Context, error) {
	actor, err := captureKnowledgeActorSelector(ctx, "")
	if err != nil {
		return ctx, err
	}
	fresh, err := s.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(fresh, knowledgeCurrentProducerWriteKey{}, knowledgeCurrentProducerWrite{actor, kb.TenantID, kb.ID}), nil
}
func (s *KnowledgeInputProvenanceStore) refreshKnowledgeCurrentProducerWrite(ctx context.Context, marker knowledgeCurrentProducerWrite) (context.Context, error) {
	fresh, err := s.restoreCurrentKnowledgeActor(ctx, marker.actor, marker.tenant)
	if err != nil {
		return ctx, err
	}
	var kb types.KnowledgeBase
	if err := s.db.WithContext(fresh).Where("id = ? AND tenant_id = ?", marker.kb, marker.tenant).Take(&kb).Error; err != nil {
		return ctx, err
	}
	return s.authorizeKnowledgeActorWrite(fresh, &kb)
}

func (s *KnowledgeInputProvenanceStore) RestoreKnowledgeProducerActor(ctx context.Context, original *types.KnowledgeBuildInputProof, producer *types.KnowledgeProducerInputProof) (context.Context, error) {
	if original == nil || producer == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	var row knowledgeProducerInput
	if err := s.db.WithContext(ctx).Where("id = ? AND admission_id = ?", producer.ID, original.AdmissionID).Take(&row).Error; err != nil {
		return ctx, err
	}
	if row.Purpose != producer.Purpose || row.InputDigest != producer.Digest {
		return ctx, ErrKnowledgeInputChanged
	}
	if row.ActorSelector == "null" {
		return RestoreKnowledgeBuildActorIfRequired(ctx, s, original)
	}
	var actor knowledgeBuildActorSelector
	if json.Unmarshal([]byte(row.ActorSelector), &actor) != nil || actor.AdmissionID != original.AdmissionID {
		return ctx, ErrKnowledgeInputChanged
	}
	fresh, err := s.restoreCurrentKnowledgeActor(ctx, actor, original.TenantID)
	if err != nil {
		return ctx, err
	}
	var kb types.KnowledgeBase
	if err := s.db.WithContext(fresh).Where("id = ? AND tenant_id = ?", original.KnowledgeBaseID, original.TenantID).Take(&kb).Error; err != nil {
		return ctx, err
	}
	fresh, err = s.authorizeKnowledgeActorWrite(fresh, &kb)
	if err != nil {
		return ctx, err
	}
	fresh = context.WithValue(fresh, knowledgeBuildActorContextKey{}, nil)
	fresh = context.WithValue(fresh, knowledgeProducerActorContextKey{}, producer)
	return fresh, nil
}

func KnowledgeProducerInputPresent(ctx context.Context) bool {
	_, ok := ctx.Value(knowledgeProducerInputContextKey{}).(knowledgeProducerInputContext)
	return ok
}
