package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type (
	faqInputBoundaryKey struct{}
	faqInputBoundary    struct {
		KB, Digest     string
		Origin         knowledgeInputOrigin
		OriginalInputs []*types.KnowledgeBuildInputProof
	}
)

type (
	faqInputSessionKey struct{}
	faqInputSession    struct {
		ID    string
		Store *KnowledgeInputProvenanceStore
	}
)

type faqInputSessionRow struct {
	ID               string `gorm:"primaryKey"`
	TenantID         uint64
	KnowledgeBaseID  string
	Operation        string
	InputDigest      string
	InputBody        string
	OriginKind       string
	SourceLineage    string
	OriginalInputs   string
	OriginalSnapshot string
	CurrentSnapshot  string
}
type faqInputActorSelector knowledgeBuildActorSelector

func (faqInputActorSelector) TableName() string { return "faq_input_actor_selectors" }

func (faqInputSessionRow) TableName() string { return "faq_input_sessions" }

type faqEntryAdmission struct {
	ID                                                               string `gorm:"primaryKey"`
	SessionID                                                        string
	TenantID                                                         uint64
	KnowledgeBaseID, KnowledgeID, ChunkID, InputDigest, ConfigDigest string
	ConfigRevision                                                   uint64
	SourceLineage                                                    string
}

func (faqEntryAdmission) TableName() string { return "faq_entry_input_admissions" }

type faqEntryMaterial struct {
	ChunkID     string `gorm:"primaryKey"`
	AdmissionID string
	Revision    uint64
	BodyDigest  string
}
type faqCatalogOrigin struct {
	KnowledgeID                         string `gorm:"primaryKey"`
	ProducerID, OriginKind, InputDigest string
	Revision                            uint64
}

func (faqCatalogOrigin) TableName() string { return "faq_catalog_input_origins" }

func (faqEntryMaterial) TableName() string { return "faq_entry_material_receipts" }

// WithIndependentFAQInput binds independent faqinput.
func WithIndependentFAQInput(
	ctx context.Context,
	kb string,
	operation types.FAQInputOperation,
) (context.Context, error) {
	if operation.Batch != nil && operation.Batch.Mode == "" {
		copied := *operation.Batch
		copied.Mode = types.FAQBatchModeAppend
		operation.Batch = &copied
	}
	digest, err := KnowledgeInputDigest(operation)
	if err != nil {
		return ctx, err
	}
	lineage, _ := types.NewCompleteSourceLineage()
	return context.WithValue(
		ctx,
		faqInputBoundaryKey{},
		faqInputBoundary{KB: kb, Digest: digest, Origin: knowledgeInputOrigin{lineage, "human"}},
	), nil
}

// WithDerivedFAQInput binds derived faqinput.
// The caller supplies the original model manifest, not a newly minted current
// lineage claim. This bridge captures and rechecks the actual original rows.
// WithDerivedFAQInput is the one-manifest form. The manifest must cover all
// actually consumed model text/config/selection inputs. It is not a citation.
func (s *KnowledgeInputProvenanceStore) WithDerivedFAQInput(
	ctx context.Context,
	kb string,
	operation types.FAQInputOperation,
	original *types.KnowledgeBuildInputProof,
) (context.Context, error) {
	return s.WithDerivedFAQInputs(ctx, kb, operation, []*types.KnowledgeBuildInputProof{original})
}

// WithDerivedFAQInputs retains each complete original material receipt. A
// future generator must supply every consumed parent, including non-output
// candidates and prompt/config/history artifacts. Unknown parents are closed.
func (s *KnowledgeInputProvenanceStore) WithDerivedFAQInputs(
	ctx context.Context,
	kb string,
	operation types.FAQInputOperation,
	originals []*types.KnowledgeBuildInputProof,
) (context.Context, error) {
	if len(originals) == 0 || len(originals) > 4096 {
		return ctx, types.ErrSourceLineageUnknown
	}
	proofs := make([]*types.KnowledgeBuildInputProof, 0, len(originals))
	lineages := make([]*types.SourceLineage, 0, len(originals))
	for _, original := range originals {
		if original == nil || original.BodyDigest == "" || original.Projection != "" ||
			original.SourceLineage.RequireComplete() != nil {
			return ctx, types.ErrSourceLineageUnknown
		}
		copied2 := cloneKnowledgeInputProof(original)
		if copied2 == nil {
			return ctx, types.ErrSourceLineageUnknown
		}
		if err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
			authority, err := s.faqAuthorityInTx(tx)
			if err != nil {
				return err
			}
			_, err = authority.captureKnowledgeBuildSnapshotInTx(
				tx,
				types.KnowledgeBuildInputCaptureRequest{
					TenantID:        copied2.TenantID,
					KnowledgeBaseID: copied2.KnowledgeBaseID,
					KnowledgeID:     copied2.KnowledgeID,
					ChunkIDs:        copied2.ChunkIDs,
					Expected:        copied2,
				},
				true,
			)
			return err
		}); err != nil {
			return ctx, err
		}
		proofs = append(proofs, copied2)
		lineages = append(lineages, copied2.SourceLineage)
	}
	digest, err := KnowledgeInputDigest(operation)
	if err != nil {
		return ctx, err
	}
	lineage, err := types.UnionSourceLineage(lineages...)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(
		ctx,
		faqInputBoundaryKey{},
		faqInputBoundary{
			KB:             kb,
			Digest:         digest,
			Origin:         knowledgeInputOrigin{lineage, "derived"},
			OriginalInputs: proofs,
		},
	), nil
}

func faqEntryDigest(chunk *types.Chunk) (string, error) {
	// Every persisted entry field belongs to the receipt, including candidate
	// membership, enabled/recommended controls, sequence and indexing status.
	return KnowledgeInputDigest(struct {
		Chunk                        *types.Chunk
		SourceContent, ContextHeader string
	}{chunk, chunk.SourceContent, chunk.ContextHeader})
}

func faqRowRevision(tx *gorm.DB, id string) (uint64, error) {
	var row struct{ Revision uint64 }
	if err := tx.Table("faq_input_row_versions").Where("chunk_id=?", id).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

func (s *KnowledgeInputProvenanceStore) readFAQEntryInputInTx(
	tx *gorm.DB,
	chunk *types.Chunk,
) (*types.FAQEntryInputProof, error) {
	if chunk == nil || chunk.ChunkType != types.ChunkTypeFAQ {
		return nil, types.ErrSourceLineageUnknown
	}
	var receipt faqEntryMaterial
	if err := tx.Where("chunk_id=?", chunk.ID).Take(&receipt).Error; err != nil {
		return nil, err
	}
	var admission faqEntryAdmission
	if err := tx.Where(("id=? AND chunk_id=? AND knowledge_id=? AND knowledge_base_id=? AND tenant_" +
		"id=?"), receipt.AdmissionID, chunk.ID, chunk.KnowledgeID, chunk.KnowledgeBaseID, chunk.TenantID).
		Take(&admission).
		Error; err != nil {
		return nil, err
	}
	digest, err := faqEntryDigest(chunk)
	if err != nil {
		return nil, err
	}
	revision, err := faqRowRevision(tx, chunk.ID)
	if err != nil {
		return nil, err
	}
	if receipt.Revision != revision || receipt.BodyDigest != digest {
		return nil, ErrKnowledgeInputChanged
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id=? AND tenant_id=?", chunk.KnowledgeBaseID, chunk.TenantID).Take(&kb).Error; err != nil {
		return nil, err
	}
	configDigest, err := knowledgeConfigDigest(&kb, &types.Knowledge{})
	if err != nil {
		return nil, err
	}
	configRevision, err := knowledgeConfigurationControlRevision(tx, kb.ID)
	if err != nil {
		return nil, err
	}
	if admission.ConfigDigest != configDigest || admission.ConfigRevision != configRevision {
		return nil, ErrKnowledgeInputChanged
	}
	var lineage types.SourceLineage
	if err := json.Unmarshal([]byte(admission.SourceLineage), &lineage); err != nil {
		return nil, err
	}
	if err := lineage.RequireComplete(); err != nil {
		return nil, err
	}
	if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
		return nil, err
	}
	var producer faqInputSessionRow
	if err := tx.Where("id=?", admission.SessionID).Take(&producer).Error; err != nil {
		return nil, err
	}
	parents, err := s.readFAQOriginalKnowledgeInputsInTx(tx, &producer)
	if err != nil {
		return nil, err
	}
	return &types.FAQEntryInputProof{
		AdmissionID:             admission.ID,
		SessionID:               admission.SessionID,
		TenantID:                admission.TenantID,
		KnowledgeBaseID:         admission.KnowledgeBaseID,
		KnowledgeID:             admission.KnowledgeID,
		ChunkID:                 admission.ChunkID,
		InputDigest:             admission.InputDigest,
		ConfigDigest:            admission.ConfigDigest,
		ConfigRevision:          admission.ConfigRevision,
		Revision:                revision,
		BodyDigest:              digest,
		SourceLineage:           &lineage,
		OriginalKnowledgeInputs: parents,
	}, nil
}

// ReadFAQEntryInputLineage reads faqentry input lineage.
func (s *KnowledgeInputProvenanceStore) ReadFAQEntryInputLineage(
	tx *gorm.DB,
	chunk *types.Chunk,
) (*types.SourceLineage, error) {
	proof, err := s.readFAQEntryInputInTx(tx, chunk)
	if err != nil {
		return nil, err
	}
	return types.UnionSourceLineage(proof.SourceLineage)
}

func (s *KnowledgeInputProvenanceStore) captureFAQInputsInTx(
	tx *gorm.DB,
	req types.FAQInputReadRequest,
) (*types.FAQInputSnapshot, error) {
	if err := s.checkFAQInputReadPlanInTx(tx, req); err != nil {
		return nil, err
	}
	if req.TenantID == 0 || req.KnowledgeBaseID == "" {
		return nil, types.ErrSourceLineageUnknown
	}
	var kb types.KnowledgeBase
	if err := tx.Where("id=? AND tenant_id=?", req.KnowledgeBaseID, req.TenantID).Take(&kb).Error; err != nil {
		return nil, err
	}
	configLineage, err := readKnowledgeConfigurationOrigin(tx, &kb)
	if err != nil {
		return nil, err
	}
	configDigest, err := knowledgeConfigDigest(&kb, &types.Knowledge{})
	if err != nil {
		return nil, err
	}
	configRevision, err := knowledgeConfigurationControlRevision(tx, kb.ID)
	if err != nil {
		return nil, err
	}
	snapshot := &types.FAQInputSnapshot{
		KnowledgeBase:  &kb,
		Request:        req,
		ConfigDigest:   configDigest,
		ConfigRevision: configRevision,
		ConfigLineage:  configLineage,
		Chunks:         []*types.Chunk{},
		Proofs:         []*types.FAQEntryInputProof{},
		Tags:           []*types.KnowledgeTag{},
		TagProofs:      []*types.KnowledgeCollectionAuxiliaryProof{},
	}
	if req.AllEntries {
		if err := tx.Where("tenant_id=? AND knowledge_base_id=?", req.TenantID, req.KnowledgeBaseID).
			Order("id ASC").
			Find(&snapshot.Tags).
			Error; err != nil {
			return nil, err
		}
		snapshot.TagProofs, err = captureTagFields(tx, snapshot.Tags)
		if err != nil {
			return nil, err
		}
	}
	if req.AllEntries {
		var version struct{ Revision uint64 }
		if err := tx.Table("faq_input_scope_versions").
			Where("tenant_id=? AND knowledge_base_id=?", req.TenantID, req.KnowledgeBaseID).
			Take(&version).
			Error; err != nil {
			return nil, err
		}
		snapshot.ScopeRevision = version.Revision
	}
	var catalogs []*types.Knowledge
	if err := tx.Where(
		"tenant_id=? AND knowledge_base_id=? AND type=?",
		req.TenantID,
		req.KnowledgeBaseID,
		types.KnowledgeTypeFAQ,
	).
		Find(&catalogs).
		Error; err != nil {
		return nil, err
	}
	if len(catalogs) > 1 {
		return nil, ErrKnowledgeInputChanged
	}
	if len(catalogs) == 0 {
		if !req.AllEntries || len(req.ChunkIDs) != 0 {
			return nil, ErrKnowledgeInputChanged
		}
		return snapshot, nil
	}
	k := catalogs[0]
	var catalogOrigin faqCatalogOrigin
	if err := tx.Where("knowledge_id=?", k.ID).Take(&catalogOrigin).Error; err != nil {
		return nil, err
	}
	codeDigest, err := faqCatalogCodeDigest(k)
	if err != nil {
		return nil, err
	}
	var catalogVersion struct{ Revision uint64 }
	if err := tx.Table("faq_catalog_control_versions").
		Where("knowledge_id=?", k.ID).
		Take(&catalogVersion).
		Error; err != nil {
		return nil, err
	}
	if catalogOrigin.ProducerID != "faq-catalog-fixed-code-v1" || catalogOrigin.OriginKind != "trusted_code" ||
		catalogOrigin.InputDigest != codeDigest ||
		catalogOrigin.Revision != catalogVersion.Revision {
		return nil, ErrKnowledgeInputChanged
	}
	if _, _, _, err := s.admissionInTx(tx, k, nil); err != nil {
		return nil, err
	}
	snapshot.KnowledgeDigest, err = knowledgeMaterialDigest(k)
	if err != nil {
		return nil, err
	}
	snapshot.KnowledgeRevision, err = inputRowRevision(tx, "knowledge", k.ID)
	if err != nil {
		return nil, err
	}
	// Header output bookkeeping does not influence FAQ entries. Original
	// semantic header material and its monotonic revision stay in the snapshot.
	snapshot.Knowledge = &types.Knowledge{
		ID:              k.ID,
		TenantID:        k.TenantID,
		KnowledgeBaseID: k.KnowledgeBaseID,
		Type:            k.Type,
	}
	q := tx.Where(
		"tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND chunk_type=?",
		req.TenantID,
		req.KnowledgeBaseID,
		k.ID,
		types.ChunkTypeFAQ,
	)
	if !req.AllEntries {
		q = q.Where("id IN (?)", req.ChunkIDs)
	}
	if err := q.Order("id ASC").Find(&snapshot.Chunks).Error; err != nil {
		return nil, err
	}
	if !req.AllEntries && len(snapshot.Chunks) != len(req.ChunkIDs) {
		return nil, ErrKnowledgeInputChanged
	}
	tagIDs := map[string]bool{}
	for _, chunk := range snapshot.Chunks {
		proof, err := s.readFAQEntryInputInTx(tx, chunk)
		if err != nil {
			return nil, err
		}
		snapshot.Proofs = append(snapshot.Proofs, proof)
		if chunk.TagID != "" {
			tagIDs[chunk.TagID] = true
		}
	}
	ids := make([]string, 0, len(tagIDs))
	for id := range tagIDs {
		ids = append(ids, id)
	}
	if len(ids) > 0 && !req.AllEntries {
		if err := tx.Where("id IN (?) AND tenant_id=? AND knowledge_base_id=?", ids, req.TenantID, req.KnowledgeBaseID).
			Order("id ASC").
			Find(&snapshot.Tags).
			Error; err != nil {
			return nil, err
		}
		if len(snapshot.Tags) != len(ids) {
			return nil, ErrKnowledgeInputChanged
		}
		snapshot.TagProofs, err = captureTagFields(tx, snapshot.Tags)
		if err != nil {
			return nil, err
		}
	}
	return snapshot, nil
}

// CaptureFAQInputs captures faqinputs.
func (s *KnowledgeInputProvenanceStore) CaptureFAQInputs(
	ctx context.Context,
	req types.FAQInputReadRequest,
) (*types.FAQInputSnapshot, error) {
	var snapshot *types.FAQInputSnapshot
	err := s.collectionTransaction(
		ctx,
		func(tx *gorm.DB) error { var err error; snapshot, err = s.captureFAQInputsInTx(tx, req); return err },
	)
	return snapshot, err
}

func (s *KnowledgeInputProvenanceStore) recheckFAQInputsInTx(tx *gorm.DB, snapshot *types.FAQInputSnapshot) error {
	if snapshot == nil {
		return types.ErrSourceLineageUnknown
	}
	actual, err := s.captureFAQInputsInTx(tx, snapshot.Request)
	if err != nil {
		return err
	}
	if !faqInputEqual(snapshot, actual) {
		a, b := reflect.ValueOf(*snapshot), reflect.ValueOf(*actual)
		for i := 0; i < a.NumField(); i++ {
			if !reflect.DeepEqual(a.Field(i).Interface(), b.Field(i).Interface()) {
				return fmt.Errorf("%w: FAQ original %s changed", ErrKnowledgeInputChanged, a.Type().Field(i).Name)
			}
		}
		return ErrKnowledgeInputChanged
	}
	return nil
}

func faqInputEqual(a, b any) bool {
	left, err := KnowledgeInputDigest(a)
	if err != nil {
		return false
	}
	right, err := KnowledgeInputDigest(b)
	return err == nil && left == right
}

// RecheckFAQInputs rechecks faqinputs.
func (s *KnowledgeInputProvenanceStore) RecheckFAQInputs(ctx context.Context, snapshot *types.FAQInputSnapshot) error {
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error { return s.recheckFAQInputsInTx(tx, snapshot) })
}

// FAQInputLineages returns the faqinput lineages result.
func FAQInputLineages(snapshot *types.FAQInputSnapshot) ([]*types.SourceLineage, error) {
	if snapshot == nil || snapshot.ConfigLineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	lineages := []*types.SourceLineage{snapshot.ConfigLineage}
	for _, p := range snapshot.Proofs {
		if p == nil || p.SourceLineage.RequireComplete() != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		lineages = append(lineages, p.SourceLineage)
	}
	for _, p := range snapshot.TagProofs {
		if p == nil || p.SourceLineage.RequireComplete() != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		lineages = append(lineages, p.SourceLineage)
	}
	return lineages, nil
}

// BeginFAQInput begins faqinput.
func (s *KnowledgeInputProvenanceStore) BeginFAQInput(
	ctx context.Context,
	kb *types.KnowledgeBase,
	operation types.FAQInputOperation,
) (context.Context, string, error) {
	boundary, ok := ctx.Value(faqInputBoundaryKey{}).(faqInputBoundary)
	digest, err := KnowledgeInputDigest(operation)
	if err != nil {
		return ctx, "", err
	}
	if !ok || boundary.KB != kb.ID || boundary.Digest != digest || boundary.Origin.lineage.RequireComplete() != nil {
		return ctx, "", types.ErrSourceLineageUnknown
	}
	ctx, err = s.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	if err != nil {
		return ctx, "", err
	}
	id := uuid.NewString()
	actor, err := captureKnowledgeActorSelector(ctx, id)
	if err != nil {
		return ctx, "", err
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		snapshot, err := s.captureFAQInputsInTx(
			tx,
			types.FAQInputReadRequest{TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, AllEntries: true},
		)
		if err != nil {
			return err
		}
		lineages, err := FAQInputLineages(snapshot)
		if err != nil {
			return err
		}
		lineage, err := types.UnionSourceLineage(append(lineages, boundary.Origin.lineage)...)
		if err != nil {
			return err
		}
		body, err := json.Marshal(operation)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(lineage)
		if err != nil {
			return err
		}
		material, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		parents := append([]*types.KnowledgeBuildInputProof{}, boundary.OriginalInputs...)
		seen := map[string]bool{}
		for _, p := range snapshot.Proofs {
			for _, parent := range p.OriginalKnowledgeInputs {
				key, err := KnowledgeInputDigest(parent)
				if err != nil {
					return err
				}
				if !seen[key] {
					parents = append(parents, parent)
					seen[key] = true
				}
			}
		}
		parentBody, err := json.Marshal(parents)
		if err != nil {
			return err
		}
		session := &faqInputSessionRow{
			ID:               id,
			TenantID:         kb.TenantID,
			KnowledgeBaseID:  kb.ID,
			Operation:        operation.Kind,
			InputDigest:      digest,
			InputBody:        string(body),
			OriginKind:       boundary.Origin.kind,
			SourceLineage:    string(encoded),
			OriginalInputs:   string(parentBody),
			OriginalSnapshot: string(material),
			CurrentSnapshot:  string(material),
		}
		if _, err := s.readFAQOriginalKnowledgeInputsInTx(tx, session); err != nil {
			return err
		}
		if err := tx.Create(session).Error; err != nil {
			return err
		}
		selector := faqInputActorSelector(actor)
		return tx.Create(&selector).Error
	}, knowledgeDisplayWriteOptions(s.db))
	if err != nil {
		return ctx, "", err
	}
	return context.WithValue(ctx, faqInputSessionKey{}, faqInputSession{id, s}), id, nil
}

// RestoreFAQInput restores faqinput.
func (s *KnowledgeInputProvenanceStore) RestoreFAQInput(
	ctx context.Context,
	id string,
	kb *types.KnowledgeBase,
	operation types.FAQInputOperation,
) (context.Context, error) {
	var session faqInputSessionRow
	if id == "" {
		return ctx, types.ErrSourceLineageUnknown
	}
	if err := s.db.WithContext(ctx).
		Where("id=? AND tenant_id=? AND knowledge_base_id=?", id, kb.TenantID, kb.ID).
		Take(&session).
		Error; err != nil {
		return ctx, err
	}
	digest, err := KnowledgeInputDigest(operation)
	if err != nil {
		return ctx, err
	}
	if digest != session.InputDigest {
		return ctx, ErrKnowledgeInputChanged
	}
	var actor faqInputActorSelector
	if err := s.db.WithContext(ctx).Where("admission_id=?", id).Take(&actor).Error; err != nil {
		return ctx, err
	}
	ctx, err = s.restoreCurrentKnowledgeActor(ctx, knowledgeBuildActorSelector(actor), kb.TenantID)
	if err != nil {
		return ctx, err
	}
	ctx, err = s.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	if err != nil {
		return ctx, err
	}
	ctx = context.WithValue(ctx, faqInputSessionKey{}, faqInputSession{id, s})
	return ctx, s.CheckFAQInput(ctx)
}

func (s *KnowledgeInputProvenanceStore) faqSessionInTx(
	ctx context.Context,
	tx *gorm.DB,
) (*faqInputSessionRow, *types.FAQInputSnapshot, error) {
	active, ok := ctx.Value(faqInputSessionKey{}).(faqInputSession)
	if !ok || active.Store != s || active.ID == "" {
		return nil, nil, types.ErrSourceLineageUnknown
	}
	var session faqInputSessionRow
	if err := tx.Where("id=?", active.ID).Take(&session).Error; err != nil {
		return nil, nil, err
	}
	var snapshot types.FAQInputSnapshot
	if err := json.Unmarshal([]byte(session.CurrentSnapshot), &snapshot); err != nil {
		return nil, nil, err
	}
	if err := s.recheckFAQInputsInTx(tx, &snapshot); err != nil {
		return nil, nil, err
	}
	var lineage types.SourceLineage
	if err := json.Unmarshal([]byte(session.SourceLineage), &lineage); err != nil {
		return nil, nil, err
	}
	if err := lineage.RequireComplete(); err != nil {
		return nil, nil, err
	}
	if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
		return nil, nil, err
	}
	if _, err := s.readFAQOriginalKnowledgeInputsInTx(tx, &session); err != nil {
		return nil, nil, err
	}
	return &session, &snapshot, nil
}

// CheckFAQInput checks faqinput.
func (s *KnowledgeInputProvenanceStore) CheckFAQInput(ctx context.Context) error {
	active, ok := ctx.Value(faqInputSessionKey{}).(faqInputSession)
	if !ok || active.Store != s {
		return types.ErrSourceLineageUnknown
	}
	var row faqInputSessionRow
	if err := s.db.WithContext(ctx).Where("id=?", active.ID).Take(&row).Error; err != nil {
		return err
	}
	kb := &types.KnowledgeBase{ID: row.KnowledgeBaseID, TenantID: row.TenantID}
	fresh, err := s.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	if err != nil {
		return err
	}
	if types.CallerFromContext(fresh) != types.CallerFromContext(ctx) {
		return ErrKnowledgeInputChanged
	}
	return s.collectionTransaction(ctx, func(tx *gorm.DB) error { _, _, err := s.faqSessionInTx(ctx, tx); return err })
}

// FAQSessionID returns the faqsession id result.
func (s *KnowledgeInputProvenanceStore) FAQSessionID(ctx context.Context) string {
	active, _ := ctx.Value(faqInputSessionKey{}).(faqInputSession)
	return active.ID
}

// WriteFAQChunks writes faqchunks.
func (s *KnowledgeInputProvenanceStore) WriteFAQChunks(ctx context.Context, chunks []*types.Chunk, create bool) error {
	if len(chunks) == 0 {
		return nil
	}
	if err := s.CheckFAQInput(ctx); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		session, snapshot, err := s.faqSessionInTx(ctx, tx)
		if err != nil {
			return err
		}
		var lineage types.SourceLineage
		if err := json.Unmarshal([]byte(session.SourceLineage), &lineage); err != nil {
			return err
		}
		known := map[string]*types.Chunk{}
		for _, c := range snapshot.Chunks {
			known[c.ID] = c
		}
		for _, c := range chunks {
			if c == nil || c.ID == "" || c.TenantID != session.TenantID ||
				c.KnowledgeBaseID != session.KnowledgeBaseID ||
				c.ChunkType != types.ChunkTypeFAQ ||
				snapshot.Knowledge == nil ||
				c.KnowledgeID != snapshot.Knowledge.ID {
				return ErrKnowledgeInputChanged
			}
			if create == (known[c.ID] != nil) {
				return ErrKnowledgeInputChanged
			}
		}
		if create {
			for _, c := range chunks {
				if c.IndexStatus == "" {
					c.IndexStatus = "ready"
				}
			}
		}
		if err := validateFAQProducerOutputs(session, snapshot, chunks, create); err != nil {
			return err
		}
		controls := map[string]struct {
			Enabled bool
			Flags   types.ChunkFlags
		}{}
		for _, c := range chunks {
			controls[c.ID] = struct {
				Enabled bool
				Flags   types.ChunkFlags
			}{c.IsEnabled, c.Flags}
		}
		if create {
			if tx.Name() == "sqlite" {
				if err := types.AssignChunkSeqIDs(tx, chunks); err != nil {
					return err
				}
			}
			for _, c := range chunks {
				if c.IndexStatus == "" {
					c.IndexStatus = "ready"
				}
			}
			if err := tx.Select("*").CreateInBatches(chunks, createChunksBatchSize).Error; err != nil {
				return err
			}
			// GORM applies struct field defaults during INSERT even with Select
			// all columns. Restore the exact requested false/zero controls in
			// this same transaction before any receipt can certify the rows.
			for _, c := range chunks {
				desired := controls[c.ID]
				if c.IsEnabled != desired.Enabled || c.Flags != desired.Flags {
					if err := tx.Model(&types.Chunk{}).
						Where("id=?", c.ID).
						Updates(map[string]any{"is_enabled": desired.Enabled, "flags": desired.Flags}).
						Error; err != nil {
						return err
					}
				}
			}
		} else {
			for _, c := range chunks {
				if err := tx.Omit("SeqID").Save(c).Error; err != nil {
					return err
				}
			}
		}
		for _, c := range chunks {
			var actual types.Chunk
			if err := tx.Where("id=?", c.ID).Take(&actual).Error; err != nil {
				return err
			}
			*c = actual
			encoded, err := json.Marshal(&lineage)
			if err != nil {
				return err
			}
			admission := faqEntryAdmission{
				ID:              uuid.NewString(),
				SessionID:       session.ID,
				TenantID:        session.TenantID,
				KnowledgeBaseID: session.KnowledgeBaseID,
				KnowledgeID:     c.KnowledgeID,
				ChunkID:         c.ID,
				InputDigest:     session.InputDigest,
				ConfigDigest:    snapshot.ConfigDigest,
				ConfigRevision:  snapshot.ConfigRevision,
				SourceLineage:   string(encoded),
			}
			if err := tx.Create(&admission).Error; err != nil {
				return err
			}
			digest, err := faqEntryDigest(c)
			if err != nil {
				return err
			}
			revision, err := faqRowRevision(tx, c.ID)
			if err != nil {
				return err
			}
			if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).
				Create(&faqEntryMaterial{c.ID, admission.ID, revision, digest}).
				Error; err != nil {
				return err
			}
		}
		return s.advanceFAQSessionInTx(tx, session)
	}, knowledgeDisplayWriteOptions(s.db))
}

func (s *KnowledgeInputProvenanceStore) advanceFAQSessionInTx(tx *gorm.DB, session *faqInputSessionRow) error {
	if err := s.advanceFAQReadPlanAfterOwnWriteInTx(tx, session); err != nil {
		return err
	}
	snapshot, err := s.captureFAQInputsInTx(
		tx,
		types.FAQInputReadRequest{
			TenantID:        session.TenantID,
			KnowledgeBaseID: session.KnowledgeBaseID,
			AllEntries:      true,
		},
	)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return tx.Model(session).Update("current_snapshot", string(encoded)).Error
}

// CreateFAQCatalog admits only this fixed server header. Entry bodies keep
// their own receipts and never inherit an empty origin from this header.
func (s *KnowledgeInputProvenanceStore) CreateFAQCatalog(ctx context.Context, k *types.Knowledge) error {
	if k.Type != types.KnowledgeTypeFAQ || k.Title != "FAQ" || k.Description != "FAQ 条目容器" ||
		k.Channel != types.ChannelWeb ||
		k.Source != types.KnowledgeTypeFAQ {
		return ErrKnowledgeInputChanged
	}
	active, ok := ctx.Value(faqInputSessionKey{}).(faqInputSession)
	if !ok || active.Store != s {
		return types.ErrSourceLineageUnknown
	}
	if err := s.CheckFAQInput(ctx); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		session, snapshot, err := s.faqSessionInTx(ctx, tx)
		if err != nil {
			return err
		}
		if snapshot.Knowledge != nil || session.TenantID != k.TenantID || session.KnowledgeBaseID != k.KnowledgeBaseID {
			return ErrKnowledgeInputChanged
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id=? AND tenant_id=?", k.KnowledgeBaseID, k.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		if k.ID == "" {
			k.ID = uuid.NewString()
		}
		if err := tx.Create(k).Error; err != nil {
			return err
		}
		lineage, _ := types.NewCompleteSourceLineage()
		// Explicit fixed-code artifact, carried as derived because the generic
		// admission schema has no code kind. No request/entry text is included.
		_, err = s.admitInTx(
			ctx,
			tx,
			k,
			&kb,
			struct{ Producer, Title, Description string }{"faq-catalog-fixed-code-v1", k.Title, k.Description},
			knowledgeInputOrigin{lineage, "derived"},
		)
		if err != nil {
			return err
		}
		codeDigest, err := faqCatalogCodeDigest(k)
		if err != nil {
			return err
		}
		var catalogVersion struct{ Revision uint64 }
		if err := tx.Table("faq_catalog_control_versions").
			Where("knowledge_id=?", k.ID).
			Take(&catalogVersion).
			Error; err != nil {
			return err
		}
		if err := tx.Create(
			&faqCatalogOrigin{k.ID, "faq-catalog-fixed-code-v1", "trusted_code", codeDigest, catalogVersion.Revision},
		).Error; err != nil {
			return err
		}
		return s.advanceFAQSessionInTx(tx, session)
	}, knowledgeDisplayWriteOptions(s.db))
}

func faqCatalogCodeDigest(k *types.Knowledge) (string, error) {
	if k.Type != types.KnowledgeTypeFAQ || k.Title != "FAQ" || k.Description != "FAQ 条目容器" ||
		k.Source != types.KnowledgeTypeFAQ ||
		k.Channel != types.ChannelWeb ||
		k.ParseStatus != "completed" ||
		k.EnableStatus != "enabled" {
		return "", ErrKnowledgeInputChanged
	}
	return KnowledgeInputDigest(
		struct{ Producer, Title, Description, Type, Source, Channel, ParseStatus, EnableStatus string }{
			"faq-catalog-fixed-code-v1",
			k.Title,
			k.Description,
			k.Type,
			k.Source,
			k.Channel,
			k.ParseStatus,
			k.EnableStatus,
		},
	)
}

// DeleteFAQChunks deletes faqchunks.
func (s *KnowledgeInputProvenanceStore) DeleteFAQChunks(ctx context.Context, ids []string) error {
	if err := s.CheckFAQInput(ctx); err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		session, snapshot, err := s.faqSessionInTx(ctx, tx)
		if err != nil {
			return err
		}
		wanted := map[string]bool{}
		for _, id := range ids {
			wanted[id] = true
		}
		var operation types.FAQInputOperation
		if err := json.Unmarshal([]byte(session.InputBody), &operation); err != nil {
			return err
		}
		if operation.Kind != "delete" &&
			(operation.Kind != "import" ||
				operation.Batch == nil || operation.Batch.Mode != types.FAQBatchModeReplace) &&
			operation.Kind != "create" {
			return ErrKnowledgeInputChanged
		}
		selected := map[int64]bool{}
		for _, seq := range operation.DeleteIDs {
			selected[seq] = true
		}
		proofs := map[string]*types.FAQEntryInputProof{}
		for _, p := range snapshot.Proofs {
			proofs[p.ChunkID] = p
		}
		for _, c := range snapshot.Chunks {
			if wanted[c.ID] {
				if operation.Kind == "delete" && !selected[c.SeqID] {
					return ErrKnowledgeInputChanged
				}
				if operation.Kind == "create" &&
					(proofs[c.ID] == nil || proofs[c.ID].SessionID != session.ID || c.Status != int(
						types.ChunkStatusStored,
					)) {
					return ErrKnowledgeInputChanged
				}
			}
			delete(wanted, c.ID)
		}
		if len(wanted) > 0 {
			return ErrKnowledgeInputChanged
		}
		if err := tx.Where(
			"id IN (?) AND tenant_id=? AND knowledge_base_id=? AND chunk_type=?",
			ids,
			session.TenantID,
			session.KnowledgeBaseID,
			types.ChunkTypeFAQ,
		).
			Delete(&types.Chunk{}).
			Error; err != nil {
			return err
		}
		return s.advanceFAQSessionInTx(tx, session)
	}, knowledgeDisplayWriteOptions(s.db))
}

// FAQSessionSnapshot returns the faqsession snapshot result.
func (s *KnowledgeInputProvenanceStore) FAQSessionSnapshot(ctx context.Context) (*types.FAQInputSnapshot, error) {
	var snapshot *types.FAQInputSnapshot
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		_, captured, err := s.faqSessionInTx(ctx, tx)
		snapshot = captured
		return err
	})
	return snapshot, err
}

// FAQSessionSourceLineage returns the faqsession source lineage result.
func (s *KnowledgeInputProvenanceStore) FAQSessionSourceLineage(ctx context.Context) (*types.SourceLineage, error) {
	active, ok := ctx.Value(faqInputSessionKey{}).(faqInputSession)
	if !ok || active.Store != s {
		return nil, types.ErrSourceLineageUnknown
	}
	var row faqInputSessionRow
	if err := s.db.WithContext(ctx).Where("id=?", active.ID).Take(&row).Error; err != nil {
		return nil, err
	}
	var lineage types.SourceLineage
	if err := json.Unmarshal([]byte(row.SourceLineage), &lineage); err != nil {
		return nil, err
	}
	return types.UnionSourceLineage(&lineage)
}

// FAQ parent reads hold the existing transaction. Every current actor, group,
// and organization lookup must use that same connection (SQLite has one).
func (s *KnowledgeInputProvenanceStore) faqAuthorityInTx(tx *gorm.DB) (*KnowledgeInputProvenanceStore, error) {
	if s.displayAuthorityFactory == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	configured := s.displayAuthorityFactory(tx)
	if configured == nil || configured.db != tx || configured.actorGroups == nil {
		return nil, types.ErrSourceLineageUnknown
	}
	authority := *configured
	// Committed history verification now uses original SQL material and the
	// transaction-bound authority, never the inherited global message service.
	return &authority, nil
}

func (s *KnowledgeInputProvenanceStore) readFAQOriginalKnowledgeInputsInTx(
	tx *gorm.DB,
	session *faqInputSessionRow,
) ([]*types.KnowledgeBuildInputProof, error) {
	var parents []*types.KnowledgeBuildInputProof
	if err := json.Unmarshal([]byte(session.OriginalInputs), &parents); err != nil {
		return nil, err
	}
	if len(parents) > 4096 {
		return nil, types.ErrSourceLineageLimit
	}
	var authority *KnowledgeInputProvenanceStore
	if len(parents) > 0 {
		var err error
		authority, err = s.faqAuthorityInTx(tx)
		if err != nil {
			return nil, err
		}
	}
	for _, p := range parents {
		if p == nil || p.BodyDigest == "" || p.Projection != "" || p.SourceLineage.RequireComplete() != nil {
			return nil, types.ErrSourceLineageUnknown
		}
		if _, err := authority.captureKnowledgeBuildSnapshotInTx(
			tx,
			types.KnowledgeBuildInputCaptureRequest{
				TenantID:        p.TenantID,
				KnowledgeBaseID: p.KnowledgeBaseID,
				KnowledgeID:     p.KnowledgeID,
				ChunkIDs:        p.ChunkIDs,
				Expected:        p,
			},
			true,
		); err != nil {
			return nil, err
		}
		readCtx := types.WithAPIKeyOperationRequirement(
			tx.Statement.Context,
			types.APIKeyOperationRequirement{
				RequireFullAccess: true,
				Capabilities: []types.APIKeyCapability{
					types.APIKeyCapabilityRetrieve,
					types.APIKeyCapabilityChat,
					types.APIKeyCapabilityMessageHistory,
				},
			},
		)
		readCtx, err := authority.RefreshCurrentReadActor(readCtx)
		if err != nil {
			return nil, err
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id=? AND tenant_id=?", p.KnowledgeBaseID, p.TenantID).Take(&kb).Error; err != nil {
			return nil, err
		}
		if _, err := access.ResolveKB(
			readCtx,
			access.KBRequest{Caller: types.CallerFromContext(readCtx)},
			&kb,
			types.OrgRoleViewer,
			authority.actorShares,
			nil,
		); err != nil {
			return nil, err
		}
		permission, err := authority.actorGroups.EffectivePermission(
			readCtx,
			kb.TenantID,
			types.GroupResourceTypeKnowledgeBase,
			kb.ID,
			types.ResourceActionRead,
			time.Now().UTC(),
		)
		if err != nil {
			return nil, err
		}
		if !permission.Allowed {
			return nil, access.ErrForbidden
		}
	}
	return parents, nil
}

// FAQSessionOriginalKnowledgeInputs returns the faqsession original knowledge inputs result.
func (s *KnowledgeInputProvenanceStore) FAQSessionOriginalKnowledgeInputs(
	ctx context.Context,
) ([]*types.KnowledgeBuildInputProof, error) {
	active, ok := ctx.Value(faqInputSessionKey{}).(faqInputSession)
	if !ok || active.Store != s {
		return nil, types.ErrSourceLineageUnknown
	}
	var parents []*types.KnowledgeBuildInputProof
	err := s.collectionTransaction(ctx, func(tx *gorm.DB) error {
		row, _, err := s.faqSessionInTx(ctx, tx)
		if err != nil {
			return err
		}
		parents, err = s.readFAQOriginalKnowledgeInputsInTx(tx, row)
		return err
	})
	return parents, err
}

// ResolveFAQInputTag resolves faqinput tag.
// Tag resolution is part of the exact FAQ operation. New names/defaults are
// written with their actual operation's union, and only that own addition may
// advance the original candidate manifest.
func (s *KnowledgeInputProvenanceStore) ResolveFAQInputTag(
	ctx context.Context,
	entry *types.FAQEntryPayload,
) (string, error) {
	if err := s.CheckFAQInput(ctx); err != nil {
		return "", err
	}
	var tagID string
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		session, snapshot, err := s.faqSessionInTx(ctx, tx)
		if err != nil {
			return err
		}
		var operation types.FAQInputOperation
		if err := json.Unmarshal([]byte(session.InputBody), &operation); err != nil {
			return err
		}
		allowed := false
		matches := func(candidate *types.FAQEntryPayload) {
			if candidate != nil && candidate.TagID == entry.TagID && candidate.TagName == entry.TagName {
				allowed = true
			}
		}
		matches(operation.Entry)
		if operation.Batch != nil {
			for i := range operation.Batch.Entries {
				matches(&operation.Batch.Entries[i])
			}
		}
		if !allowed {
			return ErrKnowledgeInputChanged
		}
		if entry.TagID != 0 {
			for _, tag := range snapshot.Tags {
				if tag.SeqID == entry.TagID {
					tagID = tag.ID
					return nil
				}
			}
			return ErrKnowledgeInputChanged
		}
		name := strings.TrimSpace(entry.TagName)
		if name == "" {
			name = types.UntaggedTagName
		}
		for _, tag := range snapshot.Tags {
			if tag.Name == name {
				tagID = tag.ID
				return nil
			}
		}
		now := time.Now()
		tag := &types.KnowledgeTag{
			ID:              uuid.NewString(),
			TenantID:        session.TenantID,
			KnowledgeBaseID: session.KnowledgeBaseID,
			Name:            name,
			CreatedAt:       now,
			UpdatedAt:       now,
		}
		if name == types.UntaggedTagName {
			tag.SortOrder = -1
		}
		if err := tx.Create(tag).Error; err != nil {
			return err
		}
		var kb types.KnowledgeBase
		if err := tx.Where("id=? AND tenant_id=?", session.KnowledgeBaseID, session.TenantID).
			Take(&kb).
			Error; err != nil {
			return err
		}
		var lineage types.SourceLineage
		if err := json.Unmarshal([]byte(session.SourceLineage), &lineage); err != nil {
			return err
		}
		var tagInput any = operation
		originKind := session.OriginKind
		if strings.TrimSpace(entry.TagName) == "" {
			fixed, _ := types.NewCompleteSourceLineage()
			lineage = *fixed
			originKind = "derived"
			tagInput = struct {
				Producer, Name, Color string
				SortOrder             int
			}{"faq-default-tag-fixed-code-v1", types.UntaggedTagName, "", -1}
		}
		for _, field := range []string{"name", "color", "sort_order"} {
			if err := admitKnowledgeDisplayOrigin(
				ctx,
				tx,
				"tag-"+field,
				tag.ID,
				&kb,
				tagDisplayFieldValue(tag, field),
				tagInput,
				&lineage,
				originKind,
			); err != nil {
				return err
			}
		}
		tagID = tag.ID
		return s.advanceFAQSessionInTx(tx, session)
	}, knowledgeDisplayWriteOptions(s.db))
	return tagID, err
}
