package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// These request markers contain only the exact, typed values consumed at an
// authenticated direct-input boundary. Neither persisted metadata nor a
// generic independent ingestion marker can certify an old display value.
type knowledgeDisplayRequestKey struct{}
type knowledgeDisplayRequest struct {
	Kind, ID, KB  string
	Fields        map[string]any
	Assignments   map[string][]string
	Original      *types.KnowledgeBuildInputProof
	AutoTag       *types.KnowledgeAutoTagInputSnapshot
	AutoTagReader *KnowledgeInputProvenanceStore `json:"-"`
}

type KnowledgeDisplayAuthorityFactory func(*gorm.DB) *KnowledgeInputProvenanceStore

func knowledgeDisplayWriteOptions(db *gorm.DB) *sql.TxOptions {
	options := &sql.TxOptions{}
	if db.Dialector.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	return options
}

// ConfigureKnowledgeDisplayInputWriters supplies transaction-bound group and
// organization lookups. A missing factory fails closed for marked writes.
func ConfigureKnowledgeDisplayInputWriters(store *KnowledgeInputProvenanceStore, tags interfaces.KnowledgeTagRepository, knowledge interfaces.KnowledgeRepository, factory KnowledgeDisplayAuthorityFactory) {
	if factory != nil {
		baseFactory := factory
		factory = func(tx *gorm.DB) *KnowledgeInputProvenanceStore {
			actual := baseFactory(tx)
			if actual != nil && actual.messageVerifier == nil {
				actual.messageVerifier = store.messageVerifier
			}
			return actual
		}
	}
	store.displayAuthorityFactory = factory
	if r, ok := tags.(*knowledgeTagRepository); ok {
		r.displayAuthorityFactory = factory
	}
	if r, ok := knowledge.(*knowledgeRepository); ok {
		r.displayAuthorityFactory = factory
	}
}

func WithIndependentKnowledgeTagCreateInput(ctx context.Context, kb, name, color string, order int) context.Context {
	if strings.TrimSpace(name) == types.UntaggedTagName {
		order = -1
	}
	return context.WithValue(ctx, knowledgeDisplayRequestKey{}, knowledgeDisplayRequest{Kind: "tag-create", KB: kb, Fields: map[string]any{"name": strings.TrimSpace(name), "color": strings.TrimSpace(color), "sort_order": order}})
}
func WithIndependentKnowledgeTagUpdateInput(ctx context.Context, id string, name, color *string, order *int) context.Context {
	fields := map[string]any{}
	if name != nil {
		fields["name"] = strings.TrimSpace(*name)
	}
	if color != nil {
		fields["color"] = strings.TrimSpace(*color)
	}
	if order != nil {
		fields["sort_order"] = *order
	}
	return context.WithValue(ctx, knowledgeDisplayRequestKey{}, knowledgeDisplayRequest{Kind: "tag-update", ID: id, Fields: fields})
}
func WithIndependentKnowledgeTagAssignmentInput(ctx context.Context, assignments map[string][]string) context.Context {
	copy := map[string][]string{}
	for id, tags := range assignments {
		copy[id] = normalizedKnowledgeDisplayTagIDs(tags)
	}
	return context.WithValue(ctx, knowledgeDisplayRequestKey{}, knowledgeDisplayRequest{Kind: "relations", Assignments: copy})
}
func WithIndependentKnowledgeBaseNameInput(ctx context.Context, id, name string) context.Context {
	return context.WithValue(ctx, knowledgeDisplayRequestKey{}, knowledgeDisplayRequest{Kind: "kb-name", ID: id, Fields: map[string]any{"name": name}})
}

// A derived display producer keeps the checked original admission and its
// complete dependency envelope. The writer rechecks it inside its SQL tx.
func WithDerivedKnowledgeDisplayInput(ctx context.Context, proof *types.KnowledgeBuildInputProof) (context.Context, error) {
	if proof == nil || proof.Projection != "" || proof.SourceLineage.RequireComplete() != nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	return context.WithValue(ctx, knowledgeDisplayRequestKey{}, knowledgeDisplayRequest{Kind: "derived", Original: cloneKnowledgeInputProof(proof)}), nil
}

func normalizedKnowledgeDisplayTagIDs(ids []string) []string {
	set := map[string]bool{}
	for _, id := range ids {
		if id != "" {
			set[id] = true
		}
	}
	result := make([]string, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

type knowledgeDisplayInputOrigin struct {
	OriginID                                                               string `gorm:"primaryKey"`
	Kind, RowID                                                            string
	TenantID                                                               uint64
	KnowledgeBaseID                                                        string
	Revision                                                               uint64
	OriginKind, ActualInput, InputDigest, BodyDigest, SourceLineage, Actor string
}

func (knowledgeDisplayInputOrigin) TableName() string { return "knowledge_display_input_origins" }

type knowledgeDisplayCacheKey struct{}
type knowledgeDisplayCache struct {
	versions map[string]uint64
	origins  map[string]knowledgeDisplayInputOrigin
}

func knowledgeDisplayCacheID(kind, id string) string { return kind + "\x00" + id }
func cacheKnowledgeDisplayInputs(tx *gorm.DB, ids []string) (*gorm.DB, error) {
	cache := &knowledgeDisplayCache{versions: map[string]uint64{}, origins: map[string]knowledgeDisplayInputOrigin{}}
	for first := 0; first < len(ids); first += 500 {
		end := min(first+500, len(ids))
		var versions []struct {
			Kind, RowID string
			Revision    uint64
		}
		if err := tx.Table("knowledge_display_versions").Where("row_id IN ?", ids[first:end]).Find(&versions).Error; err != nil {
			return nil, err
		}
		for _, v := range versions {
			cache.versions[knowledgeDisplayCacheID(v.Kind, v.RowID)] = v.Revision
		}
		var origins []knowledgeDisplayInputOrigin
		if err := tx.Table("knowledge_display_input_origins o").Select("o.*").Joins("JOIN knowledge_display_versions v ON v.kind=o.kind AND v.row_id=o.row_id AND v.revision=o.revision").Where("o.row_id IN ?", ids[first:end]).Scan(&origins).Error; err != nil {
			return nil, err
		}
		for _, o := range origins {
			cache.origins[knowledgeDisplayCacheID(o.Kind, o.RowID)] = o
		}
	}
	return tx.WithContext(context.WithValue(tx.Statement.Context, knowledgeDisplayCacheKey{}, cache)), nil
}

func knowledgeDisplayRevision(tx *gorm.DB, kind, id string) (uint64, error) {
	if cache, ok := tx.Statement.Context.Value(knowledgeDisplayCacheKey{}).(*knowledgeDisplayCache); ok {
		if revision := cache.versions[knowledgeDisplayCacheID(kind, id)]; revision > 0 {
			return revision, nil
		}
		return 0, ErrKnowledgeInputChanged
	}
	var row struct{ Revision uint64 }
	if err := tx.Table("knowledge_display_versions").Where("kind = ? AND row_id = ?", kind, id).Take(&row).Error; err != nil {
		return 0, err
	}
	if row.Revision == 0 {
		return 0, ErrKnowledgeInputChanged
	}
	return row.Revision, nil
}

func knowledgeDisplayBody(kind, id string, tenant uint64, kb string, value any) any {
	return struct {
		Kind, ID string
		Tenant   uint64
		KB       string
		Value    any
	}{kind, id, tenant, kb, value}
}

func readKnowledgeDisplayOrigin(tx *gorm.DB, kind, id string, tenant uint64, kb string, value any) (*types.KnowledgeCollectionAuxiliaryProof, error) {
	revision, err := knowledgeDisplayRevision(tx, kind, id)
	if err != nil {
		return nil, err
	}
	var origin knowledgeDisplayInputOrigin
	if cache, ok := tx.Statement.Context.Value(knowledgeDisplayCacheKey{}).(*knowledgeDisplayCache); ok {
		origin = cache.origins[knowledgeDisplayCacheID(kind, id)]
		if origin.Kind != kind || origin.RowID != id || origin.Revision != revision || origin.TenantID != tenant || origin.KnowledgeBaseID != kb {
			return nil, types.ErrSourceLineageUnknown
		}
	} else if err := tx.Where("kind = ? AND row_id = ? AND tenant_id = ? AND knowledge_base_id = ? AND revision = ?", kind, id, tenant, kb, revision).Take(&origin).Error; err != nil {
		return nil, err
	}
	digest, err := KnowledgeInputDigest(knowledgeDisplayBody(kind, id, tenant, kb, value))
	if err != nil {
		return nil, err
	}
	if digest != origin.BodyDigest {
		return nil, ErrKnowledgeInputChanged
	}
	var input any
	decoder := json.NewDecoder(strings.NewReader(origin.ActualInput))
	decoder.UseNumber()
	if decoder.Decode(&input) != nil {
		return nil, ErrKnowledgeInputChanged
	}
	inputDigest, err := KnowledgeInputDigest(input)
	if err != nil || inputDigest != origin.InputDigest {
		return nil, ErrKnowledgeInputChanged
	}
	var lineage types.SourceLineage
	if json.Unmarshal([]byte(origin.SourceLineage), &lineage) != nil || lineage.RequireComplete() != nil {
		return nil, types.ErrSourceLineageUnknown
	}
	if err := verifyKnowledgeInheritedSources(tx, &lineage); err != nil {
		return nil, err
	}
	return &types.KnowledgeCollectionAuxiliaryProof{Kind: kind, ID: id, TenantID: tenant, KnowledgeBaseID: kb, Revision: revision, Digest: digest, SourceLineage: &lineage}, nil
}

func admitKnowledgeDisplayOrigin(tx *gorm.DB, ctx context.Context, kind, id string, kb *types.KnowledgeBase, value, input any, lineage *types.SourceLineage, originKind string) error {
	if lineage.RequireComplete() != nil {
		return types.ErrSourceLineageUnknown
	}
	if err := verifyKnowledgeInheritedSources(tx, lineage); err != nil {
		return err
	}
	revision, err := knowledgeDisplayRevision(tx, kind, id)
	if err != nil {
		return err
	}
	digest, err := KnowledgeInputDigest(knowledgeDisplayBody(kind, id, kb.TenantID, kb.ID, value))
	if err != nil {
		return err
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return err
	}
	inputDigest, err := KnowledgeInputDigest(input)
	if err != nil {
		return err
	}
	lineageJSON, err := json.Marshal(lineage)
	if err != nil {
		return err
	}
	principal, _ := types.PrincipalFromContext(ctx)
	actorJSON, err := json.Marshal(struct {
		Caller    types.Caller
		Principal types.Principal
	}{types.CallerFromContext(ctx), principal})
	if err != nil {
		return err
	}
	return tx.Create(&knowledgeDisplayInputOrigin{OriginID: uuid.NewString(), Kind: kind, RowID: id, TenantID: kb.TenantID, KnowledgeBaseID: kb.ID, Revision: revision, OriginKind: originKind, ActualInput: string(inputJSON), InputDigest: inputDigest, BodyDigest: digest, SourceLineage: string(lineageJSON), Actor: string(actorJSON)}).Error
}

func knowledgeDisplayWriterOrigin(tx *gorm.DB, ctx context.Context, kb *types.KnowledgeBase, request knowledgeDisplayRequest, factory KnowledgeDisplayAuthorityFactory) (context.Context, *types.SourceLineage, string, error) {
	if factory == nil {
		return ctx, nil, "", types.ErrSourceLineageUnknown
	}
	store := factory(tx)
	if store == nil || store.db != tx {
		return ctx, nil, "", types.ErrSourceLineageUnknown
	}
	var actual context.Context
	var err error
	if request.Kind == "kb-name" {
		actual, err = store.authorizeKnowledgeBaseNameInput(ctx, kb, request.ID == "")
	} else {
		actual, err = store.AuthorizeCurrentKnowledgeInputWrite(ctx, kb)
	}
	if err != nil {
		return ctx, nil, "", err
	}
	if request.Kind != "derived" {
		lineage, _ := types.NewCompleteSourceLineage()
		return actual, lineage, "human", nil
	}
	proof := request.Original
	if proof == nil || proof.TenantID != kb.TenantID || proof.KnowledgeBaseID != kb.ID || proof.Projection != "" {
		return ctx, nil, "", ErrKnowledgeInputChanged
	}
	var k types.Knowledge
	if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", proof.KnowledgeID, proof.TenantID, proof.KnowledgeBaseID).Take(&k).Error; err != nil {
		return ctx, nil, "", err
	}
	if _, _, _, err := store.admissionInTx(tx, &k, proof); err != nil {
		return ctx, nil, "", err
	}
	lineage, err := types.UnionSourceLineage(proof.SourceLineage)
	if err == nil && request.AutoTag != nil {
		if request.AutoTagReader == nil || request.AutoTag.Original == nil || !reflect.DeepEqual(request.AutoTag.Original.Proof, proof) {
			return ctx, nil, "", ErrKnowledgeInputChanged
		}
		lineage, err = request.AutoTagReader.recheckKnowledgeAutoTagInTx(tx, actual, request.AutoTag)
	}
	return actual, lineage, "derived", err
}

func (s *KnowledgeInputProvenanceStore) authorizeKnowledgeBaseNameInput(ctx context.Context, kb *types.KnowledgeBase, create bool) (context.Context, error) {
	actor, err := captureKnowledgeActorSelector(ctx, "")
	if err != nil {
		return ctx, err
	}
	fresh, err := s.restoreCurrentKnowledgeActor(ctx, actor, kb.TenantID)
	if err != nil {
		return ctx, err
	}
	caller := types.CallerFromContext(fresh)
	if scope, ok := types.TenantAPIKeyScopeFromContext(fresh); ok {
		if !scope.FullAccess && !scope.HasCapability(types.APIKeyCapabilityManageKnowledgeBases) {
			return ctx, access.ErrForbidden
		}
		if create && caller.TenantID != kb.TenantID {
			return ctx, access.ErrForbidden
		}
		if !create {
			if err := types.AuthorizeTenantAPIKeyKnowledgeBases(fresh, kb.ID); err != nil {
				return ctx, err
			}
		}
	} else if !caller.Role.HasPermission(types.TenantRoleContributor) {
		return ctx, access.ErrForbidden
	}
	if s.actorGroups == nil {
		return ctx, types.ErrSourceLineageUnknown
	}
	permission, err := s.actorGroups.EffectivePermission(fresh, kb.TenantID, types.GroupResourceTypeKnowledgeBase, kb.ID, types.ResourceActionEdit, time.Now().UTC())
	if err != nil {
		return ctx, err
	}
	if !permission.Allowed {
		return ctx, access.ErrForbidden
	}
	if create {
		if caller.TenantID != kb.TenantID {
			return ctx, access.ErrForbidden
		}
		return fresh, nil
	}
	grant, err := access.ResolveKB(fresh, access.KBRequest{Caller: caller}, kb, types.OrgRoleEditor, s.actorShares, nil)
	if err != nil {
		return ctx, err
	}
	if _, machine := types.TenantAPIKeyScopeFromContext(fresh); !machine && caller.TenantID == kb.TenantID && !caller.Role.HasPermission(types.TenantRoleAdmin) && kb.CreatorID != caller.UserID {
		matches, err := NewGroupAccessRepository(s.db).ListResourceGroupMatches(fresh, caller.UserID, kb.TenantID, types.GroupResourceTypeKnowledgeBase, kb.ID)
		if err != nil {
			return ctx, err
		}
		allowed := false
		for _, match := range matches {
			if match.Fresh(time.Now().UTC()) && match.Permission == types.ResourcePermissionEdit {
				allowed = true
			}
		}
		if !allowed {
			return ctx, access.ErrForbidden
		}
	}
	return grant.Context(fresh), nil
}

func tagDisplayFieldValue(tag *types.KnowledgeTag, field string) any {
	switch field {
	case "name":
		return tag.Name
	case "color":
		return tag.Color
	case "sort_order":
		return tag.SortOrder
	}
	return nil
}

func writeKnowledgeTagWithDisplayInput(db *gorm.DB, ctx context.Context, tag *types.KnowledgeTag, create bool, factory KnowledgeDisplayAuthorityFactory) error {
	request, marked := ctx.Value(knowledgeDisplayRequestKey{}).(knowledgeDisplayRequest)
	// Unmarked internal/legacy writes remain unknown. They can never reuse an
	// old receipt because database versions advance on every material mutation.
	if !marked {
		if create {
			return db.WithContext(ctx).Create(tag).Error
		}
		return db.WithContext(ctx).Save(tag).Error
	}
	var captured *types.TagOutputInputSnapshot
	var captureErr error
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var kb types.KnowledgeBase
		if err := tx.Where("id = ? AND tenant_id = ?", tag.KnowledgeBaseID, tag.TenantID).Take(&kb).Error; err != nil {
			return err
		}
		if create && request.Kind != "derived" && (request.Kind != "tag-create" || request.KB != kb.ID) {
			return ErrKnowledgeInputChanged
		}
		if !create && request.Kind != "derived" && (request.Kind != "tag-update" || request.ID != tag.ID) {
			return ErrKnowledgeInputChanged
		}
		actual, lineage, origin, err := knowledgeDisplayWriterOrigin(tx, ctx, &kb, request, factory)
		if err != nil {
			return err
		}
		var old types.KnowledgeTag
		if !create {
			if err := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", tag.ID, tag.TenantID, tag.KnowledgeBaseID).Take(&old).Error; err != nil {
				return err
			}
		}
		for _, field := range []string{"name", "color", "sort_order"} {
			value := tagDisplayFieldValue(tag, field)
			if request.Kind != "derived" {
				input, provided := request.Fields[field]
				if provided && !reflect.DeepEqual(value, input) {
					return ErrKnowledgeInputChanged
				}
				if !provided && (create || !reflect.DeepEqual(value, tagDisplayFieldValue(&old, field))) {
					return ErrKnowledgeInputChanged
				}
			}
		}
		if create {
			err = tx.Create(tag).Error
		} else {
			err = tx.Save(tag).Error
		}
		if err != nil {
			return err
		}
		for _, field := range []string{"name", "color", "sort_order"} {
			value := tagDisplayFieldValue(tag, field)
			_, provided := request.Fields[field]
			if !create && request.Kind != "derived" && !provided {
				continue
			}
			if !create && reflect.DeepEqual(value, tagDisplayFieldValue(&old, field)) {
				continue
			}
			if err := admitKnowledgeDisplayOrigin(tx, actual, "tag-"+field, tag.ID, &kb, value, request, lineage, origin); err != nil {
				return err
			}
		}
		if _, wantsOutput := ctx.Value(tagCommittedOutputKey{}).(*tagCommittedOutput); wantsOutput {
			actualStore := NewKnowledgeInputProvenanceStore(tx)
			captured, captureErr = actualStore.captureTagOutputInTx(tx, types.TagOutputReadRequest{TenantID: tag.TenantID, KnowledgeBaseID: tag.KnowledgeBaseID, TagID: tag.ID})
		}
		return nil
	}, knowledgeDisplayWriteOptions(db))
	if slot, wantsOutput := ctx.Value(tagCommittedOutputKey{}).(*tagCommittedOutput); wantsOutput && err == nil {
		slot.snapshot, slot.err, slot.committed = captured, captureErr, true
	}
	return err
}

// The KB configuration writer calls this in its actual create/update tx. Only
// a supplied name input may establish a display origin; unrelated config edits
// cannot sign the current legacy name or replace its original dependency.
func admitKnowledgeBaseNameInput(tx *gorm.DB, ctx context.Context, old, actual *types.KnowledgeBase, factory KnowledgeDisplayAuthorityFactory) error {
	request, ok := ctx.Value(knowledgeDisplayRequestKey{}).(knowledgeDisplayRequest)
	if !ok {
		return nil
	}
	if old != nil && old.Name == actual.Name {
		return nil
	}
	if request.Kind != "derived" && (request.Kind != "kb-name" || request.ID != "" && request.ID != actual.ID || request.Fields["name"] != actual.Name) {
		return ErrKnowledgeInputChanged
	}
	actor, lineage, origin, err := knowledgeDisplayWriterOrigin(tx, ctx, actual, request, factory)
	if err != nil {
		return err
	}
	return admitKnowledgeDisplayOrigin(tx, actor, "kb-name", actual.ID, actual, actual.Name, request, lineage, origin)
}
