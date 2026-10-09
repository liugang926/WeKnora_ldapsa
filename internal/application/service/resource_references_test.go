package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func TestKBFileBindingRequiresLiveKnowledgeInExactKB(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/image.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	knowledge := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Type: "file"}
	require.NoError(t, db.Create(knowledge).Error)
	require.NoError(
		t,
		catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationExtractedImage),
	)
	lookup := catalog.(interfaces.KBResourceLookup)
	for _, reference := range []string{ref, "local://7/exports/image.png"} {
		ok, err := lookup.IsReferencedByKnowledgeBase(ctx, 7, "kb", reference)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = lookup.IsReferencedByKnowledgeBase(ctx, 7, "other-kb", reference)
		require.NoError(t, err)
		require.False(t, ok)
	}
	require.NoError(t, db.Delete(knowledge).Error)
	ok, err := lookup.IsReferencedByKnowledgeBase(ctx, 7, "kb", ref)
	require.NoError(t, err)
	require.False(t, ok, "a stale binding cannot authorize a deleted document")
}

func TestKBFileLegacyTextReferencesDoNotAuthorizeFiles(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	lookup := catalog.(interfaces.KBResourceLookup)
	const reference = "local://7/exports/image.png"
	chunk := &types.Chunk{
		ID:              "chunk",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Content:         "![image](" + reference + ".private)",
	}
	require.NoError(t, db.Create(chunk).Error)
	ok, err := lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, db.Model(chunk).Update("image_info", `[{"url":"`+reference+`"}]`).Error)
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok, "text is not an ownership binding")
	require.NoError(t, db.Delete(chunk).Error)
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok)
	page := &types.WikiPage{ID: "page", TenantID: 7, KnowledgeBaseID: "kb", Content: "![image](" + reference + ")"}
	require.NoError(t, db.Create(page).Error)
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok, "text is not an ownership binding")
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 8, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestResourceKnowledgeOwnersSurviveSoftDeleteAndBindingRelease(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}))
	ctx := context.Background()
	physical := "local://7/doc/source.pdf"
	ref, err := catalog.Register(ctx, 7, physical, interfaces.ResourceRegistration{})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	knowledge := &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb",
		Channel: types.ConnectorTypeNextcloud, FilePath: ref,
	}
	require.NoError(t, db.Create(knowledge).Error)
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationSourceFile))
	lookup := catalog.(interfaces.ResourceKnowledgeLookup)
	for _, path := range []string{ref, physical} {
		owners, recognized, err := lookup.ListResourceKnowledgeOwners(ctx, 7, path)
		require.NoError(t, err)
		require.True(t, recognized)
		require.Len(t, owners, 1)
		require.Equal(t, "doc", owners[0].ID)
	}
	require.NoError(t, db.Delete(knowledge).Error)
	_, err = catalog.Release(ctx, ref, types.ResourceOwnerKnowledge, "doc")
	require.NoError(t, err)
	owners, recognized, err := lookup.ListResourceKnowledgeOwners(ctx, 7, physical)
	require.NoError(t, err)
	require.True(t, recognized)
	require.Len(t, owners, 1)
	require.True(t, owners[0].DeletedAt.Valid)
	_, recognized, err = lookup.ListResourceKnowledgeOwners(ctx, 7, "local://7/unknown.pdf")
	require.NoError(t, err)
	require.False(t, recognized)
}

func TestNextcloudResourceProvenanceSurvivesHardDeleteAndMessageBinding(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	ctx := context.Background()
	reference, err := catalog.Register(ctx, 7, "local://7/source.pdf", interfaces.ResourceRegistration{
		SourceProvenance: types.ResourceProvenanceNextcloud,
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	doc := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud}
	require.NoError(t, db.Create(doc).Error)
	require.NoError(t, catalog.Bind(
		ctx,
		reference,
		types.ResourceOwnerKnowledge,
		doc.ID,
		types.ResourceRelationSourceFile,
	))
	require.NoError(t, catalog.Bind(
		ctx,
		reference,
		types.ResourceOwnerMessage,
		"message",
		types.ResourceRelationArtifact,
	))
	_, err = catalog.Release(ctx, reference, types.ResourceOwnerKnowledge, doc.ID)
	require.NoError(t, err)
	require.NoError(t, db.Unscoped().Delete(doc).Error)
	lookup := catalog.(interfaces.ResourceKnowledgeLookup)
	owners, recognized, err := lookup.ListResourceKnowledgeOwners(ctx, 7, reference)
	require.NoError(t, err)
	require.True(t, recognized)
	require.Empty(t, owners)
	provenance, err := lookup.GetResourceSourceProvenance(ctx, 7, reference)
	require.NoError(t, err)
	require.Equal(t, types.ResourceProvenanceNextcloud, provenance)
	_, err = catalog.Register(ctx, 7, "local://7/source.pdf", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	provenance, err = lookup.GetResourceSourceProvenance(ctx, 7, reference)
	require.NoError(t, err)
	require.Equal(t, types.ResourceProvenanceNextcloud, provenance, "re-registration cannot erase provenance")

	ordinary, err := catalog.Register(ctx, 7, "local://7/ordinary.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	provenance, err = lookup.GetResourceSourceProvenance(ctx, 7, ordinary)
	require.NoError(t, err)
	require.Equal(t, types.ResourceProvenanceOrdinary, provenance)
	ordinaryResource, err := catalog.Resolve(ctx, ordinary)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.StoredResource{}).Where("id = ?", ordinaryResource.ID).
		Update("source_provenance", types.ResourceProvenanceUnknown).Error)
	provenance, err = lookup.GetResourceSourceProvenance(ctx, 7, ordinary)
	require.NoError(t, err)
	require.Equal(t, types.ResourceProvenanceUnknown, provenance, "unclassified historical resource stays closed")
}

func TestKnowledgeBindingPromotesDerivedResourceProvenance(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	ctx := context.Background()
	reference, err := catalog.Register(ctx, 7, "local://7/extracted.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	doc := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud}
	require.NoError(t, db.Create(doc).Error)
	require.NoError(t, catalog.Bind(
		ctx,
		reference,
		types.ResourceOwnerKnowledge,
		doc.ID,
		types.ResourceRelationExtractedImage,
	))
	_, err = catalog.Release(ctx, reference, types.ResourceOwnerKnowledge, doc.ID)
	require.NoError(t, err)
	require.NoError(t, db.Unscoped().Delete(doc).Error)
	provenance, err := catalog.(interfaces.ResourceKnowledgeLookup).GetResourceSourceProvenance(ctx, 7, reference)
	require.NoError(t, err)
	require.Equal(t, types.ResourceProvenanceNextcloud, provenance)
}
