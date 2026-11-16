//go:build sqlite && sqlite_fts5

package dingtalk_test

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type reparseGroupGrant struct {
	interfaces.GroupAccessService
	edit bool
}

func (g *reparseGroupGrant) EffectiveTenantRole(context.Context, string, uint64, time.Time) (types.EffectiveTenantRole, error) {
	return types.EffectiveTenantRole{Member: true, Role: types.TenantRoleViewer}, nil
}
func (g *reparseGroupGrant) EffectivePermission(ctx context.Context, tenant uint64, resource types.ResourceType, id string, action types.ResourceAction, _ time.Time) (types.EffectiveResourcePermission, error) {
	allowed := g.edit && tenant == 7 && id == "kb" && resource == types.GroupResourceTypeKnowledgeBase &&
		action == types.ResourceActionEdit && types.CallerFromContext(ctx).UserID == "editor"
	return types.EffectiveResourcePermission{Mode: types.ResourceAccessRestricted, Allowed: allowed}, nil
}

func testScopedLocalReparse(t *testing.T, db *gorm.DB, store *repository.KnowledgeInputProvenanceStore, published *types.Knowledge, data []byte) {
	require.NoError(t, db.Create(&types.Tenant{ID: 7, Name: "fixture"}).Error)
	for _, model := range []types.Model{
		{ID: "summary", Name: "summary", Type: types.ModelTypeKnowledgeQA, TenantID: 7},
		{ID: "embed", Name: "embed", Type: types.ModelTypeEmbedding, TenantID: 7},
	} {
		require.NoError(t, db.Create(&model).Error)
	}
	grants := &reparseGroupGrant{edit: true}
	repository.ConfigureKnowledgeInputActorGroupAccess(store, grants)
	require.NoError(t, db.Create(&types.User{ID: "editor", Username: "editor", Email: "editor@example.test", IsActive: true}).Error)
	var before types.Knowledge
	var kb types.KnowledgeBase
	require.NoError(t, db.First(&before, "id = ?", published.ID).Error)
	require.NoError(t, db.First(&kb, "id = ?", published.KnowledgeBaseID).Error)
	caller := types.Caller{TenantID: kb.TenantID, UserID: "editor", Role: types.TenantRoleViewer}
	ctx := types.WithPrincipal(types.WithCaller(context.Background(), caller), types.Principal{Type: types.PrincipalWebUser, ID: "editor"})
	grant, err := access.ResolveKB(ctx, access.KBRequest{Caller: caller}, &kb, types.OrgRoleEditor, nil, nil)
	require.NoError(t, err)
	proposed := before
	_, err = store.AdmitDingTalkLocalReparse(ctx, &before, &proposed, &kb, data)
	require.ErrorIs(t, err, access.ErrForbidden)
	ctx = grant.Context(ctx)
	_, err = store.AuthorizeCurrentKnowledgeInputWrite(ctx, &kb)
	require.ErrorIs(t, err, access.ErrForbidden)
	badKB := kb
	badKB.ID = "other"
	_, err = store.AdmitDingTalkLocalReparse(ctx, &before, &proposed, &badKB, data)
	require.Error(t, err)
	_, err = store.AdmitDingTalkLocalReparse(ctx, &before, &proposed, &kb, []byte("substituted"))
	require.Error(t, err)
	proof, err := store.AdmitDingTalkLocalReparse(ctx, &before, &proposed, &kb, data)
	require.NoError(t, err)
	worker, err := repository.RestoreKnowledgeBuildActorIfRequired(context.Background(), store, proof)
	require.NoError(t, err)
	worker = repository.WithKnowledgeBuildInputProof(worker, store, proof)
	_, err = store.AuthorizeKnowledgeCurrentProducerWrite(worker, &kb)
	require.NoError(t, err)
	_, err = store.AuthorizeCurrentKnowledgeInputWrite(worker, &kb)
	require.ErrorIs(t, err, access.ErrForbidden)
	enrichment, err := store.CaptureKnowledgeEnrichmentInputs(worker, types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: kb.ID, KnowledgeID: before.ID, Expected: proof})
	require.NoError(t, err)
	control := "fixture summary"
	producer, err := store.AdmitKnowledgeProducerInput(worker, enrichment.Proof, "summary", control)
	require.NoError(t, err)
	resumed, err := store.RestoreKnowledgeProducerActor(context.Background(), enrichment.Proof, producer)
	require.NoError(t, err)
	resumed = repository.WithKnowledgeBuildInputProof(resumed, store, enrichment.Proof)
	require.NoError(t, store.RecheckKnowledgeProducerInput(resumed, enrichment.Proof, producer, control))
	proposed.ParseStatus = types.ParseStatusCompleted
	require.NoError(t, store.FinalizeDingTalkIndexedKnowledge(worker, &proposed))
	var current types.Knowledge
	require.NoError(t, db.First(&current, "id = ?", before.ID).Error)
	next := current
	nextProof, err := store.AdmitDingTalkLocalReparse(ctx, &current, &next, &kb, data)
	require.NoError(t, err)
	_, err = store.RestoreKnowledgeBuildActor(context.Background(), proof)
	require.ErrorIs(t, err, repository.ErrKnowledgeInputChanged)
	proof = nextProof
	worker, err = store.RestoreKnowledgeBuildActor(context.Background(), proof)
	require.NoError(t, err)
	worker = repository.WithKnowledgeBuildInputProof(worker, store, proof)
	grants.edit = false
	require.NoError(t, db.First(&current, "id = ?", before.ID).Error)
	next = current
	_, err = store.AdmitDingTalkLocalReparse(ctx, &current, &next, &kb, data)
	require.ErrorIs(t, err, access.ErrForbidden)
	_, err = store.RestoreKnowledgeBuildActor(context.Background(), proof)
	require.ErrorIs(t, err, access.ErrForbidden)
	require.Error(t, store.FinalizeDingTalkIndexedKnowledge(worker, &proposed))
}
