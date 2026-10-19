package repository

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// This test adapter reads current membership/policy from the writer tx. HTTP
// integration uses the real group service through the same factory interface.
type displayTestGroups struct {
	interfaces.GroupAccessService
	db *gorm.DB
}

func (g displayTestGroups) EffectiveTenantRole(
	ctx context.Context,
	id string,
	tenant uint64,
	now time.Time,
) (types.EffectiveTenantRole, error) {
	role, err := NewGroupAccessRepository(g.db).GetDirectTenantRole(ctx, id, tenant)
	result := types.EffectiveTenantRole{TenantID: tenant}
	if role != nil {
		result.Member = true
		result.Role = *role
	}
	return result, err
}

func (g displayTestGroups) EffectivePermission(
	ctx context.Context,
	tenant uint64,
	kind types.ResourceType,
	id string,
	action types.ResourceAction,
	now time.Time,
) (types.EffectiveResourcePermission, error) {
	policy, err := NewGroupAccessRepository(g.db).GetResourceAccessPolicy(ctx, tenant, kind, id)
	if err != nil {
		return types.EffectiveResourcePermission{}, err
	}
	return types.EffectiveResourcePermission{Allowed: policy == nil || policy.Mode == types.ResourceAccessInherit}, nil
}

func displayTestFixture(
	t *testing.T,
	pg bool,
) (*KnowledgeInputProvenanceStore, *gorm.DB, context.Context, interfaces.KnowledgeTagRepository, interfaces.KnowledgeRepository, *types.Knowledge) {
	t.Helper()
	var store *KnowledgeInputProvenanceStore
	var db *gorm.DB
	if pg {
		store, db = knowledgeInputPostgresFixture(t)
	} else {
		store, db = knowledgeInputFixture(t)
	}
	db.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(
		t,
		db.AutoMigrate(
			&types.User{},
			&types.TenantMember{},
			&types.TenantAPIKey{},
			&types.ResourceAccessPolicy{},
			&types.KnowledgeTag{},
			&types.KnowledgeTagRelation{},
		),
	)
	db.DisableForeignKeyConstraintWhenMigrating = false
	path := "../../../migrations/sqlite/000059_knowledge_collection_display_inputs.up.sql"
	migration := db
	if pg {
		path = "../../../migrations/versioned/000140_knowledge_collection_display_inputs.up.sql"
		sqlDB, err := db.DB()
		require.NoError(t, err)
		migration, err = gorm.Open(
			postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}),
			&gorm.Config{},
		)
		require.NoError(t, err)
	}
	script, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(script)).Error)
	require.NoError(
		t,
		db.Create(
			&types.User{
				ID:       "display-user",
				TenantID: 7,
				Username: "display-user",
				Email:    "display@example.test",
				IsActive: true,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.TenantMember{
				UserID:   "display-user",
				TenantID: 7,
				Role:     types.TenantRoleAdmin,
				Status:   types.TenantMemberStatusActive,
			},
		).Error,
	)
	ctx := types.WithCaller(
		context.Background(),
		types.Caller{TenantID: 7, UserID: "display-user", Role: types.TenantRoleAdmin},
	)
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "display-user"})
	ctx = context.WithValue(ctx, types.TenantIDContextKey, uint64(7))
	tags := NewKnowledgeTagRepository(db)
	knowledge := NewKnowledgeRepository(db)
	factory := func(tx *gorm.DB) *KnowledgeInputProvenanceStore {
		actual := NewKnowledgeInputProvenanceStore(tx)
		ConfigureKnowledgeInputActorGroupAccess(actual, displayTestGroups{db: tx})
		return actual
	}
	ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
	kb.Name = "actual human KB name"
	require.NoError(
		t,
		store.UpdateKnowledgeBaseWithInput(
			WithIndependentKnowledgeBaseNameInput(WithIndependentKnowledgeInput(ctx), kb.ID, kb.Name),
			&kb,
		),
	)
	_, k, _, _ := createAdmittedManual(t, store, db)
	return store, db, ctx, tags, knowledge, k
}

func captureDisplayTest(t *testing.T, db *gorm.DB, k *types.Knowledge) (*types.KnowledgeCollectionAuxiliary, error) {
	t.Helper()
	var snapshot *types.KnowledgeCollectionAuxiliary
	options := &sql.TxOptions{ReadOnly: true}
	if db.Dialector.Name() == "postgres" {
		options.Isolation = sql.LevelRepeatableRead
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		snapshot, err = captureKnowledgeCollectionAuxiliary(
			tx,
			[]types.KnowledgeSearchScope{{TenantID: k.TenantID, KBID: k.KnowledgeBaseID}},
			[]*types.Knowledge{k},
		)
		return err
	}, options)
	return snapshot, err
}

func displayTag(
	t *testing.T,
	repo interfaces.KnowledgeTagRepository,
	ctx context.Context,
	id string,
) *types.KnowledgeTag {
	t.Helper()
	tag := &types.KnowledgeTag{
		ID:              id,
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Name:            "actual human " + id,
		Color:           "blue",
		SortOrder:       3,
	}
	require.NoError(
		t,
		repo.Create(WithIndependentKnowledgeTagCreateInput(ctx, "kb", tag.Name, tag.Color, tag.SortOrder), tag),
	)
	return tag
}

func runDisplayActualInputAndABA(t *testing.T, pg bool) {
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	tag := displayTag(t, tags, ctx, "tag-one")
	require.NoError(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {tag.ID}}),
			k.ID,
			[]string{tag.ID},
		),
	)
	snapshot, err := captureDisplayTest(t, db, k)
	require.NoError(t, err)
	require.Equal(t, "actual human KB name", snapshot.KnowledgeBaseNames["kb"])
	require.Equal(t, tag.Name, snapshot.Tags[k.ID][0].Name)
	for _, proof := range snapshot.Proofs {
		require.NoError(t, proof.SourceLineage.RequireComplete())
		require.Empty(t, proof.SourceLineage.Sources)
	}
	require.Error(
		t,
		db.Table("knowledge_display_input_origins").
			Where("kind='tag-name' AND row_id=?", tag.ID).
			Update("source_lineage", `{"version":1,"state":"complete","sources":[]}`).
			Error,
	)
	oldName := tag.Name
	newName := "new explicit human name"
	tag.Name = newName
	require.NoError(t, tags.Update(WithIndependentKnowledgeTagUpdateInput(ctx, tag.ID, &newName, nil, nil), tag))
	current, err := captureDisplayTest(t, db, k)
	require.NoError(t, err)
	require.Equal(t, newName, current.Tags[k.ID][0].Name)
	require.Error(t, db.Transaction(func(tx *gorm.DB) error {
		return recheckKnowledgeCollectionAuxiliary(
			tx,
			[]types.KnowledgeSearchScope{{TenantID: 7, KBID: "kb"}},
			[]*types.Knowledge{k},
			snapshot,
		)
	}))
	require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id=?", tag.ID).Update("name", "raw ABA").Error)
	require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id=?", tag.ID).Update("name", newName).Error)
	_, err = captureDisplayTest(t, db, k)
	require.Error(t, err)
	_ = store
	_ = oldName
}
func TestKnowledgeDisplaySQLiteActualInputAndABA(t *testing.T) { runDisplayActualInputAndABA(t, false) }
func TestKnowledgeDisplayPostgresActualInputAndABA(t *testing.T) {
	runDisplayActualInputAndABA(t, true)
}

func runDisplayUnknownForgeryAndRetained(t *testing.T, pg bool) {
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	legacy := &types.KnowledgeTag{
		ID:              "legacy-tag",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Name:            "unknown model text",
		Color:           "red",
		SortOrder:       2,
	}
	require.NoError(
		t,
		tags.Create(WithIndependentKnowledgeInput(ctx), legacy),
		"generic ingestion origin cannot mark tag fields",
	)
	require.NoError(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {legacy.ID}}),
			k.ID,
			[]string{legacy.ID},
		),
	)
	_, err := captureDisplayTest(t, db, k)
	require.Error(t, err)
	name := legacy.Name
	require.NoError(t, tags.Update(WithIndependentKnowledgeTagUpdateInput(ctx, legacy.ID, &name, nil, nil), legacy))
	var count int64
	require.NoError(t, db.Table("knowledge_display_input_origins").Where("row_id=?", legacy.ID).Count(&count).Error)
	require.Zero(t, count, "same-value explicit request cannot authenticate unchanged legacy text")
	name = "actual replacement"
	legacy.Name = name
	require.NoError(t, tags.Update(WithIndependentKnowledgeTagUpdateInput(ctx, legacy.ID, &name, nil, nil), legacy))
	_, err = captureDisplayTest(t, db, k)
	require.Error(t, err, "unmodified legacy color/sort retain unknown origin")
	color := "green"
	order := 4
	legacy.Color = color
	legacy.SortOrder = order
	require.NoError(t, tags.Update(WithIndependentKnowledgeTagUpdateInput(ctx, legacy.ID, nil, &color, &order), legacy))
	_, err = captureDisplayTest(t, db, k)
	require.NoError(t, err)
	forged := types.WithCaller(
		context.Background(),
		types.Caller{TenantID: 7, UserID: "system-7", Role: types.TenantRoleOwner},
	)
	forged = types.WithPrincipal(forged, types.Principal{Type: types.PrincipalWebUser, ID: "system-7"})
	require.Error(
		t,
		tags.Create(
			WithIndependentKnowledgeTagCreateInput(forged, "kb", "forged", "", 0),
			&types.KnowledgeTag{ID: "forged", TenantID: 7, KnowledgeBaseID: "kb", Name: "forged"},
		),
	)
	require.NoError(
		t,
		db.Model(&types.TenantMember{}).Where("user_id='display-user'").Update("role", types.TenantRoleViewer).Error,
	)
	require.Error(
		t,
		tags.Create(
			WithIndependentKnowledgeTagCreateInput(ctx, "kb", "revoked", "", 0),
			&types.KnowledgeTag{ID: "revoked", TenantID: 7, KnowledgeBaseID: "kb", Name: "revoked"},
		),
		"cached admin role must not authorize current writer",
	)
	_ = store
}

func TestKnowledgeDisplaySQLiteUnknownForgeryAndRetained(t *testing.T) {
	runDisplayUnknownForgeryAndRetained(t, false)
}

func TestKnowledgeDisplayPostgresUnknownForgeryAndRetained(t *testing.T) {
	runDisplayUnknownForgeryAndRetained(t, true)
}

func runDisplayRelationshipABAAndWrongScope(t *testing.T, pg bool) {
	_, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	tag := displayTag(t, tags, ctx, "tag-relationship")
	snapshot, err := captureDisplayTest(t, db, k)
	require.NoError(t, err)
	require.NoError(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {tag.ID}}),
			k.ID,
			[]string{tag.ID},
		),
	)
	require.NoError(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {}}),
			k.ID,
			nil,
		),
	)
	require.Error(t, db.Transaction(func(tx *gorm.DB) error {
		return recheckKnowledgeCollectionAuxiliary(
			tx,
			[]types.KnowledgeSearchScope{{TenantID: 7, KBID: "kb"}},
			[]*types.Knowledge{k},
			snapshot,
		)
	}), "add/delete returns same body but keeps higher membership version")
	wrong := &types.KnowledgeTag{ID: "wrong-tag", TenantID: 8, KnowledgeBaseID: "other", Name: "wrong"}
	require.NoError(t, tags.Create(context.Background(), wrong))
	require.Error(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {wrong.ID}}),
			k.ID,
			[]string{wrong.ID},
		),
	)
	require.Error(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{"other-document": {tag.ID}}),
			k.ID,
			[]string{tag.ID},
		),
	)
}

func TestKnowledgeDisplaySQLiteRelationshipABAAndWrongScope(t *testing.T) {
	runDisplayRelationshipABAAndWrongScope(t, false)
}

func TestKnowledgeDisplayPostgresRelationshipABAAndWrongScope(t *testing.T) {
	runDisplayRelationshipABAAndWrongScope(t, true)
}

func runDisplayDerivedWithdrawal(t *testing.T, pg bool) {
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	sourceID := "candidate"
	if pg {
		sourceID = "doc-a"
	}
	_, source, err := knowledge.(*knowledgeRepository).ResolveNextcloudSourceLineage(ctx, 7, "kb", sourceID)
	require.NoError(t, err)
	lineage, err := types.NewCompleteSourceLineage(source)
	require.NoError(t, err)
	derived, err := WithDerivedKnowledgeInput(ctx, lineage)
	require.NoError(t, err)
	input := &types.Knowledge{
		ID:              "display-derived",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            types.KnowledgeTypeManual,
		Channel:         "web",
		CustomMetadata:  types.JSON(`{}`),
		ParseStatus:     types.ParseStatusCompleted,
		EnableStatus:    "enabled",
	}
	proof, err := store.CreateKnowledgeWithInput(derived, input, "actual sourced input")
	require.NoError(t, err)
	producer, err := WithDerivedKnowledgeDisplayInput(ctx, proof)
	require.NoError(t, err)
	tag := &types.KnowledgeTag{
		ID:              "derived-tag",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Name:            "sourced derived display",
		Color:           "blue",
		SortOrder:       2,
	}
	require.NoError(t, tags.Create(producer, tag))
	require.NoError(
		t,
		knowledge.SetKnowledgeTags(
			WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{k.ID: {tag.ID}}),
			k.ID,
			[]string{tag.ID},
		),
	)
	snapshot, err := captureDisplayTest(t, db, k)
	require.NoError(t, err)
	found := false
	for _, p := range snapshot.Proofs {
		if p.Kind == "tag-name" {
			require.Equal(t, lineage, p.SourceLineage)
			found = true
		}
	}
	require.True(t, found)
	sameName := tag.Name
	require.NoError(t, tags.Update(WithIndependentKnowledgeTagUpdateInput(ctx, tag.ID, &sameName, nil, nil), tag))
	current, err := captureDisplayTest(t, db, k)
	require.NoError(t, err)
	for _, p := range current.Proofs {
		if p.Kind == "tag-name" {
			require.Equal(t, lineage, p.SourceLineage)
		}
	}
	require.NoError(
		t,
		NewNextcloudContentLeaseStore(
			db,
		).RetireKnowledge(ctx, NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}),
	)
	_, err = captureDisplayTest(t, db, k)
	require.Error(t, err, "a same-value independent request never clears derived source dependency")
}
func TestKnowledgeDisplaySQLiteDerivedWithdrawal(t *testing.T) { runDisplayDerivedWithdrawal(t, false) }
func TestKnowledgeDisplayPostgresDerivedWithdrawal(t *testing.T) {
	runDisplayDerivedWithdrawal(t, true)
}

func runDisplayCurrentAPIKeyAndIngestionAssignment(t *testing.T, pg bool) {
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	tenant := uint64(7)
	key := &types.TenantAPIKey{
		ID:               31,
		TenantID:         &tenant,
		Name:             "actual key",
		ScopeType:        types.APIKeyScopeTenant,
		KeyHash:          "display-key-actual",
		KnowledgeBaseIDs: types.StringArray{"kb"},
		Capabilities:     types.StringArray{string(types.APIKeyCapabilityIngest)},
	}
	require.NoError(t, db.Create(key).Error)
	api := types.WithCaller(context.Background(), types.Caller{TenantID: 7, Role: types.TenantRoleOwner})
	api = types.WithPrincipal(api, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
	api = types.WithTenantAPIKeyScope(api, types.TenantAPIKeyScope{KeyID: key.ID, FullAccess: true})
	tag := displayTag(t, tags, api, "api-tag")
	original, err := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID},
	)
	require.NoError(t, err)
	inputCtx := WithKnowledgeBuildInputProof(
		WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{"": {tag.ID}}),
		store,
		original.Proof,
	)
	require.NoError(t, knowledge.SetKnowledgeTags(inputCtx, k.ID, []string{tag.ID}))
	_, err = captureDisplayTest(t, db, k)
	require.NoError(t, err)
	missingProof := WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{"": {tag.ID}})
	require.Error(
		t,
		knowledge.SetKnowledgeTags(missingProof, k.ID, []string{tag.ID}),
		"unknown new-document placeholder requires its actual admitted original proof",
	)
	require.NoError(
		t,
		db.Model(key).Update("capabilities", types.StringArray{string(types.APIKeyCapabilityRetrieve)}).Error,
	)
	name := "no ingest current"
	tag.Name = name
	require.Error(
		t,
		tags.Update(WithIndependentKnowledgeTagUpdateInput(api, tag.ID, &name, nil, nil), tag),
		"cached FullAccess never overrides persisted key capability revocation",
	)
	require.NoError(
		t,
		db.Model(key).
			Updates(map[string]any{"capabilities": types.StringArray{string(types.APIKeyCapabilityIngest)}, "knowledge_base_ids": types.StringArray{"other"}}).
			Error,
	)
	require.Error(
		t,
		tags.Update(WithIndependentKnowledgeTagUpdateInput(api, tag.ID, &name, nil, nil), tag),
		"current allow-list bounds the actual target",
	)
	require.NoError(
		t,
		db.Model(key).
			Updates(map[string]any{"capabilities": types.StringArray{string(types.APIKeyCapabilityManageKnowledgeBases)}, "knowledge_base_ids": types.StringArray{"kb"}}).
			Error,
	)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id='kb'").Take(&kb).Error)
	kb.Name = "API explicit rename"
	require.NoError(
		t,
		store.UpdateKnowledgeBaseWithInput(
			WithIndependentKnowledgeBaseNameInput(WithIndependentKnowledgeInput(api), kb.ID, kb.Name),
			&kb,
		),
		"current manage_kbs capability authorizes actual KB name edits without synthetic admin",
	)
	_, err = captureDisplayTest(t, db, k)
	require.NoError(t, err)
}

func TestKnowledgeDisplaySQLiteCurrentAPIKeyAndIngestionAssignment(t *testing.T) {
	runDisplayCurrentAPIKeyAndIngestionAssignment(t, false)
}

func TestKnowledgeDisplayPostgresCurrentAPIKeyAndIngestionAssignment(t *testing.T) {
	runDisplayCurrentAPIKeyAndIngestionAssignment(t, true)
}

func runAutoTagOriginalCompleteCandidateManifest(t *testing.T, pg bool) {
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	match := displayTag(t, tags, ctx, "automatic-match")
	sourceID := "candidate"
	if pg {
		sourceID = "doc-a"
	}
	_, source, err := knowledge.(*knowledgeRepository).ResolveNextcloudSourceLineage(ctx, 7, "kb", sourceID)
	require.NoError(t, err)
	lineage, err := types.NewCompleteSourceLineage(source)
	require.NoError(t, err)
	derived, err := WithDerivedKnowledgeInput(ctx, lineage)
	require.NoError(t, err)
	input := &types.Knowledge{
		ID:              "automatic-candidate-origin",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            types.KnowledgeTypeManual,
		Channel:         "web",
		CustomMetadata:  types.JSON(`{}`),
		ParseStatus:     types.ParseStatusCompleted,
		EnableStatus:    "enabled",
	}
	original, err := store.CreateKnowledgeWithInput(derived, input, "actual nonmatching sourced candidate input")
	require.NoError(t, err)
	producer, err := WithDerivedKnowledgeDisplayInput(ctx, original)
	require.NoError(t, err)
	nonmatch := &types.KnowledgeTag{
		ID:              "automatic-nonmatch",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Name:            "source-derived candidate never selected",
		SortOrder:       7,
	}
	require.NoError(t, tags.Create(producer, nonmatch))
	baseline, err := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{"manual-chunk"},
		},
	)
	require.NoError(t, err)
	manifest, err := store.CaptureKnowledgeAutoTagInputs(ctx, baseline.Proof)
	require.NoError(t, err)
	require.Len(t, manifest.Tags, 2)
	require.Equal(t, lineage, manifest.SourceLineage, "unselected candidate retains its original source dependency")
	require.NoError(t, store.RecheckKnowledgeAutoTagInputs(ctx, manifest))
	require.Error(
		t,
		db.Table("knowledge_auto_tag_input_manifests").
			Where("manifest_id=?", manifest.ManifestID).
			Update("actual_inputs", "{}").
			Error,
	)
	writeCtx, err := WithKnowledgeAutoTagInput(ctx, store, manifest)
	require.NoError(t, err)
	require.NoError(t, knowledge.AddKnowledgeTagRelations(writeCtx, 7, "kb", k.ID, []string{match.ID}))
	var relations []types.KnowledgeTagRelation
	require.NoError(t, db.Where("knowledge_id=?", k.ID).Find(&relations).Error)
	require.Len(t, relations, 1)
	proof, err := readKnowledgeDisplayOrigin(db, "relations", k.ID, 7, "kb", []string{match.ID})
	require.NoError(t, err)
	require.Equal(
		t,
		lineage,
		proof.SourceLineage,
		"the match depends on every actual model candidate, not only the selected tag",
	)
	require.NoError(
		t,
		NewNextcloudContentLeaseStore(
			db,
		).RetireKnowledge(ctx, NextcloudContentScope{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}),
	)
	_, err = captureDisplayTest(t, db, k)
	require.Error(t, err)
}

func TestKnowledgeAutoTagSQLiteOriginalCompleteCandidateManifest(t *testing.T) {
	runAutoTagOriginalCompleteCandidateManifest(t, false)
}

func TestKnowledgeAutoTagPostgresOriginalCompleteCandidateManifest(t *testing.T) {
	runAutoTagOriginalCompleteCandidateManifest(t, true)
}

func runAutoTagCandidateABAAndNewMembership(t *testing.T, pg bool) {
	store, db, ctx, tags, knowledge, k := displayTestFixture(t, pg)
	match := displayTag(t, tags, ctx, "automatic-candidate")
	baseline, err := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{"manual-chunk"},
		},
	)
	require.NoError(t, err)
	manifest, err := store.CaptureKnowledgeAutoTagInputs(ctx, baseline.Proof)
	require.NoError(t, err)
	require.NoError(
		t,
		db.Model(&types.KnowledgeTag{}).Where("id=?", match.ID).Update("name", "changed original candidate").Error,
	)
	require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id=?", match.ID).Update("name", match.Name).Error)
	require.Error(t, store.RecheckKnowledgeAutoTagInputs(ctx, manifest))
	writeCtx, err := WithKnowledgeAutoTagInput(ctx, store, manifest)
	require.NoError(t, err)
	require.Error(
		t,
		knowledge.AddKnowledgeTagRelations(writeCtx, 7, "kb", k.ID, []string{match.ID}),
		"the actual SQL writer rechecks the complete candidate manifest",
	)
	var count int64
	require.NoError(t, db.Model(&types.KnowledgeTagRelation{}).Where("knowledge_id=?", k.ID).Count(&count).Error)
	require.Zero(t, count)
}

func TestKnowledgeAutoTagSQLiteCandidateABAAndNewMembership(t *testing.T) {
	runAutoTagCandidateABAAndNewMembership(t, false)
}

func TestKnowledgeAutoTagPostgresCandidateABAAndNewMembership(t *testing.T) {
	runAutoTagCandidateABAAndNewMembership(t, true)
}

type autoTagSnapshotPauseKey struct{}

func TestKnowledgeAutoTagPostgresCompleteManifestUsesOneCoherentSnapshot(t *testing.T) {
	store, db, ctx, tags, _, k := displayTestFixture(t, true)
	tag := displayTag(t, tags, ctx, "coherent-candidate")
	baseline, err := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     k.ID,
			ChunkIDs:        []string{"manual-chunk"},
		},
	)
	require.NoError(t, err)
	fired := false
	name := "auto_tag_candidate_changed_after_list"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if fired || tx.Statement.Context.Value(autoTagSnapshotPauseKey{}) != true ||
			tx.Statement.Table != "knowledge_tags" {
			return
		}
		fired = true
		require.NoError(
			t,
			db.Model(&types.KnowledgeTag{}).Where("id=?", tag.ID).Update("name", "concurrently changed").Error,
		)
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	manifest, err := store.CaptureKnowledgeAutoTagInputs(
		context.WithValue(ctx, autoTagSnapshotPauseKey{}, true),
		baseline.Proof,
	)
	require.NoError(t, err)
	require.True(t, fired)
	require.Equal(
		t,
		tag.Name,
		manifest.Tags[0].Name,
		"RR keeps the actual candidate body, membership version and original field receipt in one snapshot",
	)
	require.Error(
		t,
		store.RecheckKnowledgeAutoTagInputs(ctx, manifest),
		"the next model/writer checkpoint rejects the committed candidate mutation",
	)
}
