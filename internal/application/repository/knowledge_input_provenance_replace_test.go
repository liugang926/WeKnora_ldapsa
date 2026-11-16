package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestReplaceKnowledgeWithInputSQLite(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.KnowledgeTag{}, &types.KnowledgeTagRelation{}))
	repo := NewKnowledgeRepository(db)
	source := &types.DataSource{ID: "source", TenantID: 7, KnowledgeBaseID: "kb", Type: "feishu_drive"}
	require.NoError(t, db.Create(source).Error)
	tag := types.KnowledgeTag{ID: "tag", TenantID: 7, KnowledgeBaseID: "kb", Name: "tag"}
	require.NoError(t, db.Create(&tag).Error)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	ctx, err := WithDerivedKnowledgeInput(context.Background(), lineage)
	require.NoError(t, err)
	previousInput := []byte("previous bytes")
	newInput := []byte("new bytes")
	before := &types.Knowledge{
		ID: "previous", TenantID: 7, KnowledgeBaseID: "kb", Type: "file", Channel: "feishu",
		Title: "previous.md", FilePath: "previous.md", FileName: "previous.md",
		ParseStatus: types.ParseStatusPending, EnableStatus: "disabled",
		Metadata: types.JSON(`{"datasource_id":"source","external_id":"remote-doc"}`),
	}
	previousProof, err := store.CreateKnowledgeWithInput(ctx, before, previousInput)
	require.NoError(t, err)
	require.NoError(t, repo.SetKnowledgeTags(ctx, before.ID, []string{tag.ID}))
	before, err = repo.GetKnowledgeByID(ctx, before.TenantID, before.ID)
	require.NoError(t, err)
	after := &types.Knowledge{
		ID: "replacement", TenantID: before.TenantID, KnowledgeBaseID: before.KnowledgeBaseID,
		Type: "file", Channel: "feishu",
		Title: "new.md", FilePath: "new.md", FileName: "new.md",
		ParseStatus: types.ParseStatusPending, EnableStatus: "disabled",
		Metadata: before.Metadata,
	}
	assertNoReplacement := func(t *testing.T) {
		t.Helper()
		actual, err := repo.GetKnowledgeByID(ctx, before.TenantID, before.ID)
		require.NoError(t, err)
		require.Equal(t, before.FilePath, actual.FilePath)
		require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, previousProof))
		var tags int64
		require.NoError(t, db.Model(&types.KnowledgeTagRelation{}).
			Where("knowledge_id = ? AND tag_id = ?", before.ID, tag.ID).Count(&tags).Error)
		require.EqualValues(t, 1, tags)
		var knowledgeCount, admissionCount, relationCount int64
		require.NoError(t, db.Unscoped().Model(&types.Knowledge{}).
			Where("id = ?", after.ID).Count(&knowledgeCount).Error)
		require.NoError(t, db.Model(&knowledgeInputAdmission{}).
			Where("knowledge_id = ?", after.ID).Count(&admissionCount).Error)
		require.NoError(t, db.Model(&types.KnowledgeTagRelation{}).
			Where("knowledge_id = ?", after.ID).Count(&relationCount).Error)
		require.Zero(t, knowledgeCount)
		require.Zero(t, admissionCount)
		require.Zero(t, relationCount)
	}
	t.Run("untrusted or mismatched fetch", func(t *testing.T) {
		_, err := store.ReplaceKnowledgeWithInput(context.Background(), before, after, nil, newInput)
		require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
		stale := *before
		stale.UpdatedAt = before.UpdatedAt.Add(time.Second)
		_, err = store.ReplaceKnowledgeWithInput(ctx, &stale, after, nil, newInput)
		require.ErrorContains(t, err, "knowledge changed")
		swapped := *after
		swapped.Metadata = types.JSON(`{"datasource_id":"source","external_id":"different"}`)
		_, err = store.ReplaceKnowledgeWithInput(ctx, before, &swapped, nil, newInput)
		require.ErrorIs(t, err, ErrKnowledgeInputChanged)
		assertNoReplacement(t)
	})
	t.Run("protected sources cannot replace through ordinary fetch", func(t *testing.T) {
		for _, protected := range []string{types.ConnectorTypeDingTalk, types.ConnectorTypeNextcloud} {
			require.NoError(t, db.Model(source).Update("type", protected).Error)
			_, err := store.ReplaceKnowledgeWithInput(ctx, before, after, nil, newInput)
			require.ErrorIs(t, err, ErrKnowledgeInputChanged)
		}
		require.NoError(t, db.Model(source).Update("type", "feishu_drive").Error)
		assertNoReplacement(t)
	})
	t.Run("rollback even after recording new input", func(t *testing.T) {
		require.NoError(t, db.Exec(`CREATE TRIGGER fail_tag BEFORE INSERT ON knowledge_tag_relations
			WHEN NEW.knowledge_id = 'replacement' BEGIN SELECT RAISE(FAIL, 'tag insert failed'); END`).Error)
		proof, err := store.ReplaceKnowledgeWithInput(ctx, before, after, []string{tag.ID}, newInput)
		require.ErrorContains(t, err, "tag insert failed")
		require.Nil(t, proof)
		require.NoError(t, db.Exec("DROP TRIGGER fail_tag").Error)
		assertNoReplacement(t)
	})
	t.Run("new original and tags survive retry", func(t *testing.T) {
		proof, err := store.ReplaceKnowledgeWithInput(ctx, before, after, []string{tag.ID}, newInput)
		require.NoError(t, err)
		require.NoError(t, KnowledgeInputArtifactMatches(proof, newInput))
		require.ErrorIs(t, KnowledgeInputArtifactMatches(proof, previousInput), ErrKnowledgeInputChanged)
		require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, proof))
		require.Error(t, store.RecheckKnowledgeBuildInputs(ctx, previousProof))
		var retired types.Knowledge
		require.NoError(t, db.Unscoped().Where("id = ?", before.ID).Take(&retired).Error)
		require.True(t, retired.DeletedAt.Valid)
		var relation types.KnowledgeTagRelation
		require.NoError(t, db.Where("knowledge_id = ? AND tag_id = ?", after.ID, tag.ID).Take(&relation).Error)
		racing := *after
		racing.ID = "racing"
		_, err = store.ReplaceKnowledgeWithInput(ctx, before, &racing, nil, newInput)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
		require.NoError(t, store.RecheckKnowledgeBuildInputs(ctx, proof))
	})
}

type replacementAckFunc func(context.Context, *types.Knowledge, *types.Knowledge, []string, any) (
	*types.KnowledgeBuildInputProof, error,
)

func (f replacementAckFunc) ReplaceKnowledgeWithInput(ctx context.Context, before, after *types.Knowledge,
	tags []string, input any,
) (*types.KnowledgeBuildInputProof, error) {
	return f(ctx, before, after, tags, input)
}

func TestKnowledgeReplacementReadbackSQLite(t *testing.T) {
	store, db := knowledgeInputFixture(t)
	for _, scenario := range []string{
		"committed", "other actor", "other replacement", "changed source", "new generation", "wrong bytes",
	} {
		t.Run(scenario, func(t *testing.T) { checkKnowledgeReplacementReadback(t, store, db, scenario) })
	}
}

func TestKnowledgeReplacementReadbackPostgres(t *testing.T) {
	store, db := knowledgeInputPostgresFixture(t)
	checkKnowledgeReplacementReadback(t, store, db, "committed")
}

func checkKnowledgeReplacementReadback(t *testing.T, store *KnowledgeInputProvenanceStore,
	db *gorm.DB, scenario string,
) {
	t.Helper()
	if !db.Migrator().HasTable(&types.DataSource{}) {
		require.NoError(t, db.AutoMigrate(&types.DataSource{}))
	}
	source := &types.DataSource{ID: scenario, TenantID: 7, KnowledgeBaseID: "kb", Type: "feishu_drive"}
	require.NoError(t, db.Create(source).Error)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	ctx, err := WithDerivedKnowledgeInput(context.Background(), lineage)
	require.NoError(t, err)
	before := &types.Knowledge{
		ID: scenario + "-old", TenantID: 7, KnowledgeBaseID: "kb", Type: "file", Channel: "feishu",
		FileName: "old.md", FilePath: "old.md", ParseStatus: types.ParseStatusPending,
	}
	before.Metadata, err = json.Marshal(map[string]string{"datasource_id": source.ID, "external_id": "file"})
	require.NoError(t, err)
	_, err = store.CreateKnowledgeWithInput(ctx, before, []byte("old"))
	require.NoError(t, err)
	before, err = NewKnowledgeRepository(db).GetKnowledgeByID(ctx, 7, before.ID)
	require.NoError(t, err)
	after := &types.Knowledge{
		ID: scenario + "-new", TenantID: 7, KnowledgeBaseID: "kb", Type: "file", Channel: before.Channel,
		FileName: "new.md", FilePath: "new.md", ParseStatus: types.ParseStatusPending, Metadata: before.Metadata,
	}
	input := []byte("same bytes")
	ackErr := errors.New("acknowledgement lost")
	var written *types.KnowledgeBuildInputProof
	writer := replacementAckFunc(func(writeCtx context.Context, old, next *types.Knowledge, tags []string,
		artifact any,
	) (*types.KnowledgeBuildInputProof, error) {
		switch scenario {
		case "other actor":
			writeCtx = context.WithValue(writeCtx, types.CallerContextKey, types.Caller{TenantID: 7, UserID: "other"})
			actor := types.Principal{Type: types.PrincipalWebUser, ID: "other"}
			writeCtx = context.WithValue(writeCtx, types.PrincipalContextKey, actor)
		case "other replacement":
			next.ID += "-other"
		case "wrong bytes":
			artifact = []byte("different")
		}
		var writeErr error
		written, writeErr = store.ReplaceKnowledgeWithInput(writeCtx, old, next, tags, artifact)
		require.NoError(t, writeErr)
		switch scenario {
		case "changed source":
			require.NoError(t, db.Model(source).Update("type", "notion").Error)
		case "new generation":
			next.Title = "another admission for identical bytes"
			other, err := store.UpdateKnowledgeWithInput(ctx, next, input)
			require.NoError(t, err)
			require.NotEqual(t, written.AdmissionID, other.AdmissionID)
		}
		return nil, ackErr
	})
	proof, err := ReplaceKnowledgeWithConfirmedInput(ctx, writer, before, after, nil, input)
	if scenario == "committed" {
		require.NoError(t, err)
		require.Equal(t, written, proof, "return the exact original proof, not a newly captured projection")
	} else {
		require.ErrorIs(t, err, ackErr)
		require.Nil(t, proof)
	}
}
