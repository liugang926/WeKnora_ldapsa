package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudSourceRotationPersistsSecretAndSwitchesExactSource(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := provenanceTestDB(t)
	require.NoError(t, db.AutoMigrate(&NextcloudSourceRotation{}))
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-rotating', 7)`).Error)
	repo := NewNextcloudSourcePairingRepository(db)
	ctx := context.Background()
	pair, _, err := repo.PrepareSourcePairing(ctx, NextcloudSourcePairing{
		OperationID: "82d62225-1eb6-47ad-9264-2759e33dfa03", TenantID: 7,
		KnowledgeBaseID: "kb-rotating", InstanceID: "instance-1",
		BindingID: "dev-published", BaseURL: "https://nextcloud.example.test",
		PublicationEpoch: 0, KeyID: "pair_82d622251eb647ad92642759e33dfa03",
	}, "original-secret")
	require.NoError(t, err)
	require.NoError(t, repo.ActivateSourcePairing(ctx, pair))
	rotation := NextcloudSourceRotation{
		OperationID:     "6e359042-69aa-489e-bd9b-8754bf5beee1",
		PairOperationID: pair.OperationID, TenantID: 7,
		KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
		InstanceID: pair.InstanceID, BindingID: pair.BindingID,
		NewKeyID: "rot_6e35904269aa489ebd9b8754bf5beee1",
	}
	first, created, err := repo.PrepareSourceRotation(ctx, rotation, "replacement-secret")
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "pending", first.State)
	require.NotContains(t, string(first.NewConfig), "replacement-secret")
	require.Contains(t, string(first.NewConfig), "enc:v1:")
	var ds types.DataSource
	require.NoError(t, db.Where("id = ?", pair.DataSourceID).Take(&ds).Error)
	original := string(ds.Config)
	require.NoError(t, db.Exec(`UPDATE data_sources SET name = name WHERE id = ?`, ds.ID).Error)
	require.Equal(t, original, string(ds.Config))
	second, created, err := repo.PrepareSourceRotation(ctx, rotation, "replacement-secret")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.NewConfigSHA, second.NewConfigSHA)
	_, _, err = repo.PrepareSourceRotation(ctx, rotation, "wrong-secret")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	other := rotation
	other.OperationID = "07ab85f6-2f7e-43ca-b68b-733152469abd"
	other.NewKeyID = "rot_07ab85f62f7e43cab68b733152469abd"
	_, _, err = repo.PrepareSourceRotation(ctx, other, "other-secret")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)

	require.NoError(t, repo.SwitchSourceRotation(ctx, first))
	require.NoError(t, repo.SwitchSourceRotation(ctx, first))
	require.NoError(t, db.Where("id = ?", pair.DataSourceID).Take(&ds).Error)
	require.NotEqual(t, original, string(ds.Config))
	cfg, err := ds.ParseConfig()
	require.NoError(t, err)
	require.Equal(t, "replacement-secret", cfg.Credentials["token"])
	require.Equal(t, rotation.NewKeyID, cfg.Credentials["key_id"])
	var current NextcloudSourcePairing
	require.NoError(t, db.Where("operation_id = ?", pair.OperationID).Take(&current).Error)
	require.Equal(t, rotation.NewKeyID, current.KeyID)
	require.Equal(t, first.NewConfigSHA, current.ConfigSHA)
	active, err := repo.HasActiveSourcePairing(ctx, &ds)
	require.NoError(t, err)
	require.True(t, active)
	require.NoError(t, repo.FinalizeSourceRotation(ctx, first))
	require.NoError(t, repo.FinalizeSourceRotation(ctx, first))
	require.NoError(t, db.Where("operation_id = ?", rotation.OperationID).Take(&first).Error)
	require.Equal(t, "finalized", first.State)
	_, created, err = repo.PrepareSourceRotation(ctx, other, "other-secret")
	require.NoError(t, err)
	require.True(t, created, "a completed rotation permits the next one")
}
