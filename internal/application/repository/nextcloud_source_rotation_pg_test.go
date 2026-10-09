package repository

import (
	"context"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// JSONB may reorder keys on write. A prepared hash must describe the bytes
// read back from PostgreSQL, or the active-source guard will reject rotation.
func TestPostgresNextcloudSourceRotationUsesPersistedJSONBHash(t *testing.T) {
	dsn := os.Getenv("WEKNORA_PAIRING_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WEKNORA_PAIRING_TEST_POSTGRES_DSN for disposable PostgreSQL")
	}
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	defer func() { require.NoError(t, sqlDB.Close()) }()
	schema := "pair_rotation_" + uuid.NewString()[:8]
	require.NoError(t, db.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
	defer db.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
	require.NoError(t, db.Exec(`SET search_path TO "`+schema+`"`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT FALSE, deleted_at TIMESTAMPTZ)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (id TEXT PRIMARY KEY, tenant_id BIGINT,
		knowledge_base_id TEXT, deleted_at TIMESTAMPTZ)`).Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}, &NextcloudSourcePairing{},
		&NextcloudSourceRotation{}))
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-paired', 7)`).Error)
	oldConfig := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Credentials: map[string]interface{}{"token": "old-secret", "key_id": "pair_old"},
		ResourceIDs: []string{"dev-published"},
		Settings:    map[string]interface{}{"base_url": "https://nextcloud.example.test"},
	}
	oldBlob, err := oldConfig.ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: "kb-paired",
		Type: types.ConnectorTypeNextcloud, Config: oldBlob, Status: types.DataSourceStatusActive,
	}
	require.NoError(t, db.Create(ds).Error)
	require.NoError(t, db.Where("id = ?", ds.ID).Take(ds).Error)
	_, oldHash, _, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	pair := &NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 7,
		KnowledgeBaseID: "kb-paired", DataSourceID: ds.ID, InstanceID: "instance-1",
		BindingID: "dev-published", BaseURL: "https://nextcloud.example.test",
		ConfigSHA: oldHash, PublicationEpoch: 0, KeyID: "pair_old", State: "active",
	}
	require.NoError(t, db.Create(pair).Error)
	repo := NewNextcloudSourcePairingRepository(db)
	ctx := context.Background()
	rotation := NextcloudSourceRotation{
		OperationID:     uuid.NewString(),
		PairOperationID: pair.OperationID, TenantID: 7, KnowledgeBaseID: pair.KnowledgeBaseID,
		DataSourceID: ds.ID, InstanceID: pair.InstanceID, BindingID: pair.BindingID,
		NewKeyID: "rot_" + uuid.NewString()[:8],
	}
	prepared, created, err := repo.PrepareSourceRotation(ctx, rotation, "new-secret")
	require.NoError(t, err)
	require.True(t, created)
	_, persistedHash, _, err := NextcloudEventDataSourceIdentity(prepared.NewConfig)
	require.NoError(t, err)
	require.Equal(t, persistedHash, prepared.NewConfigSHA)
	require.NoError(t, repo.SwitchSourceRotation(ctx, prepared))
	require.NoError(t, db.Where("id = ?", ds.ID).Take(ds).Error)
	var after NextcloudSourcePairing
	require.NoError(t, db.Where("operation_id = ?", pair.OperationID).Take(&after).Error)
	_, afterHash, _, hashErr := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, hashErr)
	require.Equal(t, prepared.NewConfigSHA, afterHash)
	require.Equal(t, prepared.NewConfigSHA, after.ConfigSHA)
	active, err := repo.HasActiveSourcePairing(ctx, ds)
	require.NoError(t, err)
	require.True(t, active)
}
