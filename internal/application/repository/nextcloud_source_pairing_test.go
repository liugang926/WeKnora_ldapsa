package repository

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestNextcloudSourcePairingPrepareRetryAndActivate(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := provenanceTestDB(t)
	require.NoError(t, db.Exec(("INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb" +
		"-paired', 7), ('kb-other', 7)")).Error)
	repo := NewNextcloudSourcePairingRepository(db)
	ctx := context.Background()
	proposal := NextcloudSourcePairing{
		OperationID: "82d62225-1eb6-47ad-9264-2759e33dfa03",
		TenantID:    7, KnowledgeBaseID: "kb-paired", InstanceID: "instance-1",
		BindingID: "dev-published", BaseURL: "https://nextcloud.example.test",
		PublicationEpoch: 0, KeyID: "pair_82d622251eb647ad92642759e33dfa03",
	}
	first, created, err := repo.PrepareSourcePairing(ctx, proposal, "one-time-pairing-token")
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, "pending", first.State)
	require.NotEmpty(t, first.DataSourceID)
	var stored types.DataSource
	require.NoError(t, db.Where("id = ?", first.DataSourceID).Take(&stored).Error)
	require.Equal(t, types.DataSourceStatusPaused, stored.Status)
	require.NotContains(t, string(stored.Config), "one-time-pairing-token")
	require.Contains(t, string(stored.Config), "enc:v1:")
	active, err := repo.HasActiveSourcePairing(ctx, &stored)
	require.NoError(t, err)
	require.False(t, active)

	second, created, err := repo.PrepareSourcePairing(ctx, proposal, "one-time-pairing-token")
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, first.DataSourceID, second.DataSourceID)
	_, _, err = repo.PrepareSourcePairing(ctx, proposal, "wrong-token")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	other := proposal
	other.OperationID = "ef085bb9-1b29-4c77-a620-33883fe9c730"
	other.KnowledgeBaseID = "kb-other"
	_, _, err = repo.PrepareSourcePairing(ctx, other, "one-time-pairing-token")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)

	require.NoError(t, repo.ActivateSourcePairing(ctx, first))
	require.NoError(t, repo.ActivateSourcePairing(ctx, first))
	require.NoError(t, db.Where("id = ?", first.DataSourceID).Take(&stored).Error)
	require.Equal(t, types.DataSourceStatusActive, stored.Status)
	active, err = repo.HasActiveSourcePairing(ctx, &stored)
	require.NoError(t, err)
	require.True(t, active)
	active, err = repo.ActiveForSync(ctx, stored.ID, stored.TenantID, "cloned-instance")
	require.NoError(t, err)
	require.False(t, active)
	active, err = repo.ActiveForSync(ctx, stored.ID, stored.TenantID, proposal.InstanceID)
	require.NoError(t, err)
	require.True(t, active)
	changed := stored
	changed.Config = types.JSON(strings.ReplaceAll(string(stored.Config), "dev-published", "other-binding"))
	require.ErrorIs(t, NewDataSourceRepository(db).Update(ctx, &changed), ErrNextcloudSourcePairingConflict)
	require.ErrorIs(t, NewDataSourceRepository(db).Delete(ctx, stored.ID), ErrNextcloudSourcePairingConflict)
}

func TestNextcloudSourcePairingPostgresJSONBActivation(t *testing.T) {
	dsn := os.Getenv("NEXTCLOUD_EVENT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set NEXTCLOUD_EVENT_TEST_POSTGRES_DSN for isolated PostgreSQL pairing test")
	}
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	random := make([]byte, 6)
	_, err = rand.Read(random)
	require.NoError(t, err)
	schema := "nextcloud_pairing_test_" + hex.EncodeToString(random)
	require.NoError(t, db.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() { _ = db.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	require.NoError(t, db.Exec("SET search_path TO "+schema).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT FALSE,
		deleted_at TIMESTAMPTZ
	)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (
		id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL,
		knowledge_base_id TEXT NOT NULL
	)`).Error)
	require.NoError(t, db.AutoMigrate(&types.DataSource{}))
	require.NoError(t, db.Exec(`CREATE TABLE sync_logs (
		id TEXT PRIMARY KEY, tenant_id BIGINT NOT NULL,
		data_source_id TEXT NOT NULL, status TEXT NOT NULL
	)`).Error)
	migrationDB, err := gorm.Open(postgres.Open(dsn+" default_query_exec_mode=simple_protocol"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	migrationSQLDB, err := migrationDB.DB()
	require.NoError(t, err)
	migrationSQLDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = migrationSQLDB.Close() })
	require.NoError(t, migrationDB.Exec("SET search_path TO "+schema).Error)
	for _, name := range []string{
		"000116_nextcloud_source_pairing.up.sql",
		"000117_nextcloud_source_pairing_guards.up.sql",
		"000120_nextcloud_source_pairing_abort.up.sql",
	} {
		migration, err := os.ReadFile("../../../migrations/versioned/" + name)
		require.NoError(t, err)
		require.NoError(t, migrationDB.Exec(string(migration)).Error)
	}
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id)
		VALUES ('kb-paired', 7)`).Error)

	repo := NewNextcloudSourcePairingRepository(db)
	ctx := context.Background()
	proposal := NextcloudSourcePairing{
		OperationID: "82d62225-1eb6-47ad-9264-2759e33dfa03", TenantID: 7,
		KnowledgeBaseID: "kb-paired", InstanceID: "instance-1",
		BindingID: "dev-published", BaseURL: "https://nextcloud.example.test",
		PublicationEpoch: 0, KeyID: "pair_82d622251eb647ad92642759e33dfa03",
	}
	pair, created, err := repo.PrepareSourcePairing(ctx, proposal, "one-time-pairing-token")
	require.NoError(t, err)
	require.True(t, created)
	var stored types.DataSource
	require.NoError(t, db.Where("id = ?", pair.DataSourceID).Take(&stored).Error)
	_, storedHash, _, err := NextcloudEventDataSourceIdentity(stored.Config)
	require.NoError(t, err)
	require.Equal(t, storedHash, pair.ConfigSHA, "pairing must pin the persisted jsonb representation")
	require.NoError(t, repo.ActivateSourcePairing(ctx, pair))
	require.NoError(t, repo.ActivateSourcePairing(ctx, pair))
	active, err := repo.ActiveForSync(ctx, pair.DataSourceID, pair.TenantID, pair.InstanceID)
	require.NoError(t, err)
	require.True(t, active)

	// Model a pending row written by the old path, where the digest was
	// taken before PostgreSQL converted the config to jsonb.
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id)
		VALUES ('kb-legacy', 7)`).Error)
	legacyProposal := proposal
	legacyProposal.OperationID = "a07c4976-b108-4e88-95ea-2294b4966504"
	legacyProposal.KeyID = "pair_a07c4976b1084e8895ea2294b4966504"
	legacyProposal.KnowledgeBaseID = "kb-legacy"
	legacyProposal.BindingID = "dev-legacy"
	legacyPair, created, err := repo.PrepareSourcePairing(ctx, legacyProposal, "legacy-pairing-token")
	require.NoError(t, err)
	require.True(t, created)
	stored = types.DataSource{}
	require.NoError(t, db.Where("id = ?", legacyPair.DataSourceID).Take(&stored).Error)
	var original types.DataSourceConfig
	require.NoError(t, json.Unmarshal(stored.Config, &original))
	oldJSON, err := json.Marshal(&original)
	require.NoError(t, err)
	oldSum := sha256.Sum256(oldJSON)
	legacyHash := hex.EncodeToString(oldSum[:])
	require.NotEqual(t, legacyPair.ConfigSHA, legacyHash)
	// Simulate the old prepare implementation before the guard was installed.
	// A digest that cannot be reconstructed must never be repaired.
	require.NoError(t, db.Exec(`DROP TRIGGER nextcloud_source_pairing_row_guard_update
		ON nextcloud_source_pairings`).Error)
	wrongHash := strings.Repeat("0", 64)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).
		Where("operation_id = ?", legacyPair.OperationID).
		UpdateColumn("datasource_config_sha256", wrongHash).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER nextcloud_source_pairing_row_guard_update
		BEFORE UPDATE ON nextcloud_source_pairings FOR EACH ROW
		EXECUTE FUNCTION nextcloud_source_pairing_row_guard()`).Error)
	legacyPair.ConfigSHA = wrongHash
	require.ErrorIs(t, repo.ActivateSourcePairing(ctx, legacyPair), ErrNextcloudSourcePairingConflict)
	current, currentSource, err := repo.SourcePairing(ctx, legacyPair.TenantID, legacyPair.OperationID)
	require.NoError(t, err)
	require.Equal(t, "pending", current.State)
	require.Equal(t, types.DataSourceStatusPaused, currentSource.Status)

	// Restore the actual pre-jsonb digest from the old prepare path. The
	// immutable row guard remains enabled during recovery and activation.
	require.NoError(t, db.Exec(`DROP TRIGGER nextcloud_source_pairing_row_guard_update
		ON nextcloud_source_pairings`).Error)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).
		Where("operation_id = ?", legacyPair.OperationID).
		UpdateColumn("datasource_config_sha256", legacyHash).Error)
	require.NoError(t, db.Exec(`CREATE TRIGGER nextcloud_source_pairing_row_guard_update
		BEFORE UPDATE ON nextcloud_source_pairings FOR EACH ROW
		EXECUTE FUNCTION nextcloud_source_pairing_row_guard()`).Error)
	legacyPair.ConfigSHA = legacyHash
	require.NoError(t, repo.ActivateSourcePairing(ctx, legacyPair))
	require.NoError(t, db.Where("id = ?", legacyPair.DataSourceID).Take(&stored).Error)
	require.Equal(t, types.DataSourceStatusActive, stored.Status)
	current, _, err = repo.SourcePairing(ctx, legacyPair.TenantID, legacyPair.OperationID)
	require.NoError(t, err)
	require.Equal(t, "active", current.State)
	require.Equal(t, storedHashForPairing(t, stored), current.ConfigSHA)
	require.NoError(t, repo.ActivateSourcePairing(ctx, current))
}

func storedHashForPairing(t *testing.T, ds types.DataSource) string {
	t.Helper()
	_, hash, _, err := NextcloudEventDataSourceIdentity(ds.Config)
	require.NoError(t, err)
	return hash
}

func TestNextcloudPendingSourcePairingAbortReleasesEmptySourceAndRetainsTombstone(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := provenanceTestDB(t)
	require.NoError(t, db.Exec(`CREATE TABLE sync_logs (data_source_id TEXT)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_event_connections (datasource_id TEXT)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-abort', 7)`).Error)
	repo := NewNextcloudSourcePairingRepository(db)
	ctx := context.Background()
	proposal := NextcloudSourcePairing{
		OperationID: "9bb82a4e-5522-4b5a-bc7c-5e70b64b32c7",
		TenantID:    7, KnowledgeBaseID: "kb-abort", InstanceID: "instance-1",
		BindingID: "abort-binding", BaseURL: "https://nextcloud.example.test",
		PublicationEpoch: 1, KeyID: "pair_9bb82a4e55224b5abc7c5e70b64b32c7",
	}
	pending, created, err := repo.PrepareSourcePairing(ctx, proposal, "one-time-abort-token")
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, repo.AbortPendingSourcePairing(ctx, pending))
	require.NoError(t, repo.AbortPendingSourcePairing(ctx, pending), "lost local ACK is idempotent")
	status, source, err := repo.SourcePairing(ctx, 7, pending.OperationID)
	require.NoError(t, err)
	require.Nil(t, source)
	require.Equal(t, "aborted", status.State)
	require.Equal(t, pending.DataSourceID, status.DataSourceID)
	var count int64
	require.NoError(t, db.Unscoped().Model(&types.DataSource{}).
		Where("id = ?", pending.DataSourceID).Count(&count).Error)
	require.Zero(t, count, "aborted paused source must not retain its encrypted token")
	_, _, err = repo.PrepareSourcePairing(ctx, proposal, "one-time-abort-token")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	replacement := proposal
	replacement.OperationID = "ce47ac5f-4a4d-429b-9a68-595ba6c88b44"
	replacement.KeyID = "pair_ce47ac5f4a4d429b9a68595ba6c88b44"
	newPair, created, err := repo.PrepareSourcePairing(ctx, replacement, "new-abort-token")
	require.NoError(t, err)
	require.True(t, created)
	require.NotEqual(t, pending.DataSourceID, newPair.DataSourceID)
	require.NoError(t, repo.ActivateSourcePairing(ctx, newPair))
	require.ErrorIs(t, repo.AbortPendingSourcePairing(ctx, newPair),
		ErrNextcloudSourcePairingConflict)
}
