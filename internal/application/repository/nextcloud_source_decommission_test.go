package repository

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func decommissionTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := provenanceTestDB(t)
	for _, statement := range []string{
		`CREATE TABLE sync_logs (id TEXT PRIMARY KEY, data_source_id TEXT, status TEXT)`,
		`CREATE TABLE nextcloud_event_connections (connection_id TEXT PRIMARY KEY, datasource_id TEXT)`,
		`CREATE TABLE nextcloud_event_inbox (connection_id TEXT, event_id INTEGER)`,
		`CREATE TABLE nextcloud_event_dispatch (connection_id TEXT, state TEXT)`,
		`CREATE TABLE nextcloud_event_checkpoint (connection_id TEXT, received_id INTEGER)`,
		`CREATE TABLE nextcloud_source_versions (id TEXT PRIMARY KEY, datasource_id TEXT)`,
		`CREATE TABLE nextcloud_gc_jobs (id TEXT PRIMARY KEY, datasource_id TEXT)`,
		`CREATE TABLE chunks (id TEXT PRIMARY KEY, knowledge_base_id TEXT)`,
		`CREATE TABLE nextcloud_source_rotations (operation_id TEXT PRIMARY KEY, pair_operation_id TEXT, state TEXT)`,
	} {
		require.NoError(t, db.Exec(statement).Error)
	}
	return db
}

func decommissionPair(t *testing.T, db *gorm.DB, kb, op string) NextcloudSourcePairing {
	t.Helper()
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES (?, 7)`, kb).Error)
	repo := NewNextcloudSourcePairingRepository(db)
	pair, _, err := repo.PrepareSourcePairing(context.Background(), NextcloudSourcePairing{
		OperationID: op, TenantID: 7, KnowledgeBaseID: kb, InstanceID: "instance-1",
		BindingID: kb, BaseURL: "https://nextcloud.example.test", PublicationEpoch: 0,
		KeyID: "pair_" + removeUUIDDashes(op),
	}, "one-time-pairing-token")
	require.NoError(t, err)
	require.NoError(t, repo.ActivateSourcePairing(context.Background(), pair))
	return pair
}

func removeUUIDDashes(value string) string {
	result := make([]byte, 0, len(value))
	for i := range value {
		if value[i] != '-' {
			result = append(result, value[i])
		}
	}
	return string(result)
}

func TestEmptyDecommissionRequiresVirginPairAndFencesFutureWrites(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := decommissionTestDB(t)
	old := decommissionPair(t, db, "kb-old", "82d62225-1eb6-47ad-9264-2759e33dfa03")
	repo := NewNextcloudSourcePairingRepository(db)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-old-pending', 7)`).Error)
	oldPending, _, err := repo.PrepareSourcePairing(context.Background(), NextcloudSourcePairing{
		OperationID: "d61cc09c-d73e-4479-a7d9-3287261bb34f", TenantID: 7,
		KnowledgeBaseID: "kb-old-pending", InstanceID: "instance-1",
		BindingID: "kb-old-pending", BaseURL: "https://nextcloud.example.test",
		PublicationEpoch: 0, KeyID: "pair_d61cc09cd73e4479a7d93287261bb34f",
	}, "old-pending-token")
	require.NoError(t, err)
	script, err := os.ReadFile("../../../migrations/sqlite/000042_nextcloud_empty_decommission.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	fortyTwo := decommissionPair(t, db, "kb-v42", "5f458442-1560-4f2c-9d0c-1426aeeb0309")
	var virgin bool
	require.NoError(t, db.Raw(`SELECT ever_touched FROM nextcloud_source_virgin
		WHERE pair_operation_id = ?`, fortyTwo.OperationID).Scan(&virgin).Error)
	require.False(t, virgin)
	script, err = os.ReadFile("../../../migrations/sqlite/000043_nextcloud_virgin_rebuild_guard.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	require.NoError(t, db.Raw(`SELECT ever_touched FROM nextcloud_source_virgin
		WHERE pair_operation_id = ?`, fortyTwo.OperationID).Scan(&virgin).Error)
	require.True(t, virgin, "a 42-era virgin marker cannot be trusted after the upgrade")
	proposed := func(pair NextcloudSourcePairing, operationID string) NextcloudSourceDecommission {
		return NextcloudSourceDecommission{
			OperationID:     operationID,
			PairOperationID: pair.OperationID, TenantID: pair.TenantID,
			KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
			InstanceID: pair.InstanceID, BindingID: pair.BindingID, KeyID: pair.KeyID,
			PublicationEpoch: 1,
		}
	}
	_, err = repo.BeginEmptySourceDecommission(context.Background(), old,
		proposed(old, "43c2678e-e5e5-4cf6-b975-18de9999cba9"))
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict,
		"a pre-migration pair has no durable proof of never having been indexed")
	_, err = repo.BeginEmptySourceDecommission(context.Background(), fortyTwo,
		proposed(fortyTwo, "92a3d8f1-2c1d-4918-8d02-7a89772bda65"))
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict,
		"a 42-era marker must be invalidated even when it currently says virgin")

	// The old PostgreSQL JSONB digest repair deletes and reinserts a pending
	// pairing. Simulate the same persisted-JSON mismatch on SQLite and require
	// the recreated row to inherit its old, unproven lineage.
	var oldSource types.DataSource
	require.NoError(t, db.Where("id = ?", oldPending.DataSourceID).Take(&oldSource).Error)
	var oldConfig types.DataSourceConfig
	require.NoError(t, json.Unmarshal(oldSource.Config, &oldConfig))
	compact, err := json.Marshal(&oldConfig)
	require.NoError(t, err)
	require.NoError(t, db.Exec(`UPDATE data_sources SET config = ? WHERE id = ?`,
		string(append([]byte("  "), compact...)), oldPending.DataSourceID).Error)
	require.NoError(t, repo.ActivateSourcePairing(context.Background(), oldPending))
	require.NoError(t, db.Raw(`SELECT ever_touched FROM nextcloud_source_virgin
		WHERE pair_operation_id = ?`, oldPending.OperationID).Scan(&virgin).Error)
	require.True(t, virgin, "pair reconstruction must not mint a false virgin proof")
	_, err = repo.BeginEmptySourceDecommission(context.Background(), oldPending,
		proposed(oldPending, "0ea06c83-1112-4858-8931-4ac5c1351981"))
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)

	// Even a pair first created after 43 loses its virgin proof if the same
	// operation ID is ever deleted and rebuilt.
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id, tenant_id) VALUES ('kb-new-pending', 7)`).Error)
	newPending, _, err := repo.PrepareSourcePairing(context.Background(), NextcloudSourcePairing{
		OperationID: "54f25b40-5fb9-4754-b29a-59c74eeb5e75", TenantID: 7,
		KnowledgeBaseID: "kb-new-pending", InstanceID: "instance-1",
		BindingID: "kb-new-pending", BaseURL: "https://nextcloud.example.test",
		PublicationEpoch: 0, KeyID: "pair_54f25b405fb94754b29a59c74eeb5e75",
	}, "new-pending-token")
	require.NoError(t, err)
	require.NoError(t, db.Raw(`SELECT ever_touched FROM nextcloud_source_virgin
		WHERE pair_operation_id = ?`, newPending.OperationID).Scan(&virgin).Error)
	require.False(t, virgin)
	require.NoError(t, db.Where("operation_id = ?", newPending.OperationID).
		Delete(&NextcloudSourcePairing{}).Error)
	// The production FK cascades this row; the small SQLite test schema may
	// run without foreign keys enabled, so remove any surviving test row.
	require.NoError(t, db.Exec(`DELETE FROM nextcloud_source_virgin WHERE pair_operation_id = ?`,
		newPending.OperationID).Error)
	require.NoError(t, db.Create(&newPending).Error)
	require.NoError(t, db.Raw(`SELECT ever_touched FROM nextcloud_source_virgin
		WHERE pair_operation_id = ?`, newPending.OperationID).Scan(&virgin).Error)
	require.True(t, virgin, "a rebuilt new pair must inherit nonvirgin lineage")
	require.NoError(t, repo.ActivateSourcePairing(context.Background(), newPending))
	_, err = repo.BeginEmptySourceDecommission(context.Background(), newPending,
		proposed(newPending, "d4835dba-5b59-4c16-9190-72f9a5522f7b"))
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)

	fresh := decommissionPair(t, db, "kb-fresh", "1cf17b55-26a2-4c04-bc9a-9c47eaa561b7")
	require.NoError(t, db.Raw(`SELECT ever_touched FROM nextcloud_source_virgin
		WHERE pair_operation_id = ?`, fresh.OperationID).Scan(&virgin).Error)
	require.False(t, virgin, "a truly new pair must still receive a virgin proof")
	intent := proposed(fresh, "be4c7f4d-5082-47fa-a54f-24ad09a05830")
	row, err := repo.BeginEmptySourceDecommission(context.Background(), fresh, intent)
	require.NoError(t, err)
	require.Equal(t, "prepared", row.State)
	var ds types.DataSource
	require.NoError(t, db.Where("id = ?", fresh.DataSourceID).Take(&ds).Error)
	require.Equal(t, types.DataSourceStatusPaused, ds.Status)
	require.Error(t, db.Model(&types.DataSource{}).Where("id = ?", ds.ID).
		Update("status", types.DataSourceStatusActive).Error,
		"direct SQL resume must be rejected after decommission begins")
	require.Error(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status) VALUES ('late', ?, 'running')`,
		fresh.DataSourceID).Error)
	require.Error(t, db.Exec(`INSERT INTO knowledges (id, tenant_id, knowledge_base_id) VALUES ('late', 7, ?)`,
		fresh.KnowledgeBaseID).Error)
	require.NoError(t, repo.RecordEmptyDecommissionAck(context.Background(), row))
	retry, err := repo.BeginEmptySourceDecommission(context.Background(), fresh, intent)
	require.NoError(t, err)
	require.Equal(t, "acknowledged", retry.State)
	require.Equal(t, EmptyNextcloudInventorySHA256, retry.InventorySHA256)
	require.NoError(t, repo.RecordEmptyDecommissionAck(context.Background(), retry))
	changed := intent
	changed.OperationID = "74ca926b-981f-460f-b9f7-b8db38f1daf4"
	_, err = repo.BeginEmptySourceDecommission(context.Background(), fresh, changed)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)

	touched := decommissionPair(t, db, "kb-touched", "6c391711-6425-4d11-8e6e-5ff693149996")
	require.NoError(t, db.Exec(`INSERT INTO sync_logs (id, data_source_id, status) VALUES ('first', ?, 'success')`,
		touched.DataSourceID).Error)
	require.NoError(t, db.Exec(`DELETE FROM sync_logs WHERE id = 'first'`).Error)
	_, err = repo.BeginEmptySourceDecommission(context.Background(), touched,
		proposed(touched, "e3efad37-7043-487f-8d6c-4ec75f108cad"))
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict,
		"pruning a sync log must not restore the virgin proof")
}

func TestVirginRebuildMigrationRequiresReviewOfExistingDecommission(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	db := decommissionTestDB(t)
	script, err := os.ReadFile("../../../migrations/sqlite/000042_nextcloud_empty_decommission.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	pair := decommissionPair(t, db, "kb-review", "b751f64a-9105-45f9-b9fd-9d263a2487be")
	repo := NewNextcloudSourcePairingRepository(db)
	_, err = repo.BeginEmptySourceDecommission(context.Background(), pair,
		NextcloudSourceDecommission{
			OperationID:     "1dbdd888-eafc-41e1-b3cd-307e60d40180",
			PairOperationID: pair.OperationID, TenantID: pair.TenantID,
			KnowledgeBaseID: pair.KnowledgeBaseID, DataSourceID: pair.DataSourceID,
			InstanceID: pair.InstanceID, BindingID: pair.BindingID,
			KeyID: pair.KeyID, PublicationEpoch: 1,
		})
	require.NoError(t, err)
	script, err = os.ReadFile("../../../migrations/sqlite/000043_nextcloud_virgin_rebuild_guard.up.sql")
	require.NoError(t, err)
	require.ErrorContains(t, db.Exec(string(script)).Error, "CHECK constraint failed",
		"a possibly false 42-era ACK must not be silently carried into 43")
}
