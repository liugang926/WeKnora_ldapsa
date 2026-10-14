package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudGlobalHistoryAdmissionIncludesAllPersistentEvidence(t *testing.T) {
	for _, kind := range []string{
		"none",
		"source-soft-deleted-other-tenant",
		"sticky-deleted-kb",
		"pairing",
		"abort",
		"tombstone",
		"malformed-marker",
		"missing-table",
		"missing-tombstone-table",
	} {
		t.Run(kind, func(t *testing.T) {
			db := provenanceTestDB(t)
			require.NoError(t, db.Exec("INSERT INTO knowledge_bases(id, tenant_id) VALUES('kb', 99)").Error)
			switch kind {
			case "source-soft-deleted-other-tenant":
				ds := &types.DataSource{
					ID:              "ds",
					TenantID:        99,
					KnowledgeBaseID: "kb",
					Type:            types.ConnectorTypeNextcloud,
				}
				require.NoError(t, db.Create(ds).Error)
				require.NoError(t, db.Delete(ds).Error)
			case "sticky-deleted-kb":
				require.NoError(t, db.Exec(("UPDATE knowledge_bases SET ever_had_nextcloud_source=TR" +
					"UE, deleted_at=CURRENT_TIMESTAMP")).Error)
			case "pairing":
				require.NoError(t, db.Create(&NextcloudSourcePairing{OperationID: "pair"}).Error)
			case "abort":
				require.NoError(t, db.Create(&NextcloudSourcePairingAbort{OperationID: "abort"}).Error)
			case "tombstone":
				require.NoError(t, db.Exec("INSERT INTO nextcloud_source_tombstones VALUES (99, 'tenant', '99')").Error)
			case "malformed-marker":
				require.NoError(t, db.Exec("UPDATE knowledge_bases SET ever_had_nextcloud_source=2").Error)
			case "missing-table":
				require.NoError(t, db.Exec("DROP TABLE nextcloud_source_pairing_aborts").Error)
			case "missing-tombstone-table":
				require.NoError(t, db.Exec("DROP TABLE nextcloud_source_tombstones").Error)
			}
			ever, err := (&DataSourceRepository{db: db}).HasEverNextcloudSourceGlobally(context.Background())
			if kind == "malformed-marker" || kind == "missing-table" || kind == "missing-tombstone-table" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, kind != "none", ever)
		})
	}
}

func TestNextcloudHistoryTombstoneSurvivesAllBusinessEvidenceDeletion(t *testing.T) {
	db := provenanceTestDB(t)
	require.NoError(t, db.Exec(("INSERT INTO knowledge_bases(id, tenant_id, ever_had_nex" +
		"tcloud_source) VALUES('kb',99,TRUE)")).Error)
	require.NoError(t, db.Create(&types.DataSource{
		ID:              "ds",
		TenantID:        99,
		KnowledgeBaseID: "kb",
		Type:            types.ConnectorTypeNextcloud,
	}).Error)
	require.NoError(t, db.Create(&NextcloudSourcePairing{
		OperationID:     "pair",
		TenantID:        99,
		KnowledgeBaseID: "kb",
		DataSourceID:    "ds",
	}).Error)
	require.NoError(t, db.Exec(("INSERT INTO nextcloud_source_tombstones VALUES (99, 'te" +
		"nant', '99'), (99, 'knowledge_base', 'kb')")).Error)
	require.NoError(t, db.Exec("DELETE FROM nextcloud_source_pairings").Error)
	require.NoError(t, db.Exec("DELETE FROM data_sources").Error)
	require.NoError(t, db.Exec("DELETE FROM knowledge_bases").Error)
	repo := &DataSourceRepository{db: db}
	ever, err := repo.HasEverNextcloudSourceGlobally(context.Background())
	require.NoError(t, err)
	require.True(t, ever)
	ever, err = repo.HasEverNextcloudSourceForTenant(context.Background(), 99)
	require.NoError(t, err)
	require.True(t, ever)
	ever, err = repo.HasEverNextcloudSourceForKnowledgeBase(context.Background(), "kb")
	require.NoError(t, err)
	require.True(t, ever)
	ever, err = repo.HasEverNextcloudSourceForTenant(context.Background(), 100)
	require.NoError(t, err)
	require.False(t, ever, "positive evidence must retain tenant scope")
	ever, err = repo.HasEverNextcloudSourceForKnowledgeBase(context.Background(), "other-kb")
	require.NoError(t, err)
	require.False(t, ever, "positive evidence must retain KB scope")
	require.NoError(t, db.Exec("DROP TABLE nextcloud_source_tombstones").Error)
	_, err = repo.HasEverNextcloudSourceForTenant(context.Background(), 99)
	require.Error(t, err)
	_, err = repo.HasEverNextcloudSourceForKnowledgeBase(context.Background(), "kb")
	require.Error(t, err)
}

func TestNextcloudScopedHistoryRetainsDeletedKBAndPairingEvidence(t *testing.T) {
	for _, kind := range []string{"sticky", "pairing", "abort"} {
		t.Run(kind, func(t *testing.T) {
			db := provenanceTestDB(t)
			require.NoError(t, db.Exec("INSERT INTO knowledge_bases(id,tenant_id) VALUES('kb',99)").Error)
			switch kind {
			case "sticky":
				require.NoError(t, db.Exec(("UPDATE knowledge_bases SET ever_had_nextcloud_source=TR" +
					"UE,deleted_at=CURRENT_TIMESTAMP")).Error)
			case "pairing":
				require.NoError(t, db.Create(&NextcloudSourcePairing{
					OperationID:     "p",
					KnowledgeBaseID: "kb",
					TenantID:        99,
				}).Error)
			case "abort":
				require.NoError(t, db.Create(&NextcloudSourcePairingAbort{
					OperationID:     "p",
					KnowledgeBaseID: "kb",
					TenantID:        99,
				}).Error)
			}
			repo := &DataSourceRepository{db: db}
			ever, err := repo.HasEverNextcloudSourceForKnowledgeBase(context.Background(), "kb")
			require.NoError(t, err)
			require.True(t, ever)
			ever, err = repo.HasEverNextcloudSourceForTenant(context.Background(), 99)
			require.NoError(t, err)
			require.True(t, ever)
		})
	}
}

func TestNextcloudHistoryNilRepositoryFailsClosed(t *testing.T) {
	var repo *DataSourceRepository
	_, err := repo.HasEverNextcloudSourceGlobally(context.Background())
	require.Error(t, err)
	_, err = repo.HasEverNextcloudSourceForTenant(context.Background(), 1)
	require.Error(t, err)
	_, err = repo.HasEverNextcloudSourceForKnowledgeBase(context.Background(), "kb")
	require.Error(t, err)
}
