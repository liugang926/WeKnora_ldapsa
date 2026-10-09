package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestPostgresNextcloudGlobalAndScopedHistoryAdmission(t *testing.T) {
	for _, kind := range []string{
		"none",
		"source",
		"sticky",
		"pairing",
		"abort",
		"tombstone-after-hard-delete",
		"unavailable",
		"tombstone-unavailable",
	} {
		t.Run(kind, func(t *testing.T) {
			db := leaseTestPostgres(t)
			require.NoError(t, db.Exec(("CREATE TABLE knowledge_bases(id TEXT PRIMARY KEY, tenan" +
				"t_id BIGINT NOT NULL, ever_had_nextcloud_source BOOLEAN" +
				" NOT NULL DEFAULT FALSE, deleted_at TIMESTAMPTZ)")).Error)
			require.NoError(t, db.AutoMigrate(
				&types.DataSource{},
				&NextcloudSourcePairing{},
				&NextcloudSourcePairingAbort{},
			))
			require.NoError(t, db.Exec(`CREATE TABLE nextcloud_source_tombstones (
				tenant_id BIGINT NOT NULL, scope_type TEXT NOT NULL, scope_id TEXT NOT NULL,
				PRIMARY KEY(tenant_id,scope_type,scope_id))`).Error)
			require.NoError(t, db.Exec("INSERT INTO knowledge_bases(id,tenant_id) VALUES('kb',99)").Error)
			switch kind {
			case "source":
				row := &types.DataSource{
					ID:              "source",
					TenantID:        99,
					KnowledgeBaseID: "kb",
					Type:            types.ConnectorTypeNextcloud,
				}
				require.NoError(t, db.Create(row).Error)
				require.NoError(t, db.Delete(row).Error)
			case "sticky":
				require.NoError(t, db.Exec(("UPDATE knowledge_bases SET ever_had_nextcloud_source=TR" +
					"UE,deleted_at=CURRENT_TIMESTAMP")).Error)
			case "pairing":
				require.NoError(t, db.Create(&NextcloudSourcePairing{
					OperationID:     "pair",
					KnowledgeBaseID: "kb",
					TenantID:        99,
				}).Error)
			case "abort":
				require.NoError(t, db.Create(&NextcloudSourcePairingAbort{
					OperationID:     "abort",
					KnowledgeBaseID: "kb",
					TenantID:        99,
				}).Error)
			case "tombstone-after-hard-delete":
				require.NoError(t, db.Create(&types.DataSource{
					ID:              "source",
					TenantID:        99,
					KnowledgeBaseID: "kb",
					Type:            types.ConnectorTypeNextcloud,
				}).Error)
				require.NoError(t, db.Create(&NextcloudSourcePairing{
					OperationID:     "pair",
					TenantID:        99,
					KnowledgeBaseID: "kb",
					DataSourceID:    "source",
				}).Error)
				require.NoError(t, db.Exec(("INSERT INTO nextcloud_source_tombstones VALUES (99,'ten" +
					"ant','99'), (99,'knowledge_base','kb')")).Error)
				require.NoError(t, db.Exec("DELETE FROM nextcloud_source_pairings").Error)
				require.NoError(t, db.Exec("DELETE FROM data_sources").Error)
				require.NoError(t, db.Exec("DELETE FROM knowledge_bases").Error)
			case "unavailable":
				require.NoError(t, db.Exec("DROP TABLE nextcloud_source_pairing_aborts").Error)
			case "tombstone-unavailable":
				require.NoError(t, db.Exec("DROP TABLE nextcloud_source_tombstones").Error)
			}
			repo := &DataSourceRepository{db: db}
			for _, check := range []func() (bool, error){
				func() (bool, error) { return repo.HasEverNextcloudSourceGlobally(context.Background()) },
				func() (bool, error) { return repo.HasEverNextcloudSourceForTenant(context.Background(), 99) },
				func() (bool, error) { return repo.HasEverNextcloudSourceForKnowledgeBase(context.Background(), "kb") },
			} {
				ever, err := check()
				if kind == "unavailable" || kind == "tombstone-unavailable" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, kind != "none", ever)
				}
			}
		})
	}
}
