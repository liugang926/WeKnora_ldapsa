package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNextcloudMachineKnowledgePublicationEpoch(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:machine-epoch-test?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE knowledges (
		id TEXT PRIMARY KEY,
		tenant_id BIGINT NOT NULL,
		knowledge_base_id TEXT NOT NULL,
		metadata TEXT NOT NULL
	)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledges (id, tenant_id, knowledge_base_id, metadata)
		VALUES
		('epoch-four', 7, 'kb', '{"nextcloud_publication_epoch":"4"}'),
		('epoch-zero', 7, 'kb', '{"nextcloud_publication_epoch":"0"}'),
		('epoch-leading-zero', 7, 'kb', '{"nextcloud_publication_epoch":"04"}'),
		('epoch-negative', 7, 'kb', '{"nextcloud_publication_epoch":"-1"}'),
		('epoch-missing', 7, 'kb', '{}')`).Error)

	epoch, err := nextcloudMachineKnowledgePublicationEpochInTx(db, 7, "kb", "epoch-four")
	require.NoError(t, err)
	require.EqualValues(t, 4, epoch)
	epoch, err = nextcloudMachineKnowledgePublicationEpochInTx(db, 7, "kb", "epoch-zero")
	require.NoError(t, err)
	require.Zero(t, epoch)

	for _, id := range []string{"epoch-leading-zero", "epoch-negative", "epoch-missing", "other-tenant"} {
		tenant := uint64(7)
		if id == "other-tenant" {
			tenant = 8
		}
		_, err := nextcloudMachineKnowledgePublicationEpochInTx(db, tenant, "kb", id)
		require.Error(t, err, id)
	}
}
