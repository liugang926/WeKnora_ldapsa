package repository_test

import (
	"context"
	"testing"
	"time"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// Real SQL admission and pairing writers, with controlled directory metadata.
// This does not claim a live LDAP login or template-input clone acceptance.
func TestNextcloudPairProducerExactScopeAndCurrentAuthority(t *testing.T) {
	for _, scenario := range []string{"success", "unmarked", "nil-factory", "wrong-binding", "wrong-kb", "credential-revoked", "actor-disabled", "directory-failed", "directory-future", "operation-retired", "changed-credential"} {
		t.Run(scenario, func(t *testing.T) {
			db, store, cred := onboardingDB(t)
			require.NoError(
				t,
				db.Model(&types.Directory{}).
					Where("id=?", cred.DirectoryID).
					Update("protocol", types.DirectoryProtocolAD).
					Error,
			)
			req := onboardingRequest(cred, uuid.NewString(), "")
			req.Mode = "create"
			op, err := store.Reserve(context.Background(), cred, req)
			require.NoError(t, err)
			supplied := cred
			if scenario == "changed-credential" {
				supplied.MaxKnowledgeBases++
			}
			ctx, err := store.BeginPairProducer(
				context.Background(),
				supplied,
				op,
				req.ActorDirectoryID,
				req.ActorObjectGUID,
			)
			if scenario == "changed-credential" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			// No human Caller or Principal is synthesized by this purpose.
			require.Empty(t, types.CallerFromContext(ctx).UserID)
			pairs := repo.NewNextcloudSourcePairingRepository(db)
			factory := func(tx *gorm.DB) *repo.KnowledgeInputProvenanceStore {
				return repo.NewKnowledgeInputProvenanceStore(tx)
			}
			if scenario != "nil-factory" {
				repo.ConfigureDataSourceNameInputWriters(repo.NewDataSourceRepository(db), pairs, factory)
			}
			proposed := repo.NextcloudSourcePairing{
				OperationID:      op.OperationID,
				TenantID:         op.TenantID,
				KnowledgeBaseID:  op.KnowledgeBaseID,
				InstanceID:       op.InstanceID,
				BindingID:        op.BindingID,
				BaseURL:          cred.BaseURL,
				PublicationEpoch: 1,
				KeyID:            "pair_" + op.OperationID,
			}
			switch scenario {
			case "unmarked":
				ctx = context.Background()
			case "wrong-binding":
				proposed.BindingID = "other-root"
			case "wrong-kb":
				proposed.KnowledgeBaseID = "template"
			case "credential-revoked":
				require.NoError(
					t,
					db.Model(&types.NextcloudOnboardingCredential{}).
						Where("id=?", cred.ID).
						Update("enabled", false).
						Error,
				)
			case "actor-disabled":
				require.NoError(
					t,
					db.Model(&types.DirectoryIdentity{}).Where("id=?", "i-owner").Update("status", "disabled").Error,
				)
			case "directory-failed":
				require.NoError(
					t,
					db.Model(&types.Directory{}).
						Where("id=?", cred.DirectoryID).
						Update("last_sync_error", "unavailable").
						Error,
				)
			case "directory-future":
				require.NoError(
					t,
					db.Model(&types.Directory{}).
						Where("id=?", cred.DirectoryID).
						Update("last_successful_sync_at", time.Now().Add(time.Hour)).
						Error,
				)
			case "operation-retired":
				require.NoError(
					t,
					db.Model(&types.NextcloudOnboardingOperation{}).
						Where("operation_id=?", op.OperationID).
						Update("state", "retired").
						Error,
				)
			}
			pair, _, err := pairs.PrepareSourcePairing(ctx, proposed, "private-test-handoff-token")
			if scenario != "success" {
				require.Error(t, err)
				var count int64
				require.NoError(t, db.Model(&types.DataSource{}).Count(&count).Error)
				require.Zero(t, count, "failed capability must roll back the source")
				require.NoError(t, db.Model(&repo.NextcloudSourcePairing{}).Count(&count).Error)
				require.Zero(t, count)
				return
			}
			require.NoError(t, err)
			replay, created, err := pairs.PrepareSourcePairing(ctx, proposed, "private-test-handoff-token")
			require.NoError(t, err)
			require.False(t, created)
			require.Equal(t, pair.DataSourceID, replay.DataSourceID)
			require.NoError(
				t,
				db.Model(&types.NextcloudOnboardingCredential{}).Where("id=?", cred.ID).Update("enabled", false).Error,
			)
			_, _, err = pairs.PrepareSourcePairing(ctx, proposed, "private-test-handoff-token")
			require.Error(t, err, "captured purpose cannot bypass revocation through exact replay")
			proof, err := repo.NewKnowledgeInputProvenanceStore(db).
				ReadDataSourceNameInput(context.Background(), op.TenantID, op.KnowledgeBaseID, pair.DataSourceID)
			require.NoError(t, err)
			require.NotEmpty(t, proof.InputDigest)
			var actual struct {
				ID           string
				ActualInputs string
			}
			// Query through the actual body-resolution callback, retaining the row
			// identity required to verify the immutable original-body reference.
			require.NoError(
				t,
				db.Table("datasource_name_inputs").Where("datasource_id=?", pair.DataSourceID).Take(&actual).Error,
			)
			require.Contains(t, actual.ActualInputs, op.RequestSHA256)
			require.Contains(t, actual.ActualInputs, "onboarding_producer")
			var members int64
			require.NoError(t, db.Model(&types.TenantMember{}).Count(&members).Error)
			require.EqualValues(t, 1, members, "only pre-existing owner membership remains")
		})
	}
}
