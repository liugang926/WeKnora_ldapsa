package service

import (
	"context"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
)

func steerMaterialActualFixture(t *testing.T, pg bool) (*messageService, *repository.KnowledgeInputProvenanceStore, *gorm.DB, context.Context, *types.Session, *types.Message) {
	messages, _, store, db, ctx, session, _ := messageBodyIntegrationFixture(t, pg, func(db *gorm.DB, _ *repository.KnowledgeInputProvenanceStore, _ context.Context) {
		path := "../../../migrations/sqlite/000073_steer_original_materials.up.sql"
		if pg {
			path = "../../../migrations/versioned/000154_steer_original_materials.up.sql"
		}
		ddl, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	})
	repository.ConfigureMessageOriginalBodyAvailability(store, repository.CheckOriginalBodyAvailableInTx)
	user, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: "actual-steer-running", Role: "user", Content: "actual initial query", IsCompleted: true, ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}})
	require.NoError(t, err)
	run, err := messages.CreateMessage(ctx, &types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant", ExecutionContext: user.ExecutionContext})
	require.NoError(t, err)
	return messages, store, db, ctx, session, run
}
func actualSteerInput(t *testing.T, messages *messageService, ctx context.Context, session *types.Session, run *types.Message) (*types.SteerMaterialInput, context.Context) {
	_, generation, err := messages.MessageNativeLiveIdentity(ctx, session.ID, run.ID)
	require.NoError(t, err)
	request := &types.SteerAuthoredRequest{TenantID: session.TenantID, SessionID: session.ID, RunMessageID: run.ID, RunGenerationID: generation, RunRequestID: run.RequestID, ClientSteerID: uuid.NewString(), Query: "  actual authored\r\nsteer guidance  ", MentionedItems: types.MentionedItems{{ID: "human-literal-name", Name: "actual typed display name", Type: "kb", KBName: "human authored KB name"}}}
	marked, err := WithActualSteerHTTPInput(ctx, request)
	require.NoError(t, err)
	input, err := messages.AdmitActualSteerInput(marked, request)
	require.NoError(t, err)
	require.Equal(t, "actual authored\nsteer guidance", input.Query)
	return input, marked
}
func TestSteerMaterialActualAuthoredVaultReadPromoteAndActorControlABA(t *testing.T) {
	for _, change := range []string{"positive", "control_aba", "actor_revoke"} {
		t.Run(change, func(t *testing.T) {
			messages, _, db, ctx, session, run := steerMaterialActualFixture(t, false)
			input, _ := actualSteerInput(t, messages, ctx, session, run)
			var storage string
			require.NoError(t, db.Raw("SELECT input_body FROM steer_authored_origins WHERE id=?", input.Proof.OriginID).Scan(&storage).Error)
			require.NotContains(t, storage, input.Query)
			next, err := messages.ApplyActualSteerControl(ctx, &types.SteerControlRequest{TenantID: session.TenantID, SessionID: session.ID, SteerID: input.Proof.SteerID, OriginalAdmissionID: input.Proof.OriginID, ExpectedHeadID: input.Control.HeadID, ExpectedRevision: input.Control.Revision, Operation: types.SteerControlPromote})
			require.NoError(t, err)
			request := &types.SteerMaterialRequest{TenantID: session.TenantID, SessionID: session.ID, RunMessageID: run.ID, RunGenerationID: input.Proof.OriginRunGenerationID, Use: types.SteerMaterialInject}
			snapshot, check, close, err := messages.BeginSteerMaterialRead(ctx, request)
			require.NoError(t, err)
			defer close()
			require.Len(t, snapshot.Inputs, 1)
			require.Equal(t, input.Query, snapshot.Inputs[0].Query)
			require.Equal(t, next.HeadID, snapshot.Inputs[0].Control.HeadID)
			require.NoError(t, check())
			switch change {
			case "control_aba":
				require.NoError(t, db.Table("steer_control_heads").Where("origin_id=?", input.Proof.OriginID).Update("head_id", input.Control.HeadID).Error)
				require.NoError(t, db.Table("steer_control_heads").Where("origin_id=?", input.Proof.OriginID).Update("head_id", next.HeadID).Error)
				require.Error(t, check())
			case "actor_revoke":
				require.NoError(t, db.Model(&types.User{}).Where("id=?", session.UserID).Update("is_active", false).Error)
				require.Error(t, check())
			default:
				require.NoError(t, check())
			}
		})
	}
}

func TestSteerMaterialActualRollbackRefusesProofDeletion(t *testing.T) {
	for _, pg := range []bool{false, true} {
		name := "sqlite"
		if pg {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			messages, _, db, ctx, session, run := steerMaterialActualFixture(t, pg)
			input, _ := actualSteerInput(t, messages, ctx, session, run)
			discarded, err := messages.ApplyActualSteerControl(ctx, &types.SteerControlRequest{
				TenantID: session.TenantID, SessionID: session.ID, SteerID: input.Proof.SteerID,
				OriginalAdmissionID: input.Proof.OriginID, ExpectedHeadID: input.Control.HeadID,
				ExpectedRevision: input.Control.Revision, Operation: types.SteerControlDiscard,
			})
			require.NoError(t, err)
			deleted, err := messages.ApplyActualSteerControl(ctx, &types.SteerControlRequest{
				TenantID: session.TenantID, SessionID: session.ID, SteerID: input.Proof.SteerID,
				OriginalAdmissionID: input.Proof.OriginID, ExpectedHeadID: discarded.HeadID,
				ExpectedRevision: discarded.Revision, Operation: types.SteerControlDelete,
			})
			require.NoError(t, err)
			path := "../../../migrations/sqlite/000073_steer_original_materials.down.sql"
			if pg {
				path = "../../../migrations/versioned/000154_steer_original_materials.down.sql"
			}
			migration, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Error(t, db.Exec(string(migration)).Error)
			for _, table := range []string{"steer_authored_origins", "steer_control_admissions", "steer_control_heads", "steer_consumption_origins", "message_completion_input_manifests"} {
				require.True(t, db.Migrator().HasTable(table), "rollback must preserve original and negative-control proof tables")
			}
			var count int64
			require.NoError(t, db.Table("steer_authored_origins").Where("id=?", input.Proof.OriginID).Count(&count).Error)
			require.EqualValues(t, 1, count)
			require.NoError(t, messages.CheckActualSteerControl(ctx, deleted))
			require.Error(t, messages.CheckActualSteerControl(ctx, input.Control), "refused downgrade must not revive the initial control")
			_, err = messages.ApplyActualSteerControl(ctx, &types.SteerControlRequest{
				TenantID: session.TenantID, SessionID: session.ID, SteerID: input.Proof.SteerID,
				OriginalAdmissionID: input.Proof.OriginID, ExpectedHeadID: deleted.HeadID,
				ExpectedRevision: deleted.Revision, Operation: types.SteerControlPromote,
			})
			require.Error(t, err, "discarded and deleted input remains unusable after refused downgrade")
		})
	}
}
func TestSteerMaterialActualConsumeOwnInsertionAndModelGuard(t *testing.T) {
	runSteerActualConsume(t, false)
}
func TestSteerMaterialPostgresActualConsumeOwnInsertionAndModelGuard(t *testing.T) {
	runSteerActualConsume(t, true)
}
func runSteerActualConsume(t *testing.T, pg bool) {
	messages, _, db, ctx, session, run := steerMaterialActualFixture(t, pg)
	input, _ := actualSteerInput(t, messages, ctx, session, run)
	_, err := messages.ApplyActualSteerControl(ctx, &types.SteerControlRequest{TenantID: session.TenantID, SessionID: session.ID, SteerID: input.Proof.SteerID, OriginalAdmissionID: input.Proof.OriginID, ExpectedHeadID: input.Control.HeadID, ExpectedRevision: 1, Operation: types.SteerControlPromote})
	require.NoError(t, err)
	snapshot, _, closeQueue, err := messages.BeginSteerMaterialRead(ctx, &types.SteerMaterialRequest{TenantID: session.TenantID, SessionID: session.ID, RunMessageID: run.ID, RunGenerationID: input.Proof.OriginRunGenerationID, Use: types.SteerMaterialInject})
	require.NoError(t, err)
	defer closeQueue()
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, run.RequestID)
	var initial struct{ ID string }
	require.NoError(t, db.Table("messages").Select("id").Where("session_id=? AND request_id=? AND role='user'", session.ID, run.RequestID).Take(&initial).Error)
	_, err = messages.GetMessage(ctx, session.ID, initial.ID)
	require.NoError(t, err)
	require.NoError(t, messages.bindInitialMessageMaterial(ctx, run.ID, initial.ID, run.RequestID))
	_, err = messages.admittedMessagePage(ctx, session.ID, time.Time{}, 20, 1, false)
	require.NoError(t, err)
	message, proof, check, close, err := messages.ConsumeActualSteerIntoMessage(ctx, snapshot.Inputs[0], run.ID, input.Proof.OriginRunGenerationID, run.RequestID)
	require.NoError(t, err)
	defer close()
	require.Equal(t, input.Query, message.Content)
	require.Equal(t, proof.PreviousMembershipRevision+1, proof.ResultMembershipRevision)
	require.NoError(t, check())
	require.NoError(t, CheckMessageMaterialOutputBoundary(ctx))
	var heads int64
	require.NoError(t, db.Table("steer_consumption_origins").Where("origin_id=?", input.Proof.OriginID).Count(&heads).Error)
	require.EqualValues(t, 1, heads)
	again, _, checkAgain, closeAgain, err := messages.ConsumeActualSteerIntoMessage(ctx, snapshot.Inputs[0], run.ID, input.Proof.OriginRunGenerationID, run.RequestID)
	require.NoError(t, err)
	defer closeAgain()
	require.Equal(t, message.ID, again.ID)
	require.NoError(t, checkAgain())
	downPath := "../../../migrations/sqlite/000073_steer_original_materials.down.sql"
	if pg {
		downPath = "../../../migrations/versioned/000154_steer_original_materials.down.sql"
	}
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)
	require.Error(t, db.Exec(string(down)).Error)
	require.NoError(t, messages.materialStore.CheckSteerConsumption(ctx, proof), "refused downgrade retains the exact original consumption proof")
	require.NoError(t, check())
	require.NoError(t, db.Table("steer_consumption_origins").Where("origin_id=?", input.Proof.OriginID).Count(&heads).Error)
	require.EqualValues(t, 1, heads)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", message.ID).Update("content", "foreign transient steer body").Error)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", message.ID).Update("content", message.Content).Error)
	require.Error(t, check())
}

func TestSteerMaterialActualAuthoredRetryRawMentionAndChannelCannotBeReSigned(t *testing.T) {
	messages, _, _, ctx, session, run := steerMaterialActualFixture(t, false)
	_, generation, err := messages.MessageNativeLiveIdentity(ctx, session.ID, run.ID)
	require.NoError(t, err)
	request := &types.SteerAuthoredRequest{TenantID: 7, SessionID: session.ID, RunMessageID: run.ID, RunGenerationID: generation, RunRequestID: run.RequestID, ClientSteerID: uuid.NewString(), Query: "actual raw retry", Channel: "web", MentionedItems: types.MentionedItems{{ID: "id", Name: "raw first name", Type: "kb"}}}
	marked, err := WithActualSteerHTTPInput(ctx, request)
	require.NoError(t, err)
	original, err := messages.AdmitActualSteerInput(marked, request)
	require.NoError(t, err)
	retry, err := messages.AdmitActualSteerInput(marked, request)
	require.NoError(t, err)
	require.Equal(t, original.Proof.OriginID, retry.Proof.OriginID)
	changed := *request
	changed.MentionedItems = types.MentionedItems{{ID: "id", Name: "same ID different human label", Type: "kb"}}
	marked, err = WithActualSteerHTTPInput(ctx, &changed)
	require.NoError(t, err)
	_, err = messages.AdmitActualSteerInput(marked, &changed)
	require.Error(t, err)
	changed = *request
	changed.Channel = "api"
	marked, err = WithActualSteerHTTPInput(ctx, &changed)
	require.NoError(t, err)
	_, err = messages.AdmitActualSteerInput(marked, &changed)
	require.Error(t, err)
	_, err = messages.AdmitActualSteerInput(ctx, request)
	require.Error(t, err, "raw current args without actual HTTP capability cannot be admitted")
}
func TestSteerMaterialActualUnseenCandidateControlAndMissingBodyLeaseReject(t *testing.T) {
	messages, store, db, ctx, session, run := steerMaterialActualFixture(t, false)
	first, _ := actualSteerInput(t, messages, ctx, session, run)
	second, _ := actualSteerInput(t, messages, ctx, session, run)
	promoted, err := messages.ApplyActualSteerControl(ctx, &types.SteerControlRequest{TenantID: 7, SessionID: session.ID, SteerID: first.Proof.SteerID, OriginalAdmissionID: first.Proof.OriginID, ExpectedHeadID: first.Control.HeadID, ExpectedRevision: 1, Operation: types.SteerControlPromote})
	require.NoError(t, err)
	request := &types.SteerMaterialRequest{TenantID: 7, SessionID: session.ID, RunMessageID: run.ID, RunGenerationID: first.Proof.OriginRunGenerationID, Use: types.SteerMaterialInject, SteerIDs: []string{first.Proof.SteerID}}
	plan, err := store.PeekSteerMaterialPlan(ctx, request)
	require.NoError(t, err)
	require.Len(t, plan.CandidateControls, 2)
	require.Equal(t, []string{first.Proof.SteerID}, plan.SelectedSteerIDs)
	noBodyLease, err := repository.WithSteerMaterialReadPlan(ctx, plan, nil)
	require.NoError(t, err)
	_, err = store.CaptureSteerMaterial(noBodyLease, plan)
	require.Error(t, err)
	snapshot, check, close, err := messages.BeginSteerMaterialRead(ctx, request)
	require.NoError(t, err)
	defer close()
	require.Len(t, snapshot.Inputs, 1)
	require.Equal(t, promoted.HeadID, snapshot.Inputs[0].Control.HeadID)
	require.NoError(t, db.Table("steer_control_heads").Where("origin_id=?", second.Proof.OriginID).Update("head_id", first.Control.HeadID).Error)
	require.NoError(t, db.Table("steer_control_heads").Where("origin_id=?", second.Proof.OriginID).Update("head_id", second.Control.HeadID).Error)
	require.Error(t, check(), "an unseen selection candidate ABA cannot be projected away")
}

func TestSteerMaterialActualNoKBVaultLeaseBlocksPurgeThenJournalDeniesOldInput(t *testing.T) {
	messages, _, db, ctx, session, run := steerMaterialActualFixture(t, false)
	input, _ := actualSteerInput(t, messages, ctx, session, run)
	request := &types.SteerMaterialRequest{TenantID: 7, SessionID: session.ID, RunMessageID: run.ID, RunGenerationID: input.Proof.OriginRunGenerationID, Use: types.SteerMaterialPending}
	snapshot, check, close, err := messages.BeginSteerMaterialRead(ctx, request)
	require.NoError(t, err)
	require.Len(t, snapshot.Inputs, 1)
	require.NoError(t, check())
	var bodyID string
	require.NoError(t, db.Raw("SELECT body_id FROM original_body_field_heads WHERE source_table='steer_authored_origins' AND row_key=?", input.Proof.OriginID).Scan(&bodyID).Error)
	require.NotEmpty(t, bodyID)
	var marker int64
	require.NoError(t, db.Table("original_body_lease_scopes").Where("body_id=? AND tenant_id=7 AND knowledge_base_id=''", bodyID).Count(&marker).Error)
	require.EqualValues(t, 1, marker)
	require.NoError(t, db.Exec("UPDATE sessions SET deleted_at=datetime('now','-2 hours') WHERE id=?", session.ID).Error)
	retention := repository.NewOriginalBodyRetentionStore(db)
	_, err = retention.PurgeDue(context.Background(), 100)
	require.NoError(t, err)
	var state string
	require.NoError(t, db.Raw("SELECT state FROM original_body_payloads WHERE id=?", bodyID).Scan(&state).Error)
	require.Equal(t, "live", state)
	require.Error(t, check(), "lease never grants permission after actual session closure")
	close()
	_, err = retention.PurgeDue(context.Background(), 100)
	require.NoError(t, err)
	require.NoError(t, db.Raw("SELECT state FROM original_body_payloads WHERE id=?", bodyID).Scan(&state).Error)
	require.Equal(t, "purged", state)
	require.Error(t, db.Transaction(func(tx *gorm.DB) error {
		return repository.CheckOriginalBodyAvailableInTx(context.Background(), tx, "steer_authored_origins", input.Proof.OriginID)
	}))
	var payload *string
	require.NoError(t, db.Raw("SELECT payload FROM original_body_payloads WHERE id=?", bodyID).Scan(&payload).Error)
	require.Nil(t, payload)
}
