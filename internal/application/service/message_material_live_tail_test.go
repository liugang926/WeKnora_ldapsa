package service

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMessageMaterialActualExternalTailOwnCompletionAndBodyABA(t *testing.T) {
	testMessageMaterialExternalTail(t, false)
}

func TestMessageMaterialPostgresExternalTailOwnCompletionAndBodyABA(t *testing.T) {
	testMessageMaterialExternalTail(t, true)
}

func testMessageMaterialExternalTail(t *testing.T, postgres bool) {
	for _, change := range []string{"positive", "parent-body-ABA", "expired-lease", "current-control-ABA"} {
		t.Run(change, func(t *testing.T) {
			messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
			if postgres {
				kg, store, pgdb, pgctx := faqPostgresActualFixture(t)
				require.NoError(t, pgdb.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").Error)
				require.NoError(t, pgdb.Exec("ALTER TABLE messages ADD COLUMN artifacts JSONB").Error)
				for _, name := range []string{"000103_message_artifacts_table.up.sql", "000107_message_artifacts_deleted_at.up.sql", "000127_nextcloud_content_leases.up.sql", "000151_message_original_materials.up.sql"} {
					ddl, err := os.ReadFile("../../../migrations/versioned/" + name)
					require.NoError(t, err)
					require.NoError(t, pgdb.Exec(string(ddl)).Error)
				}
				sqlDB, err := pgdb.DB()
				require.NoError(t, err)
				sqlDB.SetMaxOpenConns(1)
				sqlDB.SetMaxIdleConns(1)
				pgctx = context.WithValue(pgctx, types.UserIDContextKey, types.CallerFromContext(pgctx).UserID)
				var cancel context.CancelFunc
				pgctx, cancel = context.WithTimeout(pgctx, 20*time.Second)
				defer cancel()
				kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(pgdb)}
				groups := NewGroupAccessService(repository.NewGroupAccessRepository(pgdb))
				sourceGuard := access.NewNextcloudSourceLineageGuard(
					kg.repo.(access.NextcloudLineageSnapshotLookup),
					kbs,
					nil,
					nil,
					groups,
					nil,
					store,
				)
				messages = &messageService{
					messageRepo:        repository.NewMessageRepository(pgdb),
					sessionRepo:        repository.NewSessionRepository(pgdb),
					sourceLineageGuard: sourceGuard,
					kbService:          kbs,
					knowService:        kg,
				}
				ConfigureMessageMaterialProvenance(messages, store)
				session = &types.Session{TenantID: 7, UserID: types.CallerFromContext(pgctx).UserID}
				require.NoError(t, pgdb.Create(session).Error)
				db, ctx = pgdb, pgctx
			}

			require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
			_, prior := produceActualMessagePair(ctx, t, messages, session)
			currentUser, err := messages.CreateMessage(
				ctx,
				&types.Message{
					SessionID:   session.ID,
					RequestID:   "actual-http-tail-turn",
					Role:        "user",
					Content:     "actual new query",
					IsCompleted: true,
				},
			)
			require.NoError(t, err)
			current, err := messages.CreateMessage(
				ctx,
				&types.Message{SessionID: session.ID, RequestID: currentUser.RequestID, Role: "assistant"},
			)
			require.NoError(t, err)
			ctx, attach, check, closeRead := BeginMessageMaterialHTTPBoundary(ctx)
			defer closeRead()
			ctx, release, external := retainMessageMaterialProducer(ctx)
			defer release()
			require.True(t, external)
			ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, current.RequestID)
			_, err = messages.GetMessage(ctx, session.ID, currentUser.ID)
			require.NoError(t, err)
			_, err = messages.admittedMessagePage(ctx, session.ID, prior.CreatedAt, 20, 1, false)
			require.NoError(t, err)
			markMessageMaterialReady(ctx)
			require.NoError(t, check())
			if change == "current-control-ABA" {
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", current.ID).Update("request_id", "foreign request").Error,
				)
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", current.ID).Update("request_id", current.RequestID).Error,
				)
				current.Content = "actual new model answer"
				current.IsCompleted = true
				current.SourceLineage, _ = types.NewCompleteSourceLineage()
				require.Error(t, messages.UpdateMessage(ctx, current))
				return
			}
			current.Content = "actual new model answer"
			current.IsCompleted = true
			current.SourceLineage, _ = types.NewCompleteSourceLineage()
			require.NoError(t, messages.UpdateMessage(ctx, current))
			release()
			require.NoError(t, CheckMessageMaterialOutputBoundary(attach(context.WithoutCancel(ctx))))
			switch change {
			case "parent-body-ABA":
				require.NoError(
					t,
					db.Model(&types.Message{}).
						Where("id=?", prior.ID).
						Update("content", "unaccounted changed body").
						Error,
				)
				require.NoError(
					t,
					db.Model(&types.Message{}).Where("id=?", prior.ID).Update("content", prior.Content).Error,
				)
				require.Error(t, check())
			case "expired-lease":
				require.NoError(
					t,
					db.Table("nextcloud_content_leases").
						Where("kind='read' AND released_at_ms IS NULL").
						Update("expires_at_ms", int64(1)).
						Error,
				)
				require.Error(t, check())
			default:
				require.NoError(t, check())
			}
		})
	}
}

func TestMessageMaterialActualCompletionSerializesHeartbeatAndHTTPCheck(t *testing.T) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	user, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:   session.ID,
			RequestID:   "serialized-live-completion",
			Role:        "user",
			Content:     "actual original live query",
			IsCompleted: true,
		},
	)
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(
		ctx,
		&types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant"},
	)
	require.NoError(t, err)
	ctx, _, check, closeHTTP := BeginMessageMaterialHTTPBoundary(ctx)
	defer closeHTTP()
	ctx, release, _ := retainMessageMaterialProducer(ctx)
	defer release()
	ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, user.RequestID)
	_, err = messages.GetMessage(ctx, session.ID, user.ID)
	require.NoError(t, err)
	_, err = messages.admittedMessagePage(ctx, session.ID, time.Time{}, 20, 1, false)
	require.NoError(t, err)
	markMessageMaterialReady(ctx)
	slot := ctx.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
	checks := make(chan error, 32)
	var wg sync.WaitGroup
	name := "actual-completion-after-sql-write"
	require.NoError(t, db.Callback().Update().After("gorm:update").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table != "messages" {
			return
		}
		for _, guard := range slot.Guards {
			wg.Add(1)
			go func(g *MessageMaterialGuard) { defer wg.Done(); checks <- g.Recheck() }(guard)
		}
		wg.Add(1)
		go func() { defer wg.Done(); checks <- check() }()
	}))
	defer func() { require.NoError(t, db.Callback().Update().Remove(name)) }()
	assistant.Content = "actual new serialized answer"
	assistant.IsCompleted = true
	assistant.SourceLineage, _ = types.NewCompleteSourceLineage()
	require.NoError(t, messages.UpdateMessage(ctx, assistant))
	wg.Wait()
	close(checks)
	for err := range checks {
		require.NoError(t, err)
	}
	require.NoError(t, check())
}

func TestMessageMaterialActualProducerAdvanceForwardUpgradeNoLegacyAdmission(t *testing.T) {
	_, _, _, db, _, _ := messageMaterialActualFixture(t)
	require.NoError(
		t,
		db.Exec(
			("DROP TRIGGER message_producer_advances_immutable_update; DROP TRIGGER mess" +
				"age_producer_advances_immutable_delete; DROP TABLE message_producer_advanc" +
				"es;"),
		).Error,
	)
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
	require.NoError(t, repository.UpgradeMessageProducerAdvanceSchema(db))
	var count int64
	require.NoError(t, db.Table("message_producer_advances").Count(&count).Error)
	require.Zero(t, count)
}
