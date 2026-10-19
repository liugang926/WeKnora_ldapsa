package service

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestMessageNativeLiveControlUsesOnlyMetadataAndCurrentActor(t *testing.T) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	user, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:   session.ID,
			RequestID:   "actual-live-native",
			Role:        "user",
			Content:     "actual user input",
			IsCompleted: true,
		},
	)
	require.NoError(t, err)
	assistant, err := messages.CreateMessage(
		ctx,
		&types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant"},
	)
	require.NoError(t, err)
	observed := false
	name := "native-running-metadata-before-body"
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table != "messages" && tx.Statement.Table != "message_generation_origins" {
			return
		}
		observed = true
		require.NotEmpty(t, tx.Statement.Selects)
		for _, selected := range tx.Statement.Selects {
			require.NotContains(t, ","+selected+",", ",content,")
			require.NotContains(t, selected, "initial_input")
			require.NotContains(t, selected, "images")
		}
	}))
	control, generation, err := messages.MessageNativeLiveIdentity(ctx, session.ID, assistant.ID)
	require.NoError(t, err)
	require.NotEmpty(t, generation)
	require.Empty(t, control.Content)
	require.True(t, observed)
	require.NoError(t, db.Callback().Query().Remove(name))
	require.NoError(t, db.Model(&types.User{}).Where("id='actual-user'").Update("is_active", false).Error)
	_, _, err = messages.MessageNativeLiveIdentity(ctx, session.ID, assistant.ID)
	require.Error(t, err)
}

func TestMessageNativeLiveReceiverAcquiresOriginalParentsAndRejectsABA(t *testing.T) {
	testMessageNativeLiveReceiver(t, false)
}

func TestMessageNativeLivePostgresReceiverAcquiresOriginalParentsAndRejectsABA(t *testing.T) {
	testMessageNativeLiveReceiver(t, true)
}

func testMessageNativeLiveReceiver(t *testing.T, pg bool) {
	messages, _, _, db, ctx, session := messageMaterialActualFixture(t)
	if pg {
		messages, db, ctx, session = messageNativePostgresFixture(t)
	}
	_, prior := produceActualMessagePair(ctx, t, messages, session)
	user, err := messages.CreateMessage(
		ctx,
		&types.Message{
			SessionID:   session.ID,
			RequestID:   "native-live-receiver",
			Role:        "user",
			Content:     "actual human live query",
			IsCompleted: true,
		},
	)
	require.NoError(t, err)
	_, err = messages.CreateMessage(
		ctx,
		&types.Message{SessionID: session.ID, RequestID: user.RequestID, Role: "assistant"},
	)
	require.NoError(t, err)
	ctx = WithMessageMaterialOutputBoundary(ctx)
	defer CloseMessageMaterialOutputBoundary(ctx)
	ctx = context.WithValue(ctx, agentHistoryCurrentRequestKey{}, user.RequestID)
	_, err = messages.admittedMessagePage(ctx, session.ID, time.Time{}, 20, 1, false)
	require.NoError(t, err)
	parents, err := messages.CaptureNativeLiveMaterialParents(ctx)
	require.NoError(t, err)
	otherUser := &types.User{
		ID:       "same-role-native-receiver",
		TenantID: 7,
		Username: "other",
		Email:    "other@isolated.test",
		IsActive: true,
	}
	require.NoError(t, db.Create(otherUser).Error)
	require.NoError(
		t,
		db.Create(
			&types.TenantMember{
				TenantID: 7,
				UserID:   otherUser.ID,
				Role:     types.TenantRoleContributor,
				Status:   types.TenantMemberStatusActive,
			},
		).Error,
	)
	other := types.WithPrincipal(
		types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: otherUser.ID, Role: types.TenantRoleContributor}),
		types.Principal{Type: types.PrincipalWebUser, ID: otherUser.ID},
	)
	_, _, err = messages.BeginNativeLiveMaterialReceiver(other, parents)
	require.Error(t, err)
	check, closeRead, err := messages.BeginNativeLiveMaterialReceiver(ctx, parents)
	require.NoError(t, err)
	defer closeRead()
	require.NoError(t, check())
	require.NoError(
		t,
		db.Model(&types.Message{}).Where("id=?", prior.ID).Update("content", "foreign transient original").Error,
	)
	require.NoError(t, db.Model(&types.Message{}).Where("id=?", prior.ID).Update("content", prior.Content).Error)
	require.Error(t, check())
	_, _, err = messages.BeginNativeLiveMaterialReceiver(ctx, parents)
	require.Error(t, err)
	closeRead()
	closeRead()
	require.Error(t, check())
}

func messageNativePostgresFixture(t *testing.T) (*messageService, *gorm.DB, context.Context, *types.Session) {
	kg, store, db, ctx := faqPostgresActualFixture(t)
	require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"").Error)
	require.NoError(t, db.Exec("ALTER TABLE messages ADD COLUMN artifacts JSONB").Error)
	for _, name := range []string{
		"000103_message_artifacts_table.up.sql",
		"000107_message_artifacts_deleted_at.up.sql",
		"000127_nextcloud_content_leases.up.sql",
		"000151_message_original_materials.up.sql",
	} {
		ddl, err := os.ReadFile("../../../migrations/versioned/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)
	ctx = context.WithValue(ctx, types.UserIDContextKey, types.CallerFromContext(ctx).UserID)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	t.Cleanup(cancel)
	kbs := &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}
	groups := NewGroupAccessService(repository.NewGroupAccessRepository(db))
	sourceGuard := access.NewNextcloudSourceLineageGuard(
		kg.repo.(access.NextcloudLineageSnapshotLookup),
		kbs,
		nil,
		nil,
		groups,
		nil,
		store,
	)
	messages := &messageService{
		messageRepo:        repository.NewMessageRepository(db),
		sessionRepo:        repository.NewSessionRepository(db),
		sourceLineageGuard: sourceGuard,
		kbService:          kbs,
		knowService:        kg,
	}
	ConfigureMessageMaterialProvenance(messages, store)
	session := &types.Session{TenantID: 7, UserID: types.CallerFromContext(ctx).UserID}
	require.NoError(t, db.Create(session).Error)
	return messages, db, ctx, session
}
