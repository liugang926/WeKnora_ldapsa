package service

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func runMessageMaterialOwnGrowthActual(t *testing.T, pg bool) {
	for _, mode := range []string{"no-kb", "cross-kb", "source-after-content", "actor-after-content", "caller-after-content", "guard-after-content", "epoch-after-content", "cancel-after-body", "raw-unconsumed"} {
		t.Run(mode, func(t *testing.T) {
			m := newMachinePipelineFixture(t, pg)
			k := m.ingest(t)
			for i := 0; i < len(m.f.queue.tasks); i++ {
				require.NoError(t, m.run(t, m.f.queue.tasks[i]))
			}
			migration := m.f.db
			directory := "sqlite"
			names := []string{
				"000023_message_artifacts_table.up.sql",
				"000026_message_artifacts_deleted_at.up.sql",
				"000070_message_original_materials.up.sql",
			}
			artifactType := "TEXT"
			if pg {
				directory, artifactType = "versioned", "JSONB"
				names = []string{
					"000103_message_artifacts_table.up.sql",
					"000107_message_artifacts_deleted_at.up.sql",
					"000151_message_original_materials.up.sql",
				}
				pool, err := m.f.db.DB()
				require.NoError(t, err)
				migration, err = gorm.Open(
					postgres.New(postgres.Config{Conn: pool, PreferSimpleProtocol: true}),
					&gorm.Config{},
				)
				require.NoError(t, err)
				require.NoError(
					t,
					migration.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public`).Error,
				)
				require.NoError(t, migration.Exec(`ALTER EXTENSION "uuid-ossp" SET SCHEMA public`).Error)
				require.NoError(
					t,
					migration.Exec(
						`CREATE FUNCTION uuid_generate_v4() RETURNS uuid LANGUAGE sql AS $$ SELECT public.uuid_generate_v4() $$`,
					).Error,
				)
			}
			if !m.f.db.Migrator().HasColumn(&types.Message{}, "artifacts") {
				require.NoError(t, migration.Exec("ALTER TABLE messages ADD COLUMN artifacts "+artifactType).Error)
			}
			for _, name := range names {
				ddl, err := os.ReadFile("../../../migrations/" + directory + "/" + name)
				require.NoError(t, err)
				require.NoError(t, migration.Exec(string(ddl)).Error)
			}
			require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(m.f.db))
			messages := &messageService{
				messageRepo:        repository.NewMessageRepository(m.f.db),
				sessionRepo:        repository.NewSessionRepository(m.f.db),
				sourceLineageGuard: m.f.guard,
				kbService:          m.f.s.kbService,
				knowService:        m.f.s,
			}
			ConfigureMessageMaterialProvenance(messages, m.f.store)
			repository.ConfigureMessageOriginalBodyAvailability(m.f.store, repository.CheckOriginalBodyAvailableInTx)
			ctx := context.WithValue(m.f.ctx, types.UserIDContextKey, types.CallerFromContext(m.f.ctx).UserID)
			session := &types.Session{
				ID:       "actual-generated-parent-copy-session",
				TenantID: 7,
				UserID:   types.CallerFromContext(ctx).UserID,
			}
			require.NoError(t, m.f.db.Create(session).Error)
			execution := types.MessageExecutionContext{}
			if mode == "cross-kb" {
				kb := &types.KnowledgeBase{
					ID:       "ordinary-a",
					TenantID: 7,
					Name:     "actual ordinary A",
					Type:     types.KnowledgeBaseTypeDocument,
				}
				require.NoError(
					t,
					m.f.store.CreateKnowledgeBaseWithInput(
						repository.WithIndependentKnowledgeBaseNameInput(
							repository.WithIndependentKnowledgeInput(ctx),
							"",
							kb.Name,
						),
						kb,
					),
				)
				execution.KnowledgeBaseIDs = []string{kb.ID}
			}
			user, err := messages.CreateMessage(
				ctx,
				&types.Message{
					SessionID:        session.ID,
					RequestID:        "raw-no-kb",
					Role:             "user",
					Content:          "actual user raw input without a KB",
					IsCompleted:      true,
					ExecutionContext: execution,
				},
			)
			require.NoError(t, err)
			_, err = messages.CreateMessage(
				ctx,
				&types.Message{
					SessionID:   session.ID,
					RequestID:   "copy-host",
					Role:        "user",
					Content:     "actual host of legacy copied material",
					IsCompleted: true,
				},
			)
			require.NoError(t, err)
			parent, err := messages.CreateMessage(
				ctx,
				&types.Message{
					SessionID:        session.ID,
					RequestID:        "source-parent",
					Role:             "assistant",
					ExecutionContext: types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}},
				},
			)
			require.NoError(t, err)
			actual, err := m.f.store.CaptureKnowledgeOutputInputs(
				ctx,
				types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: k.ID},
			)
			require.NoError(t, err)
			parent.Content, parent.IsCompleted, parent.SourceLineage = "actual source-derived answer", true, actual.Proof.SourceLineage
			require.NoError(t, messages.UpdateMessage(ctx, parent))
			scope := access.LineageReadScope{
				Targets: []*types.SearchTarget{
					{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb"},
				},
			}
			writer := WithMessageMaterialOutputBoundary(ctx)
			defer CloseMessageMaterialOutputBoundary(writer)
			writer = types.WithRAGSourceLineage(
				writer,
				types.NewRAGSourceLineage(
					func(check context.Context, l *types.SourceLineage) error { return m.f.guard.Check(check, l, scope) },
				),
			)
			_, err = messages.GetMessage(writer, session.ID, parent.ID)
			require.NoError(t, err)
			_, err = messages.GetMessage(writer, session.ID, user.ID)
			require.NoError(t, err)

			slot := writer.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
			var userGuard *MessageMaterialGuard
			for _, guard := range slot.Guards {
				if guard.snapshot.Request.MessageIDs[0] == user.ID {
					userGuard = guard
				}
			}
			require.NotNil(t, userGuard)
			oldProof := *userGuard.snapshot.Proofs[0]
			oldCtx := userGuard.Context()
			oldLeases := append([]repository.NextcloudContentLease{}, userGuard.leases...)
			oldSnapshot := userGuard.snapshot
			beforeIDs := []string{}
			require.NoError(
				t,
				m.f.db.Table("nextcloud_content_leases").
					Where("released_at_ms IS NULL").
					Pluck("lease_id", &beforeIDs).
					Error,
			)
			requestCtx, cancelRequest := context.WithCancel(writer)
			defer cancelRequest()
			triggered := false
			revoke := func(tx *gorm.DB) {
				triggered = true
				switch mode {
				case "source-after-content":
					m.allow.Store(false)
				case "actor-after-content":
					require.NoError(
						t,
						tx.Session(&gorm.Session{NewDB: true}).
							Model(&types.User{}).
							Where("id=?", types.CallerFromContext(ctx).UserID).
							Update("is_active", false).
							Error,
					)
				case "caller-after-content":
					cancelRequest()
				case "guard-after-content", "cancel-after-body":
					userGuard.cancel()
				case "epoch-after-content":
					source := actual.Proof.SourceLineage.Sources[0]
					// The successful raw INSERT callback still carries that INSERT's
					// SQL and arguments. Keep its transaction, but begin an independent
					// statement for the real fence transition.
					cleanTx := tx.Session(&gorm.Session{NewDB: true, Initialized: true})
					require.Same(t, tx.Statement.ConnPool, cleanTx.Statement.ConnPool)
					require.NoError(
						t,
						m.f.store.ContentLeaseStore().
							RetireKnowledgeInTx(context.Background(), cleanTx, repository.NextcloudContentScope{TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, KnowledgeID: source.KnowledgeID, DataSourceID: source.DataSourceID, ExternalID: source.ExternalID}),
					)
					var fence struct {
						Epoch uint64
						State string
					}
					require.NoError(
						t,
						tx.Session(&gorm.Session{NewDB: true, Initialized: true}).
							Table("nextcloud_content_fences").
							Select("epoch", "state").
							Where("tenant_id=? AND knowledge_base_id=? AND knowledge_id=? AND datasource_id=? AND external_id=?", source.TenantID, source.KnowledgeBaseID, source.KnowledgeID, source.DataSourceID, source.ExternalID).
							Take(&fence).
							Error,
					)
					require.Equal(t, "retired", fence.State)
					require.Equal(
						t,
						source.Revision+1,
						fence.Epoch,
						"real fence transition invalidates the captured source revision",
					)
				}
			}
			if mode == "cancel-after-body" {
				require.NoError(
					t,
					m.f.db.Callback().
						Create().
						After("gorm:create").
						Register("actual-own-growth-late-refusal", func(tx *gorm.DB) {
							if triggered || tx.Statement.Table != "original_body_leases" {
								return
							}
							value := tx.Statement.ReflectValue
							for value.Kind() == reflect.Pointer {
								value = value.Elem()
							}
							if value.Kind() != reflect.Struct {
								return
							}
							var count int64
							require.NoError(
								t,
								tx.Session(&gorm.Session{NewDB: true}).
									Table("original_body_field_heads").
									Where("body_id=? AND source_table='messages' AND row_key=? AND column_name='images'", value.FieldByName("BodyID").String(), user.ID).
									Count(&count).
									Error,
							)
							if count > 0 {
								revoke(tx)
							}
						}),
				)
			} else if mode != "no-kb" && mode != "cross-kb" && mode != "raw-unconsumed" {
				// Content lease inserts use the actual native INSERT path rather
				// than ORM Create. Observe successful Raw execution, not a mock.
				require.NoError(
					t,
					m.f.db.Callback().
						Raw().
						After("gorm:raw").
						Register("actual-own-growth-late-refusal", func(tx *gorm.DB) {
							if triggered || tx.Error != nil ||
								!strings.Contains(tx.Statement.SQL.String(), "INSERT INTO nextcloud_content_leases") ||
								len(tx.Statement.Vars) < 10 {
								return
							}
							knowledge, ok := tx.Statement.Vars[3].(string)
							if !ok || knowledge != k.ID {
								return
							}
							kind, ok := tx.Statement.Vars[4].(string)
							if !ok || kind != "read" {
								return
							}
							revoke(tx)
						}),
				)
			}

			writeErr := messages.UpdateMessageImages(
				requestCtx,
				session.ID,
				user.ID,
				types.MessageImages{{URL: "https://fixture.invalid/growth-owned", Caption: "actual owned B image"}},
			)
			_ = m.f.db.Callback().Create().Remove("actual-own-growth-late-refusal")
			_ = m.f.db.Callback().Raw().Remove("actual-own-growth-late-refusal")
			positive := mode == "no-kb" || mode == "cross-kb" || mode == "raw-unconsumed"
			if !positive {
				require.True(t, triggered)
				require.Error(t, writeErr)
				require.Same(t, oldSnapshot, userGuard.snapshot, "failed transfer exposes no new snapshot")
				require.Equal(t, oldLeases, userGuard.leases, "failed transfer leaves its prior owned leases intact")
				var addedHeld int64
				require.NoError(
					t,
					m.f.db.Table("nextcloud_content_leases").
						Where("lease_id NOT IN ? AND released_at_ms IS NULL", beforeIDs).
						Count(&addedHeld).
						Error,
				)
				require.Zero(t, addedHeld, "only newly added content pins are rolled back")
			} else {
				require.NoError(t, writeErr)
				require.NoError(t, userGuard.Recheck())
				require.NoError(t, oldCtx.Err(), "original guard cancellation relationship remains")
				require.Equal(t, types.CallerFromContext(oldCtx), types.CallerFromContext(userGuard.Context()))
				require.Empty(
					t,
					userGuard.snapshot.Messages[0].Images,
					"raw-only material remains unconsumed by this guard",
				)
				require.Equal(t, oldProof.BodyDigest, userGuard.snapshot.Proofs[0].BodyDigest)
				require.Empty(
					t,
					userGuard.snapshot.Proofs[0].GeneratedFields,
					"physical hold does not recertify generated bytes as model input",
				)
				var broad, exact int
				for _, lease := range userGuard.leases {
					if lease.TenantID == 7 && lease.KnowledgeBaseID == "kb" {
						if lease.KnowledgeID == "" {
							broad++
						}
						if lease.KnowledgeID == k.ID {
							exact++
							require.Equal(t, actual.Proof.SourceLineage.Sources[0].Revision, uint64(lease.Epoch))
						}
					}
				}
				require.Equal(t, 1, broad)
				require.Equal(t, 1, exact)
				if mode == "cross-kb" {
					var ordinary int
					for _, lease := range userGuard.leases {
						if lease.KnowledgeBaseID == "ordinary-a" && lease.KnowledgeID == "" {
							ordinary++
						}
					}
					require.Equal(t, 1, ordinary)
				}
				if mode == "raw-unconsumed" {
					_, err = messages.GetMessage(writer, session.ID, user.ID)
					require.NoError(t, err)
					require.Empty(t, userGuard.snapshot.Messages[0].Images)
				}
			}
			CloseMessageMaterialOutputBoundary(writer)
			var liveContent, liveBodies int64
			require.NoError(
				t,
				m.f.db.Table("nextcloud_content_leases").
					Where("kind='read' AND released_at_ms IS NULL").
					Count(&liveContent).
					Error,
			)
			require.Zero(t, liveContent)
			require.NoError(
				t,
				m.f.db.Table("original_body_leases").
					Where("kind='read' AND released_at_ms IS NULL").
					Count(&liveBodies).
					Error,
			)
			require.Zero(t, liveBodies)
		})
	}
}

func TestMessageMaterialOwnGrowthActualSQLite(t *testing.T) {
	runMessageMaterialOwnGrowthActual(t, false)
}

func TestPostgresMessageMaterialOwnGrowthActual(t *testing.T) {
	runMessageMaterialOwnGrowthActual(t, true)
}
