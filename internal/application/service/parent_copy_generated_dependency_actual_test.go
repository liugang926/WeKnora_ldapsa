package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// Raw user input has no KB dependency. Its actual generated image field uses
// a separately admitted source-backed answer. Only the old Messages serializer
// is restored at the real generated-field Create boundary; receipts and parent
// material are obtained from the actual producers/readers, never raw SQL seals.
func runParentCopyActualGeneratedDependency(t *testing.T, pg bool) {
	for _, mode := range []string{
		"known-source-closure",
		"source-still-active",
		"missing-source-coverage",
		"missing-existing-scope",
		"corrupt-existing-scope",
		"changed-generated-source",
		"changed-generated-parent",
		"wrong-generation",
		"wrong-admission",
	} {
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
						("CREATE FUNCTION uuid_generate_v4() RETURNS uuid LANGUAGE sql AS $$ SELECT " +
							"public.uuid_generate_v4() $$"),
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
			user, err := messages.CreateMessage(
				ctx,
				&types.Message{
					SessionID:   session.ID,
					RequestID:   "raw-no-kb",
					Role:        "user",
					Content:     "actual user raw input without a KB",
					IsCompleted: true,
				},
			)
			require.NoError(t, err)
			host, err := messages.CreateMessage(
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
			parent.Content, parent.IsCompleted, parent.SourceLineage = "actual source-derived answer",
				true,
				actual.Proof.SourceLineage
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
					func(check context.Context, l *types.SourceLineage) error {
						return m.f.guard.Check(
							check,
							l,
							scope,
						)
					},
				),
			)
			_, err = messages.GetMessage(writer, session.ID, parent.ID)
			require.NoError(t, err)
			_, err = messages.GetMessage(writer, session.ID, user.ID)
			require.NoError(t, err)
			require.NoError(
				t,
				messages.UpdateMessageImages(
					writer,
					session.ID,
					user.ID,
					types.MessageImages{
						{URL: "https://fixture.invalid/actual-derived-image", Caption: "actual derived image field"},
					},
				),
			)
			CloseMessageMaterialOutputBoundary(writer)
			copyWriter := WithMessageMaterialOutputBoundary(
				context.WithValue(ctx, agentHistoryCurrentRequestKey{}, "different-current-request"),
			)
			defer CloseMessageMaterialOutputBoundary(copyWriter)
			copyWriter = types.WithRAGSourceLineage(
				copyWriter,
				types.NewRAGSourceLineage(
					func(check context.Context, l *types.SourceLineage) error {
						return m.f.guard.Check(
							check,
							l,
							scope,
						)
					},
				),
			)
			_, err = messages.GetMessage(copyWriter, session.ID, user.ID)
			require.NoError(t, err)
			parents, err := messageMaterialBoundarySnapshots(copyWriter)
			require.NoError(t, err)
			require.Len(t, parents, 1)
			require.NotEmpty(t, parents[0].Proofs[0].GeneratedFields)
			var imageProof *types.MessageGeneratedFieldProof
			for _, field := range parents[0].Proofs[0].GeneratedFields {
				if field.Field == "images" {
					imageProof = field
				}
			}
			require.NotNil(t, imageProof)
			// A genuine raw no-KB input later consumes B through the actual image
			// producer. The old serializer preserved both proof lineages.
			require.Empty(t, user.ExecutionContext.KnowledgeBaseIDs)
			require.Empty(t, user.SourceLineage.Sources)
			require.Len(t, imageProof.SourceLineage.Sources, 1)
			require.Len(t, parents[0].Proofs[0].SourceLineage.Sources, 1)
			_, err = messages.GetMessage(copyWriter, session.ID, host.ID)
			require.NoError(t, err)
			originalParents, err := messageMaterialBoundarySnapshots(copyWriter)
			require.NoError(t, err)
			require.Len(t, originalParents, 2)
			// Negative maintenance seeds represent deliberately corrupt historical
			// associations. They are never an authenticated producer or a positive
			// receipt; inventory must leave their copied body unaccounted.
			if mode == "changed-generated-source" {
				imageProof.SourceLineage, _ = types.NewCompleteSourceLineage()
			}
			if mode == "changed-generated-parent" {
				var corrupted map[string]any
				require.NoError(
					t,
					m.f.db.Table("message_generated_field_admissions").
						Where("id=?", imageProof.AdmissionID).
						Take(&corrupted).
						Error,
				)
				corrupted["id"] = "unknown-historical-generated-parent"
				corrupted["parent_materials"] = `[]`
				require.NoError(t, m.f.db.Table("message_generated_field_admissions").Create(&corrupted).Error)
				imageProof.AdmissionID = corrupted["id"].(string)
			}
			if mode == "wrong-generation" {
				parents[0].Proofs[0].GenerationID = "unknown-original-generation"
			}
			if mode == "wrong-admission" {
				parents[0].Proofs[0].AdmissionID = "unknown-material-admission"
			}
			oldParents := append(parents, originalParents[1])
			oldBytes, err := json.Marshal(oldParents)
			require.NoError(t, err)
			var carrierID string
			if mode == "missing-existing-scope" || mode == "corrupt-existing-scope" {
				require.NoError(
					t,
					m.f.db.Callback().
						Create().
						Before("gorm:create").
						Register("historical-copy-scope-corruption", func(tx *gorm.DB) {
							if tx.Statement.Table != "original_body_lease_scopes" || carrierID == "" {
								return
							}
							value := tx.Statement.ReflectValue
							for value.Kind() == reflect.Pointer {
								value = value.Elem()
							}
							if value.Kind() != reflect.Slice || value.Len() == 0 {
								return
							}
							bodyID := value.Index(0).FieldByName("BodyID").String()
							var domain string
							require.NoError(
								t,
								tx.Session(&gorm.Session{NewDB: true}).
									Table("original_body_payloads").
									Select("origin_domain_sha256").
									Where("id=?", bodyID).
									Scan(&domain).
									Error,
							)
							identity := sha256.Sum256([]byte("7///message_generated_field_admissions/" + carrierID))
							if domain != hex.EncodeToString(identity[:]) {
								return
							}
							if mode == "corrupt-existing-scope" {
								for i := 0; i < value.Len(); i++ {
									if value.Index(i).FieldByName("KnowledgeBaseID").String() == "kb" {
										value.Index(i).FieldByName("KnowledgeBaseID").SetString("unproven-original-kb")
									}
								}
								return
							}
							filtered := reflect.MakeSlice(value.Type(), 0, value.Len())
							for i := 0; i < value.Len(); i++ {
								if value.Index(i).FieldByName("KnowledgeBaseID").String() != "kb" {
									filtered = reflect.Append(filtered, value.Index(i))
								}
							}
							value.Set(filtered)
						}),
				)
			}
			require.NoError(
				t,
				m.f.db.Callback().
					Create().
					Before("gorm:create").
					Register("old-parent-copy-serializer", func(tx *gorm.DB) {
						if tx.Statement.Table != "message_generated_field_admissions" {
							return
						}
						v := tx.Statement.ReflectValue
						for v.Kind() == reflect.Pointer {
							v = v.Elem()
						}
						if v.FieldByName("Field").String() == "rendered_content" {
							carrierID = v.FieldByName("ID").String()
							v.FieldByName("ParentMaterials").SetString(string(oldBytes))
						}
					}),
			)
			writeErr := messages.UpdateMessageRenderedContent(
				copyWriter,
				session.ID,
				host.ID,
				"actual new rendered result",
			)
			if mode == "wrong-generation" || mode == "wrong-admission" {
				require.Error(t, writeErr, "the actual writer recheck must refuse an untrusted old parent association")
			} else {
				require.NoError(t, writeErr)
			}
			require.NoError(t, m.f.db.Callback().Create().Remove("old-parent-copy-serializer"))
			if mode == "missing-existing-scope" || mode == "corrupt-existing-scope" {
				require.NoError(t, m.f.db.Callback().Create().Remove("historical-copy-scope-corruption"))
			}
			var copyID string
			require.NoError(
				t,
				m.f.db.Raw(("SELECT h.body_id FROM original_body_field_heads h JOIN message_generated_f"+
					"ield_admissions a ON a.id=h.row_key WHERE h.source_table='message_generate"+
					"d_field_admissions' AND a.message_id=? AND a.field='rendered_content'"), host.ID).
					Scan(&copyID).
					Error,
			)
			require.NotEmpty(t, copyID)
			var scopeCount int64
			require.NoError(
				t,
				m.f.db.Table("original_body_lease_scopes").
					Where("body_id=? AND tenant_id=7 AND knowledge_base_id='kb'", copyID).
					Count(&scopeCount).
					Error,
			)
			if mode == "wrong-generation" || mode == "wrong-admission" || mode == "changed-generated-source" ||
				mode == "changed-generated-parent" ||
				mode == "missing-existing-scope" ||
				mode == "corrupt-existing-scope" {
				require.Zero(t, scopeCount)
				_, err = repository.NewOriginalBodyRetentionStore(m.f.db).Inventory(ctx, 100)
				require.NoError(t, err)
				require.NoError(
					t,
					m.f.db.Table("original_body_lease_scopes").
						Where("body_id=? AND knowledge_base_id='kb'", copyID).
						Count(&scopeCount).
						Error,
				)
				require.Zero(t, scopeCount, "scan must not upgrade a present inconsistent historical marker")
				CloseMessageMaterialOutputBoundary(copyWriter)
				require.NoError(t, messages.DeleteMessage(ctx, session.ID, host.ID))
				deleted := "datetime('now','-2 hours')"
				if pg {
					deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
				}
				require.NoError(t, m.f.db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", host.ID).Error)
				_, err = repository.NewOriginalBodyRetentionStore(m.f.db).PurgeDue(context.Background(), 100)
				require.NoError(t, err)
				var retained string
				require.NoError(
					t,
					m.f.db.Raw(("SELECT parent_materials FROM message_generated_field_admissions WHERE mess"+
						"age_id=? AND field='rendered_content'"), host.ID).
						Scan(&retained).
						Error,
				)
				require.Contains(t, retained, "actual user raw input without a KB")
				return
			}
			require.Positive(
				t,
				scopeCount,
				"generated KB/source dependency cannot disappear behind raw no-KB user input",
			)
			var sourceRef int64
			require.NoError(
				t,
				m.f.db.Raw(("SELECT count(*) FROM original_body_parent_refs p JOIN knowledge_input_admi"+
					"ssions a ON a.admission_id=p.admission_id WHERE p.body_id=? AND a.knowledg"+
					"e_id=? AND a.build_generation=?"), copyID, k.ID, actual.Proof.SourceLineage.Sources[0].Revision).
					Scan(&sourceRef).
					Error,
			)
			require.Equal(
				t,
				int64(1),
				sourceRef,
				"journal scope must name the original captured source admission/version",
			)
			guard, err := messages.beginMessageMaterialRead(
				ctx,
				types.MessageMaterialReadRequest{TenantID: 7, SessionID: session.ID, MessageIDs: []string{host.ID}},
			)
			require.NoError(t, err)
			var held int64
			require.NoError(
				t,
				m.f.db.Table("original_body_leases").
					Where("body_id=? AND released_at_ms IS NULL", copyID).
					Count(&held).
					Error,
			)
			require.Positive(t, held)
			var exactHeld int64
			require.NoError(
				t,
				m.f.db.Table("nextcloud_content_leases").
					Where(
						("tenant_id=7 AND knowledge_base_id='kb' AND knowledge_id=? AND epoch=? AND "+
							"kind='read' AND released_at_ms IS NULL"),
						k.ID,
						actual.Proof.SourceLineage.Sources[0].Revision,
					).
					Count(&exactHeld).
					Error,
			)
			require.Positive(
				t,
				exactHeld,
				"the real material guard also holds the exact originally captured source version",
			)
			require.NoError(t, messages.DeleteMessage(ctx, session.ID, host.ID))
			deleted := "datetime('now','-2 hours')"
			if pg {
				deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
			}
			require.NoError(t, m.f.db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", host.ID).Error)
			bodyStore := repository.NewOriginalBodyRetentionStore(m.f.db)
			_, err = bodyStore.PurgeDue(context.Background(), 100)
			require.NoError(t, err)
			assertCopyLive := func() {
				var value string
				require.NoError(
					t,
					m.f.db.Raw("SELECT state FROM original_body_payloads WHERE id=?", copyID).Scan(&value).Error,
				)
				require.Equal(t, "live", value)
			}
			assertCopyLive()
			guard.Close()
			CloseMessageMaterialOutputBoundary(copyWriter)
			declareCoverage := func() {
				// Explicit isolated test-contract rollout declaration, not a
				// producer/material seal or production coverage evidence. All
				// real fixture workers and material/source guards have drained.
				clock := "CAST((julianday('now')-2440587.5)*86400000 AS BIGINT)"
				if pg {
					clock = "CAST(EXTRACT(EPOCH FROM clock_timestamp())*1000 AS BIGINT)"
				}
				require.NoError(
					t,
					m.f.db.Exec(
						("INSERT INTO nextcloud_content_lease_coverage(tenant_id,knowledge_base_id,a"+
							"ctivated_at_ms,legacy_drained_at_ms,reader_revision,builder_revision) VALU"+
							"ES(7,'kb',")+
							clock+","+clock+
							(",'actual-copy-pipeline-reader-contract','actual-copy-pipeline-builder-cont"+
								"ract')"),
					).Error,
				)
			}
			if mode != "missing-source-coverage" {
				declareCoverage()
			}
			if mode == "source-still-active" {
				_, err = bodyStore.PurgeDue(context.Background(), 100)
				require.NoError(t, err)
				assertCopyLive()
				return
			}
			versions := m.f.s.repo.(interface {
				TombstoneNextcloudVersion(context.Context, uint64, string, string, string) error
			})
			require.NoError(t, versions.TombstoneNextcloudVersion(ctx, 7, "kb", m.ds.ID, "nextcloud:instance:77"))
			m.allow.Store(false)
			_, err = messages.GetMessage(
				context.WithValue(ctx, agentHistoryCurrentRequestKey{}, "source-consuming-reader"),
				session.ID,
				user.ID,
			)
			require.Error(t, err, "original generated consumer must refuse retired source before copy purge")
			_, err = messages.GetMessage(ctx, session.ID, host.ID)
			require.Error(t, err, "closed host must not read or restore the copied field")

			_, err = bodyStore.PurgeDue(context.Background(), 100)
			require.NoError(t, err)
			if mode == "missing-source-coverage" {
				assertCopyLive()
				return
			}
			var cleared string
			require.NoError(
				t,
				m.f.db.Raw(("SELECT parent_materials FROM message_generated_field_admissions WHERE mess"+
					"age_id=? AND field='rendered_content'"), host.ID).
					Scan(&cleared).
					Error,
			)
			require.NotContains(t, cleared, "actual user raw input without a KB")
			require.NotContains(t, cleared, "actual-derived-image")
			require.Contains(t, cleared, "MetadataDigest")
			require.False(t, strings.Contains(cleared, `"Messages":[`))
		})
	}
}

func TestParentCopyActualGeneratedDependencySQLite(t *testing.T) {
	runParentCopyActualGeneratedDependency(t, false)
}

func TestPostgresParentCopyActualGeneratedDependency(t *testing.T) {
	runParentCopyActualGeneratedDependency(t, true)
}
