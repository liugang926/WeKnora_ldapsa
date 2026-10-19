package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func runParentBodyCopyActual793(t *testing.T, pg bool) {
	runParentBodyCopyCase(t, pg, "positive")
}

func runParentBodyCopyCase(t *testing.T, pg bool, mode string) {
	var user *types.Message
	var rawBefore string
	messages, _, _, db, ctx, session, body := messageBodyIntegrationFixture(
		t,
		pg,
		func(messages *messageService, db *gorm.DB, store *repository.KnowledgeInputProvenanceStore, ctx context.Context, session *types.Session) {
			var e error
			input := &types.Message{
				SessionID:   session.ID,
				RequestID:   "actual-old-793-parent",
				Role:        "user",
				Content:     "real old parent body sentinel",
				IsCompleted: true,
			}
			if mode == "unknown-scope" {
				input.ExecutionContext = types.MessageExecutionContext{KnowledgeBaseIDs: []string{"kb"}}
			}
			user, e = messages.CreateMessage(ctx, input)
			require.NoError(t, e)
			writer := WithMessageMaterialOutputBoundary(ctx)
			defer CloseMessageMaterialOutputBoundary(writer)
			turn := types.NewRAGSourceLineage(func(checkCtx context.Context, lineage *types.SourceLineage) error {
				if e := lineage.RequireComplete(); e != nil {
					return e
				}
				return CheckMessageMaterialOutputBoundary(writer)
			})
			writer = types.WithRAGSourceLineage(writer, turn)
			_, e = messages.GetMessage(writer, session.ID, user.ID)
			require.NoError(t, e)
			slot := writer.Value(messageMaterialOutputKey{}).(*messageMaterialOutputSlot)
			bytes, e := json.Marshal([]*types.MessageMaterialSnapshot{slot.Guards[0].snapshot})
			require.NoError(t, e)
			// Restore the exact persistence expression in frozen 7930: marshal actual
			// boundary.Parents, with all genuine actor/read/admission controls intact.
			name := "actual-793-parent-serializer"
			require.NoError(t, db.Callback().Create().Before("gorm:create").Register(name, func(tx *gorm.DB) {
				if tx.Statement.Table != "message_generated_field_admissions" {
					return
				}
				value := tx.Statement.ReflectValue
				for value.Kind() == reflect.Pointer {
					value = value.Elem()
				}
				field := value.FieldByName("ParentMaterials")
				require.True(t, field.CanSet())
				field.SetString(string(bytes))
			}))
			require.NoError(
				t,
				messages.UpdateMessageRenderedContent(writer, session.ID, user.ID, "actual admitted rendered output"),
			)
			require.NoError(t, db.Callback().Create().Remove(name))
			require.NoError(
				t,
				db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE message_id=? AND field='rendered_content'", user.ID).
					Scan(&rawBefore).
					Error,
			)
			require.Contains(t, rawBefore, user.Content)
		},
	)
	if mode == "unknown-scope" {
		_, err := body.Inventory(ctx, 100)
		require.NoError(t, err)
		var scopes int64
		require.NoError(
			t,
			db.Raw("SELECT count(*) FROM original_body_lease_scopes s JOIN original_body_field_heads h ON h.body_id=s.body_id JOIN message_generated_field_admissions a ON a.id=h.row_key WHERE h.source_table='message_generated_field_admissions' AND a.message_id=?", user.ID).
				Scan(&scopes).
				Error,
		)
		require.Zero(
			t,
			scopes,
			"an actual pre152 KB selector without immutable resolved scope cannot be backfilled from current KB metadata",
		)
		require.NoError(t, messages.DeleteMessage(ctx, session.ID, user.ID))
		deleted := "datetime('now','-2 hours')"
		if pg {
			deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
		}
		require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", user.ID).Error)
		_, err = body.PurgeDue(context.Background(), 100)
		require.NoError(t, err)
		var retained string
		require.NoError(
			t,
			db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE message_id=? AND field='rendered_content'", user.ID).
				Scan(&retained).
				Error,
		)
		require.Contains(t, retained, user.Content)
		return
	}
	if mode == "unknown-proof" {
		// A historical corrupted extra body is not covered by the admitted
		// source digest. Maintenance seeding cannot make it an actual producer.
		var admission map[string]any
		require.NoError(
			t,
			db.Table("message_generated_field_admissions").
				Where("message_id=? AND field='rendered_content'", user.ID).
				Take(&admission).
				Error,
		)
		var snapshots []*types.MessageMaterialSnapshot
		require.NoError(t, json.Unmarshal([]byte(rawBefore), &snapshots))
		snapshots[0].Messages[0].Content = "unaccounted historical copied bytes"
		altered, e := json.Marshal(snapshots)
		require.NoError(t, e)
		admission["id"] = "unknown-old-parent-copy"
		admission["parent_materials"] = string(altered)
		require.NoError(t, db.Table("message_generated_field_admissions").Create(&admission).Error)
		require.NoError(t, messages.DeleteMessage(ctx, session.ID, user.ID))
		deleted := "datetime('now','-2 hours')"
		if pg {
			deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
		}
		require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", user.ID).Error)
		_, e = body.Inventory(ctx, 100)
		require.NoError(t, e)
		_, e = body.PurgeDue(context.Background(), 100)
		require.NoError(t, e)
		var marker int64
		require.NoError(
			t,
			db.Raw("SELECT count(*) FROM original_body_lease_scopes s JOIN original_body_field_heads h ON h.body_id=s.body_id WHERE h.source_table='message_generated_field_admissions' AND h.row_key='unknown-old-parent-copy' AND s.knowledge_base_id='' ").
				Scan(&marker).
				Error,
		)
		require.Zero(t, marker)
		var remains string
		require.NoError(
			t,
			db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE id='unknown-old-parent-copy'").
				Scan(&remains).
				Error,
		)
		require.Contains(t, remains, "unaccounted historical copied bytes")
		return
	}
	// Real legacy rows are now present before migration 152 callbacks/inventory.
	_, e := body.Inventory(ctx, 100)
	require.NoError(t, e)
	var bodyID string
	require.NoError(
		t,
		db.Raw("SELECT h.body_id FROM original_body_field_heads h JOIN message_generated_field_admissions a ON a.id=h.row_key WHERE h.source_table='message_generated_field_admissions' AND a.message_id=? AND a.field='rendered_content'", user.ID).
			Scan(&bodyID).
			Error,
	)
	require.NotEmpty(t, bodyID)
	var marker int64
	require.NoError(
		t,
		db.Table("original_body_lease_scopes").
			Where("body_id=? AND tenant_id=7 AND knowledge_base_id=''", bodyID).
			Count(&marker).
			Error,
	)
	require.Equal(
		t,
		int64(1),
		marker,
		"immutable original no-KB controls account for the old copy without current-pointer backfill",
	)
	restore := parentCopyDatabaseSnapshot(t, db, pg)
	n, e := body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "actual current host protects its old immutable parent copy")
	guard, e := messages.beginMessageMaterialRead(
		ctx,
		types.MessageMaterialReadRequest{TenantID: 7, SessionID: session.ID, MessageIDs: []string{user.ID}},
	)
	require.NoError(t, e)
	var held int64
	require.NoError(
		t,
		db.Table("original_body_leases").Where("body_id=? AND released_at_ms IS NULL", bodyID).Count(&held).Error,
	)
	require.Positive(t, held, "actual original reader owns copied parent body lease")
	require.NoError(t, messages.DeleteMessage(ctx, session.ID, user.ID))
	deleted := "datetime('now','-2 hours')"
	if pg {
		deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", user.ID).Error)
	n, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "actual held reader prevents purge after real closure")
	guard.Close()
	n, e = body.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Positive(t, n)
	var cleared string
	require.NoError(
		t,
		db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE message_id=? AND field='rendered_content'", user.ID).
			Scan(&cleared).
			Error,
	)
	require.NotContains(t, cleared, user.Content)
	var a, b []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(rawBefore), &a))
	require.NoError(t, json.Unmarshal([]byte(cleared), &b))
	for _, entry := range a {
		delete(entry, "Messages")
	}
	for _, entry := range b {
		delete(entry, "Messages")
	}
	require.Equal(t, a, b, "only exact Messages leaf is cleared; all immutable metadata/proofs survive")
	restore()
	// Restoring all DB rows leaves the independent current external decision.
	body = repository.NewOriginalBodyRetentionStore(db)
	require.Error(t, body.VerifyJournalReady())
	_, e = body.ReconcileExternalPurge(context.Background())
	require.Error(t, e, "restored current ownership must be closed again before replay")
	// App reads remain fail-closed during restore reconciliation. The offline
	// operator reestablishes the already observed deletion independently.
	require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", user.ID).Error)
	_, e = body.ReconcileExternalPurge(context.Background())
	require.NoError(t, e)
	require.NoError(t, body.VerifyJournalReady())
	var rowID string
	require.NoError(
		t,
		db.Raw("SELECT id FROM message_generated_field_admissions WHERE message_id=? AND field='rendered_content'", user.ID).
			Scan(&rowID).
			Error,
	)

	require.NoError(
		t,
		db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE id=?", rowID).
			Scan(&cleared).
			Error,
	)
	require.NotContains(t, cleared, user.Content)
}
func TestParentBodyCopyActual793SQLite(t *testing.T)   { runParentBodyCopyActual793(t, false) }
func TestPostgresParentBodyCopyActual793(t *testing.T) { runParentBodyCopyActual793(t, true) }

func parentCopyDatabaseSnapshot(t *testing.T, db *gorm.DB, pg bool) func() {
	if pg {
		var schema string
		require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
		backup := "parent_snapshot_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		require.NoError(t, db.Exec("CREATE SCHEMA "+backup).Error)
		t.Cleanup(func() { db.Exec("DROP SCHEMA " + backup + " CASCADE") })
		var tables []string
		require.NoError(
			t,
			db.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema=? AND table_type='BASE TABLE' ORDER BY table_name", schema).
				Scan(&tables).
				Error,
		)
		for _, table := range tables {
			require.NoError(t, db.Exec("CREATE TABLE "+backup+"."+table+" AS TABLE "+schema+"."+table).Error)
		}
		return func() {
			qualified := []string{}
			for _, table := range tables {
				qualified = append(qualified, schema+"."+table)
			}
			require.NoError(t, db.Exec("TRUNCATE "+strings.Join(qualified, ",")+" CASCADE").Error)
			require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
				if e := tx.Exec("SET LOCAL session_replication_role=replica").Error; e != nil {
					return e
				}
				for _, table := range tables {
					if e := tx.Exec(
						"INSERT INTO " + schema + "." + table + " SELECT * FROM " + backup + "." + table,
					).Error; e != nil {
						return e
					}
				}
				return nil
			}))
		}
	}
	var files []struct {
		Seq        int
		Name, File string
	}
	require.NoError(t, db.Raw("PRAGMA database_list").Scan(&files).Error)
	var file string
	for _, f := range files {
		if f.Name == "main" {
			file = f.File
		}
	}
	require.NotEmpty(t, file)
	snapshot := filepath.Join(t.TempDir(), "complete-copy-snapshot.sqlite")
	require.NoError(t, db.Exec("VACUUM INTO ?", snapshot).Error)
	return func() {
		sql, e := db.DB()
		require.NoError(t, e)
		require.NoError(t, sql.Close())
		bytes, e := os.ReadFile(snapshot)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(file, bytes, 0o600))
		fresh, e := gorm.Open(sqlite.Open(file), &gorm.Config{})
		require.NoError(t, e)
		sql, e = fresh.DB()
		require.NoError(t, e)
		sql.SetMaxOpenConns(1)
		t.Cleanup(func() { sql.Close() })
		db.Config.ConnPool = fresh.Config.ConnPool
		db.Statement.ConnPool = fresh.Statement.ConnPool
	}
}

func TestParentBodyCopyUnknownProofSQLite(t *testing.T) {
	runParentBodyCopyCase(t, false, "unknown-proof")
}

func TestPostgresParentBodyCopyUnknownProof(t *testing.T) {
	runParentBodyCopyCase(t, true, "unknown-proof")
}

func TestParentBodyCopyUnknownOriginalScopeSQLite(t *testing.T) {
	runParentBodyCopyCase(t, false, "unknown-scope")
}

func TestPostgresParentBodyCopyUnknownOriginalScope(t *testing.T) {
	runParentBodyCopyCase(t, true, "unknown-scope")
}

// Current receipt disappearance is deliberately corrupted volatile bookkeeping,
// not a lifetime close or a new producer proof. Both old copies below came from
// the actual writer/old serializer; each original host must close independently.
func runParentBodyCopyMultipleLiveHosts(t *testing.T, pg bool) {
	var hosts []*types.Message
	messages, _, _, db, ctx, session, body := messageBodyIntegrationFixture(
		t,
		pg,
		func(messages *messageService, db *gorm.DB, store *repository.KnowledgeInputProvenanceStore, ctx context.Context, session *types.Session) {
			source, err := messages.CreateMessage(
				ctx,
				&types.Message{
					SessionID:   session.ID,
					RequestID:   "actual-multi-copy-source",
					Role:        "user",
					Content:     "actual original shared parent input",
					IsCompleted: true,
				},
			)
			require.NoError(t, err)
			for _, request := range []string{"old-copy-host-one", "old-copy-host-two"} {
				host, err := messages.CreateMessage(
					ctx,
					&types.Message{
						SessionID:   session.ID,
						RequestID:   request,
						Role:        "user",
						Content:     request,
						IsCompleted: true,
					},
				)
				require.NoError(t, err)
				hosts = append(hosts, host)
			}
			for _, host := range hosts {
				writer := WithMessageMaterialOutputBoundary(ctx)
				defer CloseMessageMaterialOutputBoundary(writer)
				writer = types.WithRAGSourceLineage(
					writer,
					types.NewRAGSourceLineage(func(check context.Context, lineage *types.SourceLineage) error {
						if err := lineage.RequireComplete(); err != nil {
							return err
						}
						return CheckMessageMaterialOutputBoundary(writer)
					}),
				)
				_, err := messages.GetMessage(writer, session.ID, source.ID)
				require.NoError(t, err)
				_, err = messages.GetMessage(writer, session.ID, host.ID)
				require.NoError(t, err)
				parents, err := messageMaterialBoundarySnapshots(writer)
				require.NoError(t, err)
				oldBytes, err := json.Marshal(parents)
				require.NoError(t, err)
				require.NoError(
					t,
					db.Callback().
						Create().
						Before("gorm:create").
						Register("actual-multi-old-serializer", func(tx *gorm.DB) {
							if tx.Statement.Table != "message_generated_field_admissions" {
								return
							}
							value := tx.Statement.ReflectValue
							for value.Kind() == reflect.Pointer {
								value = value.Elem()
							}
							if value.FieldByName("MessageID").String() == host.ID &&
								value.FieldByName("Field").String() == "rendered_content" {
								value.FieldByName("ParentMaterials").SetString(string(oldBytes))
							}
						}),
				)
				require.NoError(
					t,
					messages.UpdateMessageRenderedContent(
						writer,
						session.ID,
						host.ID,
						"actual derived output for "+host.RequestID,
					),
				)
				require.NoError(t, db.Callback().Create().Remove("actual-multi-old-serializer"))
				CloseMessageMaterialOutputBoundary(writer)
			}
		},
	)
	_, err := body.Inventory(ctx, 100)
	require.NoError(t, err)
	ids := map[string]string{}
	for _, host := range hosts {
		var id string
		require.NoError(
			t,
			db.Raw("SELECT h.body_id FROM original_body_field_heads h JOIN message_generated_field_admissions a ON a.id=h.row_key WHERE h.source_table='message_generated_field_admissions' AND a.message_id=? AND a.field='rendered_content'", host.ID).
				Scan(&id).
				Error,
		)
		require.NotEmpty(t, id)
		ids[host.ID] = id
		var complete int64
		require.NoError(
			t,
			db.Table("original_body_lease_scopes").
				Where("body_id=? AND tenant_id=7 AND knowledge_base_id=''", id).
				Count(&complete).
				Error,
		)
		require.Equal(t, int64(1), complete)
	}
	require.NotEqual(
		t,
		ids[hosts[0].ID],
		ids[hosts[1].ID],
		"independent original admissions keep distinct physical copy identities",
	)
	// Do not alter any immutable original admission or body proof. Simulate
	// loss of only the current incoming field-receipt bookkeeping for both hosts.
	require.NoError(
		t,
		db.Exec(
			"DELETE FROM message_generated_field_receipts WHERE message_id IN ? AND field='rendered_content'",
			[]string{hosts[0].ID, hosts[1].ID},
		).Error,
	)
	assertState := func(host *types.Message, want string) {
		var state string
		require.NoError(
			t,
			db.Table("original_body_payloads").Select("state").Where("id=?", ids[host.ID]).Scan(&state).Error,
		)
		require.Equal(t, want, state)
	}
	_, err = body.PurgeDue(context.Background(), 100)
	require.NoError(t, err)
	for _, host := range hosts {
		assertState(host, "live")
	}
	closeHost := func(host *types.Message) {
		require.NoError(t, messages.DeleteMessage(ctx, session.ID, host.ID))
		deleted := "datetime('now','-2 hours')"
		if pg {
			deleted = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
		}
		require.NoError(t, db.Exec("UPDATE messages SET deleted_at="+deleted+" WHERE id=?", host.ID).Error)
	}
	closeHost(hosts[0])
	_, err = body.PurgeDue(context.Background(), 100)
	require.NoError(t, err)
	assertState(hosts[0], "purged")
	assertState(hosts[1], "live")
	_, err = messages.GetMessage(ctx, session.ID, hosts[0].ID)
	require.Error(t, err, "closed consumer cannot restore the cleared copy")
	closeHost(hosts[1])
	_, err = body.PurgeDue(context.Background(), 100)
	require.NoError(t, err)
	assertState(hosts[1], "purged")
	for _, host := range hosts {
		var carrier string
		require.NoError(
			t,
			db.Raw("SELECT parent_materials FROM message_generated_field_admissions WHERE message_id=? AND field='rendered_content'", host.ID).
				Scan(&carrier).
				Error,
		)
		require.NotContains(t, carrier, "actual original shared parent input")
		require.Contains(t, carrier, "MetadataDigest")
	}
}

func TestParentBodyCopyMultipleLiveHostsSQLite(t *testing.T) {
	runParentBodyCopyMultipleLiveHosts(t, false)
}

func TestPostgresParentBodyCopyMultipleLiveHosts(t *testing.T) {
	runParentBodyCopyMultipleLiveHosts(t, true)
}
