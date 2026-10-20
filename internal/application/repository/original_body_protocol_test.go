package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type bodyProtocolRow struct {
	ManifestID                                                             string `gorm:"primaryKey"`
	TenantID                                                               uint64
	KnowledgeBaseID, KnowledgeID, AdmissionID, SourceLineage, ActualInputs string
}

func (bodyProtocolRow) TableName() string { return "knowledge_auto_tag_input_manifests" }
func bodyProtocolFixture(t *testing.T, pg bool, ownedKeys ...[]byte) (*gorm.DB, *OriginalBodyRetentionStore) {
	t.Helper()
	var db *gorm.DB
	var e error
	if pg {
		dsn := os.Getenv("WEKNORA_GC_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("disposable PG DSN required")
		}
		admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		require.NoError(t, err)
		schema := "body_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
		require.NoError(t, admin.Exec("CREATE SCHEMA "+schema).Error)
		db, e = gorm.Open(
			postgres.Open(dsn+" search_path="+schema),
			&gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableForeignKeyConstraintWhenMigrating: true},
		)
		require.NoError(t, e)
		t.Cleanup(func() {
			sql, _ := db.DB()
			require.NoError(t, sql.Close())
			admin.Exec("DROP SCHEMA " + schema + " CASCADE")
			sql, _ = admin.DB()
			require.NoError(t, sql.Close())
		})
	} else {
		db, e = gorm.Open(
			sqlite.Open(filepath.Join(t.TempDir(), "body.sqlite")),
			&gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableForeignKeyConstraintWhenMigrating: true},
		)
		require.NoError(t, e)
		sql, _ := db.DB()
		sql.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, sql.Close()) })
	}
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &bodyProtocolRow{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "owned"}).Error)
	for _, id := range []string{"one", "two"} {
		require.NoError(
			t,
			db.Create(
				&types.Knowledge{
					ID:              id,
					TenantID:        7,
					KnowledgeBaseID: "kb",
					Type:            types.KnowledgeTypeManual,
					Metadata: types.JSON(
						("{\"datasource_id\":\"stable-source\",\"external_id\":\"stable-file\",\"man" +
							"ual\":{\"content\":\"manual original body\",\"other\":\"preserved\"},\"poi" +
							"nter\":\"preserved\"}"),
					),
				},
			).Error,
		)
	}
	file := "../../../migrations/sqlite/000071_original_body_retention.up.sql"
	if pg {
		file = "../../../migrations/versioned/000152_original_body_retention.up.sql"
	}
	script, e := os.ReadFile(file)
	require.NoError(t, e)
	require.NoError(t, db.Exec(string(script)).Error)
	require.NoError(t, InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, InstallOriginalBodyRetentionCallbacks(db))
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	key := filepath.Join(dir, "key")
	pin := filepath.Join(dir, "pin")
	keyBytes := []byte(strings.Repeat("owned-secret-", 4))
	if len(ownedKeys) > 0 {
		keyBytes = ownedKeys[0]
	}
	require.NoError(t, os.WriteFile(key, keyBytes, 0o600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", dir)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", key)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", pin)
	store := NewOriginalBodyRetentionStore(db)
	require.NoError(t, store.InitializeJournal())
	return db, store
}

func bodyProtocolCreate(t *testing.T, db *gorm.DB, id, knowledge string) string {
	t.Helper()
	require.NoError(
		t,
		db.Create(
			&bodyProtocolRow{
				ManifestID:      id,
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     knowledge,
				ActualInputs:    "shared original exact text",
			},
		).Error,
	)
	var head originalBodyFieldHead
	require.NoError(
		t,
		bodyInternal(db).Where("source_table='knowledge_auto_tag_input_manifests' AND row_key=?", id).Take(&head).Error,
	)
	return head.BodyID
}

func bodyProtocolDeleted(t *testing.T, db *gorm.DB, id string, old bool) {
	t.Helper()
	expr := "datetime('now','-30 minutes')"
	if old {
		expr = "datetime('now','-2 hours')"
	}
	if db.Name() == "postgres" {
		expr = "CURRENT_TIMESTAMP-INTERVAL '30 minutes'"
		if old {
			expr = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
		}
	}
	require.NoError(t, db.Exec("UPDATE knowledges SET deleted_at="+expr+" WHERE id=?", id).Error)
}

func bodyProtocolShared(t *testing.T, pg bool) {
	db, s := bodyProtocolFixture(t, pg)
	ctx := context.Background()
	id := bodyProtocolCreate(t, db, "owner-one", "one")
	bodyProtocolCreate(t, db, "owner-two", "two")
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		return s.ShareOriginalBodyFieldInTx(
			ctx,
			tx,
			id,
			"knowledge_auto_tag_input_manifests",
			"owner-two",
			"actual_inputs",
		)
	}))
	bodyProtocolDeleted(t, db, "one", true)
	_, e := s.PurgeDue(ctx, 100)
	require.NoError(t, e)
	var body originalBodyPayload
	require.NoError(t, bodyInternal(db).Where("id=?", id).Take(&body).Error)
	require.Equal(t, "live", body.State)
	// Revoking one actor cannot remove the second current incoming owner.
	lease, e := s.AcquireLease(ctx, id, "build", "actual-build", time.Minute)
	require.NoError(t, e)
	bodyProtocolDeleted(t, db, "two", true)
	_, e = s.PurgeDue(ctx, 100)
	require.NoError(t, e)
	require.NoError(t, s.ReleaseLease(ctx, lease))
	n, e := s.PurgeDue(ctx, 100)
	require.NoError(t, e)
	require.Positive(t, n)
	require.ErrorIs(
		t,
		CheckOriginalBodyAvailableInTx(ctx, db, "knowledge_auto_tag_input_manifests", "owner-two"),
		ErrOriginalBodyUnavailable,
	)
}

func bodyProtocolNestedTTL(t *testing.T, pg bool) {
	db, s := bodyProtocolFixture(t, pg)
	ctx := context.Background()
	_, e := s.Inventory(ctx, 100)
	require.NoError(t, e)
	bodyProtocolDeleted(t, db, "one", true)
	n, e := s.PurgeDue(ctx, 100)
	require.NoError(t, e)
	require.Positive(t, n)
	var raw string
	require.NoError(t, db.Raw("SELECT metadata FROM knowledges WHERE id='one'").Scan(&raw).Error)
	require.NotContains(t, raw, "manual original body")
	require.Contains(t, raw, "stable-source")
	require.Contains(t, raw, "stable-file")
	require.Contains(t, raw, "preserved")
	var current types.Knowledge
	require.NoError(t, db.Where("id='two'").Take(&current).Error)
	require.Contains(t, string(current.Metadata), "manual original body")
	require.NoError(t, db.Exec("UPDATE knowledges SET deleted_at=NULL WHERE id='one'").Error)
	current = types.Knowledge{}
	require.ErrorIs(t, db.Where("id='one'").Take(&current).Error, ErrOriginalBodyUnavailable)
}

func bodyProtocolFailedACK(t *testing.T, pg bool) {
	db, s := bodyProtocolFixture(t, pg)
	ctx := context.Background()
	id := bodyProtocolCreate(t, db, "owner", "one")
	bodyProtocolDeleted(t, db, "one", true)
	fail := errors.New("owned exact DB ACK failure")
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("body_ack_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "original_body_payloads" {
			if data, ok := tx.Statement.Dest.(map[string]any); ok && data["state"] == "purged" {
				_ = tx.AddError(fail)
			}
		}
	}))
	_, e := s.PurgeDue(ctx, 100)
	require.ErrorIs(t, e, fail)
	require.NoError(t, db.Callback().Update().Remove("body_ack_failure"))
	require.ErrorIs(
		t,
		CheckOriginalBodyAvailableInTx(ctx, db, "knowledge_auto_tag_input_manifests", "owner"),
		ErrOriginalBodyUnavailable,
	)
	var body originalBodyPayload
	require.NoError(t, bodyInternal(db).Where("id=?", id).Take(&body).Error)
	require.Equal(t, "live", body.State)
	require.NotNil(t, body.Payload)
	n, e := s.ReconcileExternalPurge(ctx)
	require.NoError(t, e)
	require.Equal(t, 1, n)
	n, e = s.ReconcileExternalPurge(ctx)
	require.NoError(t, e)
	require.Zero(t, n)
	require.NoError(t, bodyInternal(db).Where("id=?", id).Take(&body).Error)
	require.Nil(t, body.Payload)
	require.Equal(t, "purged", body.State)
	ledger := filepath.Join(s.journal.paths.Directory, "purge-ledger.json")
	raw, e := os.ReadFile(ledger)
	require.NoError(t, e)
	require.NotContains(t, string(raw), "shared original exact text")
	require.NoError(t, os.WriteFile(ledger, []byte("[]"), 0o600))
	require.ErrorIs(
		t,
		CheckOriginalBodyAvailableInTx(ctx, db, "knowledge_auto_tag_input_manifests", "owner"),
		ErrOriginalBodyUnavailable,
	)
}

func TestOriginalBodyProtocolSQLite(t *testing.T) {
	for _, c := range []struct {
		name string
		fn   func(*testing.T, bool)
	}{{"shared", bodyProtocolShared}, {"nestedTTL", bodyProtocolNestedTTL}, {"failedACK", bodyProtocolFailedACK}} {
		t.Run(c.name, func(t *testing.T) { c.fn(t, false) })
	}
}

func TestPostgresOriginalBodyProtocol(t *testing.T) {
	for _, c := range []struct {
		name string
		fn   func(*testing.T, bool)
	}{{"shared", bodyProtocolShared}, {"nestedTTL", bodyProtocolNestedTTL}, {"failedACK", bodyProtocolFailedACK}} {
		t.Run(c.name, func(t *testing.T) { c.fn(t, true) })
	}
}

// Restore every table in the disposable database/schema while leaving the
// independently stored journal and pin at the post-purge generation.
func bodyProtocolDatabaseRollback(t *testing.T, pg bool) {
	db, s := bodyProtocolFixture(t, pg)
	ctx := context.Background()
	_, e := s.Inventory(ctx, 100)
	require.NoError(t, e)
	id := bodyProtocolCreate(t, db, "rollback-owner", "one")
	var restore func()
	if pg {
		var schema string
		require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
		snapshot := "snapshot_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
		require.NoError(t, db.Exec("CREATE SCHEMA "+snapshot).Error)
		t.Cleanup(func() { db.Exec("DROP SCHEMA " + snapshot + " CASCADE") })
		var tables []string
		require.NoError(
			t,
			db.Raw(("SELECT table_name FROM information_schema.tables WHERE table_schema=? AND "+
				"table_type='BASE TABLE' ORDER BY table_name"), schema).
				Scan(&tables).
				Error,
		)
		for _, table := range tables {
			require.NoError(
				t,
				db.Exec(
					"CREATE TABLE "+bodyQuoted(
						snapshot,
					)+"."+bodyQuoted(
						table,
					)+" AS TABLE "+bodyQuoted(
						schema,
					)+"."+bodyQuoted(
						table,
					),
				).Error,
			)
		}
		restore = func() {
			// All table data is restored; the migrated SQL triggers remain installed.
			quoted := []string{}
			for _, table := range tables {
				quoted = append(quoted, bodyQuoted(schema)+"."+bodyQuoted(table))
			}
			require.NoError(t, db.Exec("TRUNCATE "+strings.Join(quoted, ",")+" CASCADE").Error)
			order := []string{
				"knowledge_bases",
				"knowledges",
				"knowledge_auto_tag_input_manifests",
				"original_body_payloads",
				"original_body_field_heads",
				"original_body_field_revisions",
				"original_body_parent_refs",
				"original_body_leases",
				"original_body_purge_receipts",
				"original_body_journal_heads",
				"original_body_rewrite_authorities",
				"original_body_gc_items",
				"original_body_lease_scopes",
			}
			require.Len(t, tables, len(order))
			for _, table := range order {
				require.NoError(
					t,
					db.Exec(
						"INSERT INTO "+bodyQuoted(
							schema,
						)+"."+bodyQuoted(
							table,
						)+" SELECT * FROM "+bodyQuoted(
							snapshot,
						)+"."+bodyQuoted(
							table,
						),
					).Error,
				)
			}
		}
	} else {
		var files []struct {
			Seq        int
			Name, File string
		}
		require.NoError(t, db.Raw("PRAGMA database_list").Scan(&files).Error)
		var path string
		for _, file := range files {
			if file.Name == "main" {
				path = file.File
			}
		}
		require.NotEmpty(t, path)
		backup := filepath.Join(t.TempDir(), "full-snapshot.sqlite")
		require.NoError(t, db.Exec("VACUUM INTO ?", backup).Error)
		restore = func() {
			sql, e := db.DB()
			require.NoError(t, e)
			require.NoError(t, sql.Close())
			data, e := os.ReadFile(backup)
			require.NoError(t, e)
			require.NoError(t, os.WriteFile(path, data, 0o600))
			db, e = gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
			require.NoError(t, e)
			sql, e = db.DB()
			require.NoError(t, e)
			sql.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, sql.Close()) })
			require.NoError(t, InstallOriginalBodyRetentionCallbacks(db))
			s = NewOriginalBodyRetentionStore(db)
		}
	}
	bodyProtocolDeleted(t, db, "one", true)
	n, e := s.PurgeDue(ctx, 100)
	require.NoError(t, e)
	require.Positive(t, n)
	restore()
	require.ErrorIs(t, s.VerifyJournalReady(), ErrOriginalBodyUnavailable)
	var restored string
	require.NoError(t, db.Raw("SELECT metadata FROM knowledges WHERE id='one'").Scan(&restored).Error)
	require.Contains(t, restored, "manual original body")
	require.ErrorIs(
		t,
		CheckOriginalBodyAvailableInTx(ctx, db, "knowledge_auto_tag_input_manifests", "rollback-owner"),
		ErrOriginalBodyUnavailable,
	)
	_, e = s.ReconcileExternalPurge(ctx)
	require.ErrorIs(t, e, ErrOriginalBodyRetained, "restored current ownership must not be erased by replay")
	// The source is independently closed again; replay may now erase the exact
	// restored payload and the exact inline leaf, never the current second owner.
	bodyProtocolDeleted(t, db, "one", true)
	if cli := os.Getenv("WEKNORA_ORIGINAL_BODY_TEST_CLI"); cli != "" {
		dsn := ""
		driver := "sqlite"
		if pg {
			var schema string
			require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
			dsn = os.Getenv("WEKNORA_GC_TEST_POSTGRES_DSN") + " search_path=" + schema
			driver = "postgres"
		} else {
			var files []struct {
				Seq        int
				Name, File string
			}
			require.NoError(t, db.Raw("PRAGMA database_list").Scan(&files).Error)
			for _, file := range files {
				if file.Name == "main" {
					dsn = file.File
				}
			}
		}
		command := exec.Command(cli, "-driver", driver, "-mode", "reconcile")
		command.Env = append(os.Environ(), "WEKNORA_ORIGINAL_BODY_MAINTENANCE_DSN="+dsn)
		output, err := command.CombinedOutput()
		require.NoError(t, err, string(output))
		_, err = fmt.Sscanf(string(output), "original body reconcile complete; replayed=%d", &n)
		require.NoError(t, err)
		t.Logf("offline CLI %s whole database/schema restore replayed=%d", driver, n)
	} else {
		n, e = s.ReconcileExternalPurge(ctx)
		require.NoError(t, e)
	}
	require.Positive(t, n)
	require.NoError(t, s.VerifyJournalReady())
	var payload originalBodyPayload
	require.NoError(t, bodyInternal(db).Where("id=?", id).Take(&payload).Error)
	require.Nil(t, payload.Payload)
	require.NoError(t, db.Raw("SELECT metadata FROM knowledges WHERE id='one'").Scan(&restored).Error)
	require.NotContains(t, restored, "manual original body")
	require.Contains(t, restored, "stable-source")
	var current types.Knowledge
	require.NoError(t, db.Where("id='two'").Take(&current).Error)
	require.Contains(t, string(current.Metadata), "manual original body")
}
func TestOriginalBodyFullDatabaseRollbackSQLite(t *testing.T) { bodyProtocolDatabaseRollback(t, false) }
func TestPostgresOriginalBodyFullSchemaRollback(t *testing.T) { bodyProtocolDatabaseRollback(t, true) }

func bodyProtocolSourceWindows(t *testing.T, pg bool) {
	for _, mode := range []string{
		"current",
		"one-hour-open",
		"one-hour-expired",
		"failed-open",
		"failed-expired",
		"cancelled-retry",
		"source-unreachable",
		"confirmed-delete",
		"whole-binding-withdrawal",
	} {
		t.Run(mode, func(t *testing.T) {
			db, s := bodyProtocolFixture(t, pg)
			require.NoError(
				t,
				db.AutoMigrate(
					&nextcloudSourceVersion{},
					&nextcloudContentFence{},
					&NextcloudIndexedWithdrawal{},
					&types.DataSource{},
				),
			)
			require.NoError(
				t,
				db.Create(
					&types.DataSource{
						ID:              "stable-source",
						TenantID:        7,
						KnowledgeBaseID: "kb",
						Name:            "owned source",
						Status:          types.DataSourceStatusActive,
					},
				).Error,
			)
			require.NoError(
				t,
				db.Exec(
					("UPDATE knowledges SET channel='nextcloud',parse_status='completed',pending" +
						"_subtasks_count=0 WHERE id='one'"),
				).Error,
			)
			now, e := nextcloudLeaseNowMS(bodyInternal(db))
			require.NoError(t, e)
			retired := now - int64(2*time.Hour/time.Millisecond)
			if mode == "one-hour-open" {
				retired = now - int64(30*time.Minute/time.Millisecond)
			}
			if mode == "failed-expired" {
				retired = now - int64(25*time.Hour/time.Millisecond)
			}
			candidate, state := "new", "published"
			if mode == "current" {
				candidate = "one"
			}
			if mode == "confirmed-delete" {
				candidate = ""
				state = "tombstone"
				retired = now - 1
			}
			require.NoError(
				t,
				db.Create(
					&nextcloudSourceVersion{
						TenantID:             7,
						KnowledgeBaseID:      "kb",
						DataSourceID:         "stable-source",
						ExternalID:           "stable-file",
						CandidateKnowledgeID: candidate,
						State:                state,
					},
				).Error,
			)
			require.NoError(
				t,
				db.Create(
					&nextcloudContentFence{
						TenantID:        7,
						KnowledgeBaseID: "kb",
						KnowledgeID:     "one",
						State:           "retired",
						RetiredAtMS:     &retired,
					},
				).Error,
			)
			if strings.HasPrefix(mode, "failed-") {
				require.NoError(t, db.Exec("UPDATE knowledges SET parse_status='failed' WHERE id='one'").Error)
			}
			if mode == "cancelled-retry" {
				require.NoError(
					t,
					db.Exec(
						"UPDATE knowledges SET parse_status='cancelled',pending_subtasks_count=1 WHERE id='one'",
					).Error,
				)
			}
			if mode == "source-unreachable" {
				require.NoError(t, db.Exec("UPDATE data_sources SET status='error'").Error)
			}
			if mode == "whole-binding-withdrawal" {
				retired = now - 1
				require.NoError(t, db.Exec("UPDATE nextcloud_content_fences SET retired_at_ms=?", retired).Error)
				require.NoError(
					t,
					db.Create(
						&NextcloudIndexedWithdrawal{
							OperationID:     "owned-withdraw",
							TenantID:        7,
							KnowledgeBaseID: "kb",
							DataSourceID:    "stable-source",
							State:           "withdrawn_inventory_incomplete",
						},
					).Error,
				)
			}
			// Protocol eligibility reads the actual source state and DB clock; the
			// lower-level unit calls no scheduler and sets no production coverage.
			id := bodyProtocolCreate(t, db, "window-owner", "one")
			var body originalBodyPayload
			require.NoError(t, bodyInternal(db).Where("id=?", id).Take(&body).Error)
			var heads []originalBodyFieldHead
			require.NoError(
				t,
				bodyInternal(db).Table("original_body_field_revisions").Where("body_id=?", id).Find(&heads).Error,
			)
			deadline, e := s.bodyEligibility(db, body, heads, now)
			switch mode {
			case "current", "cancelled-retry", "source-unreachable":
				require.ErrorIs(t, e, ErrOriginalBodyRetained)
			case "one-hour-open", "failed-open":
				require.NoError(t, e)
				require.Greater(t, deadline, now)
			default:
				require.NoError(t, e)
				require.LessOrEqual(t, deadline, now)
			}
		})
	}
}
func TestOriginalBodySourceWindowsSQLite(t *testing.T)   { bodyProtocolSourceWindows(t, false) }
func TestPostgresOriginalBodySourceWindows(t *testing.T) { bodyProtocolSourceWindows(t, true) }

func bodyProtocolSelectedAdmin(t *testing.T, pg bool) {
	for _, mode := range []string{"selected-only", "current-shared-owner", "missing-coverage", "admin-revoked"} {
		t.Run(mode, func(t *testing.T) {
			_, db, jobID, _, _ := gcActualObjectFixture(t, pg)
			require.NoError(
				t,
				db.AutoMigrate(
					&bodyProtocolRow{},
					&types.Tenant{},
					&types.User{},
					&types.TenantMember{},
					&types.ResourceAccessPolicy{},
				),
			)
			path := "../../../migrations/sqlite/000071_original_body_retention.up.sql"
			if pg {
				path = "../../../migrations/versioned/000152_original_body_retention.up.sql"
			}
			script, e := os.ReadFile(path)
			require.NoError(t, e)
			require.NoError(t, db.Exec(string(script)).Error)
			require.NoError(t, InstallOriginalBodyRetentionTriggers(db))
			require.NoError(t, InstallOriginalBodyRetentionCallbacks(db))
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0o700))
			key := filepath.Join(dir, "key")
			pin := filepath.Join(dir, "pin")
			require.NoError(t, os.WriteFile(key, []byte(strings.Repeat("owned-secret-", 4)), 0o600))
			t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", dir)
			t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", key)
			t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", pin)
			s := NewOriginalBodyRetentionStore(db)
			require.NoError(t, s.InitializeJournal())
			require.NoError(t, db.Create(&types.Tenant{ID: 7, Status: "active", Name: "owned"}).Error)
			require.NoError(
				t,
				db.Create(
					&types.User{
						ID:       "body-admin",
						TenantID: 7,
						Username: "body-admin",
						Email:    "body@example.test",
						IsActive: true,
					},
				).Error,
			)
			require.NoError(
				t,
				db.Create(
					&types.TenantMember{
						TenantID: 7,
						UserID:   "body-admin",
						Role:     types.TenantRoleAdmin,
						Status:   types.TenantMemberStatusActive,
					},
				).Error,
			)
			factory := func(tx *gorm.DB) *KnowledgeInputProvenanceStore {
				actor := NewKnowledgeInputProvenanceStore(tx)
				ConfigureKnowledgeInputActorGroupAccess(actor, displayTestGroups{db: tx})
				return actor
			}
			authority := factory(db)
			ConfigureKnowledgeDisplayInputWriters(authority, nil, nil, factory)
			ctx := types.WithExecutionTenant(context.Background(), 7)
			ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "body-admin", Role: types.TenantRoleAdmin})
			ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "body-admin"})
			ctx = context.WithValue(
				ctx,
				nextcloudGCManualKey{},
				nextcloudGCManual{Tenant: 7, Job: jobID, KB: "kb", User: "body-admin", Authority: authority},
			)
			id := bodyProtocolCreate(t, db, "selected-owner", "old")
			require.NoError(
				t,
				db.Create(
					&types.Knowledge{
						ID:              "unrelated",
						TenantID:        7,
						KnowledgeBaseID: "kb",
						Type:            types.KnowledgeTypeManual,
						Metadata:        types.JSON(`{}`),
					},
				).Error,
			)
			other := bodyProtocolCreate(t, db, "unrelated-owner", "unrelated")
			if mode == "current-shared-owner" {
				require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
					return s.ShareOriginalBodyFieldInTx(
						ctx,
						tx,
						id,
						"knowledge_auto_tag_input_manifests",
						"unrelated-owner",
						"actual_inputs",
					)
				}))
			}
			if mode == "missing-coverage" {
				require.NoError(t, db.Exec("DELETE FROM nextcloud_content_lease_coverage").Error)
			}
			if mode == "admin-revoked" {
				require.NoError(t, db.Exec("UPDATE users SET is_active=false WHERE id='body-admin'").Error)
			}
			n, e := s.PurgeSelectedJob(ctx, 7, jobID, 100)
			switch mode {
			case "selected-only":
				require.NoError(t, e)
				require.Positive(t, n)
			case "current-shared-owner":
				require.NoError(t, e)

			case "missing-coverage":
				require.ErrorIs(t, e, ErrNextcloudContentGCUncovered)
			case "admin-revoked":
				require.ErrorIs(t, e, ErrNextcloudGCAdmin)
			}
			var body originalBodyPayload
			require.NoError(t, bodyInternal(db).Where("id=?", other).Take(&body).Error)
			require.Equal(t, "live", body.State)
			body = originalBodyPayload{}
			require.NoError(t, bodyInternal(db).Where("id=?", id).Take(&body).Error)
			if mode == "selected-only" {
				require.Equal(t, "purged", body.State)
			} else {
				require.Equal(t, "live", body.State)
			}
			var items int64
			require.NoError(
				t,
				db.Model(&originalBodyGCItem{}).Where("job_id=? AND body_id=?", jobID, other).Count(&items).Error,
			)
			require.Zero(t, items)
		})
	}
}
func TestOriginalBodySelectedAdminSQLite(t *testing.T)   { bodyProtocolSelectedAdmin(t, false) }
func TestPostgresOriginalBodySelectedAdmin(t *testing.T) { bodyProtocolSelectedAdmin(t, true) }

func bodyProtocolBatchFairness(t *testing.T, pg bool) {
	db, s := bodyProtocolFixture(t, pg)
	ctx := context.Background()
	current := bodyProtocolCreate(t, db, "first-current", "two")
	expired := bodyProtocolCreate(t, db, "second-expired", "one")
	bodyProtocolDeleted(t, db, "one", true)
	require.NoError(
		t,
		bodyInternal(db).Model(&originalBodyPayload{}).Where("id=?", current).Update("created_at_ms", 1).Error,
	)
	n, e := s.PurgeDue(ctx, 1)
	require.NoError(t, e)
	require.Zero(t, n)
	n, e = s.PurgeDue(ctx, 1)
	require.NoError(t, e)
	require.Equal(t, 1, n, "a protected first body cannot starve a later due body")
	var body originalBodyPayload
	require.NoError(t, bodyInternal(db).Where("id=?", expired).Take(&body).Error)
	require.Equal(t, "purged", body.State)
}
func TestOriginalBodyBatchFairnessSQLite(t *testing.T)   { bodyProtocolBatchFairness(t, false) }
func TestPostgresOriginalBodyBatchFairness(t *testing.T) { bodyProtocolBatchFairness(t, true) }

func bodyProtocolAnchorIdentity(t *testing.T, pg bool) {
	db, first := bodyProtocolFixture(t, pg)
	_, other := bodyProtocolFixture(t, pg)
	first.ConfigureJournal(other.journal.paths)
	require.ErrorIs(
		t,
		first.VerifyJournalReady(),
		ErrOriginalBodyUnavailable,
		"zero sequence from another initialized database is not an anchor",
	)
	var head originalBodyJournalHead
	require.NoError(t, bodyInternal(db).Where("id=1").Take(&head).Error)
	require.NotEmpty(t, head.JournalID)
}
func TestOriginalBodyAnchorIdentitySQLite(t *testing.T)   { bodyProtocolAnchorIdentity(t, false) }
func TestPostgresOriginalBodyAnchorIdentity(t *testing.T) { bodyProtocolAnchorIdentity(t, true) }

func bodyProtocolCLIForwardScopes(t *testing.T, pg bool) {
	cli := os.Getenv("WEKNORA_ORIGINAL_BODY_TEST_CLI")
	if cli == "" {
		t.Skip("actual offline CLI executable required")
	}
	db, store := bodyProtocolFixture(t, pg)
	dsn := ""
	driver := "sqlite"
	if pg {
		var schema string
		require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
		dsn = os.Getenv("WEKNORA_GC_TEST_POSTGRES_DSN") + " search_path=" + schema
		driver = "postgres"
	} else {
		var files []struct {
			Seq        int
			Name, File string
		}
		require.NoError(t, db.Raw("PRAGMA database_list").Scan(&files).Error)
		for _, file := range files {
			if file.Name == "main" {
				dsn = file.File
			}
		}
	}
	require.NotEmpty(t, dsn)
	run := func(mode string, success bool) {
		command := exec.Command(cli, "-driver", driver, "-mode", mode)
		command.Env = append(os.Environ(), "WEKNORA_ORIGINAL_BODY_MAINTENANCE_DSN="+dsn)
		output, e := command.CombinedOutput()
		if success {
			require.NoError(t, e, string(output))
		} else {
			require.Error(t, e, string(output))
		}
		t.Logf("actual offline CLI %s %s success=%t output=%s", driver, mode, e == nil, string(output))
	}
	require.NoError(t, db.Exec("DROP TABLE original_body_lease_scopes").Error)
	run("verify", false)
	run("upgrade-scopes", true)
	run("upgrade-scopes", true)
	run("verify", true)
	require.NoError(t, store.VerifyJournalReady())
	var count int64
	require.NoError(t, db.Table("original_body_lease_scopes").Count(&count).Error)
	require.Zero(t, count, "forward command must not invent original scope inventory")
	trigger := "original_body_lease_scope_immutable_update"
	suffix := ""
	if pg {
		trigger = "original_body_lease_scope_immutable"
		suffix = " ON original_body_lease_scopes"
	}
	require.NoError(t, db.Exec("DROP TRIGGER "+trigger+suffix).Error)
	run("verify", false)
	run("upgrade-scopes", true)
	run("verify", true)
}
func TestOriginalBodyCLIForwardScopesSQLite(t *testing.T)   { bodyProtocolCLIForwardScopes(t, false) }
func TestPostgresOriginalBodyCLIForwardScopes(t *testing.T) { bodyProtocolCLIForwardScopes(t, true) }
