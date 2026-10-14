package repository_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	repo "github.com/Tencent/WeKnora/internal/application/repository"
	service "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func onboardingDB(t *testing.T) (*gorm.DB, *repo.NextcloudOnboardingStore, types.NextcloudOnboardingCredential) {
	t.Helper()
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	dialect := "sqlite"
	var db *gorm.DB
	var err error
	if dsn := os.Getenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN"); dsn != "" {
		dialect = "versioned"
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	} else {
		db, err = gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "onboarding.db")+"?_busy_timeout=5000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	}
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	if dialect == "versioned" {
		schema := "onboarding_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		require.NoError(t, db.Exec("CREATE SCHEMA "+schema).Error)
		require.NoError(t, db.Exec("SET search_path TO "+schema).Error)
		t.Cleanup(func() {
			require.NoError(t, db.Exec("DROP SCHEMA "+schema+" CASCADE").Error)
			require.NoError(t, sqlDB.Close())
		})
	} else {
		t.Cleanup(func() { _ = sqlDB.Close() })
	}

	require.NoError(t, db.AutoMigrate(&types.User{}, &types.Message{}, &types.KnowledgeTag{}, &types.KnowledgeTagRelation{}, &types.Tenant{}, &types.Chunk{}, &types.KnowledgeBase{}, &types.Knowledge{}, &types.DataSource{}, &types.Model{}, &types.StorageBackend{}, &types.TenantMember{}, &types.ResourceAccessPolicy{}, &types.ResourceGroupGrant{}, &types.TenantGroupRoleGrant{}, &types.PermissionVersion{}, &types.Directory{}, &types.DirectoryIdentity{}, &types.DirectoryGroup{}, &types.DirectoryGroupMembership{}, &types.Session{}, &repo.NextcloudSourcePairing{}, &repo.NextcloudSourcePairingAbort{}))
	for _, sql := range []string{"CREATE UNIQUE INDEX policies_key ON resource_access_policies(tenant_id,resource_type,resource_id)", "CREATE TABLE wiki_pages(tenant_id INTEGER,knowledge_base_id TEXT)", "CREATE TABLE nextcloud_source_tombstones(tenant_id INTEGER,scope_type TEXT,scope_id TEXT)", "CREATE TABLE nextcloud_event_connections(connection_id TEXT PRIMARY KEY,tenant_id BIGINT,knowledge_base_id TEXT,nextcloud_instance_id TEXT,binding_id TEXT,datasource_id TEXT,status TEXT)"} {
		require.NoError(t, db.Exec(sql).Error)
	}
	require.NoError(t, db.Create(&types.Tenant{ID: 7, Name: "onboarding-owner"}).Error)
	// A KB born before the guarded inventory boundary must stay unselectable.
	old := types.KnowledgeBase{ID: "legacy-zero-count", TenantID: 7, Name: "Legacy", Type: "document"}
	require.NoError(t, db.Create(&old).Error)
	migrationPath := "../../../migrations/sqlite/000074_nextcloud_onboarding.up.sql"
	if dialect == "versioned" {
		migrationPath = "../../../migrations/versioned/000155_nextcloud_onboarding.up.sql"
	}
	migration, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	controls := []string{"../../../migrations/sqlite/000076_nextcloud_directory_migrations.up.sql", "../../../migrations/sqlite/000078_nextcloud_control_recovery.up.sql", "../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql", "../../../migrations/sqlite/000081_nextcloud_target_closure.up.sql", "../../../migrations/sqlite/000083_nextcloud_current_manager_purpose.up.sql"}
	if dialect == "versioned" {
		controls = []string{"../../../migrations/versioned/000157_nextcloud_directory_migrations.up.sql", "../../../migrations/versioned/000159_nextcloud_control_recovery.up.sql", "../../../migrations/versioned/000127_nextcloud_content_leases.up.sql", "../../../migrations/versioned/000162_nextcloud_target_closure.up.sql", "../../../migrations/versioned/000164_nextcloud_current_manager_purpose.up.sql"}
	}
	// Producer/config/name fixtures use the actual immutable origin and body
	// schema. No current template row is retroactively declared independent.
	provenance := []string{"000053_knowledge_input_provenance.up.sql", "000054_knowledge_configuration_history_inputs.up.sql", "000055_knowledge_actual_config_controls.up.sql", "000056_knowledge_original_worker_actor.up.sql", "000059_knowledge_collection_display_inputs.up.sql", "000066_knowledge_enrichment_template_origins.up.sql", "000069_datasource_name_inputs.up.sql", "000071_original_body_retention.up.sql", "000082_knowledge_configuration_original_body.up.sql"}
	if dialect == "versioned" {
		provenance = []string{"000134_knowledge_input_provenance.up.sql", "000135_knowledge_configuration_history_inputs.up.sql", "000136_knowledge_actual_config_controls.up.sql", "000137_knowledge_original_worker_actor.up.sql", "000140_knowledge_collection_display_inputs.up.sql", "000147_knowledge_enrichment_template_origins.up.sql", "000150_datasource_name_inputs.up.sql", "000152_original_body_retention.up.sql", "000163_knowledge_configuration_original_body.up.sql"}
	}
	for _, name := range provenance {
		part, e := os.ReadFile("../../../migrations/" + dialect + "/" + name)
		require.NoError(t, e)
		// AutoMigrate already provides this column; preserve every actual trigger.
		lines := strings.Split(string(part), "\n")
		kept := make([]string, 0, len(lines))
		for _, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "ALTER TABLE knowledge_bases ADD COLUMN enrichment_templates ") {
				continue
			}
			kept = append(kept, line)
		}
		migration = append(migration, append([]byte("\n"), []byte(strings.Join(kept, "\n"))...)...)
	}
	for _, path := range controls {
		part, e := os.ReadFile(path)
		require.NoError(t, e)
		migration = append(migration, append([]byte("\n"), part...)...)
	}
	require.NoError(t, db.AutoMigrate(&repo.NextcloudRecoveryReceipt{}))
	// Session column is already present through AutoMigrate above.
	require.NoError(t, db.Migrator().DropColumn(&types.Session{}, "nextcloud_scoped"))
	if dialect == "versioned" {
		var schema string
		require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
		migrationDB, err := gorm.Open(postgres.Open(os.Getenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN")+" search_path="+schema+" default_query_exec_mode=simple_protocol"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(t, err)
		migrationSQL, err := migrationDB.DB()
		require.NoError(t, err)
		migrationSQL.SetMaxOpenConns(1)
		require.NoError(t, migrationDB.Exec(string(migration)).Error)
		require.NoError(t, migrationSQL.Close())
	} else {
		require.NoError(t, db.Exec(string(migration)).Error)
	}
	now := time.Now().UTC()
	d := types.Directory{ID: "directory", Protocol: types.DirectoryProtocolAD, Enabled: true, LastSuccessfulSyncAt: &now, SnapshotVersion: 1}
	require.NoError(t, db.Create(&d).Error)
	owner := "owner"
	reader := "reader"
	require.NoError(t, db.Create(&types.User{ID: owner, Username: owner, Email: "owner@onboarding.test", TenantID: 7, IsActive: true}).Error)
	require.NoError(t, db.Create(&types.User{ID: reader, Username: reader, Email: "reader@onboarding.test", IsActive: true}).Error)
	require.NoError(t, db.Create(&types.DirectoryIdentity{ID: "i-owner", DirectoryID: d.ID, ObjectGUID: "guid-owner", UserID: &owner, Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryIdentity{ID: "i-reader", DirectoryID: d.ID, ObjectGUID: "guid-reader", UserID: &reader, Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryIdentity{ID: "i-unlogged", DirectoryID: d.ID, ObjectGUID: "guid-unlogged", Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryGroup{ID: "g-engineering", DirectoryID: d.ID, ObjectGUID: "guid-engineering", Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.DirectoryGroupMembership{DirectoryID: d.ID, GroupID: "g-engineering", IdentityID: "i-reader", SnapshotVersion: 1, Source: types.DirectoryMembershipNested}).Error)
	require.NoError(t, db.Create(&types.TenantMember{TenantID: 7, UserID: owner, Role: types.TenantRoleOwner, Status: types.TenantMemberStatusActive}).Error)
	require.NoError(t, db.Create(&types.Model{ID: "embedding", TenantID: 7, Type: types.ModelTypeEmbedding, Status: types.ModelStatusActive}).Error)
	require.NoError(t, db.Create(&types.StorageBackend{ID: "storage", TenantID: 7, Name: "local", Provider: "local", Status: types.StorageBackendStatusActive}).Error)
	h := sha256.Sum256([]byte("test-onboarding-server-credential-0123456789"))
	cred := types.NextcloudOnboardingCredential{ID: "automation", TokenSHA256: hex.EncodeToString(h[:]), InstanceID: "nc-instance", BaseURL: "https://nc.example.test", TenantID: 7, DirectoryID: d.ID, TemplateKBID: "template", Enabled: true, MaxKnowledgeBases: 10}
	require.NoError(t, db.Create(&cred).Error)
	journal := t.TempDir()
	require.NoError(t, os.Chmod(journal, 0700))
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	keyPath := filepath.Join(journal, "key")
	require.NoError(t, os.WriteFile(keyPath, key, 0600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", journal)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", keyPath)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", filepath.Join(journal, "pin"))
	retention := repo.NewOriginalBodyRetentionStore(db)
	require.NoError(t, retention.InitializeJournal())
	require.NoError(t, repo.InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, repo.InstallOriginalBodyRetentionCallbacks(db))
	template := freshOnboardingKB("template")
	createOnboardingTemplate(t, db, &template)
	return db, repo.NewNextcloudOnboardingStore(db), cred
}

// Create a template through the same authenticated independent-input writer
// as a human author; never certify an already-loaded legacy configuration.
func createOnboardingTemplate(t *testing.T, db *gorm.DB, kb *types.KnowledgeBase) {
	t.Helper()
	store := repo.NewKnowledgeInputProvenanceStore(db)
	factory := func(tx *gorm.DB) *repo.KnowledgeInputProvenanceStore {
		actual := repo.NewKnowledgeInputProvenanceStore(tx)
		repo.ConfigureKnowledgeInputActorGroupAccess(actual, service.NewGroupAccessService(repo.NewGroupAccessRepository(tx)))
		return actual
	}
	repo.ConfigureKnowledgeDisplayInputWriters(store, repo.NewKnowledgeTagRepository(db), repo.NewKnowledgeRepository(db), factory)
	admin := types.WithExecutionTenant(types.WithPrincipal(types.WithCaller(context.Background(), types.Caller{TenantID: 7, UserID: "owner", Role: types.TenantRoleOwner}), types.Principal{Type: types.PrincipalWebUser, ID: "owner"}), 7)
	require.NoError(t, store.CreateKnowledgeBaseWithInput(repo.WithIndependentKnowledgeBaseNameInput(repo.WithIndependentKnowledgeInput(admin), kb.ID, kb.Name), kb))
}

func freshOnboardingKB(id string) types.KnowledgeBase {
	storage := "storage"
	return types.KnowledgeBase{ID: id, TenantID: 7, Name: id, Type: "document", EmbeddingModelID: "embedding", StorageBackendID: &storage, IndexingStrategy: types.DefaultIndexingStrategy()}
}
func onboardingRequest(c types.NextcloudOnboardingCredential, id, kb string) types.NextcloudOnboardingRequest {
	return types.NextcloudOnboardingRequest{OperationID: id, TenantID: c.TenantID, InstanceID: c.InstanceID, BindingID: "binding-" + id, ActorDirectoryID: c.DirectoryID, ActorObjectGUID: "guid-owner", Mode: "existing", Name: "Root", KnowledgeBaseID: kb, ProcessingTemplateID: c.TemplateKBID}
}
func activateOnboarding(t *testing.T, db *gorm.DB, store *repo.NextcloudOnboardingStore, op types.NextcloudOnboardingOperation) {
	t.Helper()
	pairer := repo.NewNextcloudSourcePairingRepository(db)
	factory := func(tx *gorm.DB) *repo.KnowledgeInputProvenanceStore {
		actual := repo.NewKnowledgeInputProvenanceStore(tx)
		repo.ConfigureKnowledgeInputActorGroupAccess(actual, service.NewGroupAccessService(repo.NewGroupAccessRepository(tx)))
		return actual
	}
	repo.ConfigureDataSourceNameInputWriters(repo.NewDataSourceRepository(db), pairer, factory)
	var credential types.NextcloudOnboardingCredential
	require.NoError(t, db.Where("id=?", op.CredentialID).Take(&credential).Error)
	producer, err := store.BeginPairProducer(context.Background(), credential, op, op.ActorDirectoryID, op.ActorObjectGUID)
	require.NoError(t, err)
	pair, _, err := pairer.PrepareSourcePairing(producer, repo.NextcloudSourcePairing{OperationID: op.OperationID, TenantID: op.TenantID, KnowledgeBaseID: op.KnowledgeBaseID, InstanceID: op.InstanceID, BindingID: op.BindingID, BaseURL: "https://nc.example.test", PublicationEpoch: 1, KeyID: "pair_" + op.OperationID}, "handoff-token")
	require.NoError(t, err)
	require.NoError(t, pairer.ActivateSourcePairing(context.Background(), pair))
	require.NoError(t, store.MarkActive(context.Background(), op))
}
func TestNextcloudOnboardingBlankHistoryReservationAndRetry(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	_, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), "legacy-zero-count"))
	require.ErrorIs(t, err, repo.ErrNextcloudKBBlankUnverified)
	kb := freshOnboardingKB("blank")
	createOnboardingTemplate(t, db, &kb)
	req := onboardingRequest(c, uuid.NewString(), kb.ID)
	first, err := store.Reserve(ctx, c, req)
	require.NoError(t, err)
	again, err := store.Reserve(ctx, c, req)
	require.NoError(t, err)
	require.Equal(t, first.KnowledgeBaseID, again.KnowledgeBaseID)
	req.Name = "Changed"
	_, err = store.Reserve(ctx, c, req)
	require.ErrorIs(t, err, repo.ErrNextcloudOnboardingConflict)
	doc := types.Knowledge{ID: "late-document", TenantID: 7, KnowledgeBaseID: kb.ID, Channel: "manual"}
	require.Error(t, db.Create(&doc).Error)
	var n int64
	require.NoError(t, db.Model(&types.Knowledge{}).Count(&n).Error)
	require.Zero(t, n)
	_, _, err = repo.NewNextcloudSourcePairingRepository(db).PrepareSourcePairing(ctx, repo.NextcloudSourcePairing{OperationID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: kb.ID, InstanceID: c.InstanceID, BindingID: "wrong-root", BaseURL: c.BaseURL, PublicationEpoch: 1, KeyID: "wrong"}, "token")
	require.ErrorIs(t, err, repo.ErrNextcloudSourcePairingConflict)
	// Tenant containing unrelated business content is a valid target.
	other := freshOnboardingKB("other")
	require.NoError(t, db.Create(&other).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "other-document", TenantID: 7, KnowledgeBaseID: other.ID, Channel: "manual"}).Error)
	create := onboardingRequest(c, uuid.NewString(), "")
	create.Mode = "create"
	op, err := store.Reserve(ctx, c, create)
	require.NoError(t, err)
	require.NotEqual(t, other.ID, op.KnowledgeBaseID)
}
func TestNextcloudOnboardingCompetingRootsAndConcurrentContent(t *testing.T) {
	db, store, c := onboardingDB(t)
	kb := freshOnboardingKB("contended")
	createOnboardingTemplate(t, db, &kb)
	start := make(chan struct{})
	out := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.Reserve(context.Background(), c, onboardingRequest(c, uuid.NewString(), kb.ID))
			out <- err
		}()
	}
	close(start)
	wg.Wait()
	close(out)
	successes := 0
	for err := range out {
		if err == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	touched := freshOnboardingKB("touched")
	require.NoError(t, db.Create(&touched).Error)
	doc := types.Knowledge{ID: "removed", TenantID: 7, KnowledgeBaseID: touched.ID, Channel: "manual"}
	require.NoError(t, db.Create(&doc).Error)
	require.NoError(t, db.Unscoped().Delete(&doc).Error)
	_, err := store.Reserve(context.Background(), c, onboardingRequest(c, uuid.NewString(), touched.ID))
	require.ErrorIs(t, err, repo.ErrNextcloudKBBlankUnverified, "even physically removed content cannot erase inventory history")
	require.Error(t, db.Exec("UPDATE nextcloud_kb_blank_witnesses SET virgin=1 WHERE knowledge_base_id=?", touched.ID).Error)
}
func TestNextcloudOnboardingLedgerScopeRevocationAndDirectoryFailure(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	ops := []types.NextcloudOnboardingOperation{}
	for _, id := range []string{"root-a", "root-b"} {
		kb := freshOnboardingKB(id)
		createOnboardingTemplate(t, db, &kb)
		op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), id))
		require.NoError(t, err)
		activateOnboarding(t, db, store, op)
		ops = append(ops, op)
	}
	snap := types.NextcloudGrantSnapshot{PermissionVersion: 1, PublicationEpoch: 1, Complete: true, Subjects: []types.NextcloudGrantSubject{{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-reader", SourceRelationID: "direct-share"}, {Kind: "group", DirectoryID: c.DirectoryID, ObjectGUID: "guid-engineering", SourceRelationID: "team-base"}, {Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-unlogged", SourceRelationID: "not-yet-logged"}}}
	for _, op := range ops {
		require.NoError(t, store.ApplyGrantSnapshot(ctx, op, snap))
	}
	access := service.NewGroupAccessService(repo.NewGroupAccessRepository(db))
	role, err := access.EffectiveTenantRole(ctx, "reader", 7, time.Now())
	require.NoError(t, err)
	require.True(t, role.Member)
	require.True(t, role.ScopedOnly)
	caller := types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: "reader", Role: types.TenantRoleViewer})
	caller = types.WithPrincipal(caller, types.Principal{Type: types.PrincipalWebUser, ID: "reader"})
	require.NoError(t, access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-a", types.ResourceActionRead))
	require.ErrorIs(t, access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "unrelated-inherit", types.ResourceActionRead), service.ErrResourceAccessDenied)
	require.ErrorIs(t, access.Authorize(caller, 7, types.GroupResourceTypeAgent, "some-agent", types.ResourceActionUse), service.ErrResourceAccessDenied)
	snap.PermissionVersion = 2
	snap.Complete = false
	require.ErrorIs(t, store.ApplyGrantSnapshot(ctx, ops[0], snap), repo.ErrNextcloudOnboardingConflict)
	snap.Complete = true
	snap.Subjects = nil
	require.NoError(t, store.ApplyGrantSnapshot(ctx, ops[0], snap))
	require.ErrorIs(t, access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-a", types.ResourceActionRead), service.ErrResourceAccessDenied)
	require.NoError(t, access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-b", types.ResourceActionRead))
	snap.PermissionVersion = 1
	require.ErrorIs(t, store.ApplyGrantSnapshot(ctx, ops[0], snap), repo.ErrNextcloudOnboardingConflict)
	require.NoError(t, db.Model(&types.Directory{}).Where("id = ?", c.DirectoryID).Update("last_sync_error", "ldaps_unreachable").Error)
	require.ErrorIs(t, access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-b", types.ResourceActionRead), service.ErrResourceAccessDenied)
	role, err = access.EffectiveTenantRole(ctx, "reader", 7, time.Now())
	require.NoError(t, err)
	require.False(t, role.Member)
	require.True(t, role.ScopedOnly, "legacy RBAC fallback must remain fenced after snapshot failure")
	var n int64
	require.NoError(t, db.Model(&types.TenantMember{}).Where("user_id='reader'").Count(&n).Error)
	require.Zero(t, n)
}

func TestNextcloudOnboardingSyncPauseCASIsRecoverable(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	if !db.Migrator().HasColumn(&types.DataSource{}, "sync_enabled") {
		require.NoError(t, db.Exec("ALTER TABLE data_sources ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT TRUE").Error)
	}
	kb := freshOnboardingKB("pausable")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	activateOnboarding(t, db, store, op)
	paused, err := store.SetSyncEnabled(ctx, op, false, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), paused.Revision)
	retry, err := store.SetSyncEnabled(ctx, op, false, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), retry.Revision)
	_, err = store.SetSyncEnabled(ctx, op, true, 1)
	require.ErrorIs(t, err, repo.ErrNextcloudOnboardingConflict)
	enabled, err := store.SyncEnabled(ctx, op)
	require.NoError(t, err)
	require.False(t, enabled)
	var ds types.DataSource
	require.NoError(t, db.Where("knowledge_base_id=?", kb.ID).Take(&ds).Error)
	require.Equal(t, types.DataSourceStatusActive, ds.Status, "sync pause does not withdraw current read publication")
}

func TestNextcloudOnboardingAuthorityEpochSupersedesPairingReceipt(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB("resumed")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	activateOnboarding(t, db, store, op)
	require.NoError(t, db.Exec("CREATE TABLE nextcloud_source_status_epochs(connection_id TEXT PRIMARY KEY,publication_epoch BIGINT,pair_operation_id TEXT,verified_at BIGINT)").Error)
	var pair repo.NextcloudSourcePairing
	require.NoError(t, db.Where("operation_id=?", op.OperationID).Take(&pair).Error)
	require.NoError(t, db.Exec("INSERT INTO nextcloud_event_connections(connection_id,tenant_id,knowledge_base_id,datasource_id,status) VALUES(?,?,?,?,?)", "conn", uint64(7), kb.ID, pair.DataSourceID, "active").Error)
	require.NoError(t, db.Exec("INSERT INTO nextcloud_source_status_epochs VALUES(?,?,?,?)", "conn", int64(1), op.OperationID, time.Now().Unix()).Error)
	snap := types.NextcloudGrantSnapshot{PermissionVersion: 1, PublicationEpoch: 1, Complete: true, Subjects: []types.NextcloudGrantSubject{{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-reader", SourceRelationID: "share"}}}
	require.NoError(t, store.ApplyGrantSnapshot(ctx, op, snap))
	matches, err := store.ScopedMatches(ctx, "reader", 7, kb.ID, time.Now())
	require.NoError(t, err)
	require.Len(t, matches, 1)
	require.NoError(t, db.Exec("UPDATE nextcloud_source_status_epochs SET publication_epoch=3").Error)
	matches, err = store.ScopedMatches(ctx, "reader", 7, kb.ID, time.Now())
	require.NoError(t, err)
	require.Empty(t, matches)
	require.ErrorIs(t, store.ApplyGrantSnapshot(ctx, op, snap), repo.ErrNextcloudOnboardingConflict, "even exact old retries are fenced by the new authority epoch")
	snap.PermissionVersion = 2
	snap.PublicationEpoch = 3
	require.NoError(t, store.ApplyGrantSnapshot(ctx, op, snap))
	matches, err = store.ScopedMatches(ctx, "reader", 7, kb.ID, time.Now())
	require.NoError(t, err)
	require.Len(t, matches, 1)
}

func TestNextcloudOnboardingPostgresConcurrentAdmission(t *testing.T) {
	if os.Getenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN") == "" {
		t.Skip("isolated PostgreSQL DSN required")
	}
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	var schema string
	require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
	peer, err := gorm.Open(postgres.Open(os.Getenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN")+" search_path="+schema), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	peerSQL, err := peer.DB()
	require.NoError(t, err)
	peerSQL.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = peerSQL.Close() })
	kb := freshOnboardingKB("pg-root-race")
	createOnboardingTemplate(t, db, &kb)
	start := make(chan struct{})
	out := make(chan error, 2)
	for _, writer := range []*repo.NextcloudOnboardingStore{store, repo.NewNextcloudOnboardingStore(peer)} {
		go func(s *repo.NextcloudOnboardingStore) {
			<-start
			_, err := s.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
			out <- err
		}(writer)
	}
	close(start)
	successes := 0
	for range 2 {
		if <-out == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	for range 5 {
		id := uuid.NewString()
		kb := freshOnboardingKB(id)
		createOnboardingTemplate(t, db, &kb)
		start := make(chan struct{})
		out := make(chan error, 2)
		go func() {
			<-start
			_, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), id))
			out <- err
		}()
		go func() {
			<-start
			out <- peer.Create(&types.Knowledge{ID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: id, Channel: "manual"}).Error
		}()
		close(start)
		successes := 0
		for range 2 {
			if <-out == nil {
				successes++
			}
		}
		require.Equal(t, 1, successes, "DB reservation and content writer must share one admission lock")
	}
}

func TestNextcloudOnboardingCurrentSchemaMissingControlsOrPairingFailsClosed(t *testing.T) {
	t.Setenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN", "")
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	kb := freshOnboardingKB("missing-control-root")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), kb.ID))
	require.NoError(t, err)
	activateOnboarding(t, db, store, op)
	snapshot := types.NextcloudGrantSnapshot{PermissionVersion: 1, PublicationEpoch: 1, Complete: true, Subjects: []types.NextcloudGrantSubject{{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-reader", SourceRelationID: "direct"}}}
	require.NoError(t, store.ApplyGrantSnapshot(ctx, op, snapshot))
	ddl, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	if !db.Migrator().HasTable("nextcloud_content_fences") {
		require.NoError(t, db.Exec(string(ddl)).Error)
	}
	require.NoError(t, db.Exec("DROP TABLE nextcloud_control_recovery_gates").Error)
	closed, err := store.InstanceControlsClosed(ctx, c.InstanceID)
	require.NoError(t, err)
	require.True(t, closed)
	_, err = store.Credential(ctx, "test-onboarding-server-credential-0123456789")
	require.Error(t, err)
	matches, err := store.ScopedMatches(ctx, "reader", 7, kb.ID, time.Now())
	require.NoError(t, err)
	require.Empty(t, matches)
	leases := repo.NewNextcloudContentLeaseStore(db)
	_, err = leases.AcquireKBRead(ctx, 7, kb.ID, "ordinary-owner", time.Minute)
	require.ErrorIs(t, err, repo.ErrNextcloudContentLeaseDenied)
	require.NoError(t, db.Exec("DROP TABLE nextcloud_source_pairings").Error)
	allowed, err := repo.NextcloudBuildControlsAllowed(ctx, db, 7, kb.ID, "unknown-source", "new-candidate")
	require.NoError(t, err)
	require.False(t, allowed)
	_, err = leases.AcquireKBRead(ctx, 7, kb.ID, "ordinary-owner", time.Minute)
	require.ErrorIs(t, err, repo.ErrNextcloudContentLeaseDenied)
}

func TestNextcloudOnboardingExistingBlankPreservesLegalConfigurationAtomically(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	require.NoError(t, db.Create(&types.Model{ID: "graph-model", TenantID: 7, Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive}).Error)
	for _, required := range []bool{false, true} {
		template := freshOnboardingKB(uuid.NewString())
		template.IndexingStrategy.GraphEnabled = true
		template.IndexingStrategy.GraphRequired = required
		template.ExtractConfig = &types.ExtractConfig{Enabled: true}
		template.SummaryModelID = "graph-model"
		template.ChunkingConfig.ChunkSize = 1234
		createOnboardingTemplate(t, db, &template)
		credential := c
		credential.ID = "template-credential-" + uuid.NewString()
		credential.TokenSHA256 = onboardingTestSHA(credential.ID)
		credential.TemplateKBID = template.ID
		require.NoError(t, db.Create(&credential).Error)
		kb := freshOnboardingKB(uuid.NewString())
		kb.Name = "Retained human title"
		kb.Description = "Retained human description"
		kb.CreatorID = "existing-owner"
		kb.ChunkingConfig.ChunkSize = 567
		kb.IndexingStrategy.GraphEnabled = true
		kb.IndexingStrategy.GraphRequired = !required
		kb.ExtractConfig = &types.ExtractConfig{Enabled: true}
		kb.SummaryModelID = "graph-model"
		createOnboardingTemplate(t, db, &kb)
		req := onboardingRequest(credential, uuid.NewString(), kb.ID)
		op, err := store.Reserve(ctx, credential, req)
		require.NoError(t, err)
		require.Equal(t, kb.ID, op.KnowledgeBaseID)
		var current types.KnowledgeBase
		require.NoError(t, db.Where("id=?", kb.ID).Take(&current).Error)
		require.True(t, current.IndexingStrategy.GraphEnabled)
		require.Equal(t, !required, current.IndexingStrategy.GraphRequired)
		require.Equal(t, kb.ExtractConfig, current.ExtractConfig)
		require.Equal(t, "graph-model", current.SummaryModelID)
		require.Equal(t, 567, current.ChunkingConfig.ChunkSize)
		require.Equal(t, kb.Name, current.Name)
		require.Equal(t, kb.Description, current.Description)
		require.Equal(t, kb.CreatorID, current.CreatorID)
		require.Equal(t, kb.CreatedAt.Unix(), current.CreatedAt.Unix())
		_, err = store.Reserve(ctx, credential, req)
		require.NoError(t, err)
	}
}

func onboardingTestSHA(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])
}
