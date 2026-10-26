package repository_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
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
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Error)})
	} else {
		db, err = gorm.Open(
			sqlite.Open(filepath.Join(t.TempDir(), "onboarding.db")+"?_busy_timeout=5000"),
			&gorm.Config{Logger: logger.Default.LogMode(logger.Error)},
		)
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

	require.NoError(
		t,
		db.AutoMigrate(
			&types.User{},
			&types.Session{},
			&types.Message{},
			&types.KnowledgeTag{},
			&types.KnowledgeTagRelation{},
			&types.Tenant{},
			&types.Chunk{},
			&types.KnowledgeBase{},
			&types.Knowledge{},
			&types.DataSource{},
			&types.Model{},
			&types.StorageBackend{},
			&types.TenantMember{},
			&types.ResourceAccessPolicy{},
			&types.ResourceGroupGrant{},
			&types.TenantGroupRoleGrant{},
			&types.PermissionVersion{},
			&types.Directory{},
			&types.DirectoryIdentity{},
			&types.DirectoryGroup{},
			&types.DirectoryGroupMembership{},
			&repo.NextcloudSourcePairing{},
			&repo.NextcloudSourcePairingAbort{},
		),
	)
	for _, sql := range []string{
		("CREATE UNIQUE INDEX policies_key ON resource_access_policies(tenant_id,res" +
			"ource_type,resource_id)"),
		"CREATE TABLE wiki_pages(tenant_id INTEGER,knowledge_base_id TEXT)",
		("CREATE TABLE nextcloud_source_tombstones(tenant_id INTEGER,scope_type TEXT" +
			",scope_id TEXT)"),
		("CREATE TABLE nextcloud_event_connections(connection_id TEXT PRIMARY KEY,te" +
			"nant_id BIGINT,knowledge_base_id TEXT,nextcloud_instance_id TEXT,binding_i" +
			"d TEXT,datasource_id TEXT,status TEXT)"),
		"CREATE TABLE nextcloud_event_inbox(connection_id TEXT)",
		("CREATE TABLE nextcloud_event_dispatch(connection_id TEXT,state TEXT NOT NU" +
			"LL,updated_at TIMESTAMP NOT NULL)"),
		"CREATE TABLE nextcloud_event_checkpoint(connection_id TEXT)",
	} {
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
	controls := []string{
		"../../../migrations/sqlite/000076_nextcloud_directory_migrations.up.sql",
		"../../../migrations/sqlite/000078_nextcloud_control_recovery.up.sql",
		"../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql",
		"../../../migrations/sqlite/000081_nextcloud_target_closure.up.sql",
		"../../../migrations/sqlite/000083_nextcloud_current_manager_purpose.up.sql",
		"../../../migrations/sqlite/000084_nextcloud_machine_tasks.up.sql",
	}
	if dialect == "versioned" {
		controls = []string{
			"../../../migrations/versioned/000157_nextcloud_directory_migrations.up.sql",
			"../../../migrations/versioned/000159_nextcloud_control_recovery.up.sql",
			"../../../migrations/versioned/000127_nextcloud_content_leases.up.sql",
			"../../../migrations/versioned/000162_nextcloud_target_closure.up.sql",
			"../../../migrations/versioned/000164_nextcloud_current_manager_purpose.up.sql",
			"../../../migrations/versioned/000165_nextcloud_machine_tasks.up.sql",
			"../../../migrations/versioned/000167_nextcloud_multisource.up.sql",
		}
	}
	// Producer/config/name fixtures use the actual immutable origin and body
	// schema. No current template row is retroactively declared independent.
	provenance := []string{
		"000053_knowledge_input_provenance.up.sql",
		"000054_knowledge_configuration_history_inputs.up.sql",
		"000055_knowledge_actual_config_controls.up.sql",
		"000056_knowledge_original_worker_actor.up.sql",
		"000059_knowledge_collection_display_inputs.up.sql",
		"000062_knowledge_enrichment_inputs.up.sql",
		"000066_knowledge_enrichment_template_origins.up.sql",
		"000069_datasource_name_inputs.up.sql",
		"000071_original_body_retention.up.sql",
		"000082_knowledge_configuration_original_body.up.sql",
	}
	if dialect == "versioned" {
		provenance = []string{
			"000134_knowledge_input_provenance.up.sql",
			"000135_knowledge_configuration_history_inputs.up.sql",
			"000136_knowledge_actual_config_controls.up.sql",
			"000137_knowledge_original_worker_actor.up.sql",
			"000140_knowledge_collection_display_inputs.up.sql",
			"000143_knowledge_enrichment_inputs.up.sql",
			"000147_knowledge_enrichment_template_origins.up.sql",
			"000150_datasource_name_inputs.up.sql",
			"000152_original_body_retention.up.sql",
			"000163_knowledge_configuration_original_body.up.sql",
		}
	}
	for _, name := range provenance {
		part, e := os.ReadFile("../../../migrations/" + dialect + "/" + name)
		require.NoError(t, e)
		// AutoMigrate already provides this column; preserve every actual trigger.
		lines := strings.Split(string(part), "\n")
		kept := make([]string, 0, len(lines))
		for _, line := range lines {
			if strings.HasPrefix(
				strings.TrimSpace(line),
				"ALTER TABLE knowledge_bases ADD COLUMN enrichment_templates ",
			) {
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
		migrationDSN, err := postgresDSNWithRuntimeParams(
			os.Getenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN"),
			map[string]string{
				"search_path":             schema,
				"default_query_exec_mode": "simple_protocol",
			},
		)
		require.NoError(t, err)
		migrationDB, err := gorm.Open(
			postgres.Open(migrationDSN),
			&gorm.Config{Logger: logger.Default.LogMode(logger.Error)},
		)
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
	d := types.Directory{
		ID:                   "directory",
		Protocol:             types.DirectoryProtocolAD,
		Enabled:              true,
		LastSuccessfulSyncAt: &now,
		SnapshotVersion:      1,
	}
	require.NoError(t, db.Create(&d).Error)
	owner := "owner"
	reader := "reader"
	require.NoError(
		t,
		db.Create(
			&types.User{ID: owner, Username: owner, Email: "owner@onboarding.test", TenantID: 7, IsActive: true},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.User{ID: reader, TenantID: 7, Username: reader, Email: "reader@onboarding.test", IsActive: true},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.DirectoryIdentity{
				ID:              "i-owner",
				DirectoryID:     d.ID,
				ObjectGUID:      "guid-owner",
				UserID:          &owner,
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 1,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.DirectoryIdentity{
				ID:              "i-reader",
				DirectoryID:     d.ID,
				ObjectGUID:      "guid-reader",
				UserID:          &reader,
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 1,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.DirectoryIdentity{
				ID:              "i-unlogged",
				DirectoryID:     d.ID,
				ObjectGUID:      "guid-unlogged",
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 1,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.DirectoryGroup{
				ID:              "g-engineering",
				DirectoryID:     d.ID,
				ObjectGUID:      "guid-engineering",
				Status:          types.DirectoryObjectActive,
				SnapshotVersion: 1,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.DirectoryGroupMembership{
				DirectoryID:     d.ID,
				GroupID:         "g-engineering",
				IdentityID:      "i-reader",
				SnapshotVersion: 1,
				Source:          types.DirectoryMembershipNested,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.TenantMember{
				TenantID: 7,
				UserID:   owner,
				Role:     types.TenantRoleOwner,
				Status:   types.TenantMemberStatusActive,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.Model{ID: "embedding", TenantID: 7, Type: types.ModelTypeEmbedding, Status: types.ModelStatusActive},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.StorageBackend{
				ID:       "storage",
				TenantID: 7,
				Name:     "local",
				Provider: "local",
				Status:   types.StorageBackendStatusActive,
			},
		).Error,
	)
	h := sha256.Sum256([]byte("test-onboarding-server-credential-0123456789"))
	cred := types.NextcloudOnboardingCredential{
		ID:                "automation",
		TokenSHA256:       hex.EncodeToString(h[:]),
		InstanceID:        "nc-instance",
		BaseURL:           "https://nc.example.test",
		TenantID:          7,
		DirectoryID:       d.ID,
		TemplateKBID:      "template",
		Enabled:           true,
		MaxKnowledgeBases: 10,
	}
	require.NoError(t, db.Create(&cred).Error)
	journal := t.TempDir()
	require.NoError(t, os.Chmod(journal, 0o700))
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	keyPath := filepath.Join(journal, "key")
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))
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

func postgresDSNWithRuntimeParams(dsn string, params map[string]string) (string, error) {
	if !strings.Contains(dsn, "://") {
		parts := []string{strings.TrimSpace(dsn)}
		for key, value := range params {
			parts = append(parts, key+"="+value)
		}
		return strings.Join(parts, " "), nil
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	for key, value := range params {
		query.Set(key, value)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func TestPostgresDSNWithRuntimeParamsSupportsURLAndKeywordForms(t *testing.T) {
	urlDSN, err := postgresDSNWithRuntimeParams(
		"postgres://user:pass@db.example/weknora?sslmode=disable",
		map[string]string{"search_path": "onboarding_test"},
	)
	require.NoError(t, err)
	require.Contains(t, urlDSN, "search_path=onboarding_test")
	require.Contains(t, urlDSN, "sslmode=disable")

	keywordDSN, err := postgresDSNWithRuntimeParams(
		"host=db.example port=5432 user=user dbname=weknora sslmode=disable",
		map[string]string{"search_path": "onboarding_test"},
	)
	require.NoError(t, err)
	require.Contains(t, keywordDSN, "host=db.example")
	require.Contains(t, keywordDSN, "search_path=onboarding_test")
}

func applySQLiteMultisourceMigration(t *testing.T, db *gorm.DB) {
	t.Helper()
	if db.Name() != "sqlite" {
		return
	}
	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	for _, statement := range []string{
		"CREATE TABLE nextcloud_graph_records(operation_id TEXT PRIMARY KEY,tenant_id BIGINT," +
			"knowledge_base_id TEXT,knowledge_id TEXT,generation BIGINT,build_lease_id TEXT)",
		"CREATE TABLE nextcloud_gc_jobs(id TEXT PRIMARY KEY,datasource_id TEXT)",
		"CREATE TABLE nextcloud_source_rotations(operation_id TEXT,pair_operation_id TEXT," +
			"datasource_id TEXT,state TEXT,old_key_id TEXT,new_key_id TEXT," +
			"old_config_sha256 TEXT,new_config_sha256 TEXT,old_config TEXT,new_config TEXT)",
	} {
		require.NoError(t, db.Exec(statement).Error)
	}
	for _, name := range []string{
		"000031_nextcloud_source_versions",
		"000042_nextcloud_empty_decommission",
		"000043_nextcloud_virgin_rebuild_guard",
		"000044_nextcloud_indexed_withdrawal",
	} {
		migration, err := os.ReadFile("../../../migrations/sqlite/" + name + ".up.sql")
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(migration)).Error)
	}
	migration, err := os.ReadFile(
		"../../../migrations/sqlite/000086_nextcloud_multisource.up.sql",
	)
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(migration)).Error)
}

// Create a template through the same authenticated independent-input writer
// as a human author; never certify an already-loaded legacy configuration.
func createOnboardingTemplate(t *testing.T, db *gorm.DB, kb *types.KnowledgeBase) {
	t.Helper()
	store := repo.NewKnowledgeInputProvenanceStore(db)
	factory := func(tx *gorm.DB) *repo.KnowledgeInputProvenanceStore {
		actual := repo.NewKnowledgeInputProvenanceStore(tx)
		repo.ConfigureKnowledgeInputActorGroupAccess(
			actual,
			service.NewGroupAccessService(repo.NewGroupAccessRepository(tx)),
		)
		return actual
	}
	repo.ConfigureKnowledgeDisplayInputWriters(
		store,
		repo.NewKnowledgeTagRepository(db),
		repo.NewKnowledgeRepository(db),
		factory,
	)
	admin := types.WithExecutionTenant(
		types.WithPrincipal(
			types.WithCaller(
				context.Background(),
				types.Caller{TenantID: 7, UserID: "owner", Role: types.TenantRoleOwner},
			),
			types.Principal{Type: types.PrincipalWebUser, ID: "owner"},
		),
		7,
	)
	require.NoError(
		t,
		store.CreateKnowledgeBaseWithInput(
			repo.WithIndependentKnowledgeBaseNameInput(repo.WithIndependentKnowledgeInput(admin), kb.ID, kb.Name),
			kb,
		),
	)
}

func freshOnboardingKB(id string) types.KnowledgeBase {
	storage := "storage"
	return types.KnowledgeBase{
		ID:               id,
		TenantID:         7,
		Name:             id,
		Type:             "document",
		EmbeddingModelID: "embedding",
		StorageBackendID: &storage,
		IndexingStrategy: types.DefaultIndexingStrategy(),
	}
}

func onboardingRequest(c types.NextcloudOnboardingCredential, id, kb string) types.NextcloudOnboardingRequest {
	return types.NextcloudOnboardingRequest{
		OperationID:          id,
		TenantID:             c.TenantID,
		InstanceID:           c.InstanceID,
		BindingID:            "binding-" + id,
		ActorDirectoryID:     c.DirectoryID,
		ActorObjectGUID:      "guid-owner",
		Mode:                 "existing",
		Name:                 "Root",
		KnowledgeBaseID:      kb,
		ProcessingTemplateID: c.TemplateKBID,
	}
}

func activateOnboarding(
	t *testing.T,
	db *gorm.DB,
	store *repo.NextcloudOnboardingStore,
	op types.NextcloudOnboardingOperation,
) {
	t.Helper()
	pairer := repo.NewNextcloudSourcePairingRepository(db)
	factory := func(tx *gorm.DB) *repo.KnowledgeInputProvenanceStore {
		actual := repo.NewKnowledgeInputProvenanceStore(tx)
		repo.ConfigureKnowledgeInputActorGroupAccess(
			actual,
			service.NewGroupAccessService(repo.NewGroupAccessRepository(tx)),
		)
		return actual
	}
	repo.ConfigureDataSourceNameInputWriters(repo.NewDataSourceRepository(db), pairer, factory)
	var credential types.NextcloudOnboardingCredential
	require.NoError(t, db.Where("id=?", op.CredentialID).Take(&credential).Error)
	producer, err := store.BeginPairProducer(
		context.Background(),
		credential,
		op,
		op.ActorDirectoryID,
		op.ActorObjectGUID,
	)
	require.NoError(t, err)
	pair, _, err := pairer.PrepareSourcePairing(
		producer,
		repo.NextcloudSourcePairing{
			OperationID:      op.OperationID,
			TenantID:         op.TenantID,
			KnowledgeBaseID:  op.KnowledgeBaseID,
			InstanceID:       op.InstanceID,
			BindingID:        op.BindingID,
			BaseURL:          "https://nc.example.test",
			PublicationEpoch: 1,
			KeyID:            "pair_" + op.OperationID,
		},
		"handoff-token",
	)
	require.NoError(t, err)
	require.NoError(t, pairer.ActivateSourcePairing(context.Background(), pair))
	require.NoError(t, store.MarkActive(context.Background(), op))
}

func TestNextcloudOnboardingBlankHistoryReservationAndRetry(t *testing.T) {
	db, store, c := onboardingDB(t)
	ctx := context.Background()
	_, err := store.Reserve(ctx, c, onboardingRequest(c, uuid.NewString(), "legacy-zero-count"))
	require.ErrorIs(t, err, repo.ErrNextcloudKBUnavailable)
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
	_, _, err = repo.NewNextcloudSourcePairingRepository(db).
		PrepareSourcePairing(ctx, repo.NextcloudSourcePairing{
			OperationID:      uuid.NewString(),
			TenantID:         7,
			KnowledgeBaseID:  kb.ID,
			InstanceID:       c.InstanceID,
			BindingID:        "wrong-root",
			BaseURL:          c.BaseURL,
			PublicationEpoch: 1,
			KeyID:            "wrong",
		}, "token")
	require.ErrorIs(t, err, repo.ErrNextcloudSourcePairingConflict)
	// Tenant containing unrelated business content is a valid target.
	other := freshOnboardingKB("other")
	require.NoError(t, db.Create(&other).Error)
	require.NoError(
		t,
		db.Create(
			&types.Knowledge{ID: "other-document", TenantID: 7, KnowledgeBaseID: other.ID, Channel: "manual"},
		).Error,
	)
	create := onboardingRequest(c, uuid.NewString(), "")
	create.Mode = "create"
	op, err := store.Reserve(ctx, c, create)
	require.NoError(t, err)
	require.NotEqual(t, other.ID, op.KnowledgeBaseID)
}

func TestNextcloudOnboardingCandidatesIncludeSharedKnowledgeBase(t *testing.T) {
	db, store, c := onboardingDB(t)
	kb := freshOnboardingKB("already-bound")
	createOnboardingTemplate(t, db, &kb)
	op, err := store.Reserve(
		context.Background(),
		c,
		onboardingRequest(c, uuid.NewString(), kb.ID),
	)
	require.NoError(t, err)
	activateOnboarding(t, db, store, op)

	page, err := store.CandidatePage(
		context.Background(),
		c,
		c.DirectoryID,
		"guid-owner",
		"",
		"",
		50,
		"req_already_bound",
	)
	require.NoError(t, err)
	var found types.NextcloudKnowledgeBaseCandidate
	var foundShared bool
	for _, candidate := range page.Candidates {
		if candidate.ID == kb.ID {
			found = candidate
			foundShared = true
		}
	}
	require.True(t, foundShared)
	require.EqualValues(t, 1, found.NextcloudSourceCount)
	for _, candidate := range page.Ineligible {
		require.NotEqual(t, kb.ID, candidate.ID)
	}
}

func TestNextcloudOnboardingCompetingRootsAndConcurrentContent(t *testing.T) {
	db, store, c := onboardingDB(t)
	applySQLiteMultisourceMigration(t, db)
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
	require.Equal(t, 2, successes,
		"v3 admits independent bindings concurrently into the same knowledge base")
	touched := freshOnboardingKB("touched")
	createOnboardingTemplate(t, db, &touched)
	doc := types.Knowledge{ID: "removed", TenantID: 7, KnowledgeBaseID: touched.ID, Channel: "manual"}
	require.NoError(t, db.Create(&doc).Error)
	require.NoError(t, db.Unscoped().Delete(&doc).Error)
	_, err := store.Reserve(context.Background(), c, onboardingRequest(c, uuid.NewString(), touched.ID))
	require.NoError(t, err, "an existing non-empty KB with verified configuration is a legal target")
	require.Error(
		t,
		db.Exec("UPDATE nextcloud_kb_blank_witnesses SET virgin=1 WHERE knowledge_base_id=?", touched.ID).Error,
	)
}

func TestNextcloudOnboardingAllowsExistingNonEmptyKnowledgeBase(t *testing.T) {
	db, store, c := onboardingDB(t)
	kb := freshOnboardingKB("nonempty-existing")
	createOnboardingTemplate(t, db, &kb)
	require.NoError(t, db.Create(&types.Knowledge{
		ID:              "existing-content",
		TenantID:        7,
		KnowledgeBaseID: kb.ID,
		Channel:         "manual",
	}).Error)
	candidates, err := store.Candidates(context.Background(), c, "directory", "guid-owner")
	require.NoError(t, err)
	found := false
	for _, candidate := range candidates {
		if candidate["id"] == kb.ID {
			found = true
			require.Equal(t, "1", candidate["content_count"])
			break
		}
	}
	require.True(t, found)
	op, err := store.Reserve(
		context.Background(),
		c,
		onboardingRequest(c, uuid.NewString(), kb.ID),
	)
	require.NoError(t, err)
	require.Equal(t, kb.ID, op.KnowledgeBaseID)
}

func TestNextcloudOnboardingReservesIndependentBindingsForSharedKnowledgeBase(t *testing.T) {
	db, store, credential := onboardingDB(t)
	applySQLiteMultisourceMigration(t, db)
	kb := freshOnboardingKB("shared-target")
	createOnboardingTemplate(t, db, &kb)
	credential.MaxKnowledgeBases = 1
	require.NoError(t, db.Model(&types.NextcloudOnboardingCredential{}).
		Where("id = ?", credential.ID).
		Update("max_knowledge_bases", 1).Error)

	first, err := store.Reserve(
		context.Background(), credential,
		onboardingRequest(credential, uuid.NewString(), kb.ID),
	)
	require.NoError(t, err)
	second, err := store.Reserve(
		context.Background(), credential,
		onboardingRequest(credential, uuid.NewString(), kb.ID),
	)
	require.NoError(t, err)
	require.Equal(t, kb.ID, first.KnowledgeBaseID)
	require.Equal(t, kb.ID, second.KnowledgeBaseID)
	require.NotEqual(t, first.BindingID, second.BindingID)
	require.NotEqual(t, first.OperationID, second.OperationID)

	other := freshOnboardingKB("quota-second-unique-target")
	createOnboardingTemplate(t, db, &other)
	_, err = store.Reserve(
		context.Background(), credential,
		onboardingRequest(credential, uuid.NewString(), other.ID),
	)
	require.ErrorIs(t, err, repo.ErrNextcloudOnboardingConflict,
		"a second unique KB must still consume the configured quota")

	pairings := repo.NewNextcloudSourcePairingRepository(db)
	var sharedPairs []repo.NextcloudSourcePairing
	for i, op := range []types.NextcloudOnboardingOperation{first, second} {
		pair, created, err := pairings.PrepareSourcePairing(
			context.Background(),
			repo.NextcloudSourcePairing{
				OperationID: op.OperationID, TenantID: op.TenantID,
				KnowledgeBaseID: op.KnowledgeBaseID, InstanceID: op.InstanceID,
				BindingID: op.BindingID, BaseURL: credential.BaseURL,
				KeyID: "pair_" + strings.Repeat(string(rune('a'+i)), 32),
			},
			"shared-binding-token-"+op.OperationID,
		)
		require.NoError(t, err)
		require.True(t, created)
		require.Equal(t, op.BindingID, pair.BindingID)
		require.NoError(t, pairings.ActivateSourcePairing(context.Background(), pair))
		sharedPairs = append(sharedPairs, pair)
	}
	var active int64
	require.NoError(t, db.Model(&repo.NextcloudSourcePairing{}).
		Where("tenant_id=? AND knowledge_base_id=? AND state='active'", 7, kb.ID).
		Count(&active).Error)
	require.EqualValues(t, 2, active)
	page, err := store.CandidatePage(
		context.Background(),
		credential,
		credential.DirectoryID,
		"guid-owner",
		"",
		"",
		50,
		"req_shared_target",
	)
	require.NoError(t, err)
	var sharedCandidate *types.NextcloudKnowledgeBaseCandidate
	for i := range page.Candidates {
		if page.Candidates[i].ID == kb.ID {
			sharedCandidate = &page.Candidates[i]
		}
	}
	require.NotNil(t, sharedCandidate)
	require.EqualValues(t, 2, sharedCandidate.NextcloudSourceCount)

	if db.Name() == "sqlite" {
		for _, pair := range sharedPairs {
			var virgin bool
			require.NoError(t, db.Raw(
				`SELECT ever_touched FROM nextcloud_source_virgin WHERE pair_operation_id=?`,
				pair.OperationID,
			).Scan(&virgin).Error)
			require.False(t, virgin)
			var source types.DataSource
			require.NoError(t, db.Where("id=?", pair.DataSourceID).Take(&source).Error)
			require.Equal(t, types.ConnectorTypeNextcloud, source.Type)
			require.Equal(t, types.DataSourceStatusActive, source.Status)
			require.Nil(t, source.LastSyncAt)
			require.Empty(t, source.LastSyncCursor)
			require.Empty(t, source.LastSyncResult)
			base, hash, binding, err := repo.NextcloudEventDataSourceIdentity(source.Config)
			require.NoError(t, err)
			require.Equal(t, pair.BaseURL, base)
			require.Equal(t, pair.ConfigSHA, hash)
			require.Equal(t, pair.BindingID, binding)
			config, err := source.ParseConfig()
			require.NoError(t, err)
			require.Equal(t, pair.KeyID, config.Credentials["key_id"])
			for table, column := range map[string]string{
				"sync_logs":                   "data_source_id",
				"nextcloud_event_connections": "datasource_id",
				"nextcloud_source_versions":   "datasource_id",
				"nextcloud_gc_jobs":           "datasource_id",
			} {
				var count int64
				require.NoError(t, db.Table(table).Where(column+"=?", pair.DataSourceID).
					Count(&count).Error)
				require.Zero(t, count, table)
			}
			var sourceRows int64
			require.NoError(t, db.Table("knowledges").
				Where("tenant_id=? AND knowledge_base_id=? AND metadata->>'datasource_id'=?",
					7, kb.ID, pair.DataSourceID).Count(&sourceRows).Error)
			require.Zero(t, sourceRows)
			_, err = pairings.BeginEmptySourceDecommission(
				context.Background(),
				pair,
				repo.NextcloudSourceDecommission{
					OperationID: uuid.NewString(), PairOperationID: pair.OperationID,
					TenantID: pair.TenantID, KnowledgeBaseID: pair.KnowledgeBaseID,
					DataSourceID: pair.DataSourceID,
					InstanceID:   pair.InstanceID, BindingID: pair.BindingID,
					KeyID:            pair.KeyID,
					PublicationEpoch: 1,
				},
			)
			require.NoError(t, err, pair.OperationID)
		}
		var decommissions int64
		require.NoError(t, db.Model(&repo.NextcloudSourceDecommission{}).
			Where("tenant_id=? AND knowledge_base_id=?", 7, kb.ID).
			Count(&decommissions).Error)
		require.EqualValues(t, 2, decommissions)
	}

	require.NoError(t, repo.NewKnowledgeBaseRepository(db).DeleteKnowledgeBase(
		context.Background(), kb.ID,
	))
	var closures int64
	require.NoError(t, db.Model(&repo.NextcloudTargetClosure{}).
		Where("tenant_id=? AND knowledge_base_id=?", 7, kb.ID).
		Count(&closures).Error)
	require.EqualValues(t, 2, closures)
}

func TestNextcloudCandidatePageExplainsIneligibleTargetsAndRejectsStaleRevision(t *testing.T) {
	db, store, c := onboardingDB(t)
	valid := freshOnboardingKB("selectable")
	valid.Name = "Alpha selectable"
	createOnboardingTemplate(t, db, &valid)

	wiki := freshOnboardingKB("wiki-target")
	wiki.Name = "Beta wiki"
	wiki.Type = types.KnowledgeBaseTypeWiki
	createOnboardingTemplate(t, db, &wiki)

	bound := freshOnboardingKB("bound-target")
	bound.Name = "Gamma bound"
	createOnboardingTemplate(t, db, &bound)
	require.NoError(t, db.Model(&types.KnowledgeBase{}).Where("id=?", bound.ID).
		Update("ever_had_nextcloud_source", true).Error)
	require.NoError(t, db.Create(&types.DataSource{
		ID: "shared-source", TenantID: 7, KnowledgeBaseID: bound.ID,
		Name: "Shared source", Type: types.ConnectorTypeNextcloud,
		Config: types.JSON(`{"resource_ids":["root-a"],"settings":{},"credentials":{}}`),
		Status: types.DataSourceStatusPaused,
	}).Error)
	require.NoError(t, db.Create(&repo.NextcloudSourcePairing{
		OperationID: "shared-pair", TenantID: 7, KnowledgeBaseID: bound.ID,
		DataSourceID: "shared-source", InstanceID: "instance",
		BindingID: "root-a", BaseURL: "https://cloud.example.test",
		ConfigSHA: strings.Repeat("a", 64), PublicationEpoch: 1,
		KeyID: "pair_shared", State: "active",
	}).Error)
	require.NoError(t, db.Model(&types.DataSource{}).
		Where("id = ?", "shared-source").
		Update("status", types.DataSourceStatusActive).Error)

	unpaired := freshOnboardingKB("unpaired-target")
	createOnboardingTemplate(t, db, &unpaired)
	require.NoError(t, db.Create(&types.DataSource{
		ID: "legacy-unpaired", TenantID: 7, KnowledgeBaseID: unpaired.ID,
		Name: "Legacy unpaired", Type: types.ConnectorTypeNextcloud,
		Config: types.JSON(`{"resource_ids":["legacy-root"],"settings":{},"credentials":{}}`),
		Status: types.DataSourceStatusPaused,
	}).Error)
	mixed := freshOnboardingKB("mixed-target")
	createOnboardingTemplate(t, db, &mixed)
	require.NoError(t, db.Create(&types.DataSource{
		ID: "mixed-source", TenantID: 7, KnowledgeBaseID: mixed.ID,
		Name: "Other source", Type: types.ConnectorTypeGitLab,
		Config: types.JSON(`{}`), Status: types.DataSourceStatusActive,
	}).Error)

	page, err := store.CandidatePage(context.Background(), c, "directory", "guid-owner",
		"", "", 50, "req_test")
	require.NoError(t, err)
	require.Equal(t, types.NextcloudOnboardingContractVersion, page.ContractVersion)
	require.Equal(t, "req_test", page.RequestID)
	require.True(t, strings.HasPrefix(page.CandidateRevision, "sha256:"))
	require.Contains(t, candidateIDs(page.Candidates), valid.ID)
	require.Contains(t, candidateIDs(page.Candidates), c.TemplateKBID)
	reasons := map[string]string{}
	for _, candidate := range page.Ineligible {
		reasons[candidate.ID] = candidate.ReasonCode
	}
	require.Equal(t, "kb_wiki_not_supported", reasons[wiki.ID])
	require.Equal(t, "kb_unpaired_sources", reasons[unpaired.ID])
	require.Equal(t, "kb_mixed_sources", reasons[mixed.ID])
	require.NotContains(t, reasons, bound.ID)
	require.Contains(t, candidateIDs(page.Candidates), bound.ID)
	for _, candidate := range page.Candidates {
		if candidate.ID == bound.ID {
			require.EqualValues(t, 1, candidate.NextcloudSourceCount)
		}
	}
	for _, candidate := range page.Candidates {
		require.True(t, candidate.Eligible)
		require.True(t, candidate.CanTakeOver)
		require.Empty(t, candidate.ReasonCode)
	}

	searched, err := store.CandidatePage(context.Background(), c, "directory", "guid-owner",
		"Alpha", "", 50, "req_search")
	require.NoError(t, err)
	require.Equal(t, page.CandidateRevision, searched.CandidateRevision)
	require.Equal(t, []string{valid.ID}, candidateIDs(searched.Candidates))

	first, err := store.CandidatePage(context.Background(), c, "directory", "guid-owner",
		"", "", 1, "req_page_one")
	require.NoError(t, err)
	require.NotEmpty(t, first.NextCursor)
	second, err := store.CandidatePage(context.Background(), c, "directory", "guid-owner",
		"", first.NextCursor, 1, "req_page_two")
	require.NoError(t, err)
	require.NotEqual(t,
		append(candidateIDs(first.Candidates), candidateIDs(first.Ineligible)...),
		append(candidateIDs(second.Candidates), candidateIDs(second.Ineligible)...),
	)

	stale := onboardingRequest(c, uuid.NewString(), valid.ID)
	stale.CandidateRevision = "sha256:" + strings.Repeat("0", 64)
	_, err = store.Reserve(context.Background(), c, stale)
	require.ErrorIs(t, err, repo.ErrNextcloudCandidateRevisionChanged)

	updated, err := store.CandidatePage(context.Background(), c, "directory", "guid-owner",
		"", "", 50, "req_after")
	require.NoError(t, err)
	stale.CandidateRevision = updated.CandidateRevision
	_, err = store.Reserve(context.Background(), c, stale)
	require.NoError(t, err)
}

func candidateIDs(candidates []types.NextcloudKnowledgeBaseCandidate) []string {
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.ID)
	}
	return ids
}

func TestNextcloudConnectionAuthorizesAndRevokesWorkspaceCredential(t *testing.T) {
	db, store, _ := onboardingDB(t)
	ctx := context.Background()
	spaces, err := store.ConnectionWorkspaces(ctx, "owner", false)
	require.NoError(t, err)
	require.Len(t, spaces, 1)
	require.True(t, spaces[0].Eligible)
	require.Equal(t, uint64(7), spaces[0].TenantID)
	require.NotEmpty(t, spaces[0].ProcessingTemplateID)

	result, err := store.AuthorizeConnection(ctx, types.NextcloudConnectionAuthorizeRequest{
		InstanceID:  "nc-instance",
		BaseURL:     "https://nc.example.test",
		DirectoryID: "directory",
		TenantIDs:   []uint64{7},
	}, "owner", false)
	require.NoError(t, err)
	require.Len(t, result.Credentials, 1)
	credential := result.Credentials[0]
	require.NotEmpty(t, credential.Token)
	require.Equal(t, uint64(7), credential.TenantID)
	require.Equal(t, spaces[0].ProcessingTemplateID, credential.ProcessingTemplateID)

	reconnected, err := store.AuthorizeConnection(ctx, types.NextcloudConnectionAuthorizeRequest{
		InstanceID:  "nc-instance",
		BaseURL:     "https://nc.example.test",
		DirectoryID: "directory",
		TenantIDs:   []uint64{7},
	}, "owner", false)
	require.NoError(t, err)
	require.Equal(t, credential.ProcessingTemplateID, reconnected.Credentials[0].ProcessingTemplateID)

	var stored types.NextcloudOnboardingCredential
	require.NoError(t, db.Where("id=?", reconnected.Credentials[0].CredentialID).Take(&stored).Error)
	require.True(t, stored.Enabled)
	require.NotEqual(t, reconnected.Credentials[0].Token, stored.TokenSHA256)

	revoked, err := store.RevokeConnectionByCredential(ctx, stored, []string{stored.ID})
	require.NoError(t, err)
	require.Equal(t, 1, revoked)
	require.NoError(t, db.Where("id=?", stored.ID).Take(&stored).Error)
	require.False(t, stored.Enabled)
}

func TestNextcloudConnectionHealthUsesIndependentWorkerHeartbeat(t *testing.T) {
	db, store, _ := onboardingDB(t)
	ctx := context.Background()
	initial, err := store.ConnectionHealth(ctx, nil)
	require.NoError(t, err)
	require.False(t, initial.BindingAllowed)
	require.Contains(t, initial.ReasonCode, "unverified")

	require.NoError(t, db.AutoMigrate(&types.SyncLog{}))
	now := time.Now().UTC()
	require.NoError(t, db.Create(&types.DataSource{
		ID: "health-ds", TenantID: 7, KnowledgeBaseID: "health-kb",
		Name: "health", Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive,
	}).Error)
	require.NoError(t, db.Create(&types.SyncLog{
		ID: "health-sync", DataSourceID: "health-ds", TenantID: 7,
		Status: types.SyncLogStatusSuccess, StartedAt: now.Add(-time.Second),
		FinishedAt: &now, UpdatedAt: now, CreatedAt: now,
	}).Error)
	require.NoError(t, db.Exec("INSERT INTO nextcloud_event_dispatch(state,updated_at) VALUES(?,?)",
		"idle", now).Error)

	// A recent successful file sync is useful diagnostics, but it is not a
	// worker heartbeat and must not open the binding gate on its own.
	withoutHeartbeat, err := store.ConnectionHealth(ctx, nil)
	require.NoError(t, err)
	require.False(t, withoutHeartbeat.BindingAllowed)
	require.Equal(t, "unverified", withoutHeartbeat.WorkerStatus)
	require.NotNil(t, withoutHeartbeat.WorkerLastSuccessAt)
	require.Nil(t, withoutHeartbeat.WorkerHeartbeatAt)

	staleHeartbeat, err := store.ConnectionHealth(ctx, &repo.NextcloudWorkerObservation{
		Status: "running", ObservedAt: now.Add(-3 * time.Minute),
	})
	require.NoError(t, err)
	require.False(t, staleHeartbeat.BindingAllowed)
	require.Equal(t, "stale", staleHeartbeat.WorkerStatus)
	require.NotNil(t, staleHeartbeat.WorkerHeartbeatAt)

	healthy, err := store.ConnectionHealth(ctx, &repo.NextcloudWorkerObservation{
		Status: "running", ObservedAt: now,
	})
	require.NoError(t, err)
	require.True(t, healthy.BindingAllowed)
	require.Equal(t, "healthy", healthy.Status)
	require.Equal(t, "running", healthy.WorkerStatus)
	require.NotNil(t, healthy.WorkerHeartbeatAt)
	require.NotNil(t, healthy.WorkerLastSuccessAt)
	require.Equal(t, 3, healthy.ContractVersion)
}

func TestNextcloudConnectionCreatesAndReusesWorkspaceTemplate(t *testing.T) {
	db, store, _ := onboardingDB(t)
	ctx := context.Background()
	require.NoError(t, db.Where("tenant_id=?", 7).Delete(&types.KnowledgeBase{}).Error)
	require.NoError(t, db.Create(&types.KnowledgeBase{
		ID:       "invalid-template",
		TenantID: 7,
		Name:     "Invalid legacy template",
		Type:     types.KnowledgeBaseTypeDocument,
	}).Error)

	spaces, err := store.ConnectionWorkspaces(ctx, "owner", false)
	require.NoError(t, err)
	require.Len(t, spaces, 1)
	require.True(t, spaces[0].Eligible)
	require.Empty(t, spaces[0].ProcessingTemplateID)
	require.Empty(t, spaces[0].Reason)

	req := types.NextcloudConnectionAuthorizeRequest{
		InstanceID:  "nc-auto-template",
		BaseURL:     "https://nc.example.test",
		DirectoryID: "directory",
		TenantIDs:   []uint64{7},
	}
	first, err := store.AuthorizeConnection(ctx, req, "owner", false)
	require.NoError(t, err)
	require.Len(t, first.Credentials, 1)
	templateID := first.Credentials[0].ProcessingTemplateID
	require.NotEmpty(t, templateID)

	var template types.KnowledgeBase
	require.NoError(t, db.Where("tenant_id=? AND id=? AND deleted_at IS NULL", 7, templateID).Take(&template).Error)
	require.True(t, template.IndexingStrategy.NeedsEmbedding())
	require.Equal(t, "embedding", template.EmbeddingModelID)
	require.NotNil(t, template.StorageBackendID)
	require.Equal(t, "storage", *template.StorageBackendID)

	discovered, err := store.ConnectionWorkspaces(ctx, "owner", false)
	require.NoError(t, err)
	require.Len(t, discovered, 1)
	require.Equal(t, templateID, discovered[0].ProcessingTemplateID)
	second, err := store.AuthorizeConnection(ctx, req, "owner", false)
	require.NoError(t, err)
	require.Equal(t, templateID, second.Credentials[0].ProcessingTemplateID)

	var count int64
	require.NoError(t, db.Model(&types.KnowledgeBase{}).
		Where("tenant_id=? AND name=? AND deleted_at IS NULL", 7, "Nextcloud processing template").
		Count(&count).Error)
	require.Equal(t, int64(1), count)
}

func TestNextcloudConnectionCreatesGraphOnlyTemplateWithoutEmbeddingModel(t *testing.T) {
	db, store, _ := onboardingDB(t)
	ctx := context.Background()
	require.NoError(t, db.Where("tenant_id=?", 7).Delete(&types.KnowledgeBase{}).Error)
	require.NoError(t, db.Where("tenant_id=? AND type=?", 7, types.ModelTypeEmbedding).Delete(&types.Model{}).Error)
	require.NoError(t, db.Create(&types.Model{
		ID:       "chat",
		TenantID: 7,
		Type:     types.ModelTypeKnowledgeQA,
		Status:   types.ModelStatusActive,
	}).Error)

	spaces, err := store.ConnectionWorkspaces(ctx, "owner", false)
	require.NoError(t, err)
	require.Len(t, spaces, 1)
	require.True(t, spaces[0].Eligible)
	require.Empty(t, spaces[0].ProcessingTemplateID)

	result, err := store.AuthorizeConnection(ctx, types.NextcloudConnectionAuthorizeRequest{
		InstanceID:  "nc-graph-only",
		BaseURL:     "https://nc.example.test",
		DirectoryID: "directory",
		TenantIDs:   []uint64{7},
	}, "owner", false)
	require.NoError(t, err)
	require.Len(t, result.Credentials, 1)

	var template types.KnowledgeBase
	require.NoError(t, db.Where(
		"tenant_id=? AND id=? AND deleted_at IS NULL",
		7,
		result.Credentials[0].ProcessingTemplateID,
	).Take(&template).Error)
	require.Empty(t, template.EmbeddingModelID)
	require.Equal(t, "chat", template.SummaryModelID)
	require.True(t, template.IndexingStrategy.GraphEnabled)
	require.False(t, template.IndexingStrategy.GraphRequired)
	require.False(t, template.IndexingStrategy.NeedsEmbedding())
	require.NotNil(t, template.ExtractConfig)
	require.True(t, template.ExtractConfig.Enabled)
}

func TestNextcloudConnectionCreatesTemplatesForMultipleWorkspaces(t *testing.T) {
	db, store, _ := onboardingDB(t)
	ctx := context.Background()
	require.NoError(t, db.Where("tenant_id IN ?", []uint64{7, 8}).Delete(&types.KnowledgeBase{}).Error)
	require.NoError(t, db.Create(&types.Tenant{ID: 8, Name: "onboarding-second"}).Error)
	require.NoError(t, db.Create(&types.TenantMember{
		TenantID: 8,
		UserID:   "owner",
		Role:     types.TenantRoleOwner,
		Status:   types.TenantMemberStatusActive,
	}).Error)
	require.NoError(t, db.Create(&types.Model{
		ID:       "embedding-8",
		TenantID: 8,
		Type:     types.ModelTypeEmbedding,
		Status:   types.ModelStatusActive,
	}).Error)
	require.NoError(t, db.Create(&types.StorageBackend{
		ID:       "storage-8",
		TenantID: 8,
		Name:     "local",
		Provider: "local",
		Status:   types.StorageBackendStatusActive,
	}).Error)

	spaces, err := store.ConnectionWorkspaces(ctx, "owner", false)
	require.NoError(t, err)
	require.Len(t, spaces, 2)
	for _, space := range spaces {
		require.True(t, space.Eligible)
		require.Empty(t, space.ProcessingTemplateID)
	}

	result, err := store.AuthorizeConnection(ctx, types.NextcloudConnectionAuthorizeRequest{
		InstanceID:  "nc-multiple",
		BaseURL:     "https://nc.example.test",
		DirectoryID: "directory",
		TenantIDs:   []uint64{8, 7},
	}, "owner", false)
	require.NoError(t, err)
	require.Len(t, result.Credentials, 2)
	templates := map[uint64]string{}
	for _, credential := range result.Credentials {
		require.NotEmpty(t, credential.Token)
		require.NotEmpty(t, credential.ProcessingTemplateID)
		templates[credential.TenantID] = credential.ProcessingTemplateID
	}
	require.NotEmpty(t, templates[7])
	require.NotEmpty(t, templates[8])
	require.NotEqual(t, templates[7], templates[8])
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
	snap := types.NextcloudGrantSnapshot{
		PermissionVersion: 1,
		PublicationEpoch:  1,
		Complete:          true,
		Subjects: []types.NextcloudGrantSubject{
			{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-reader", SourceRelationID: "direct-share"},
			{Kind: "group", DirectoryID: c.DirectoryID, ObjectGUID: "guid-engineering", SourceRelationID: "team-base"},
			{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-unlogged", SourceRelationID: "not-yet-logged"},
		},
	}
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
	require.NoError(
		t,
		access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-a", types.ResourceActionRead),
	)
	require.ErrorIs(
		t,
		access.Authorize(
			caller,
			7,
			types.GroupResourceTypeKnowledgeBase,
			"unrelated-inherit",
			types.ResourceActionRead,
		),
		service.ErrResourceAccessDenied,
	)
	require.ErrorIs(
		t,
		access.Authorize(caller, 7, types.GroupResourceTypeAgent, "some-agent", types.ResourceActionUse),
		service.ErrResourceAccessDenied,
	)
	snap.PermissionVersion = 2
	snap.Complete = false
	require.ErrorIs(t, store.ApplyGrantSnapshot(ctx, ops[0], snap), repo.ErrNextcloudOnboardingConflict)
	snap.Complete = true
	snap.Subjects = nil
	require.NoError(t, store.ApplyGrantSnapshot(ctx, ops[0], snap))
	require.ErrorIs(
		t,
		access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-a", types.ResourceActionRead),
		service.ErrResourceAccessDenied,
	)
	require.NoError(
		t,
		access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-b", types.ResourceActionRead),
	)
	snap.PermissionVersion = 1
	require.ErrorIs(t, store.ApplyGrantSnapshot(ctx, ops[0], snap), repo.ErrNextcloudOnboardingConflict)
	require.NoError(
		t,
		db.Model(&types.Directory{}).
			Where("id = ?", c.DirectoryID).
			Update("last_sync_error", "ldaps_unreachable").
			Error,
	)
	require.ErrorIs(
		t,
		access.Authorize(caller, 7, types.GroupResourceTypeKnowledgeBase, "root-b", types.ResourceActionRead),
		service.ErrResourceAccessDenied,
	)
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
		require.NoError(
			t,
			db.Exec("ALTER TABLE data_sources ADD COLUMN sync_enabled BOOLEAN NOT NULL DEFAULT TRUE").Error,
		)
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
	require.NoError(
		t,
		db.Exec(
			("CREATE TABLE nextcloud_source_status_epochs(connection_id TEXT PRIMARY KEY" +
				",publication_epoch BIGINT,pair_operation_id TEXT,verified_at BIGINT)"),
		).Error,
	)
	var pair repo.NextcloudSourcePairing
	require.NoError(t, db.Where("operation_id=?", op.OperationID).Take(&pair).Error)
	require.NoError(
		t,
		db.Exec(
			("INSERT INTO nextcloud_event_connections(connection_id,tenant_id,knowledge_"+
				"base_id,datasource_id,status) VALUES(?,?,?,?,?)"),
			"conn",
			uint64(7),
			kb.ID,
			pair.DataSourceID,
			"active",
		).Error,
	)
	require.NoError(
		t,
		db.Exec(
			"INSERT INTO nextcloud_source_status_epochs VALUES(?,?,?,?)",
			"conn",
			int64(1),
			op.OperationID,
			time.Now().Unix(),
		).Error,
	)
	snap := types.NextcloudGrantSnapshot{
		PermissionVersion: 1,
		PublicationEpoch:  1,
		Complete:          true,
		Subjects: []types.NextcloudGrantSubject{
			{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-reader", SourceRelationID: "share"},
		},
	}
	require.NoError(t, store.ApplyGrantSnapshot(ctx, op, snap))
	matches, err := store.ScopedMatches(ctx, "reader", 7, kb.ID, time.Now())
	require.NoError(t, err)
	require.Len(t, matches, 1)
	require.NoError(t, db.Exec("UPDATE nextcloud_source_status_epochs SET publication_epoch=3").Error)
	matches, err = store.ScopedMatches(ctx, "reader", 7, kb.ID, time.Now())
	require.NoError(t, err)
	require.Empty(t, matches)
	require.ErrorIs(
		t,
		store.ApplyGrantSnapshot(ctx, op, snap),
		repo.ErrNextcloudOnboardingConflict,
		"even exact old retries are fenced by the new authority epoch",
	)
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
	peerDSN, err := postgresDSNWithRuntimeParams(
		os.Getenv("NEXTCLOUD_ONBOARDING_TEST_POSTGRES_DSN"),
		map[string]string{"search_path": schema},
	)
	require.NoError(t, err)
	peer, err := gorm.Open(
		postgres.Open(peerDSN),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Error)},
	)
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
	require.Equal(t, 2, successes,
		"v3 admits independent bindings concurrently into the same knowledge base")
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
			out <- peer.Create(&types.Knowledge{
				ID:              uuid.NewString(),
				TenantID:        7,
				KnowledgeBaseID: id,
				Channel:         "manual",
			}).Error
		}()
		close(start)
		successes := 0
		for range 2 {
			if <-out == nil {
				successes++
			}
		}
		require.GreaterOrEqual(t, successes, 1, "both admission and content writer failed")
		require.LessOrEqual(t, successes, 2)
		var operations int64
		require.NoError(
			t,
			db.Model(&types.NextcloudOnboardingOperation{}).
				Where("tenant_id = ? AND knowledge_base_id = ?", c.TenantID, id).
				Count(&operations).
				Error,
		)
		if operations == 1 {
			// A writer that committed before admission is now legal existing
			// content. Once reservation commits, every later writer must be
			// rejected by the shared target guard.
			require.Error(
				t,
				peer.Create(&types.Knowledge{
					ID:              uuid.NewString(),
					TenantID:        7,
					KnowledgeBaseID: id,
					Channel:         "manual",
				}).Error,
				"reserved target admitted a later ordinary-content writer",
			)
		}
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
	snapshot := types.NextcloudGrantSnapshot{
		PermissionVersion: 1,
		PublicationEpoch:  1,
		Complete:          true,
		Subjects: []types.NextcloudGrantSubject{
			{Kind: "user", DirectoryID: c.DirectoryID, ObjectGUID: "guid-reader", SourceRelationID: "direct"},
		},
	}
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
	require.NoError(
		t,
		db.Create(
			&types.Model{
				ID:       "graph-model",
				TenantID: 7,
				Type:     types.ModelTypeKnowledgeQA,
				Status:   types.ModelStatusActive,
			},
		).Error,
	)
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
