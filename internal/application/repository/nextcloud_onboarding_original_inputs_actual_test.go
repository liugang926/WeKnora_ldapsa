//go:build sqlite_fts5

package repository

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/database"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const onboardingActualGUID = "00112233-4455-6677-8899-aabbccddeeff"
const onboardingActualToken = "actual-test-paired-source-token-0123456789"

type onboardingOriginalActualFixture struct {
	bindings       sync.Map
	db             *gorm.DB
	store          *KnowledgeInputProvenanceStore
	onboarding     *NextcloudOnboardingStore
	pairs          *NextcloudSourcePairingRepository
	credential     types.NextcloudOnboardingCredential
	admin          context.Context
	allow, unknown atomic.Bool
	readCalls      atomic.Int64
}

func newOnboardingOriginalActualFixture(t *testing.T) *onboardingOriginalActualFixture {
	t.Helper()
	f := &onboardingOriginalActualFixture{}
	f.allow.Store(true)
	// This controlled signed HTTP origin exercises the real adapter. It is an
	// actual SQL/producer-boundary test, not a claim of live NC or LDAPS acceptance.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "Bearer "+onboardingActualToken, r.Header.Get("Authorization"))
		require.Empty(t, r.URL.RawQuery)
		hash := sha256.Sum256(raw)
		canonical := strings.Join([]string{"weknora-hmac-sha256-v1", r.Method, r.URL.EscapedPath(), "", hex.EncodeToString(hash[:]), r.Header.Get("X-WeKnora-Timestamp"), r.Header.Get("X-WeKnora-Nonce"), r.Header.Get("X-WeKnora-Key-Id")}, "\n")
		key := sha256.Sum256([]byte(onboardingActualToken))
		mac := hmac.New(sha256.New, key[:])
		_, err = mac.Write([]byte(canonical))
		require.NoError(t, err)
		signature, err := hex.DecodeString(r.Header.Get("X-WeKnora-Signature"))
		require.NoError(t, err)
		require.True(t, hmac.Equal(mac.Sum(nil), signature))
		if strings.HasSuffix(r.URL.Path, "/capabilities") {
			fmt.Fprint(w, `{"protocol_version":"1","instance_id":"onboarding-actual-instance"}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/bindings") {
			rows := []map[string]any{}
			f.bindings.Range(func(key, value any) bool {
				rows = append(rows, map[string]any{"id": key, "name": "Controlled root", "root_file_id": 77, "publication_state": "active", "publication_epoch": 1})
				return true
			})
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"bindings": rows}))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/publication-check") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/authorize") {
			f.readCalls.Add(1)
			var request struct {
				DirectoryID     string `json:"directory_id"`
				ObjectGUID      string `json:"object_guid"`
				FileID          int64  `json:"file_id"`
				ContentRevision string `json:"content_revision"`
			}
			require.NoError(t, json.Unmarshal(raw, &request))
			require.Equal(t, "onboarding-actual-ad", request.DirectoryID)
			require.Equal(t, onboardingActualGUID, request.ObjectGUID)
			require.EqualValues(t, 77, request.FileID)
			require.Equal(t, "0", request.ContentRevision)
			if f.unknown.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if !f.allow.Load() {
				fmt.Fprint(w, `{"allow":false,"reason":"read_permission_denied","checked_at":1780000000}`)
				return
			}
			fmt.Fprint(w, `{"allow":true,"reason":"authorized","policy_revision":"actual-1","source_etag":"onboarding-actual-etag","checked_at":1780000000}`)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	temp := t.TempDir()
	dbPath := filepath.Join(temp, "producer.db")
	cwd, err := os.Getwd()
	require.NoError(t, err)
	root, err := filepath.Abs("../../..")
	require.NoError(t, err)
	require.NoError(t, os.Chdir(root))
	migrationErr := database.RunMigrationsWithOptions("sqlite3://"+dbPath, database.MigrationOptions{SQLiteDBPath: dbPath})
	require.NoError(t, os.Chdir(cwd))
	require.NoError(t, migrationErr)
	db, err := gorm.Open(sqlite.Open(dbPath+"?_busy_timeout=5000"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	f.db = db
	journal := filepath.Join(temp, "journal")
	require.NoError(t, os.MkdirAll(journal, 0700))
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(journal, "key"), key, 0600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", journal)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", filepath.Join(journal, "key"))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", filepath.Join(journal, "pin"))
	t.Setenv("SYSTEM_AES_KEY", "0123456789abcdef0123456789abcdef")
	retention := NewOriginalBodyRetentionStore(db)
	require.NoError(t, retention.InitializeJournal())
	require.NoError(t, InstallOriginalBodyRetentionTriggers(db))
	require.NoError(t, InstallOriginalBodyRetentionCallbacks(db))
	require.NoError(t, retention.VerifyJournalReady())
	require.NoError(t, db.Create(&types.Tenant{ID: 7, Name: "Existing operator workspace", Status: "active"}).Error)
	require.NoError(t, db.Create(&types.User{ID: "onboarding-template-operator", Username: "onboarding-template-operator", Email: "onboarding-template-operator@example.test", IsActive: true, TenantID: 7}).Error)
	require.NoError(t, db.Create(&types.TenantMember{TenantID: 7, UserID: "onboarding-template-operator", Role: types.TenantRoleAdmin, Status: types.TenantMemberStatusActive}).Error)
	// Directory/delegation rows are operator control fixtures in this SQL test.
	// The separate first-login runner uses the actual default LDAP adapter.
	now := time.Now().UTC()
	require.NoError(t, db.Create(&types.Directory{ID: "onboarding-actual-ad", Protocol: types.DirectoryProtocolAD, Enabled: true, SnapshotVersion: 1, LastSuccessfulSyncAt: &now}).Error)
	require.NoError(t, db.Create(&types.DirectoryIdentity{ID: "onboarding-actual-unlogged", DirectoryID: "onboarding-actual-ad", ObjectGUID: onboardingActualGUID, Status: types.DirectoryObjectActive, SnapshotVersion: 1}).Error)
	require.NoError(t, db.Create(&types.NextcloudOnboardingDelegation{TenantID: 7, DirectoryID: "onboarding-actual-ad", ObjectGUID: onboardingActualGUID}).Error)
	require.NoError(t, db.Create(&types.StorageBackend{ID: "onboarding-actual-storage", TenantID: 7, Name: "Approved local backend", Provider: "local", Status: types.StorageBackendStatusActive}).Error)
	f.admin = types.WithExecutionTenant(types.WithPrincipal(types.WithCaller(context.Background(), types.Caller{TenantID: 7, UserID: "onboarding-template-operator", Role: types.TenantRoleAdmin}), types.Principal{Type: types.PrincipalWebUser, ID: "onboarding-template-operator"}), 7)
	f.store = NewKnowledgeInputProvenanceStore(db)
	factory := func(tx *gorm.DB) *KnowledgeInputProvenanceStore {
		actual := NewKnowledgeInputProvenanceStore(tx)
		ConfigureKnowledgeInputActorGroupAccess(actual, displayTestGroups{db: tx})
		return actual
	}
	ConfigureKnowledgeDisplayInputWriters(f.store, NewKnowledgeTagRepository(db), NewKnowledgeRepository(db), factory)
	f.pairs = NewNextcloudSourcePairingRepository(db)
	ConfigureDataSourceNameInputWriters(NewDataSourceRepository(db), f.pairs, factory)
	require.NoError(t, db.Create(&types.Model{ID: "onboarding-actual-embedding", TenantID: 7, Type: types.ModelTypeEmbedding, Status: types.ModelStatusActive}).Error)
	storage := "onboarding-actual-storage"
	template := &types.KnowledgeBase{ID: "onboarding-actual-template", TenantID: 7, Name: "Actual operator template", Type: types.KnowledgeBaseTypeDocument, StorageBackendID: &storage, EmbeddingModelID: "onboarding-actual-embedding", ChunkingConfig: types.ChunkingConfig{ChunkSize: 128}, IndexingStrategy: types.IndexingStrategy{KeywordEnabled: true}, EnrichmentTemplates: &types.KnowledgeEnrichmentTemplates{Summary: "Actual independent summary {{content}}", Question: "Actual independent question {{content}}"}}
	require.NoError(t, f.store.CreateKnowledgeBaseWithInput(WithIndependentKnowledgeBaseNameInput(WithIndependentKnowledgeInput(f.admin), template.ID, template.Name), template))
	tokenHash := sha256.Sum256([]byte("actual-scoped-onboarding-credential-0123456789"))
	f.credential = types.NextcloudOnboardingCredential{ID: "onboarding-actual-credential", TokenSHA256: hex.EncodeToString(tokenHash[:]), InstanceID: "onboarding-actual-instance", BaseURL: server.URL, TenantID: 7, DirectoryID: "onboarding-actual-ad", TemplateKBID: template.ID, Enabled: true, MaxKnowledgeBases: 20}
	require.NoError(t, db.Create(&f.credential).Error)
	f.onboarding = NewNextcloudOnboardingStore(db)
	actual, err := f.onboarding.Credential(context.Background(), "actual-scoped-onboarding-credential-0123456789")
	require.NoError(t, err)
	require.Equal(t, f.credential, actual)
	return f
}
func (f *onboardingOriginalActualFixture) request(name string) types.NextcloudOnboardingRequest {
	id := uuid.NewString()
	return types.NextcloudOnboardingRequest{OperationID: id, TenantID: 7, InstanceID: f.credential.InstanceID, BindingID: "actual-" + strings.ReplaceAll(id, "-", ""), ActorDirectoryID: f.credential.DirectoryID, ActorObjectGUID: onboardingActualGUID, Mode: "create", Name: name, ProcessingTemplateID: f.credential.TemplateKBID}
}
func (f *onboardingOriginalActualFixture) pair(t *testing.T, op types.NextcloudOnboardingOperation) (NextcloudSourcePairing, *types.DataSource) {
	t.Helper()
	machine, err := f.onboarding.BeginPairProducer(context.Background(), f.credential, op, f.credential.DirectoryID, onboardingActualGUID)
	require.NoError(t, err)
	require.Zero(t, types.CallerFromContext(machine).TenantID)
	require.Empty(t, types.CallerFromContext(machine).UserID)
	require.Equal(t, types.CallerFromContext(context.Background()), types.CallerFromContext(machine))
	_, hasPrincipal := types.PrincipalFromContext(machine)
	require.False(t, hasPrincipal)
	pair, _, err := f.pairs.PrepareSourcePairing(machine, NextcloudSourcePairing{OperationID: op.OperationID, TenantID: 7, KnowledgeBaseID: op.KnowledgeBaseID, InstanceID: op.InstanceID, BindingID: op.BindingID, BaseURL: f.credential.BaseURL, PublicationEpoch: 1, KeyID: "pair_" + strings.ReplaceAll(op.OperationID, "-", "")}, onboardingActualToken)
	require.NoError(t, err)
	require.NoError(t, f.pairs.ActivateSourcePairing(context.Background(), pair))
	require.NoError(t, f.onboarding.MarkActive(context.Background(), op))
	f.bindings.Store(op.BindingID, true)
	_, err = NewNextcloudEventInboxRepository(f.db).Pair(context.Background(), NextcloudEventPairingIdentity{TenantID: 7, KnowledgeBaseID: op.KnowledgeBaseID, DatasourceID: pair.DataSourceID, NextcloudInstanceID: op.InstanceID, BindingID: op.BindingID, DatasourceBaseURL: pair.BaseURL, DatasourceConfigSHA: pair.ConfigSHA, PublicationEpoch: 1, PublicationState: "active"})
	require.NoError(t, err)
	require.NoError(t, f.onboarding.RefreshGrantEpoch(context.Background(), op))
	require.NoError(t, f.onboarding.ApplyGrantSnapshot(context.Background(), op, types.NextcloudGrantSnapshot{PermissionVersion: 1, PublicationEpoch: 1, Complete: true, Subjects: []types.NextcloudGrantSubject{{Kind: "user", DirectoryID: f.credential.DirectoryID, ObjectGUID: onboardingActualGUID, SourceRelationID: "actual-read-share"}}}))
	current, ds, err := f.pairs.SourcePairing(context.Background(), 7, op.OperationID)
	require.NoError(t, err)
	return current, ds
}
func (f *onboardingOriginalActualFixture) assertNoEscalationOrOwnedLeases(t *testing.T) {
	t.Helper()
	var identity types.DirectoryIdentity
	require.NoError(t, f.db.Where("id='onboarding-actual-unlogged'").Take(&identity).Error)
	require.Nil(t, identity.UserID)
	var users, members, physical, bodies int64
	require.NoError(t, f.db.Model(&types.User{}).Count(&users).Error)
	require.EqualValues(t, 1, users)
	require.NoError(t, f.db.Model(&types.TenantMember{}).Count(&members).Error)
	require.EqualValues(t, 1, members)
	require.NoError(t, f.db.Table("nextcloud_content_leases").Where("released_at_ms IS NULL").Count(&physical).Error)
	require.Zero(t, physical)
	require.NoError(t, bodyInternal(f.db).Table("original_body_leases").Where("released_at_ms IS NULL").Count(&bodies).Error)
	require.Zero(t, bodies)
}

func TestNextcloudOnboardingActualIndependentConfigurationAndScopedNameProducer(t *testing.T) {
	f := newOnboardingOriginalActualFixture(t)
	req := f.request("Actual new NC manager name")
	op, err := f.onboarding.Reserve(context.Background(), f.credential, req)
	require.NoError(t, err)
	replay, err := f.onboarding.Reserve(context.Background(), f.credential, req)
	require.NoError(t, err)
	require.Equal(t, op.OperationID, replay.OperationID)
	var target types.KnowledgeBase
	require.NoError(t, f.db.Where("id=?", op.KnowledgeBaseID).Take(&target).Error)
	require.Equal(t, req.Name, target.Name)
	require.NotNil(t, target.EnrichmentTemplates)
	require.Contains(t, target.EnrichmentTemplates.Summary, "Actual independent summary")
	origin, lineage, err := onboardingConfigurationMetadata(f.db, 7, target.ID)
	require.NoError(t, err)
	require.Equal(t, "derived", origin.OriginKind)
	require.Empty(t, lineage.Sources)
	var manifests, knowledgeCount int64
	var virgin bool
	require.NoError(t, f.db.Table("knowledge_configuration_original_body_manifests").Where("origin_id=?", origin.OriginID).Count(&manifests).Error)
	require.EqualValues(t, 1, manifests)
	require.NoError(t, f.db.Table("nextcloud_kb_blank_witnesses").Select("virgin").Where("tenant_id=7 AND knowledge_base_id=?", target.ID).Scan(&virgin).Error)
	require.True(t, virgin)
	require.NoError(t, f.db.Model(&types.Knowledge{}).Where("knowledge_base_id=?", target.ID).Count(&knowledgeCount).Error)
	require.Zero(t, knowledgeCount)
	pair, ds := f.pair(t, op)
	proof, err := f.store.ReadDataSourceNameInput(context.Background(), 7, target.ID, ds.ID)
	require.NoError(t, err)
	require.NotEmpty(t, proof.InputDigest)
	var name datasourceNameInput
	require.NoError(t, f.db.Where("datasource_id=?", ds.ID).Take(&name).Error)
	var actual datasourceNameEdit
	require.NoError(t, json.Unmarshal([]byte(name.ActualInputs), &actual))
	require.NotNil(t, actual.Producer)
	require.Equal(t, op.RequestSHA256, actual.Producer.RequestSHA256)
	require.Equal(t, onboardingActualGUID, actual.Producer.ActorObjectGUID)
	require.Equal(t, pair.OperationID, actual.Producer.OperationID)
	// A blank legacy name is not configuration input. Full replacement must not
	// read it or create a display origin for retained, unproved old text.
	legacy := &types.KnowledgeBase{ID: "onboarding-actual-legacy-name", TenantID: 7, Type: types.KnowledgeBaseTypeDocument, Name: "Unproved retained legacy name"}
	require.NoError(t, f.db.Create(legacy).Error)
	existing := f.request("Ignored requested name")
	existing.Mode = "existing"
	existing.KnowledgeBaseID = legacy.ID
	result, err := f.onboarding.Reserve(context.Background(), f.credential, existing)
	require.NoError(t, err)
	require.Equal(t, legacy.ID, result.KnowledgeBaseID)
	var nameCount int64
	require.NoError(t, f.db.Table("knowledge_display_input_origins").Where("kind='kb-name' AND row_id=?", legacy.ID).Count(&nameCount).Error)
	require.Zero(t, nameCount)
	var storedName string
	require.NoError(t, f.db.Table("knowledge_bases").Select("name").Where("id=?", legacy.ID).Scan(&storedName).Error)
	require.Equal(t, legacy.Name, storedName)
	// A configured real factory is still mandatory; the machine marker cannot
	// use a nil factory as authority, and its transaction rolls back the source.
	missingReq := f.request("Missing factory target")
	missing, err := f.onboarding.Reserve(context.Background(), f.credential, missingReq)
	require.NoError(t, err)
	machine, err := f.onboarding.BeginPairProducer(context.Background(), f.credential, missing, f.credential.DirectoryID, onboardingActualGUID)
	require.NoError(t, err)
	noFactory := NewNextcloudSourcePairingRepository(f.db)
	_, _, err = noFactory.PrepareSourcePairing(machine, NextcloudSourcePairing{OperationID: missing.OperationID, TenantID: 7, KnowledgeBaseID: missing.KnowledgeBaseID, InstanceID: missing.InstanceID, BindingID: missing.BindingID, BaseURL: f.credential.BaseURL, PublicationEpoch: 1, KeyID: "pair_" + strings.ReplaceAll(missing.OperationID, "-", "")}, onboardingActualToken)
	require.ErrorIs(t, err, ErrNextcloudOnboardingDenied)
	var count int64
	require.NoError(t, f.db.Model(&types.DataSource{}).Where("knowledge_base_id=?", missing.KnowledgeBaseID).Count(&count).Error)
	require.Zero(t, count)
	changed := f.credential
	changed.MaxKnowledgeBases++
	_, err = f.onboarding.BeginPairProducer(context.Background(), changed, op, changed.DirectoryID, onboardingActualGUID)
	require.ErrorIs(t, err, ErrNextcloudOnboardingDenied)
	_, err = f.onboarding.BeginScopedOnboardingConfigurationRead(context.Background(), 7, target.ID, lineage)
	require.ErrorIs(t, err, types.ErrSourceLineageUnknown)
	f.assertNoEscalationOrOwnedLeases(t)
}

func (f *onboardingOriginalActualFixture) produceSourceBoundTemplate(t *testing.T) *types.SourceLineage {
	t.Helper()
	req := f.request("Actual original source")
	sourceOp, err := f.onboarding.Reserve(context.Background(), f.credential, req)
	require.NoError(t, err)
	pair, _ := f.pair(t, sourceOp)
	repository := NewKnowledgeRepository(f.db).(*knowledgeRepository)
	repository.nextcloudPublicationCheck = nextcloud.CheckCurrentPublication
	external := "nextcloud:" + sourceOp.InstanceID + ":77"
	metadata, err := json.Marshal(map[string]string{"datasource_id": pair.DataSourceID, "external_id": external, "nextcloud_instance_id": sourceOp.InstanceID, "nextcloud_binding_id": sourceOp.BindingID, "nextcloud_file_id": "77", "nextcloud_etag": "", "source_resource_id": sourceOp.BindingID, "nextcloud_path": "original.md"})
	require.NoError(t, err)
	k := &types.Knowledge{ID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: sourceOp.KnowledgeBaseID, Type: "file", Channel: types.ConnectorTypeNextcloud, Title: "Actual original file", FileName: "original.md", Metadata: types.JSON(metadata), ParseStatus: types.ParseStatusFinalizing, EnableStatus: "disabled"}
	require.NoError(t, repository.CreateKnowledge(context.Background(), k))
	require.NoError(t, repository.StageNextcloudVersion(context.Background(), 7, k.KnowledgeBaseID, pair.DataSourceID, external, "onboarding-actual-etag", k.ID))
	leases := NewNextcloudContentLeaseStore(f.db)
	scope := NextcloudContentScope{TenantID: 7, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, DataSourceID: pair.DataSourceID, ExternalID: external}
	build, err := leases.AcquireKnowledge(context.Background(), scope, NextcloudContentBuildLease, "actual-source-builder", time.Minute)
	require.NoError(t, err)
	ctx := WithNextcloudBuildLease(f.admin, leases, build, scope)
	// The actual nextcloud build producer mints the immutable original proof
	// from its committed exact lease and actual artifact, never a test-made proof.
	artifact := []byte("Actual original source artifact for configuration")
	proof, err := f.store.AdmitNextcloudBuildInputs(ctx, artifact)
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, f.store, proof)
	// Select the fixture's local SQL backend through the real lease-bound writer;
	// this test covers original-input provenance, not vector indexing capacity.
	require.NoError(t, RecordNextcloudDerivedBackendSelection(ctx, []types.RetrieverEngineType{types.SQLiteRetrieverEngineType}))
	chunk := &types.Chunk{ID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: k.KnowledgeBaseID, KnowledgeID: k.ID, Content: string(artifact), ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready"}
	require.NoError(t, NewChunkRepository(f.db).CreateChunks(ctx, []*types.Chunk{chunk}))
	require.NoError(t, f.db.Where("id=?", k.ID).Take(k).Error)
	k.ParseStatus = types.ParseStatusCompleted
	k.EnableStatus = "enabled"
	require.NoError(t, repository.UpdateKnowledge(ctx, k))
	published, err := repository.PublishNextcloudVersion(ctx, k.ID)
	require.NoError(t, err)
	require.True(t, published)
	require.NoError(t, leases.ReleaseLease(context.Background(), build.ID))
	source := proof.SourceLineage
	require.Len(t, source.Sources, 1)
	read, err := f.onboarding.beginOnboardingTemplateRead(context.Background(), f.credential, f.request("Original independent template read"))
	require.NoError(t, err)
	require.NoError(t, read.Close())
	// The direct configuration producer consumes the original influencing input
	// envelope from the real build, retaining it in the actual config receipt.
	var template types.KnowledgeBase
	require.NoError(t, f.db.Where("id=?", f.credential.TemplateKBID).Take(&template).Error)
	template.EnrichmentTemplates = &types.KnowledgeEnrichmentTemplates{Summary: "Actual derived template: " + string(artifact), Question: "Actual derived questions: " + string(artifact)}
	derived, err := WithDerivedKnowledgeInput(f.admin, source)
	require.NoError(t, err)
	require.NoError(t, f.store.ReplaceKnowledgeBaseConfigurationWithInput(derived, &template))
	return source
}
func TestNextcloudOnboardingActualSourceBoundConfigurationCurrentReadAndRevocation(t *testing.T) {
	f := newOnboardingOriginalActualFixture(t)
	source := f.produceSourceBoundTemplate(t)
	req := f.request("Actual source-bound clone")
	op, err := f.onboarding.Reserve(context.Background(), f.credential, req)
	require.NoError(t, err)
	_, cloned, err := onboardingConfigurationMetadata(f.db, 7, op.KnowledgeBaseID)
	require.NoError(t, err)
	require.Equal(t, source, cloned)
	require.Positive(t, f.readCalls.Load())
	var target types.KnowledgeBase
	require.NoError(t, f.db.Where("id=?", op.KnowledgeBaseID).Take(&target).Error)
	require.Contains(t, target.EnrichmentTemplates.Summary, "Actual original source artifact")
	// A retained read is re-authorized after ALLOW. Clone-time ALLOW cannot be
	// reused as an authorization for a later model/consumer checkpoint.
	retained, err := f.onboarding.beginOnboardingTemplateRead(context.Background(), f.credential, f.request("Retained test input"))
	require.NoError(t, err)
	own := retained.Context(context.Background())
	require.Zero(t, types.CallerFromContext(own).TenantID)
	require.Empty(t, types.CallerFromContext(own).UserID)
	var deniedBodyReads atomic.Int64
	require.NoError(t, f.db.Callback().Query().Before("gorm:query").Register("onboarding-denied-original-body-read", func(tx *gorm.DB) {
		if tx.Statement.Table != "knowledge_bases" && tx.Statement.Table != "knowledges" {
			return
		}
		body := false
		if len(tx.Statement.Selects) == 0 {
			switch tx.Statement.Dest.(type) {
			case *types.KnowledgeBase, *types.Knowledge:
				body = true
			}
		}
		for _, field := range tx.Statement.Selects {
			if field == "*" || field == "enrichment_templates" || field == "chunking_config" || field == "metadata" || field == "title" {
				body = true
			}
		}
		if body {
			deniedBodyReads.Add(1)
		}
	}))
	defer f.db.Callback().Query().Remove("onboarding-denied-original-body-read")
	f.allow.Store(false)
	require.ErrorIs(t, retained.Checkpoint(own), access.ErrNextcloudPublicationDenied)
	require.NoError(t, retained.Close())
	require.Error(t, own.Err())
	denied := f.request("Must never be cloned after revoke")
	_, err = f.onboarding.Reserve(context.Background(), f.credential, denied)
	require.ErrorIs(t, err, ErrNextcloudOnboardingDenied)
	var count int64
	require.NoError(t, f.db.Model(&types.NextcloudOnboardingOperation{}).Where("operation_id=?", denied.OperationID).Count(&count).Error)
	require.Zero(t, count)
	f.allow.Store(true)
	f.unknown.Store(true)
	unavailable := f.request("Unknown source must not clone")
	_, err = f.onboarding.Reserve(context.Background(), f.credential, unavailable)
	require.ErrorIs(t, err, access.ErrNextcloudPublicationUnavailable)
	require.NoError(t, f.db.Model(&types.NextcloudOnboardingOperation{}).Where("operation_id=?", unavailable.OperationID).Count(&count).Error)
	require.Zero(t, count)
	require.Zero(t, deniedBodyReads.Load(), "denied/unavailable current authority must precede every original config/source-body SELECT")
	f.unknown.Store(false)
	// Current WK scoped entitlement is independently necessary, even while the
	// real NC adapter returns ALLOW for the same directory GUID/file.
	sourceOp := source.Sources[0].PairOperationID
	var actual types.NextcloudOnboardingOperation
	require.NoError(t, f.db.Where("operation_id=?", sourceOp).Take(&actual).Error)
	require.NoError(t, f.onboarding.ApplyGrantSnapshot(context.Background(), actual, types.NextcloudGrantSnapshot{PermissionVersion: 2, PublicationEpoch: 1, Complete: true}))
	_, err = f.onboarding.Reserve(context.Background(), f.credential, f.request("NC allow without WK entitlement"))
	require.ErrorIs(t, err, ErrNextcloudOnboardingDenied)
	require.Zero(t, deniedBodyReads.Load())
	f.assertNoEscalationOrOwnedLeases(t)
}

func TestNextcloudOnboardingActualRevocationDuringCloneRollsBack(t *testing.T) {
	f := newOnboardingOriginalActualFixture(t)
	f.produceSourceBoundTemplate(t)
	req := f.request("Revoke during actual clone write")
	tables := []string{"knowledge_bases", "nextcloud_onboarding_operations", "knowledge_config_input_origins", "knowledge_configuration_original_body_manifests", "original_body_payloads", "knowledge_display_input_origins"}
	before := map[string]int64{}
	for _, table := range tables {
		var n int64
		require.NoError(t, bodyInternal(f.db).Table(table).Count(&n).Error)
		before[table] = n
	}
	var reached atomic.Bool
	require.NoError(t, f.db.Callback().Create().Before("gorm:create").Register("revoke-during-onboarding-clone", func(tx *gorm.DB) {
		if kb, ok := tx.Statement.Dest.(*types.KnowledgeBase); ok && kb.Name == req.Name {
			reached.Store(true)
			f.allow.Store(false)
		}
	}))
	defer f.db.Callback().Create().Remove("revoke-during-onboarding-clone")
	_, err := f.onboarding.Reserve(context.Background(), f.credential, req)
	require.True(t, reached.Load(), "the source must be revoked after the real clone writer begins")
	require.ErrorIs(t, err, access.ErrNextcloudPublicationDenied)
	for _, table := range tables {
		var n int64
		require.NoError(t, bodyInternal(f.db).Table(table).Count(&n).Error)
		require.Equal(t, before[table], n, table+" must roll back with the denied clone")
	}
	f.assertNoEscalationOrOwnedLeases(t)
}
