package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Tencent/WeKnora/internal/application/repository"
	storagefile "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"mime/multipart"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type capacityFailureCatalog struct {
	interfaces.ResourceCatalog
	fail bool
}

func (c *capacityFailureCatalog) Register(ctx context.Context, tenant uint64, path string, meta interfaces.ResourceRegistration) (string, error) {
	if c.fail {
		return "", errors.New("actual catalog registration unavailable")
	}
	return c.ResourceCatalog.Register(ctx, tenant, path, meta)
}

type capacityIntentKnowledge struct {
	interfaces.KnowledgeService
	repo      interfaces.KnowledgeRepository
	db        *gorm.DB
	fs        interfaces.FileService
	mode      string
	candidate *types.Knowledge
}

func (k *capacityIntentKnowledge) GetRepository() interfaces.KnowledgeRepository { return k.repo }
func (k *capacityIntentKnowledge) CreateKnowledgeFromFile(ctx context.Context, kb string, h *multipart.FileHeader, metadata map[string]string, _ *bool, _ string, _ []string, _ string, _ *types.KnowledgeProcessOverrides) (*types.Knowledge, error) {
	if k.mode == "duplicate" {
		return k.candidate, types.NewDuplicateFileError(k.candidate)
	}
	bytes, _ := json.Marshal(metadata)
	row := &types.Knowledge{ID: uuid.NewString(), TenantID: 7, KnowledgeBaseID: kb, Channel: types.ConnectorTypeNextcloud, Type: "file", Metadata: bytes, ParseStatus: types.ParseStatusPending}
	if e := k.db.Create(row).Error; e != nil {
		return nil, e
	}
	k.candidate = row
	if k.mode == "partial" {
		short, e := bytesToFileHeader([]byte("part"), h.Filename)
		if e != nil {
			return nil, e
		}
		short.Size = h.Size
		h = short
	}
	path, e := k.fs.SaveFile(types.WithResourceProvenance(ctx, types.ResourceProvenanceNextcloud), h, 7, row.ID)
	if e != nil {
		return nil, e
	}
	row.FilePath = path
	e = k.db.Model(row).Update("file_path", path).Error
	return row, e
}

type capacityIntentFixture struct {
	db                *gorm.DB
	store             *repository.NextcloudCapacityStore
	admission         *NextcloudCapacityAdmission
	kb                *types.KnowledgeBase
	fs                interfaces.FileService
	base, dsn, driver string
	catalog           *capacityFailureCatalog
	item              *types.FetchedItem
}

func newCapacityIntentFixture(t *testing.T, pg bool) *capacityIntentFixture {
	t.Helper()
	f := &capacityIntentFixture{base: t.TempDir(), driver: "sqlite"}
	if pg {
		dsn := os.Getenv("WEKNORA_CAPACITY_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("owned stock PG capacity fixture required")
		}
		admin, e := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(t, e)
		schema := "capacity_intent_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
		require.NoError(t, admin.Exec(`CREATE SCHEMA "`+schema+`"`).Error)
		f.dsn = dsn + " search_path=" + schema
		f.driver = "postgres"
		t.Cleanup(func() {
			_ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`).Error
			sql, _ := admin.DB()
			_ = sql.Close()
		})
		f.db, e = gorm.Open(postgres.Open(f.dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(t, e)
	} else {
		f.dsn = filepath.Join(t.TempDir(), "capacity.sqlite") + "?_busy_timeout=5000&_foreign_keys=on"
		var e error
		f.db, e = gorm.Open(sqlite.Open(f.dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		require.NoError(t, e)
	}
	sql, e := f.db.DB()
	require.NoError(t, e)
	sql.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = sql.Close() })
	require.NoError(t, f.db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.StoredResource{}, &types.ResourceBinding{}, &types.ResourceAccessGrant{}, &types.Tenant{}, &types.StorageBackend{}))
	migrations := []string{"sqlite/000065_nextcloud_capacity_admission", "sqlite/000067_nextcloud_capacity_write_intents"}
	if pg {
		migrations = []string{"versioned/000146_nextcloud_capacity_admission", "versioned/000148_nextcloud_capacity_write_intents"}
	}
	for _, name := range migrations {
		b, e := os.ReadFile("../../../migrations/" + name + ".up.sql")
		require.NoError(t, e)
		require.NoError(t, f.db.Exec(string(b)).Error)
	}
	backend := "backend"
	f.kb = &types.KnowledgeBase{ID: "kb", TenantID: 7, StorageBackendID: &backend}
	require.NoError(t, f.db.Create(f.kb).Error)
	require.NoError(t, f.db.Create(&types.Tenant{ID: 7, Name: "owned capacity fixture", DefaultStorageBackendID: &backend}).Error)
	require.NoError(t, f.db.Create(&types.StorageBackend{ID: backend, TenantID: 7, Name: "owned local backend", Provider: "local", Status: types.StorageBackendStatusActive}).Error)
	f.store = repository.NewNextcloudCapacityStore(f.db)
	f.catalog = &capacityFailureCatalog{ResourceCatalog: NewResourceCatalog(repository.NewResourceRepository(f.db))}
	f.fs = storagefile.NewResourceCatalogFileService(storagefile.NewBackendScopedFileService(backend, storagefile.NewLocalFileService(f.base, "")), f.catalog)
	f.admission = NewNextcloudCapacityAdmission(f.store, func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) { return f.fs, nil })
	return f
}
func (f *capacityIntentFixture) ingest(t *testing.T, mode string, stageErr, admitErr error) (*capacityIntentKnowledge, error) {
	t.Helper()
	version := &deletionLookupKnowledgeRepo{stageErr: stageErr, admitErr: admitErr}
	knowledge := &capacityIntentKnowledge{repo: version, db: f.db, fs: f.fs, mode: mode}
	source := &DataSourceService{knowledgeService: knowledge, kbService: &processSyncKBService{kb: f.kb}, capacityAdmission: f.admission}
	if f.item == nil {
		f.item = capacityNextcloudFetchedItem(t, "actual retained original source bytes", true)
	}
	_, e := source.ingestNextcloudItem(context.Background(), &types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb"}, f.item, capacityNextcloudMetadata(f.item), nil)
	return knowledge, e
}
func capacityIntentIngestionContract(t *testing.T, pg bool) {
	for _, mode := range []string{"before-io", "partial", "register-fail", "stage-fail", "success"} {
		t.Run(mode, func(t *testing.T) {
			f := newCapacityIntentFixture(t, pg)
			ctx := context.Background()
			var stageErr, admitErr error
			if mode == "before-io" {
				admitErr = errors.New("current candidate retry denied before IO")
			}
			if mode == "stage-fail" {
				stageErr = errors.New("stage crash boundary")
			}
			f.catalog.fail = mode == "register-fail"
			k, e := f.ingest(t, mode, stageErr, admitErr)
			if mode == "success" {
				require.NoError(t, e)
			} else {
				require.Error(t, e)
			}
			var reservations []repository.NextcloudCapacityReservation
			require.NoError(t, f.db.Find(&reservations).Error)
			require.Len(t, reservations, 1)
			r := reservations[0]
			var intents []repository.NextcloudCapacityWriteIntent
			require.NoError(t, f.db.Find(&intents).Error)
			if mode == "before-io" {
				require.Equal(t, "cancelled", r.State)
				require.Empty(t, intents)
			} else {
				require.Equal(t, "committed", r.State)
				require.Len(t, intents, 1)
				i := intents[0]
				require.Equal(t, "charged", i.State)
				id, path, ok := types.ParseStorageBackendPath(i.PhysicalPath)
				require.True(t, ok)
				require.Equal(t, "backend", id)
				raw := filepath.Join(f.base, strings.TrimPrefix(path, "local://"))
				actual, e := os.ReadFile(raw)
				require.NoError(t, e)
				expected := []byte("actual retained original source bytes")
				if mode == "partial" {
					expected = []byte("part")
				}
				require.Equal(t, expected, actual)
				require.Equal(t, int64(len(actual)), i.ObservedBytes)
				if mode == "partial" || mode == "register-fail" {
					require.Equal(t, i.ObservedBytes, i.OrphanChargeBytes)
				} else {
					require.Zero(t, i.OrphanChargeBytes)
				}
				require.Error(t, f.store.ValidateWriter(ctx, &r, i.WriterID, i.PhysicalPath, i.KnowledgeID), "late writer/catalog after charge must fail")
				require.NoError(t, f.admission.Reconcile(ctx, f.kb, &r), "an identical actual observation is idempotent")
				wrong := r
				wrong.BackendID = "foreign"
				require.Error(t, f.admission.Reconcile(ctx, f.kb, &wrong))
				wrong = r
				wrong.KnowledgeBaseID = "foreign"
				require.Error(t, f.admission.Reconcile(ctx, f.kb, &wrong))
			}
			m, e := f.fs.(interfaces.StorageCapacityProvider).MeasureStorageCapacity(ctx)
			require.NoError(t, e)
			status, e := f.store.Status(ctx, 7, "kb", "backend", m)
			require.NoError(t, e)
			if mode == "partial" {
				require.Equal(t, int64(4), status.OrphanChargeBytes)
				require.Equal(t, int64(4), status.LogicalRetainedBytes)
			}
			// A failed partial upload is charged for its actual retained four bytes,
			// permitting a real retry under a quota that would reject the old full hold.
			if mode == "partial" || mode == "before-io" || mode == "register-fail" {
				f.catalog.fail = false
				quota := int64(len("actual retained original source bytes")) + status.OrphanChargeBytes
				require.NoError(t, f.store.SetPolicy(ctx, repository.NextcloudCapacityPolicy{TenantID: 7, KnowledgeBaseID: "kb", RetentionQuotaBytes: quota, WarningPercent: 80, PausePercent: 90}))
				_, retryErr := f.ingest(t, "success", nil, nil)
				require.NoError(t, retryErr)
				var pending int64
				require.NoError(t, f.db.Model(&repository.NextcloudCapacityReservation{}).Where("state='pending'").Count(&pending).Error)
				require.Zero(t, pending)
				finalStatus, statusErr := f.store.Status(ctx, 7, "kb", "backend", mustCapacityMeasure(t, f.fs))
				require.NoError(t, statusErr)
				require.Equal(t, quota, finalStatus.LogicalRetainedBytes)
				require.Equal(t, "paused", finalStatus.State)
			}
			if mode == "stage-fail" {
				// Retry uses the retained exact same node; no second physical write or
				// reservation charge is fabricated for a duplicate catalog object.
				version := &deletionLookupKnowledgeRepo{}
				k.mode = "duplicate"
				k.repo = version
				source := &DataSourceService{knowledgeService: k, kbService: &processSyncKBService{kb: f.kb}, capacityAdmission: f.admission}
				_, e = source.ingestNextcloudItem(ctx, &types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb"}, f.item, capacityNextcloudMetadata(f.item), nil)
				require.NoError(t, e)
				var count int64
				require.NoError(t, f.db.Model(&repository.NextcloudCapacityWriteIntent{}).Count(&count).Error)
				require.Equal(t, int64(1), count)
			}
		})
	}
}
func TestCapacityIntentActualSQLiteIngestion(t *testing.T) { capacityIntentIngestionContract(t, false) }
func TestPostgresCapacityIntentActualIngestion(t *testing.T) {
	capacityIntentIngestionContract(t, true)
}

func TestCapacityIntentWriterFenceAndUnknown(t *testing.T) {
	f := newCapacityIntentFixture(t, false)
	ctx := context.Background()
	r, e := f.admission.Begin(ctx, f.kb, "ds", "node", "etag", 6)
	require.NoError(t, e)
	writeCtx := f.admission.WriteContext(ctx, r)
	header, e := bytesToFileHeader([]byte("actual"), "file.txt")
	require.NoError(t, e)
	_, e = f.fs.SaveFile(writeCtx, header, 7, "candidate")
	require.NoError(t, e)
	require.Error(t, f.admission.Reconcile(ctx, f.kb, r), "actual current writer OS fence must prevent reconciliation even if expired")
	require.NoError(t, f.db.Model(&repository.NextcloudCapacityReservation{}).Where("token=?", r.Token).Update("lease_until_ms", 1).Error)
	require.Error(t, f.admission.Reconcile(ctx, f.kb, r))
	i, e := f.store.Intent(ctx, r)
	require.NoError(t, e)
	require.Error(t, types.CapacityWriteSessionFromContext(writeCtx).ValidateCatalog(ctx, i.PhysicalPath, "candidate"))
	f.admission.End(writeCtx, r, f.kb)
	require.NoError(t, f.db.Where("token=?", r.Token).Take(r).Error)
	require.Equal(t, "committed", r.State)
	_, e = f.fs.SaveFile(writeCtx, header, 7, "late-candidate")
	require.Error(t, e)
	legacy, e := f.store.Reserve(ctx, 7, "kb", "backend", "legacy", 1, mustCapacityMeasure(t, f.fs))
	require.NoError(t, e)
	require.Error(t, f.store.CancelUnstarted(ctx, legacy))
	require.Error(t, f.admission.Reconcile(ctx, f.kb, legacy))
}
func mustCapacityMeasure(t *testing.T, fs interfaces.FileService) types.StorageCapacity {
	m, e := fs.(interfaces.StorageCapacityProvider).MeasureStorageCapacity(context.Background())
	require.NoError(t, e)
	return m
}

func TestCapacityIntentAbsentLinkedRootSharedAndScope(t *testing.T) {
	f := newCapacityIntentFixture(t, false)
	ctx := context.Background()
	r, e := f.admission.Begin(ctx, f.kb, "ds", "node", "etag", 6)
	require.NoError(t, e)
	writeCtx := f.admission.WriteContext(ctx, r)
	require.NoError(t, os.MkdirAll(filepath.Join(f.base, "7"), 0700))
	outside := t.TempDir()
	require.NoError(t, os.Symlink(outside, filepath.Join(f.base, "7", "candidate")))
	header, e := bytesToFileHeader([]byte("actual"), "file.txt")
	require.NoError(t, e)
	_, e = f.fs.SaveFile(writeCtx, header, 7, "candidate")
	require.Error(t, e)
	f.admission.End(writeCtx, r, f.kb)
	require.NoError(t, f.db.Where("token=?", r.Token).Take(r).Error)
	require.Equal(t, "pending", r.State, "linked directory cannot prove absence")
	require.NoError(t, os.Remove(filepath.Join(f.base, "7", "candidate")))
	require.NoError(t, f.admission.Reconcile(ctx, f.kb, r))
	require.NoError(t, f.db.Where("token=?", r.Token).Take(r).Error)
	require.Equal(t, "released", r.State, "exclusive fence + actual absence + no catalog owners proves release")
	// A different configured root on the very same filesystem is not the object.
	f2 := newCapacityIntentFixture(t, false)
	r2, e := f2.admission.Begin(ctx, f2.kb, "ds", "node", "etag", 6)
	require.NoError(t, e)
	w2 := f2.admission.WriteContext(ctx, r2)
	path, e := f2.fs.SaveFile(w2, header, 7, "candidate")
	require.NoError(t, e)
	s := types.CapacityWriteSessionFromContext(w2).(*capacityWriteSession)
	s.mutex.Lock()
	require.NoError(t, s.lock.Close())
	s.lock = nil
	s.closed = true
	s.mutex.Unlock()
	changed := NewNextcloudCapacityAdmission(f2.store, func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) {
		return storagefile.NewBackendScopedFileService("backend", storagefile.NewLocalFileService(t.TempDir(), "")), nil
	})
	require.Error(t, changed.Reconcile(ctx, f2.kb, r2))
	require.NoError(t, f2.db.Where("token=?", r2.Token).Take(r2).Error)
	require.Equal(t, "pending", r2.State)
	// Other KB/message ownership is kept; this reconciliation never deletes.
	var actual types.StoredResource
	require.NoError(t, f2.db.Where("physical_path LIKE ?", "%capacity-"+r2.Token+"%").Take(&actual).Error)
	backend := "backend"
	require.NoError(t, f2.db.Create(&types.KnowledgeBase{ID: "other-kb", TenantID: 7, StorageBackendID: &backend}).Error)
	other := types.Knowledge{ID: "other-owner", TenantID: 7, KnowledgeBaseID: "other-kb", FilePath: path}
	require.NoError(t, f2.db.Create(&other).Error)
	require.NoError(t, f2.db.Create(&types.ResourceBinding{ResourceID: actual.ID, TenantID: 7, OwnerType: "knowledge", OwnerID: other.ID, Relation: "source_file"}).Error)
	require.NoError(t, f2.admission.Reconcile(ctx, f2.kb, r2))
	require.NoError(t, f2.db.Where("id=?", actual.ID).Take(&actual).Error)
	require.Equal(t, "active", actual.State)
	reader, e := f2.fs.GetFile(ctx, path)
	require.NoError(t, e)
	require.NoError(t, reader.Close())
	var count int64
	require.NoError(t, f2.db.Model(&types.ResourceBinding{}).Where("resource_id=?", actual.ID).Count(&count).Error)
	require.Equal(t, int64(2), count)
	wrong := *r2
	wrong.KnowledgeBaseID = "other-kb"
	require.Error(t, f2.store.CancelUnstarted(ctx, &wrong))
	missing, e := f2.admission.Begin(ctx, f2.kb, "ds", "missing", "etag", 1)
	require.NoError(t, e)
	writeMissing := f2.admission.WriteContext(ctx, missing)
	_, e = f2.fs.SaveFile(writeMissing, header, 7, "../escaped")
	require.Error(t, e)
	f2.admission.End(writeMissing, missing, f2.kb)
	require.NoError(t, f2.db.Where("token=?", missing.Token).Take(missing).Error)
	require.Equal(t, "cancelled", missing.State)
	unsupported := NewNextcloudCapacityAdmission(f2.store, func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) {
		return capacityMeasuredFile{FileService: &createKnowledgeFileServiceStub{}, measurement: mustCapacityMeasure(t, f2.fs)}, nil
	})
	_, e = unsupported.Begin(ctx, f2.kb, "ds", "unknown", "etag", 1)
	require.Error(t, e)
}

func TestCapacityIntentCannotAdoptExistingLegacyObject(t *testing.T) {
	f := newCapacityIntentFixture(t, false)
	ctx := context.Background()
	r, e := f.admission.Begin(ctx, f.kb, "ds", "node", "etag", 6)
	require.NoError(t, e)
	dir := filepath.Join(f.base, "7", "candidate")
	require.NoError(t, os.MkdirAll(dir, 0700))
	physical := filepath.Join(dir, "capacity-"+r.Token+".txt")
	require.NoError(t, os.WriteFile(physical, []byte("legacy"), 0600))
	writeCtx := f.admission.WriteContext(ctx, r)
	h, e := bytesToFileHeader([]byte("actual"), "file.txt")
	require.NoError(t, e)
	_, e = f.fs.SaveFile(writeCtx, h, 7, "candidate")
	require.Error(t, e)
	f.admission.End(writeCtx, r, f.kb)
	require.NoError(t, f.db.Where("token=?", r.Token).Take(r).Error)
	require.Equal(t, "pending", r.State)
	i, e := f.store.Intent(ctx, r)
	require.NoError(t, e)
	require.Empty(t, i.ObjectIdentity)
	require.Error(t, f.admission.Reconcile(ctx, f.kb, r))
	actual, e := os.ReadFile(physical)
	require.NoError(t, e)
	require.Equal(t, []byte("legacy"), actual)
}

func capacityConcurrentWriterContract(t *testing.T, pg bool) {
	f := newCapacityIntentFixture(t, pg)
	ctx := context.Background()
	r, e := f.admission.Begin(ctx, f.kb, "ds", "same-node", "etag", 6)
	require.NoError(t, e)
	h, e := bytesToFileHeader([]byte("actual"), "source.txt")
	require.NoError(t, e)
	type outcome struct {
		ctx  context.Context
		path string
		err  error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for n := 0; n < 2; n++ {
		writer := f.admission.WriteContext(ctx, r)
		go func() {
			<-start
			path, err := f.fs.SaveFile(writer, h, 7, "same-knowledge")
			results <- outcome{writer, path, err}
		}()
	}
	close(start)
	first, second := <-results, <-results
	var winner, loser outcome
	if first.err == nil {
		winner, loser = first, second
	} else {
		winner, loser = second, first
	}
	require.NoError(t, winner.err)
	require.Error(t, loser.err)
	f.admission.End(loser.ctx, r, f.kb)
	require.NoError(t, f.db.Where("token=?", r.Token).Take(r).Error)
	require.Equal(t, "pending", r.State, "failed concurrent retry cannot close the active writer")
	k := &types.Knowledge{ID: "same-knowledge", TenantID: 7, KnowledgeBaseID: "kb", FilePath: winner.path}
	require.NoError(t, f.db.Create(k).Error)
	require.NoError(t, f.admission.Finish(winner.ctx, r, k.ID, true))
	f.admission.End(winner.ctx, r, f.kb)
	var count int64
	require.NoError(t, f.db.Model(&repository.NextcloudCapacityWriteIntent{}).Count(&count).Error)
	require.Equal(t, int64(1), count)
	require.NoError(t, f.db.Where("token=?", r.Token).Take(r).Error)
	require.Equal(t, "committed", r.State)
}
func TestCapacityIntentConcurrentActualWritersSQLite(t *testing.T) {
	capacityConcurrentWriterContract(t, false)
}
func TestPostgresCapacityIntentConcurrentActualWriters(t *testing.T) {
	capacityConcurrentWriterContract(t, true)
}

func TestCapacityCrashChild(t *testing.T) {
	if os.Getenv("WEKNORA_CAPACITY_CRASH_CHILD") != "1" {
		t.Skip("owned subprocess crash fixture")
	}
	var d struct{ Driver, DSN, Base, Output string }
	b, e := os.ReadFile(os.Getenv("WEKNORA_CAPACITY_CRASH_DESCRIPTOR"))
	require.NoError(t, e)
	require.NoError(t, json.Unmarshal(b, &d))
	var db *gorm.DB
	if d.Driver == "postgres" {
		db, e = gorm.Open(postgres.Open(d.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	} else {
		db, e = gorm.Open(sqlite.Open(d.DSN), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	}
	require.NoError(t, e)
	fs := storagefile.NewResourceCatalogFileService(storagefile.NewBackendScopedFileService("backend", storagefile.NewLocalFileService(d.Base, "")), NewResourceCatalog(repository.NewResourceRepository(db)))
	a := NewNextcloudCapacityAdmission(repository.NewNextcloudCapacityStore(db), func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) { return fs, nil })
	backend := "backend"
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7, StorageBackendID: &backend}
	r, e := a.Begin(context.Background(), kb, "ds", "crash-node", "etag", 32)
	require.NoError(t, e)
	raw, e := json.Marshal(r)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(d.Output, raw, 0600))
	h, e := bytesToFileHeader([]byte("crash"), "source.txt")
	require.NoError(t, e)
	h.Size = 32
	_, e = fs.SaveFile(a.WriteContext(context.Background(), r), h, 7, "crash-candidate")
	require.Error(t, e)
	os.Exit(77) // No End/defer: leaves a durable partial object and writing intent.
}
func capacityCrashContract(t *testing.T, pg bool) {
	f := newCapacityIntentFixture(t, pg)
	dir := t.TempDir()
	descriptor := filepath.Join(dir, "descriptor")
	output := filepath.Join(dir, "reservation")
	b, e := json.Marshal(map[string]string{"Driver": f.driver, "DSN": f.dsn, "Base": f.base, "Output": output})
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(descriptor, b, 0600))
	child := exec.Command(os.Args[0], "-test.run=^TestCapacityCrashChild$", "-test.v")
	child.Env = append(os.Environ(), "WEKNORA_CAPACITY_CRASH_CHILD=1", "WEKNORA_CAPACITY_CRASH_DESCRIPTOR="+descriptor)
	err := child.Run()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 77, exit.ExitCode())
	var r repository.NextcloudCapacityReservation
	b, e = os.ReadFile(output)
	require.NoError(t, e)
	require.NoError(t, json.Unmarshal(b, &r))
	i, e := f.store.Intent(context.Background(), &r)
	require.NoError(t, e)
	require.Equal(t, "writing", i.State)
	if cli := os.Getenv("WEKNORA_CAPACITY_RECONCILE_CLI"); cli != "" {
		private := t.TempDir()
		require.NoError(t, os.Chmod(private, 0700))
		args := []string{"--mode=reconcile", "--reservation-token=" + r.Token, "--local-storage-base-dir=" + f.base, "--output=" + filepath.Join(private, "receipt")}
		if f.driver == "postgres" {
			dsnFile := filepath.Join(private, "dsn")
			require.NoError(t, os.WriteFile(dsnFile, []byte(f.dsn), 0600))
			args = append(args, "--postgres-connection-file="+dsnFile)
		} else {
			path := strings.Split(f.dsn, "?")[0]
			require.NoError(t, os.Chmod(path, 0600))
			require.NoError(t, os.Chmod(filepath.Dir(path), 0700))
			args = append(args, "--sqlite-db="+path)
		}
		command := exec.Command(cli, args...)
		result, err := command.CombinedOutput()
		require.NoError(t, err, string(result))
	} else {
		require.NoError(t, f.admission.Reconcile(context.Background(), f.kb, &r))
	}
	require.NoError(t, f.db.Where("token=?", r.Token).Take(&r).Error)
	require.Equal(t, "committed", r.State)
	i, e = f.store.Intent(context.Background(), &r)
	require.NoError(t, e)
	require.Equal(t, int64(5), i.OrphanChargeBytes)
	require.Error(t, f.store.ValidateWriter(context.Background(), &r, i.WriterID, i.PhysicalPath, i.KnowledgeID))
	require.NoError(t, f.store.SetPolicy(context.Background(), repository.NextcloudCapacityPolicy{TenantID: 7, KnowledgeBaseID: "kb", RetentionQuotaBytes: 42, WarningPercent: 80, PausePercent: 90}))
	_, e = f.ingest(t, "success", nil, nil)
	require.NoError(t, e, "crash partial bytes must not retain an inflated full reservation forever")
}
func TestCapacityIntentActualSQLiteProcessCrash(t *testing.T)   { capacityCrashContract(t, false) }
func TestPostgresCapacityIntentActualProcessCrash(t *testing.T) { capacityCrashContract(t, true) }
