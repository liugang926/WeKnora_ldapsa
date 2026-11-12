package access_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appaccess "github.com/Tencent/WeKnora/internal/application/access"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	appservice "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// These tests cover the retained atomic user-snapshot component and the
// runtime login-snapshot API after publication authorization was retired.
// Transaction overlap, driver errors and snapshots are never injected.
type (
	publicationConcurrentContextKey struct{}
	publicationConcurrentToken      [1]byte
)

type publicationConcurrentRead struct {
	identities []*types.DirectoryIdentity
	snapshot   *types.DirectoryLoginSnapshot
	err        error
}

type publicationConcurrentLookup struct {
	interfaces.DirectoryRepository
	reads []publicationConcurrentRead
}

func (l *publicationConcurrentLookup) GetUserLoginSnapshot(
	ctx context.Context,
	userID string,
) ([]*types.DirectoryIdentity, *types.DirectoryLoginSnapshot, error) {
	identities, snapshot, err := l.DirectoryRepository.GetUserLoginSnapshot(ctx, userID)
	l.reads = append(l.reads, publicationConcurrentRead{identities, snapshot, err})
	return identities, snapshot, err
}

// GetLoginSnapshot is the active DirectoryRuntime Login/catalog snapshot API.
func (l *publicationConcurrentLookup) GetLoginSnapshot(
	ctx context.Context, directoryID, objectGUID string,
) (*types.DirectoryLoginSnapshot, error) {
	snapshot, err := l.DirectoryRepository.GetLoginSnapshot(ctx, directoryID, objectGUID)
	l.reads = append(l.reads, publicationConcurrentRead{snapshot: snapshot, err: err})
	return snapshot, err
}

type publicationConcurrentFixture struct {
	reader, writer *gorm.DB
	observer       *sql.Conn
	readerPID      int
	writerPID      int
	directory      *types.Directory
	identity       *types.DirectoryIdentity
	beforeVersion  uint64
	guard          *appaccess.NextcloudPublicationGuard
	lookup         *publicationConcurrentLookup
	caller         context.Context
	sourceCalls    atomic.Int64
	sourceRequests atomic.Int64
}

const (
	publicationConcurrentUser  = "publication-concurrent-person"
	publicationConcurrentGUID  = "00112233-4455-6677-8899-aabbccddeeff"
	publicationConcurrentGroup = "11112233-4455-6677-8899-aabbccddeeff"
)

func publicationConcurrentSnapshot(directoryID string, enabled bool) *types.DirectorySnapshot {
	snapshot := &types.DirectorySnapshot{
		DirectoryID:        directoryID,
		Complete:           true,
		PaginationComplete: true,
		Trigger:            types.DirectorySyncTriggerScheduled,
		StartedAt:          time.Now().UTC(),
		Identities: []types.DirectoryIdentitySnapshot{
			{
				ObjectGUID:     publicationConcurrentGUID,
				ObjectSID:      "S-1-5-21-1000",
				DN:             "cn=Alice,dc=example,dc=test",
				SAMAccountName: "alice",
				Enabled:        enabled,
			},
		},
		Groups: []types.DirectoryGroupSnapshot{
			{
				ObjectGUID:     publicationConcurrentGroup,
				ObjectSID:      "S-1-5-21-1001",
				DN:             "cn=Team,dc=example,dc=test",
				SAMAccountName: "team",
			},
		},
	}
	if enabled {
		snapshot.Memberships = []types.DirectoryMembershipSnapshot{
			{
				GroupObjectGUID: publicationConcurrentGroup,
				UserObjectGUID:  publicationConcurrentGUID,
				Direct:          true,
				Source:          types.DirectoryMembershipDirect,
			},
		}
	}
	return snapshot
}

func publicationConcurrentCreate(t *testing.T, svc interfaces.DirectoryService, name string) *types.Directory {
	t.Helper()
	directory, err := svc.Create(context.Background(), &types.Directory{
		Name: name, Protocol: types.DirectoryProtocolAD, Enabled: true,
		TLSMode: types.DirectoryTLSLDAPS, ServerURLs: types.StringArray{"ldaps://dc.example.test:636"},
		BaseDN: "dc=example,dc=test", ServiceAccountDN: "cn=reader,dc=example,dc=test",
		PasswordCiphertext: "test-fixture-ciphertext", SyncIntervalSeconds: 15, StaleAfterSeconds: 120,
	})
	require.NoError(t, err)
	return directory
}

func publicationConcurrentActualFixture(t *testing.T) *publicationConcurrentFixture {
	t.Helper()
	f := &publicationConcurrentFixture{reader: publicationSnapshotActualDB(t, true)}
	var schema string
	require.NoError(t, f.reader.Raw("SELECT current_schema(), pg_backend_pid()").Row().Scan(&schema, &f.readerPID))
	require.Regexp(t, regexp.MustCompile(`^publication_directory_[0-9a-f]{32}$`), schema)
	// Both peers are independently reserved connections to the same owned DB and
	// exact private schema. No public search_path or foreign fixture is changed.
	pool, err := sql.Open("pgx", os.Getenv("WEKNORA_LEASE_TEST_POSTGRES_DSN"))
	require.NoError(t, err)
	pool.SetMaxOpenConns(2)
	var writer *sql.Conn
	t.Cleanup(func() {
		if f.observer != nil {
			require.NoError(t, f.observer.Close())
		}
		if writer != nil {
			require.NoError(t, writer.Close())
		}
		require.NoError(t, pool.Close())
	})
	writer, err = pool.Conn(context.Background())
	require.NoError(t, err)
	f.observer, err = pool.Conn(context.Background())
	require.NoError(t, err)
	for _, conn := range []*sql.Conn{writer, f.observer} {
		_, err = conn.ExecContext(context.Background(), `SET search_path TO "`+schema+`"`)
		require.NoError(t, err)
		var actualSchema string
		require.NoError(t, conn.QueryRowContext(context.Background(), "SELECT current_schema()").Scan(&actualSchema))
		require.Equal(t, schema, actualSchema)
	}
	require.NoError(t, writer.QueryRowContext(context.Background(), "SELECT pg_backend_pid()").Scan(&f.writerPID))
	require.NotEqual(t, f.readerPID, f.writerPID)
	f.writer, err = gorm.Open(
		postgres.New(postgres.Config{Conn: writer, PreferSimpleProtocol: true}),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	require.NoError(t, err)
	repo := apprepo.NewDirectoryRepository(f.reader)
	svc := appservice.NewDirectoryService(repo)
	f.directory = publicationConcurrentCreate(t, svc, "concurrent-publication")
	_, err = svc.ApplySnapshot(context.Background(), publicationConcurrentSnapshot(f.directory.ID, true))
	require.NoError(t, err)
	f.identity, err = repo.GetIdentityByObjectGUID(context.Background(), f.directory.ID, publicationConcurrentGUID)
	require.NoError(t, err)
	require.NotNil(t, f.identity)
	require.NoError(t, svc.LinkIdentity(context.Background(), f.identity.ID, publicationConcurrentUser))
	before, err := repo.GetLoginSnapshot(context.Background(), f.directory.ID, publicationConcurrentGUID)
	require.NoError(t, err)
	require.NotNil(t, before)
	require.NotNil(t, before.Identity)
	require.Equal(t, []string{publicationConcurrentGroup}, before.EffectiveGroupObjectGUIDs)
	f.beforeVersion = before.Directory.SnapshotVersion
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.sourceRequests.Add(1)
		if r.Header.Get("Authorization") != "Bearer publication-concurrent-secret" ||
			r.Header.Get("X-WeKnora-Signature") == "" {
			t.Error("source transport omitted paired authentication")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/index.php/apps/integration_weknora/api/v1/capabilities":
			if _, err := fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance-1"}`); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case "/index.php/apps/integration_weknora/api/v1/bindings/binding-1/authorize":
			f.sourceCalls.Add(1)
			var payload struct {
				DirectoryID string `json:"directory_id"`
				ObjectGUID  string `json:"object_guid"`
				FileID      int64  `json:"file_id"`
			}
			if err := json.NewDecoder(r.Body).
				Decode(
					&payload,
				); err != nil || payload.DirectoryID != f.directory.ID ||
				payload.ObjectGUID != publicationConcurrentGUID ||
				payload.FileID != 42 {
				t.Error("source call did not bind the exact current identity")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if _, err := fmt.Fprint(
				w,
				("{\"allow\":true,\"reason\":\"authorized\",\"policy_revision\":\"policy-1\"" +
					",\"source_etag\":\"etag-42\",\"checked_at\":1780000000}"),
			); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	config, err := json.Marshal(
		types.DataSourceConfig{
			Type:        types.ConnectorTypeNextcloud,
			Settings:    map[string]interface{}{"base_url": server.URL},
			Credentials: map[string]interface{}{"token": "publication-concurrent-secret"},
			ResourceIDs: []string{"binding-1"},
		},
	)
	require.NoError(t, err)
	source := &types.DataSource{
		ID:              "concurrent-source",
		TenantID:        12,
		KnowledgeBaseID: "publication-kb",
		Type:            types.ConnectorTypeNextcloud,
		Status:          types.DataSourceStatusActive,
		Config:          types.JSON(config),
	}
	f.lookup = &publicationConcurrentLookup{DirectoryRepository: repo}
	f.guard = appaccess.NewNextcloudPublicationGuard(f.lookup, publicationSnapshotSourceLookup{source: source})
	f.caller = types.WithCaller(context.Background(), types.Caller{TenantID: 12, UserID: publicationConcurrentUser})
	f.caller = types.WithPrincipal(
		f.caller,
		types.Principal{Type: types.PrincipalWebUser, ID: publicationConcurrentUser},
	)
	return f
}

// A canceled pgx query may discard its database/sql connection. Give this
// one read its own connection so the shared fixture's ordinary connection
// cleanup remains strict, while sql.ErrConnDone is valid for this canceled one.
func publicationConcurrentCancelableReader(t *testing.T, f *publicationConcurrentFixture) {
	t.Helper()
	var schema string
	require.NoError(t, f.reader.Raw("SELECT current_schema()").Scan(&schema).Error)
	require.Regexp(t, regexp.MustCompile(`^publication_directory_[0-9a-f]{32}$`), schema)
	pool, err := sql.Open("pgx", os.Getenv("WEKNORA_LEASE_TEST_POSTGRES_DSN"))
	require.NoError(t, err)
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	conn, err := pool.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := conn.Close(); !errors.Is(err, sql.ErrConnDone) {
			require.NoError(t, err)
		}
	})
	_, err = conn.ExecContext(context.Background(), `SET search_path TO "`+schema+`"`)
	require.NoError(t, err)
	var actualSchema string
	require.NoError(t, conn.QueryRowContext(context.Background(),
		"SELECT current_schema(), pg_backend_pid()").Scan(&actualSchema, &f.readerPID))
	require.Equal(t, schema, actualSchema)
	require.NotEqual(t, f.writerPID, f.readerPID)
	f.reader, err = gorm.Open(postgres.New(postgres.Config{Conn: conn, PreferSimpleProtocol: true}),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	f.lookup.DirectoryRepository = apprepo.NewDirectoryRepository(f.reader)
}

func publicationConcurrentKnowledge(t *testing.T) *types.Knowledge {
	t.Helper()
	metadata, err := json.Marshal(
		map[string]string{
			"datasource_id":         "concurrent-source",
			"external_id":           "nextcloud:instance-1:42",
			"source_resource_id":    "binding-1",
			"nextcloud_instance_id": "instance-1",
			"nextcloud_binding_id":  "binding-1",
			"nextcloud_file_id":     "42",
			"nextcloud_etag":        "etag-42",
		},
	)
	require.NoError(t, err)
	return &types.Knowledge{
		TenantID:        12,
		KnowledgeBaseID: "publication-kb",
		Channel:         types.ConnectorTypeNextcloud,
		Metadata:        types.JSON(metadata),
	}
}

func TestPostgresNextcloudPublicationDirectorySnapshotConcurrentTransactions(t *testing.T) {
	for _, mode := range []string{
		"unchanged_complete_sync",
		"disabled",
		"out_of_scope",
		"directory_disabled",
		"link_phantom_point_in_time",
	} {
		t.Run(mode, func(t *testing.T) {
			f := publicationConcurrentActualFixture(t)
			writerRepo := apprepo.NewDirectoryRepository(f.writer)
			writerSvc := appservice.NewDirectoryService(writerRepo)
			var otherIdentity *types.DirectoryIdentity
			if mode == "link_phantom_point_in_time" {
				other := publicationConcurrentCreate(t, writerSvc, "concurrent-other-directory")
				_, err := writerSvc.ApplySnapshot(context.Background(), publicationConcurrentSnapshot(other.ID, true))
				require.NoError(t, err)
				otherIdentity, err = writerRepo.GetIdentityByObjectGUID(
					context.Background(),
					other.ID,
					publicationConcurrentGUID,
				)
				require.NoError(t, err)
				require.NotNil(t, otherIdentity)
			}
			ctx, cancel := context.WithTimeout(f.caller, 10*time.Second)
			defer cancel()
			writerCtx, cancelWriter := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelWriter()
			token := &publicationConcurrentToken{}
			ctx = context.WithValue(ctx, publicationConcurrentContextKey{}, token)
			var versions []uint64
			var isolation []string
			var driver40001 int
			var hookErr error
			committed := false
			name := "publication-concurrent-after-links"
			require.NoError(t, f.reader.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
				if tx.Statement.Context.Value(publicationConcurrentContextKey{}) != token {
					return
				}
				if tx.Statement.Table == "directories" && tx.Error != nil {
					var pgErr *pgconn.PgError
					if errors.As(tx.Error, &pgErr) && pgErr.Code == "40001" {
						driver40001++
					}
				}
				rows, isUserLinks := tx.Statement.Dest.(*[]*types.DirectoryIdentity)
				if tx.Statement.Table != "directory_identities" || !isUserLinks || tx.Error != nil {
					return
				}
				var level string
				if err := tx.Statement.ConnPool.QueryRowContext(tx.Statement.Context, "SHOW transaction_isolation").
					Scan(&level); err != nil {
					hookErr = err
					return
				}
				isolation = append(isolation, level)
				if len(*rows) == 1 && (*rows)[0] != nil {
					versions = append(versions, (*rows)[0].SnapshotVersion)
				}
				if committed || hookErr != nil {
					return
				}
				// This is after the real links SELECT and before the production
				// directory SHARE query. The independent writer really commits.
				snapshot := publicationConcurrentSnapshot(f.directory.ID, mode != "disabled")
				switch mode {
				case "out_of_scope":
					snapshot.Identities, snapshot.Memberships = nil, nil
					_, hookErr = writerSvc.ApplySnapshot(writerCtx, snapshot)
				case "directory_disabled":
					copied := *f.directory
					copied.Enabled = false
					_, hookErr = writerSvc.Update(writerCtx, &copied)
				case "link_phantom_point_in_time":
					hookErr = writerSvc.LinkIdentity(writerCtx, otherIdentity.ID, publicationConcurrentUser)
				default:
					_, hookErr = writerSvc.ApplySnapshot(writerCtx, snapshot)
				}
				committed = hookErr == nil
			}))
			t.Cleanup(func() { require.NoError(t, f.reader.Callback().Query().Remove(name)) })
			// The retired publication adapter performs no directory or HTTP read.
			require.NoError(t, f.guard.CheckKnowledge(ctx, publicationConcurrentKnowledge(t)))
			require.Empty(t, f.lookup.reads)
			require.False(t, committed)
			require.Zero(t, f.sourceRequests.Load())
			identities, current, err := f.lookup.GetUserLoginSnapshot(ctx, publicationConcurrentUser)
			require.NoError(t, err)
			require.NoError(t, hookErr)
			require.True(t, committed, "a real separate connection committed the controlled writer")
			require.NotEmpty(t, isolation)
			for _, level := range isolation {
				require.Equal(t, "repeatable read", level)
			}
			require.Len(t, f.lookup.reads, 1, "one direct repository call records one whole atomic operation")
			// The retained user-link operation above is component coverage. The
			// runtime Login/catalog path uses this explicit-directory API instead.
			login, loginErr := apprepo.NewDirectoryRepository(f.reader).GetLoginSnapshot(
				f.caller, f.directory.ID, publicationConcurrentGUID,
			)
			require.NoError(t, loginErr)
			require.NotNil(t, login)
			require.NotNil(t, login.Directory)
			if mode == "disabled" || mode == "out_of_scope" {
				require.Nil(t, login.Identity)
				require.Empty(t, login.EffectiveGroupObjectGUIDs)
			} else {
				require.NotNil(t, login.Identity)
				require.Equal(t, []string{publicationConcurrentGroup}, login.EffectiveGroupObjectGUIDs)
			}
			if mode == "directory_disabled" {
				require.False(t, login.Directory.Enabled, "runtime login validation receives the disabled flag")
			} else {
				require.True(t, login.Directory.Enabled)
			}
			if mode == "link_phantom_point_in_time" {
				// RR cardinality is a point-in-time snapshot. A new link whose
				// commit is after the first SELECT is not retroactively visible.
				// This is not an after-output linearizability or lifetime claim.
				require.NoError(t, err)
				require.Zero(t, driver40001)
				require.Len(t, f.lookup.reads[0].identities, 1)
				require.Equal(t, []uint64{f.beforeVersion}, versions)
				require.Len(t, identities, 1)
				require.NotNil(t, current.Identity)
				identities, current, err = f.lookup.GetUserLoginSnapshot(f.caller, publicationConcurrentUser)
				require.NoError(t, err)
				require.Len(t, identities, 2, "the next independent snapshot sees all-directory cardinality")
				require.Nil(t, current, "multiple links do not select a login identity")
				require.Zero(t, f.sourceCalls.Load())
				require.Zero(t, f.sourceRequests.Load())
				return
			}
			require.Equal(
				t,
				1,
				driver40001,
				"the PostgreSQL driver, not the hook, reports the actual SHARE serialization conflict",
			)
			require.Len(t, isolation, 2, "the complete links read repeats in a new transaction")
			if mode == "unchanged_complete_sync" {
				require.NoError(t, err)
				require.Equal(t, []uint64{f.beforeVersion, f.beforeVersion + 1}, versions)
				require.Equal(t, f.beforeVersion+1, f.lookup.reads[0].snapshot.Directory.SnapshotVersion)
				require.Equal(t, f.identity.ID, f.lookup.reads[0].snapshot.Identity.ID)
				require.Equal(t, publicationConcurrentUser, *f.lookup.reads[0].snapshot.Identity.UserID)
				require.Equal(
					t,
					[]string{publicationConcurrentGroup},
					f.lookup.reads[0].snapshot.EffectiveGroupObjectGUIDs,
				)
				require.Len(t, identities, 1)
				require.NotNil(t, current.Identity)
			} else {
				require.Len(t, identities, 1, "inactive links remain visible to the atomic routing read")
				require.NotNil(t, current)
				if mode == "directory_disabled" {
					require.False(t, current.Directory.Enabled, "login admission sees the disabled directory")
				} else {
					require.Equal(t, f.beforeVersion+1, current.Directory.SnapshotVersion)
					require.Nil(t, current.Identity, "inactive identities are not eligible login snapshots")
					require.Empty(t, current.EffectiveGroupObjectGUIDs)
					stored, lookupErr := writerRepo.GetIdentityByObjectGUID(
						context.Background(), f.directory.ID, publicationConcurrentGUID,
					)
					require.NoError(t, lookupErr)
					require.NotNil(t, stored)
					if mode == "disabled" {
						require.Equal(t, types.DirectoryObjectDisabled, stored.Status)
					} else {
						require.Equal(t, types.DirectoryObjectOutOfScope, stored.Status)
					}
				}
			}
			require.Zero(t, f.sourceCalls.Load())
			require.Zero(t, f.sourceRequests.Load())
		})
	}
	t.Run("cancel_waiting_for_share", func(t *testing.T) {
		f := publicationConcurrentActualFixture(t)
		publicationConcurrentCancelableReader(t, f)
		readerToken, writerToken := &publicationConcurrentToken{}, &publicationConcurrentToken{}
		readerCtx, cancelReader := context.WithCancel(
			context.WithValue(f.caller, publicationConcurrentContextKey{}, readerToken),
		)
		writerCtx, cancelWriter := context.WithTimeout(
			context.WithValue(context.Background(), publicationConcurrentContextKey{}, writerToken),
			10*time.Second,
		)
		locked, release := make(chan struct{}), make(chan struct{})
		var unlock sync.Once
		var writerHold sync.Once
		writerDone, readerDone := make(chan error, 1), make(chan error, 1)
		writerStarted, readerStarted := false, false
		writerJoined, readerJoined := false, false
		var identityReads atomic.Int64
		var directoryErrors atomic.Int64
		var serializationErrors atomic.Int64
		writerName, readerName := "publication-concurrent-held-update", "publication-concurrent-cancel-share"
		require.NoError(t, f.writer.Callback().Query().After("gorm:query").Register(writerName, func(tx *gorm.DB) {
			if tx.Statement.Context.Value(publicationConcurrentContextKey{}) != writerToken ||
				tx.Statement.Table != "directories" ||
				tx.Error != nil {
				return
			}
			lock, ok := tx.Statement.Clauses["FOR"].Expression.(clause.Locking)
			if !ok || lock.Strength != "UPDATE" {
				return
			}
			writerHold.Do(func() {
				close(locked)
				select {
				case <-release:
				case <-writerCtx.Done():
				}
			})
		}))
		require.NoError(t, f.reader.Callback().Query().After("gorm:query").Register(readerName, func(tx *gorm.DB) {
			if tx.Statement.Context.Value(publicationConcurrentContextKey{}) != readerToken {
				return
			}
			if _, ok := tx.Statement.Dest.(*types.DirectoryIdentity); ok &&
				tx.Statement.Table == "directory_identities" &&
				tx.Error == nil {
				identityReads.Add(1)
			}
			if tx.Statement.Table == "directories" && tx.Error != nil {
				directoryErrors.Add(1)
				var pgErr *pgconn.PgError
				if errors.As(tx.Error, &pgErr) && pgErr.Code == "40001" {
					serializationErrors.Add(1)
				}
			}
		}))
		t.Cleanup(func() {
			require.NoError(t, f.reader.Callback().Query().Remove(readerName))
			require.NoError(t, f.writer.Callback().Query().Remove(writerName))
		})
		t.Cleanup(func() {
			cancelReader()
			unlock.Do(func() { close(release) })
			cancelWriter()
			for _, child := range []struct {
				started bool
				joined  bool
				done    chan error
			}{{readerStarted, readerJoined, readerDone}, {writerStarted, writerJoined, writerDone}} {
				if child.started && !child.joined {
					select {
					case <-child.done:
					case <-time.After(3 * time.Second):
						t.Error("actual owned transaction worker did not return during cleanup")
					}
				}
			}
		})
		writerSvc := appservice.NewDirectoryService(apprepo.NewDirectoryRepository(f.writer))
		writerStarted = true
		go func() {
			_, err := writerSvc.ApplySnapshot(writerCtx, publicationConcurrentSnapshot(f.directory.ID, true))
			writerDone <- err
		}()
		select {
		case <-locked:
		case <-time.After(3 * time.Second):
			t.Fatal("the real ApplySnapshot did not acquire its directory UPDATE lock")
		}
		readerStarted = true
		go func() {
			_, err := f.lookup.GetLoginSnapshot(readerCtx, f.directory.ID, publicationConcurrentGUID)
			readerDone <- err
		}()
		// Observe the real reader backend blocked by this exact writer PID.
		// Timing alone, callback counts and a synthetic SQL error are insufficient.
		require.Eventually(t, func() bool {
			var blocked bool
			err := f.observer.QueryRowContext(
				writerCtx,
				("SELECT COALESCE(wait_event_type = 'Lock', false) AND $2 = ANY(pg_blocking_" +
					"pids(pid)) FROM pg_stat_activity WHERE pid = $1 AND datname = current_data" +
					"base() AND usename = current_user"),
				f.readerPID,
				f.writerPID,
			).
				Scan(&blocked)
			return err == nil && blocked
		}, 3*time.Second, 5*time.Millisecond, "actual reader SHARE waits on the owned ApplySnapshot writer")
		cancelReader()
		select {
		case err := <-readerDone:
			readerJoined = true
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(3 * time.Second):
			t.Fatal("canceling the real blocked SHARE did not return")
		}
		require.Len(t, f.lookup.reads, 1)
		require.ErrorIs(t, f.lookup.reads[0].err, context.Canceled)
		require.Nil(t, f.lookup.reads[0].identities)
		require.Nil(t, f.lookup.reads[0].snapshot)
		require.Zero(t, identityReads.Load(), "canceled SHARE cannot proceed to identity or group reads")
		require.Equal(t, int64(1), directoryErrors.Load())
		require.Zero(t, serializationErrors.Load())
		require.Zero(t, f.sourceCalls.Load())
		require.Zero(t, f.sourceRequests.Load())
		// A canceled repository read remains canceled; the retired compatibility
		// method is independently a no-op and cannot retry or authorize it.
		require.NoError(t, f.guard.CheckKnowledge(readerCtx, publicationConcurrentKnowledge(t)))
		require.Len(t, f.lookup.reads, 1)
		unlock.Do(func() { close(release) })
		select {
		case err := <-writerDone:
			writerJoined = true
			require.NoError(t, err, "the real writer may finish only after the refused reader returns")
		case <-time.After(3 * time.Second):
			t.Fatal("the actual writer did not finish after release")
		}
		after, err := apprepo.NewDirectoryRepository(f.writer).
			GetLoginSnapshot(context.Background(), f.directory.ID, publicationConcurrentGUID)
		require.NoError(t, err)
		require.Equal(t, f.beforeVersion+1, after.Directory.SnapshotVersion)
	})
}
