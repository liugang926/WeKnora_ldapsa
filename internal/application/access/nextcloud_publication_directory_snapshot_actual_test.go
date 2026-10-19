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
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	appaccess "github.com/Tencent/WeKnora/internal/application/access"
	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	appservice "github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// This is a directory/publication getter contract regression. It uses real
// Create/ApplySnapshot/LinkIdentity transactions, but does not certify original
// message bodies, a long HTTP stream, or a restored application.
type publicationSnapshotRaceLookup struct {
	interfaces.DirectoryRepository
	between   func(context.Context) error
	triggered bool
	outer     []*types.DirectoryIdentity
}

// Predeclared using existing types so the baseline and future atomic guard can
// execute byte-identical transitions and assertions. Absence of the real atomic
// repository method is an error, never a legacy authorization fallback.
type publicationAtomicUserSnapshot interface {
	GetUserLoginSnapshot(context.Context, string) ([]*types.DirectoryIdentity, *types.DirectoryLoginSnapshot, error)
}

func (l *publicationSnapshotRaceLookup) transition(ctx context.Context) error {
	if l.between == nil || l.triggered {
		return nil
	}
	l.triggered = true
	return l.between(ctx)
}

func (l *publicationSnapshotRaceLookup) GetIdentityByUserID(
	ctx context.Context,
	userID string,
) ([]*types.DirectoryIdentity, error) {
	rows, err := l.DirectoryRepository.GetIdentityByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	l.outer = rows
	if err := l.transition(ctx); err != nil {
		return nil, err
	}
	return rows, nil
}

func (l *publicationSnapshotRaceLookup) GetUserLoginSnapshot(
	ctx context.Context,
	userID string,
) ([]*types.DirectoryIdentity, *types.DirectoryLoginSnapshot, error) {
	// This old-view read measures the same N boundary as the baseline fixture.
	// It is not returned as authority: the real atomic method reads after the
	// committed transition and alone supplies the result to the future guard.
	rows, err := l.DirectoryRepository.GetIdentityByUserID(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	l.outer = rows
	if err := l.transition(ctx); err != nil {
		return nil, nil, err
	}
	atomicRepo, ok := l.DirectoryRepository.(publicationAtomicUserSnapshot)
	if !ok {
		return nil, nil, errors.New("real atomic user snapshot repository is required")
	}
	return atomicRepo.GetUserLoginSnapshot(ctx, userID)
}

type publicationSnapshotSourceLookup struct{ source *types.DataSource }

func (l publicationSnapshotSourceLookup) FindByID(_ context.Context, id string) (*types.DataSource, error) {
	if l.source == nil || l.source.ID != id {
		return nil, errors.New("unexpected publication fixture source")
	}
	return l.source, nil
}

func publicationSnapshotActualDB(t *testing.T, pg bool) *gorm.DB {
	t.Helper()
	var db *gorm.DB
	var err error
	migration := filepath.Join("..", "..", "..", "migrations", "sqlite", "000028_directory_group_access.up.sql")
	if !pg {
		db, err = gorm.Open(
			sqlite.Open(filepath.Join(t.TempDir(), "directory.db")+"?_foreign_keys=on&_busy_timeout=5000"),
			&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
		)
		require.NoError(t, err)
		pool, err := db.DB()
		require.NoError(t, err)
		pool.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, pool.Close()) })
	} else {
		dsn := os.Getenv("WEKNORA_LEASE_TEST_POSTGRES_DSN")
		if strings.TrimSpace(dsn) == "" {
			t.Skip("owned PostgreSQL directory fixture DSN is required")
		}
		// Reserve one connection and its private schema. There is no mutation of
		// another fixture's search_path or of the shared public schema.
		pool, err := sql.Open("pgx", dsn)
		require.NoError(t, err)
		schema := "publication_directory_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		var conn *sql.Conn
		created := false
		t.Cleanup(func() {
			if conn != nil {
				require.NoError(t, conn.Close())
			}
			if created {
				_, err := pool.ExecContext(context.Background(), `DROP SCHEMA "`+schema+`" CASCADE`)
				require.NoError(t, err)
			}
			require.NoError(t, pool.Close())
		})
		_, err = pool.ExecContext(context.Background(), `CREATE SCHEMA "`+schema+`"`)
		require.NoError(t, err)
		created = true
		conn, err = pool.Conn(context.Background())
		require.NoError(t, err)
		_, err = conn.ExecContext(context.Background(), `SET search_path TO "`+schema+`"`)
		require.NoError(t, err)
		// Keep table lookup confined to the private schema while exposing the
		// real UUID extension function required by the forward migration.
		_, err = conn.ExecContext(
			context.Background(),
			`CREATE FUNCTION uuid_generate_v4() RETURNS uuid LANGUAGE sql AS 'SELECT public.uuid_generate_v4()'`,
		)
		require.NoError(t, err)
		db, err = gorm.Open(
			postgres.New(postgres.Config{Conn: conn, PreferSimpleProtocol: true}),
			&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
		)
		require.NoError(t, err)
		migration = filepath.Join("..", "..", "..", "migrations", "versioned", "000109_directory_group_access.up.sql")
	}
	ddl, err := os.ReadFile(migration)
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(ddl)).Error)
	require.NoError(t, db.AutoMigrate(&types.TenantMember{}, &types.AuthToken{}))
	return db
}

func runPublicationDirectorySnapshotActual(t *testing.T, pg bool) {
	t.Helper()
	for _, mode := range []string{
		"current",
		"unchanged_complete_sync",
		"disabled",
		"out_of_scope",
		"unlinked",
		"sync_failure",
		"cross_directory_link",
	} {
		t.Run(mode, func(t *testing.T) {
			db := publicationSnapshotActualDB(t, pg)
			repo := apprepo.NewDirectoryRepository(db)
			svc := appservice.NewDirectoryService(repo)
			ctx := context.Background()
			const userID = "publication-person"
			const guid = "00112233-4455-6677-8899-aabbccddeeff"
			create := func(name string) *types.Directory {
				directory, err := svc.Create(ctx, &types.Directory{
					Name: name, Protocol: types.DirectoryProtocolAD, Enabled: true,
					TLSMode: types.DirectoryTLSLDAPS, ServerURLs: types.StringArray{"ldaps://dc.example.test:636"},
					BaseDN: "dc=example,dc=test", ServiceAccountDN: "cn=reader,dc=example,dc=test",
					PasswordCiphertext: "test-fixture-ciphertext", SyncIntervalSeconds: 15, StaleAfterSeconds: 120,
				})
				require.NoError(t, err)
				return directory
			}
			directory := create("publication-directory")
			snapshot := func(id string, enabled bool) *types.DirectorySnapshot {
				return &types.DirectorySnapshot{
					DirectoryID:        id,
					Complete:           true,
					PaginationComplete: true,
					Trigger:            types.DirectorySyncTriggerScheduled,
					StartedAt:          time.Now().UTC(),
					Identities: []types.DirectoryIdentitySnapshot{
						{
							ObjectGUID:     guid,
							ObjectSID:      "S-1-5-21-1000",
							DN:             "cn=Alice,dc=example,dc=test",
							SAMAccountName: "alice",
							Enabled:        enabled,
						},
					},
				}
			}
			_, err := svc.ApplySnapshot(ctx, snapshot(directory.ID, true))
			require.NoError(t, err)
			identity, err := repo.GetIdentityByObjectGUID(ctx, directory.ID, guid)
			require.NoError(t, err)
			require.NotNil(t, identity)
			require.NoError(t, svc.LinkIdentity(ctx, identity.ID, userID))
			before, err := repo.GetLoginSnapshot(ctx, directory.ID, guid)
			require.NoError(t, err)
			require.NotNil(t, before.Identity)
			beforeVersion := before.Directory.SnapshotVersion
			var sourceCalls atomic.Int64
			var sourceRequests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sourceRequests.Add(1)
				if r.Header.Get("Authorization") != "Bearer publication-test-secret" ||
					r.Header.Get("X-WeKnora-Signature") == "" {
					t.Error("source check omitted paired authentication")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				switch r.URL.Path {
				case "/index.php/apps/integration_weknora/api/v1/capabilities":
					if _, err := fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance-1"}`); err != nil {
						t.Errorf("write fixture response: %v", err)
					}
				case "/index.php/apps/integration_weknora/api/v1/bindings/binding-1/authorize":
					sourceCalls.Add(1)
					var payload struct {
						DirectoryID string `json:"directory_id"`
						ObjectGUID  string `json:"object_guid"`
						FileID      int64  `json:"file_id"`
					}
					if err := json.NewDecoder(r.Body).
						Decode(
							&payload,
						); err != nil || payload.DirectoryID != directory.ID || payload.ObjectGUID != guid ||
						payload.FileID != 42 {
						t.Error("source check did not use the exact current linked identity")
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
					Credentials: map[string]interface{}{"token": "publication-test-secret"},
					ResourceIDs: []string{"binding-1"},
				},
			)
			require.NoError(t, err)
			dataSource := &types.DataSource{
				ID:              "publication-source",
				TenantID:        12,
				KnowledgeBaseID: "publication-kb",
				Type:            types.ConnectorTypeNextcloud,
				Status:          types.DataSourceStatusActive,
				Config:          types.JSON(config),
			}
			metadata, err := json.Marshal(
				map[string]string{
					"datasource_id":         dataSource.ID,
					"external_id":           "nextcloud:instance-1:42",
					"source_resource_id":    "binding-1",
					"nextcloud_instance_id": "instance-1",
					"nextcloud_binding_id":  "binding-1",
					"nextcloud_file_id":     "42",
					"nextcloud_etag":        "etag-42",
				},
			)
			require.NoError(t, err)
			knowledge := &types.Knowledge{
				TenantID:        12,
				KnowledgeBaseID: dataSource.KnowledgeBaseID,
				Channel:         types.ConnectorTypeNextcloud,
				Metadata:        types.JSON(metadata),
			}
			lookup := &publicationSnapshotRaceLookup{DirectoryRepository: repo}
			if mode != "current" {
				lookup.between = func(ctx context.Context) error {
					switch mode {
					case "unchanged_complete_sync", "disabled":
						_, err := svc.ApplySnapshot(ctx, snapshot(directory.ID, mode != "disabled"))
						return err
					case "out_of_scope":
						empty := snapshot(directory.ID, true)
						empty.Identities = nil
						_, err := svc.ApplySnapshot(ctx, empty)
						return err
					case "unlinked":
						return svc.UnlinkIdentity(ctx, identity.ID)
					case "sync_failure":
						_, err := repo.RecordSyncFailure(
							ctx,
							directory.ID,
							"test_failure",
							"controlled complete-sync failure",
							time.Now().UTC(),
							beforeVersion,
							before.Directory.ConfigVersion,
							types.DirectorySyncTriggerScheduled,
						)
						return err
					case "cross_directory_link":
						other := create("second-publication-directory")
						_, err := svc.ApplySnapshot(ctx, snapshot(other.ID, true))
						if err != nil {
							return err
						}
						otherIdentity, err := repo.GetIdentityByObjectGUID(ctx, other.ID, guid)
						if err != nil || otherIdentity == nil {
							return fmt.Errorf("second directory identity was not created: %w", err)
						}
						return svc.LinkIdentity(ctx, otherIdentity.ID, userID)
					}
					return errors.New("unknown controlled directory transition")
				}
			}
			callerCtx := types.WithCaller(ctx, types.Caller{TenantID: 12, UserID: userID})
			callerCtx = types.WithPrincipal(callerCtx, types.Principal{Type: types.PrincipalWebUser, ID: userID})
			guard := appaccess.NewNextcloudPublicationGuard(lookup, publicationSnapshotSourceLookup{source: dataSource})
			err = guard.CheckKnowledge(callerCtx, knowledge)
			if mode == "current" || mode == "unchanged_complete_sync" {
				require.NoError(
					t,
					err,
					"unchanged active principal must remain usable after a genuine complete snapshot",
				)
				require.Equal(t, int64(1), sourceCalls.Load())
				require.Equal(t, int64(2), sourceRequests.Load())
			} else {
				require.ErrorIs(t, err, appaccess.ErrNextcloudPublicationDenied)
				require.Zero(
					t,
					sourceCalls.Load(),
					"a revoked, failed or ambiguous directory binding must not reach source HTTP",
				)
				require.Zero(
					t,
					sourceRequests.Load(),
					"a denied directory binding must not reach even capabilities HTTP",
				)
			}
			if mode != "current" {
				require.True(t, lookup.triggered, "the controlled production transition must actually happen")
			}
			if mode == "unchanged_complete_sync" {
				after, err := repo.GetLoginSnapshot(ctx, directory.ID, guid)
				require.NoError(t, err)
				require.Equal(t, beforeVersion+1, after.Directory.SnapshotVersion)
				require.Equal(t, before.Identity.ID, after.Identity.ID)
				require.Equal(t, before.Identity.UserID, after.Identity.UserID)
				require.Equal(t, beforeVersion, lookup.outer[0].SnapshotVersion)
			}
		})
	}
}

func TestNextcloudPublicationDirectorySnapshotActualSQLite(t *testing.T) {
	runPublicationDirectorySnapshotActual(t, false)
}

func TestPostgresNextcloudPublicationDirectorySnapshotActual(t *testing.T) {
	runPublicationDirectorySnapshotActual(t, true)
}
