package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	storagefile "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/nextcloud"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Fetch the original bytes through the production connector. The intake gate
// accepts its private transport artifact only while the fetched item is intact.
func capacityNextcloudFetchedItem(t *testing.T, body string, incremental bool) *types.FetchedItem {
	t.Helper()
	const api = "/index.php/apps/integration_weknora/api/v1"
	const token, keyID, etag = "isolated-capacity-token", "capacity-key", "opaque-etag"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer "+token ||
			r.Header.Get("X-WeKnora-Key-Id") != keyID {
			t.Errorf("unexpected capacity connector request credentials or method")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		stamp, nonce := r.Header.Get("X-WeKnora-Timestamp"), r.Header.Get("X-WeKnora-Nonce")
		when, err := strconv.ParseInt(stamp, 10, 64)
		nonceBytes, nonceErr := hex.DecodeString(nonce)
		if err != nil || when < time.Now().Unix()-60 || when > time.Now().Unix()+60 || nonceErr != nil ||
			len(nonceBytes) != 16 ||
			r.URL.RawQuery != "" {
			t.Errorf("invalid capacity connector signed request controls")
			http.Error(w, "invalid signature controls", http.StatusUnauthorized)
			return
		}
		emptyHash := sha256.Sum256(nil)
		canonical := strings.Join(
			[]string{
				"weknora-hmac-sha256-v1",
				r.Method,
				r.URL.EscapedPath(),
				r.URL.RawQuery,
				hex.EncodeToString(emptyHash[:]),
				stamp,
				nonce,
				keyID,
			},
			"\n",
		)
		key := sha256.Sum256([]byte(token))
		mac := hmac.New(sha256.New, key[:])
		_, _ = mac.Write([]byte(canonical))
		provided, err := hex.DecodeString(r.Header.Get("X-WeKnora-Signature"))
		if err != nil || !hmac.Equal(provided, mac.Sum(nil)) {
			t.Errorf("capacity connector HMAC signature did not match")
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case api + "/capabilities":
			if _, err := fmt.Fprint(w, `{"protocol_version":"1","instance_id":"instance"}`); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case api + "/bindings/binding/manifest":
			if _, err := fmt.Fprintf(
				w,
				("{\"generation\":\"capacity-generation\",\"complete\":true,\"next_cursor\":" +
					"null,\"items\":[{\"file_id\":77,\"etag\":\"%s\",\"name\":\"source.txt\",\"" +
					"path\":\"source.txt\",\"mime_type\":\"text/plain\",\"size\":%d,\"mtime\":1" +
					"700000000}]}"),
				etag,
				len(body),
			); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		case api + "/bindings/binding/changes":
			http.NotFound(w, r)
		case api + "/bindings/binding/files/77/content":
			if r.Header.Get("If-Match") != `"`+etag+`"` {
				t.Errorf("capacity connector content request lacks the manifest If-Match")
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			w.Header().Set("ETag", `"`+etag+`"`)
			w.Header().Set("Content-Type", "text/plain")
			if _, err := fmt.Fprint(w, body); err != nil {
				t.Errorf("write fixture response: %v", err)
			}
		default:
			t.Errorf("unexpected capacity connector path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	t.Setenv("WEKNORA_NEXTCLOUD_DEV_HTTP", "1")
	t.Setenv("WEKNORA_NEXTCLOUD_ALLOWED_ORIGINS", server.URL)
	config := &types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		Settings:    map[string]interface{}{"base_url": server.URL},
		Credentials: map[string]interface{}{"token": token, "key_id": keyID},
		ResourceIDs: []string{"binding"},
	}
	connector := nextcloud.NewConnector()
	var items []types.FetchedItem
	var err error
	if incremental {
		var cursor *types.SyncCursor
		items, cursor, err = connector.FetchIncremental(context.Background(), config, nil)
		require.NotNil(t, cursor)
	} else {
		items, err = connector.FetchAll(context.Background(), config, config.ResourceIDs)
	}
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, body, string(items[0].Content))
	_, err = nextcloud.VerifyOriginalFetchedArtifact(&items[0])
	require.NoError(t, err)
	return &items[0]
}

func capacityNextcloudMetadata(item *types.FetchedItem) map[string]string {
	metadata := make(map[string]string, len(item.Metadata)+3)
	for key, value := range item.Metadata {
		metadata[key] = value
	}
	metadata["datasource_id"] = "ds"
	metadata["external_id"] = item.ExternalID
	metadata["source_resource_id"] = item.SourceResourceID
	return metadata
}

type capacityMeasuredFile struct {
	interfaces.FileService
	measurement types.StorageCapacity
}

func (f capacityMeasuredFile) SupportsLocalCapacityIntents() bool {
	p, ok := f.FileService.(interfaces.LocalCapacityIntentProvider)
	return ok && p.SupportsLocalCapacityIntents()
}

func (f capacityMeasuredFile) InspectCapacityIntent(
	ctx context.Context,
	o types.CapacityLocalObject,
	callback func(types.CapacityLocalObject) error,
) error {
	return f.FileService.(interfaces.LocalCapacityIntentProvider).InspectCapacityIntent(ctx, o, callback)
}

func (f capacityMeasuredFile) MeasureStorageCapacity(context.Context) (types.StorageCapacity, error) {
	return f.measurement, nil
}

type capacityIngestKnowledge struct {
	interfaces.KnowledgeService
	repo  interfaces.KnowledgeRepository
	db    *gorm.DB
	fs    interfaces.FileService
	calls int
}

func (s *capacityIngestKnowledge) GetRepository() interfaces.KnowledgeRepository { return s.repo }

func (s *capacityIngestKnowledge) CreateKnowledgeFromFile(
	ctx context.Context,
	kb string,
	header *multipart.FileHeader,
	_ map[string]string,
	_ *bool,
	_ string,
	_ []string,
	_ string,
	_ *types.KnowledgeProcessOverrides,
) (*types.Knowledge, error) {
	s.calls++
	row := &types.Knowledge{ID: "candidate", TenantID: 7, KnowledgeBaseID: kb, ParseStatus: types.ParseStatusPending}
	requireErr := s.db.Create(row).Error
	if requireErr != nil {
		return nil, requireErr
	}
	path, err := s.fs.SaveFile(ctx, header, 7, row.ID)
	if err == nil {
		row.FilePath = path
		err = s.db.Model(row).Update("file_path", path).Error
	}
	return row, err
}

func TestNextcloudCapacityActualIngestAndPausedNoSideEffects(t *testing.T) {
	for _, mode := range []string{"normal", "paused", "unconfigured"} {
		t.Run(mode, func(t *testing.T) {
			db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "capacity.db")), &gorm.Config{})
			require.NoError(t, err)
			require.NoError(
				t,
				db.AutoMigrate(
					&types.KnowledgeBase{},
					&types.Knowledge{},
					&types.StoredResource{},
					&types.ResourceBinding{},
					&types.ResourceAccessGrant{},
				),
			)
			sql, err := os.ReadFile("../../../migrations/sqlite/000065_nextcloud_capacity_admission.up.sql")
			require.NoError(t, err)
			require.NoError(t, db.Exec(string(sql)).Error)
			intentSQL, intentErr := os.ReadFile(
				"../../../migrations/sqlite/000067_nextcloud_capacity_write_intents.up.sql",
			)
			require.NoError(t, intentErr)
			require.NoError(t, db.Exec(string(intentSQL)).Error)
			backend := "backend"
			kb := &types.KnowledgeBase{ID: "kb", TenantID: 7, StorageBackendID: &backend}
			require.NoError(t, db.Create(kb).Error)
			base := t.TempDir()
			physical := storagefile.NewBackendScopedFileService(backend, storagefile.NewLocalFileService(base, ""))
			catalog := NewResourceCatalog(repository.NewResourceRepository(db))
			fs := storagefile.NewResourceCatalogFileService(physical, catalog)
			version := &deletionLookupKnowledgeRepo{}
			knowledge := &capacityIngestKnowledge{repo: version, db: db, fs: fs}
			source := &DataSourceService{knowledgeService: knowledge, kbService: &processSyncKBService{kb: kb}}
			measure := types.StorageCapacity{
				ScopeID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				TotalBytes:     100000,
				AvailableBytes: 90000,
				MeasuredAt:     time.Now().UTC(),
			}
			actualMeasure, actualErr := fs.(interfaces.StorageCapacityProvider).MeasureStorageCapacity(
				context.Background(),
			)
			require.NoError(t, actualErr)
			measure.ScopeID = actualMeasure.ScopeID
			if mode == "paused" {
				measure.AvailableBytes = 9000
			}
			if mode != "unconfigured" {
				source.capacityAdmission = NewNextcloudCapacityAdmission(
					repository.NewNextcloudCapacityStore(db),
					func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) {
						return capacityMeasuredFile{FileService: fs, measurement: measure}, nil
					},
				)
			}
			item := capacityNextcloudFetchedItem(t, "actual source bytes", false)
			_, err = source.ingestNextcloudItem(
				context.Background(),
				&types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb"},
				item,
				capacityNextcloudMetadata(item),
				nil,
			)
			if mode == "normal" {
				require.NoError(t, err)
				require.Equal(t, 1, knowledge.calls)
				require.Equal(t, 1, version.stageCalls)
				var resources []types.StoredResource
				require.NoError(t, db.Find(&resources).Error)
				require.Len(t, resources, 1)
				reader, e := fs.GetFile(context.Background(), types.BuildResourcePath(resources[0].Handle))
				require.NoError(t, e)
				bytes, e := io.ReadAll(reader)
				require.NoError(t, e)
				require.NoError(t, reader.Close())
				require.Equal(t, item.Content, bytes)
			} else {
				require.Error(t, err)
				if mode == "paused" {
					require.ErrorIs(t, err, repository.ErrNextcloudCapacityPaused)
				} else {
					require.ErrorIs(t, err, repository.ErrNextcloudCapacityUnavailable)
				}
				require.Zero(t, knowledge.calls)
				require.Zero(t, version.stageCalls)
				entries, e := os.ReadDir(base)
				require.NoError(t, e)
				require.Empty(t, entries)
			}
		})
	}
}
