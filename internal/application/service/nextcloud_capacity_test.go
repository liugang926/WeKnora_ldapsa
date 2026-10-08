package service

import (
	"context"
	"io"
	"mime/multipart"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	storagefile "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type capacityMeasuredFile struct {
	interfaces.FileService
	measurement types.StorageCapacity
}

func (f capacityMeasuredFile) SupportsLocalCapacityIntents() bool {
	p, ok := f.FileService.(interfaces.LocalCapacityIntentProvider)
	return ok && p.SupportsLocalCapacityIntents()
}
func (f capacityMeasuredFile) InspectCapacityIntent(ctx context.Context, o types.CapacityLocalObject, callback func(types.CapacityLocalObject) error) error {
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
func (s *capacityIngestKnowledge) CreateKnowledgeFromFile(ctx context.Context, kb string, header *multipart.FileHeader, metadata map[string]string, _ *bool, _ string, _ []string, _ string, _ *types.KnowledgeProcessOverrides) (*types.Knowledge, error) {
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
			require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.StoredResource{}, &types.ResourceBinding{}, &types.ResourceAccessGrant{}))
			sql, err := os.ReadFile("../../../migrations/sqlite/000065_nextcloud_capacity_admission.up.sql")
			require.NoError(t, err)
			require.NoError(t, db.Exec(string(sql)).Error)
			intentSQL, intentErr := os.ReadFile("../../../migrations/sqlite/000067_nextcloud_capacity_write_intents.up.sql")
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
			measure := types.StorageCapacity{ScopeID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", TotalBytes: 100000, AvailableBytes: 90000, MeasuredAt: time.Now().UTC()}
			actualMeasure, actualErr := fs.(interfaces.StorageCapacityProvider).MeasureStorageCapacity(context.Background())
			require.NoError(t, actualErr)
			measure.ScopeID = actualMeasure.ScopeID
			if mode == "paused" {
				measure.AvailableBytes = 9000
			}
			if mode != "unconfigured" {
				source.capacityAdmission = NewNextcloudCapacityAdmission(repository.NewNextcloudCapacityStore(db), func(context.Context, *types.KnowledgeBase) (interfaces.FileService, error) {
					return capacityMeasuredFile{FileService: fs, measurement: measure}, nil
				})
			}
			item := &types.FetchedItem{ExternalID: "nextcloud:instance:77", FileName: "source.txt", Content: []byte("actual source bytes")}
			_, err = source.ingestNextcloudItem(context.Background(), &types.DataSource{ID: "ds", TenantID: 7, KnowledgeBaseID: "kb"}, item, map[string]string{"nextcloud_etag": "opaque-etag"}, nil)
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
				require.Zero(t, knowledge.calls)
				require.Zero(t, version.stageCalls)
				entries, e := os.ReadDir(base)
				require.NoError(t, e)
				require.Empty(t, entries)
			}
		})
	}
}
