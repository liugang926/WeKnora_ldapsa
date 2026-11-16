package service

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	filesvc "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestIngestItem_QuotedFilename(t *testing.T) {
	// Notion page titles become filenames without replacing double quotes.
	item := &types.FetchedItem{
		ExternalID:  "notion-page",
		Title:       `Q4 "Final"`,
		FileName:    `Q4 "Final".md`,
		Content:     []byte("# Q4 report\n\n季度报告\n"),
		ContentType: "text/markdown",
	}
	f, ds := connectorFileFixture(t, types.ChannelNotion)
	knowledge := importNewConnectorFile(t, f, ds, item)
	require.Equal(t, item.FileName, knowledge.FileName)
	require.Equal(t, item.FileName, knowledge.Title)
	file, err := f.s.fileSvc.GetFile(f.ctx, knowledge.FilePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	got, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, item.Content, got)
}

func connectorFileFixture(t *testing.T, kind string) (*enrichmentTestFixture, *types.DataSource) {
	t.Helper()
	f := newEnrichmentTestFixture(t, false)
	caller := types.CallerFromContext(f.ctx)
	f.ctx = context.WithValue(f.ctx, types.UserIDContextKey, caller.UserID)
	f.ctx = context.WithValue(f.ctx, types.TenantInfoContextKey, &types.Tenant{ID: caller.TenantID})
	f.s.groupAccess = NewGroupAccessService(repository.NewGroupAccessRepository(f.db))
	storageRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	f.s.fileSvc = filesvc.NewLocalFileService(storageRoot, "")
	cfg, err := (&types.DataSourceConfig{Type: kind}).ToJSON()
	require.NoError(t, err)
	ds := &types.DataSource{
		ID: "ds-1", TenantID: caller.TenantID, KnowledgeBaseID: f.k.KnowledgeBaseID,
		Type: kind, Config: cfg, SyncMode: types.SyncModeFull, Status: types.DataSourceStatusActive,
	}
	require.NoError(t, f.db.AutoMigrate(&types.DataSource{}))
	require.NoError(t, f.db.Create(ds).Error)
	return f, ds
}

func importNewConnectorFile(
	t *testing.T, f *enrichmentTestFixture, ds *types.DataSource, item *types.FetchedItem,
) *types.Knowledge {
	t.Helper()
	before := len(f.queue.tasks)
	log, err := syncConnectorFile(f.ctx, t, f.s, ds, item)
	require.NoError(t, err)
	require.Equal(t, 1, log.ItemsCreated, log.ErrorMessage)
	require.Zero(t, log.ItemsSkipped)
	require.Zero(t, log.ItemsFailed)
	knowledge, err := f.s.repo.FindByDataSourceExternalID(
		f.ctx, ds.TenantID, ds.KnowledgeBaseID, ds.ID, item.ExternalID,
	)
	require.NoError(t, err)
	require.NotNil(t, knowledge)
	require.Len(t, f.queue.tasks, before+1)
	var job types.DocumentProcessPayload
	require.NoError(t, json.Unmarshal(f.queue.tasks[before].Payload(), &job))
	require.NotNil(t, job.OriginalInput)
	snapshot, err := f.store.CaptureKnowledgeBuildInputs(f.ctx, types.KnowledgeBuildInputCaptureRequest{
		TenantID: ds.TenantID, KnowledgeBaseID: ds.KnowledgeBaseID,
		KnowledgeID: knowledge.ID, Expected: job.OriginalInput,
	})
	require.NoError(t, err)
	require.NoError(t, repository.KnowledgeInputArtifactMatches(snapshot.Proof, item.Content))
	return knowledge
}
