package service

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"github.com/xuri/excelize/v2"
)

// Read an actual cell at the DocReader boundary so the searchable text can
// only come from the workbook delivered by the reparse worker.
type webDAVWorkbookReader struct{ interfaces.DocumentReader }

func (webDAVWorkbookReader) Read(_ context.Context, req *types.ReadRequest) (*types.ReadResult, error) {
	book, err := excelize.OpenReader(bytes.NewReader(req.FileContent))
	if err != nil {
		return nil, err
	}
	defer func() { _ = book.Close() }()
	cell, err := book.GetCellValue(book.GetSheetList()[0], "A1")
	return &types.ReadResult{MarkdownContent: cell}, err
}

type webDAVReparseGroups struct{ backgroundTaskGroupAccessStub }

func (*webDAVReparseGroups) EffectiveTenantRole(
	context.Context, string, uint64, time.Time,
) (types.EffectiveTenantRole, error) {
	return types.EffectiveTenantRole{Member: true, Role: types.TenantRoleAdmin}, nil
}

func TestWebDAVLocalReparse(t *testing.T) {
	for _, scenario := range []string{
		"valid", "changed artifact", "source moved", "changed controls", "missing write grant", "inactive source actor",
	} {
		t.Run(scenario, func(t *testing.T) {
			s, store, db, queue, ctx := knowledgeInputServiceFixture(t)
			kb, err := s.kbService.GetKnowledgeBaseByID(ctx, "kb")
			require.NoError(t, err)
			require.NoError(t, db.AutoMigrate(&types.DataSource{}, &types.User{}))
			source := &types.DataSource{
				ID: "webdav-source", TenantID: 7, KnowledgeBaseID: kb.ID, Type: types.ConnectorTypeNextcloudWebDAV,
			}
			require.NoError(t, db.Create(source).Error)
			body, err := os.ReadFile("../../infrastructure/docparser/anydoc/testdata/upstream/xlsx/sheet.xlsx")
			require.NoError(t, err)
			files := &imageReaderFiles{body: body}
			s.fileSvc = files
			s.chunkService = replaceFileChunkService{}
			s.documentReader = webDAVWorkbookReader{}
			index := &parentChildRetrieveEngine{}
			s.retrieveEngine = parentChildRetrieveRegistry{engine: index}
			k := &types.Knowledge{
				ID: "webdav-file", TenantID: 7, KnowledgeBaseID: kb.ID, Type: "file", Channel: source.Type,
				Title: "sheet.xlsx", FileName: "sheet.xlsx", FileType: "xlsx", FilePath: "stored/sheet.xlsx",
				ParseStatus: types.ParseStatusFailed, EnableStatus: "disabled", EmbeddingModelID: kb.EmbeddingModelID,
				Metadata: types.JSON(`{"datasource_id":"webdav-source","external_id":"webdav-file-id"}`),
			}
			lineage, err := types.NewCompleteSourceLineage()
			require.NoError(t, err)
			groups := &webDAVReparseGroups{}
			s.groupAccess = groups
			repository.ConfigureKnowledgeInputActorGroupAccess(store, groups)
			syncUser := &types.User{ID: "source-editor", TenantID: 7, IsActive: true}
			require.NoError(t, db.Create(syncUser).Error)
			syncCtx := types.WithTaskAuthorization(ctx, 7, types.TaskInitiator{
				UserID: syncUser.ID, CallerTenantID: 7, Role: types.TenantRoleAdmin,
			})
			derived, err := repository.WithDerivedKnowledgeInput(syncCtx, lineage)
			require.NoError(t, err)
			original, err := store.CreateKnowledgeWithInput(derived, k, body)
			require.NoError(t, err)

			var overrides *types.KnowledgeProcessOverrides
			if scenario != "missing write grant" {
				ctx = types.WithTaskAuthorization(ctx, 7, types.TaskInitiator{
					UserID: "reparse-editor", CallerTenantID: 7, Role: types.TenantRoleAdmin,
				})
				ctx, err = access.WithKBTaskWrite(ctx, kb, 7)
				require.NoError(t, err)
			}
			switch scenario {
			case "inactive source actor":
				require.NoError(t, db.Model(syncUser).Update("is_active", false).Error)
			case "changed artifact":
				files.body = []byte("changed after import")
			case "source moved":
				require.NoError(t, db.Model(source).Update("knowledge_base_id", "different-kb").Error)
			case "changed controls":
				disabled := false
				overrides = &types.KnowledgeProcessOverrides{SummaryEnabled: &disabled}
			}
			previous := &types.Chunk{
				ID: "old-chunk", TenantID: k.TenantID, KnowledgeBaseID: kb.ID,
				KnowledgeID: k.ID, Content: "previous output",
			}
			require.NoError(t, db.Create(previous).Error)
			_, err = s.ReparseKnowledge(ctx, k.ID, overrides)
			if scenario != "valid" {
				require.Error(t, err)
				require.Empty(t, queue.tasks)
				require.NoError(t, db.First(&types.Chunk{}, "id = ?", previous.ID).Error)
				fresh, err := s.repo.GetKnowledgeByID(ctx, k.TenantID, k.ID)
				require.NoError(t, err)
				require.Equal(t, types.ParseStatusFailed, fresh.ParseStatus)
				return
			}
			require.NoError(t, err)
			require.NotEmpty(t, queue.tasks)
			task := queue.tasks[0]
			require.Equal(t, types.TypeDocumentProcess, task.Type())
			var payload types.DocumentProcessPayload
			require.NoError(t, json.Unmarshal(task.Payload(), &payload))
			require.Equal(t, original, payload.OriginalInput)
			require.Equal(t, "reparse-editor", payload.Initiator.UserID)
			require.NoError(t, s.ProcessDocument(context.Background(), task))
			var chunks []types.Chunk
			require.NoError(t, db.Where(
				"tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ? AND chunk_type = ?",
				7, kb.ID, k.ID, types.ChunkTypeText,
			).Find(&chunks).Error)
			require.Len(t, chunks, 1)
			require.Contains(t, chunks[0].Content, "Kind")
			require.NotEqual(t, previous.ID, chunks[0].ID)
			require.Len(t, index.indexed, 1)
			require.Equal(t, k.ID, index.indexed[0].KnowledgeID)
			require.Equal(t, kb.ID, index.indexed[0].KnowledgeBaseID)
			require.Equal(t, chunks[0].ID, index.indexed[0].ChunkID)
			require.Contains(t, index.indexed[0].Content, "Kind")
			var receipts int64
			require.NoError(t, db.Table("knowledge_input_material_receipts").
				Where("row_kind = 'chunk' AND row_id = ? AND admission_id = ?", chunks[0].ID, original.AdmissionID).
				Count(&receipts).Error)
			require.EqualValues(t, 1, receipts)
		})
	}
}
