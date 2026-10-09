package mcpserver

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/mark3labs/mcp-go/mcp"
)

type pagedDocumentsForMCP struct {
	interfaces.KnowledgeService
	docs     []*types.Knowledge
	reloaded map[string]*types.Knowledge
}

func (s *pagedDocumentsForMCP) ListPagedKnowledgeByKnowledgeBaseID(
	_ context.Context, _ string, page *types.Pagination, _ types.KnowledgeListFilter,
) (*types.PageResult, error) {
	start := page.Offset()
	if start > len(s.docs) {
		start = len(s.docs)
	}
	end := start + page.Limit()
	if end > len(s.docs) {
		end = len(s.docs)
	}
	return types.NewPageResult(int64(len(s.docs)), page, s.docs[start:end]), nil
}

func (s *pagedDocumentsForMCP) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	if current, ok := s.reloaded[id]; ok {
		return current, nil
	}
	for _, doc := range s.docs {
		if doc.ID == id {
			return doc, nil
		}
	}
	return nil, errors.New("not found")
}

func TestMCPListDocumentsRechecksSourceBeforeOutput(t *testing.T) {
	service := &permissiveDocumentsForMCP{pagedDocumentsForMCP: &pagedDocumentsForMCP{
		docs: []*types.Knowledge{{ID: "doc", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "once public"}},
		reloaded: map[string]*types.Knowledge{"doc": {
			ID: "doc", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "now private",
			Channel: types.ConnectorTypeNextcloud,
		}},
	}}
	srv := &Server{knowledgeService: service}
	docs, total, err := srv.listVisibleDocuments(context.Background(), 1, "kb-1", "", 1, 20)
	if err == nil || len(docs) != 0 || total != 0 {
		t.Fatalf("source change must cancel the listing, docs=%+v total=%d err=%v", docs, total, err)
	}
}

type unavailableKBOnMCPReload struct{ *stubKBService }

func (s *unavailableKBOnMCPReload) GetKnowledgeBaseByIDOnly(context.Context, string) (*types.KnowledgeBase, error) {
	return nil, errors.New("withdrawn")
}

func TestMCPListDocumentsRechecksKBGrantBeforeOutput(t *testing.T) {
	kb := &types.KnowledgeBase{ID: "kb-1", TenantID: 1}
	srv := &Server{
		kbService: &unavailableKBOnMCPReload{stubKBService: &stubKBService{
			kbs: map[string]*types.KnowledgeBase{"kb-1": kb},
		}},
		knowledgeService: &pagedDocumentsForMCP{docs: []*types.Knowledge{
			{ID: "ordinary", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "public title"},
		}},
	}
	ep := &types.MCPEndpoint{ID: "ep", TenantID: 1}
	result, err := srv.handleListDocuments(mcpCallContext(1, ep), mcp.CallToolRequest{
		Params: mcp.CallToolParams{Arguments: map[string]any{"knowledge_base_id": "kb-1"}},
	})
	if err != nil || result == nil || !result.IsError {
		t.Fatalf("withdrawn KB must cancel the result, result=%+v err=%v", result, err)
	}
}

type checkedDocumentsForMCP struct {
	*pagedDocumentsForMCP
	seenCaller string
}

func (s *checkedDocumentsForMCP) CheckKnowledgePublication(ctx context.Context, k *types.Knowledge) error {
	caller := types.CallerFromContext(ctx)
	s.seenCaller = caller.UserID
	if k.Channel == types.ConnectorTypeNextcloud {
		return errors.New("machine principal denied")
	}
	return nil
}

type permissiveDocumentsForMCP struct {
	*pagedDocumentsForMCP
	checked []string
}

func (s *permissiveDocumentsForMCP) CheckKnowledgePublication(_ context.Context, k *types.Knowledge) error {
	s.checked = append(s.checked, k.ID)
	return nil
}

func TestMCPListDocumentsNeverTrustsPermissiveCheckerForNextcloud(t *testing.T) {
	service := &permissiveDocumentsForMCP{pagedDocumentsForMCP: &pagedDocumentsForMCP{docs: []*types.Knowledge{
		{
			ID: "source-channel", TenantID: 1, KnowledgeBaseID: "kb-1", Channel: types.ConnectorTypeNextcloud,
			Title: "private title",
		},
		{
			ID: "source-metadata", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "another private title",
			Metadata: types.JSON(`{"nextcloud_file_id":"42"}`),
		},
		{ID: "ordinary", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "public title"},
	}}}
	srv := &Server{knowledgeService: service}
	docs, total, err := srv.listVisibleDocuments(context.Background(), 1, "kb-1", "", 1, 1)
	if err != nil || total != 1 || len(docs) != 1 || docs[0].ID != "ordinary" {
		t.Fatalf("docs = %+v total=%d err=%v", docs, total, err)
	}
	if len(service.checked) != 2 || service.checked[0] != "ordinary" || service.checked[1] != "ordinary" {
		t.Fatalf("source rows reached permissive checker: %v", service.checked)
	}
}

func TestMCPListDocumentsFiltersNextcloudBeforePagination(t *testing.T) {
	service := &pagedDocumentsForMCP{docs: []*types.Knowledge{
		{ID: "ordinary-1", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "first"},
		{
			ID: "secret", TenantID: 1, KnowledgeBaseID: "kb-1", Channel: types.ConnectorTypeNextcloud,
			Title: "secret Nextcloud title", Metadata: types.JSON(
				`{"datasource_id":"source-1","nextcloud_file_id":"42"}`),
		},
		{ID: "ordinary-2", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "second"},
	}}
	srv := &Server{knowledgeService: service}
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 1, UserID: "human-1"})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "human-1"})
	first, total, err := srv.listVisibleDocuments(ctx, 1, "kb-1", "", 1, 1)
	if err != nil || total != 2 || len(first) != 1 || first[0].ID != "ordinary-1" {
		t.Fatalf("first page = %+v total=%d err=%v", first, total, err)
	}
	second, total, err := srv.listVisibleDocuments(ctx, 1, "kb-1", "", 2, 1)
	if err != nil || total != 2 || len(second) != 1 || second[0].ID != "ordinary-2" {
		t.Fatalf("second page = %+v total=%d err=%v", second, total, err)
	}
}

func TestMCPListDocumentsUsesMachineCallerForProductionChecker(t *testing.T) {
	service := &checkedDocumentsForMCP{pagedDocumentsForMCP: &pagedDocumentsForMCP{docs: []*types.Knowledge{
		{
			ID: "secret", TenantID: 1, KnowledgeBaseID: "kb-1", Channel: types.ConnectorTypeNextcloud,
			Title: "secret Nextcloud title",
		},
		{ID: "ordinary", TenantID: 1, KnowledgeBaseID: "kb-1", Title: "public title"},
	}}}
	srv := &Server{knowledgeService: service}
	docs, total, err := srv.listVisibleDocuments(context.Background(), 1, "kb-1", "", 1, 20)
	if err != nil || total != 1 || len(docs) != 1 || docs[0].ID != "ordinary" {
		t.Fatalf("docs = %+v total=%d err=%v", docs, total, err)
	}
	if service.seenCaller != "system-1" {
		t.Fatalf("publication checker saw caller %q, want synthetic machine", service.seenCaller)
	}
}
