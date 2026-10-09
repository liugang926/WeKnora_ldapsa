package chatpipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type publicationAwareKnowledgeService struct {
	interfaces.KnowledgeService
	rows map[string]*types.Knowledge
}

func (s *publicationAwareKnowledgeService) GetKnowledgeByIDOnly(_ context.Context, id string) (
	*types.Knowledge,
	error,
) {
	if row := s.rows[id]; row != nil {
		return row, nil
	}
	return nil, errors.New("knowledge not found")
}

func (
	s *publicationAwareKnowledgeService,
) CheckKnowledgePublication(_ context.Context, knowledge *types.Knowledge) error {
	if knowledge.Channel == types.ConnectorTypeNextcloud {
		return errors.New("source publication was withdrawn")
	}
	return nil
}

func TestIntoChatMessageRechecksHistoricalReferencesBeforeModelContext(t *testing.T) {
	service := &publicationAwareKnowledgeService{rows: map[string]*types.Knowledge{
		"withdrawn": {ID: "withdrawn", TenantID: 7, KnowledgeBaseID: "kb-1", Channel: types.ConnectorTypeNextcloud},
		"ordinary":  {ID: "ordinary", TenantID: 7, KnowledgeBaseID: "kb-1", Channel: "web"},
	}}
	manage := &types.ChatManage{
		PipelineRequest: types.PipelineRequest{
			Query: "question", SummaryConfig: types.SummaryConfig{
				ContextTemplate: "Question: {{query}}\nContexts: {{contexts}}",
			},
		},
		PipelineState: types.PipelineState{MergeResult: []*types.SearchResult{
			{ID: "old-chunk", KnowledgeID: "withdrawn", KnowledgeBaseID: "kb-1", Content: "withdrawn secret"},
			{ID: "new-chunk", KnowledgeID: "ordinary", KnowledgeBaseID: "kb-1", Content: "allowed context"},
		}},
	}
	plugin := &PluginIntoChatMessage{knowledgeService: service}
	if err := plugin.OnEvent(
		context.Background(),
		types.INTO_CHAT_MESSAGE,
		manage,
		func() *PluginError { return nil },
	); err !=
		nil {
		t.Fatal(err)
	}
	if len(manage.MergeResult) != 1 || manage.MergeResult[0].KnowledgeID != "ordinary" ||
		strings.Contains(manage.UserContent, "withdrawn secret") ||
		!strings.Contains(manage.UserContent, "allowed context") {
		t.Fatalf(("stale source reference reached model context: results=%" +
			"+v content=%q"), manage.MergeResult, manage.UserContent)
	}
}
