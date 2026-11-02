package service

import (
	"context"
	"errors"
	"github.com/Tencent/WeKnora/internal/application/repository"
	chatpipeline "github.com/Tencent/WeKnora/internal/application/service/chat_pipeline"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEvaluationReadinessActualPipeline(t *testing.T) {
	model := &ragEntryModel{}
	f := NewNormalReferencesHTTPFixtureForTest(t, false, model)
	svc := f.Sessions.(normalReferencesSessionForTest).SessionService.(*sessionService)
	ConfigureGroupAccessUserEligibility(svc.groupAccess, &userService{userRepo: repository.NewUserRepository(f.DB)})
	chatpipeline.NewPluginSearch(svc.eventManager, f.KBs, svc.knowledgeService, svc.chunkService, f.Config, nil, svc.tenantService, nil, nil, nil)
	chatpipeline.NewPluginChatCompletion(svc.eventManager, f.Models)
	var k types.Knowledge
	require.NoError(t, f.DB.Where("knowledge_base_id = ?", "kb").First(&k).Error)
	repo := repository.NewKnowledgeRepository(f.DB)
	set := func(state string, pending int) {
		require.NoError(t, f.DB.Model(&types.Knowledge{}).Where("id = ?", k.ID).Updates(map[string]any{"parse_status": state, "pending_subtasks_count": pending, "enable_status": "enabled"}).Error)
	}
	cm := func() *types.ChatManage {
		c := &types.ChatManage{}
		c.Query = "What is the current employee policy?"
		c.RewriteQuery = c.Query
		c.ChatModelID = "normal-reference-model"
		c.KnowledgeBaseIDs = []string{"kb"}
		c.SearchTargets = types.SearchTargets{{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: "kb"}}
		c.EmbeddingTopK = 2
		c.RerankTopK = 2
		c.SummaryConfig.ContextTemplate = "{{query}}\n{{contexts}}"
		return c
	}
	set(types.ParseStatusProcessing, 1)
	require.Error(t, svc.KnowledgeQAByEvent(f.Ctx, cm(), types.Pipline["rag"]))
	require.Zero(t, model.calls())
	done := make(chan error, 1)
	go func() {
		done <- waitForEvaluationKnowledge(f.Ctx, repo, 7, "kb", k.ID, time.Second, time.Millisecond*5)
	}()
	select {
	case err := <-done:
		t.Fatalf("processing returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	set(types.ParseStatusFinalizing, 1)
	select {
	case err := <-done:
		t.Fatalf("finalizing returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	set(types.ParseStatusCompleted, 0)
	require.NoError(t, <-done)
	c := cm()
	require.NoError(t, svc.KnowledgeQAByEvent(f.Ctx, c, types.Pipline["rag"]))
	require.NotEmpty(t, c.ChatResponse.Content)
	for _, state := range []string{types.ParseStatusFailed, types.ParseStatusCancelled, types.ParseStatusDeleting} {
		set(state, 0)
		require.Error(t, waitForEvaluationKnowledge(f.Ctx, repo, 7, "kb", k.ID, time.Second, time.Millisecond))
	}
	set(types.ParseStatusProcessing, 1)
	require.ErrorIs(t, waitForEvaluationKnowledge(f.Ctx, repo, 7, "kb", k.ID, 20*time.Millisecond, time.Millisecond), context.DeadlineExceeded)
	cancelled, cancel := context.WithCancel(f.Ctx)
	cancel()
	require.True(t, errors.Is(waitForEvaluationKnowledge(cancelled, repo, 7, "kb", k.ID, time.Second, time.Millisecond), context.Canceled))
	set(types.ParseStatusCompleted, 1)
	require.Error(t, waitForEvaluationKnowledge(f.Ctx, repo, 7, "kb", k.ID, time.Second, time.Millisecond))
	set(types.ParseStatusCompleted, 0)
	require.NoError(t, f.DB.Model(&types.Knowledge{}).Where("id=?", k.ID).Update("enable_status", "disabled").Error)
	require.Error(t, waitForEvaluationKnowledge(f.Ctx, repo, 7, "kb", k.ID, time.Second, time.Millisecond))
	require.Error(t, waitForEvaluationKnowledge(f.Ctx, repo, 7, "other-kb", k.ID, time.Second, time.Millisecond))
	require.Error(t, waitForEvaluationKnowledge(f.Ctx, repo, 8, "kb", k.ID, time.Second, time.Millisecond))
}
