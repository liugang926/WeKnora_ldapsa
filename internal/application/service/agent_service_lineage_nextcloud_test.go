//go:build sqlite_fts5

package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestAgentPromptActualNextcloudNonManagerViewerAndRevocation(t *testing.T) {
	h := newHistoryVectorActualFixture(t)
	db, store := h.m.f.db, h.m.f.store
	// A genuine new human-name producer is distinct from adopting an old raw
	// fixture label. All file/config bodies still come from the actual NC chain.
	require.NoError(
		t,
		db.Model(&types.TenantMember{}).
			Where("tenant_id=? AND user_id=?", 7, "enrich-human").
			Update("role", types.TenantRoleAdmin).
			Error,
	)
	admin := historyVectorActualCaller(t, h.m, "enrich-human", types.TenantRoleAdmin)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id=?", h.source.KnowledgeBaseID).Take(&kb).Error)
	kb.Name = "Actual Nextcloud metadata for authorized viewer"
	producer := repository.WithIndependentKnowledgeBaseNameInput(
		repository.WithIndependentKnowledgeInput(admin),
		kb.ID,
		kb.Name,
	)
	require.NoError(t, store.UpdateKnowledgeBaseWithInput(producer, &kb))
	require.True(t, kb.EverHadNextcloudSource)
	require.NoError(
		t,
		db.Create(
			&types.ResourceAccessPolicy{
				ID:           "agent-source-policy",
				TenantID:     7,
				ResourceType: types.GroupResourceTypeKnowledgeBase,
				ResourceID:   kb.ID,
				Mode:         types.ResourceAccessRestricted,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.ResourceGroupGrant{
				ID:               "agent-source-viewer-grant",
				TenantID:         7,
				ResourceType:     types.GroupResourceTypeKnowledgeBase,
				ResourceID:       kb.ID,
				DirectoryGroupID: "history-target-group",
				Permission:       types.ResourcePermissionRead,
				Origin:           types.GrantOriginManual,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Model(&types.TenantMember{}).
			Where("tenant_id=? AND user_id=?", 7, "enrich-human").
			Update("role", types.TenantRoleViewer).
			Error,
	)
	viewer := historyVectorActualCaller(t, h.m, "enrich-human", types.TenantRoleViewer)
	viewer = WithMessageMaterialOutputBoundary(viewer)
	defer CloseMessageMaterialOutputBoundary(viewer)
	h.m.f.s.kbService = h.kb
	h.m.f.s.contentLeases = store.ContentLeaseStore()
	h.m.f.s.knowledgeInputs = store
	service := &agentService{knowledgeService: h.m.f.s, messageService: h.messages}
	settings := &types.AgentConfig{
		KnowledgeBases: []string{kb.ID},
		SearchTargets: types.SearchTargets{
			&types.SearchTarget{Type: types.SearchTargetTypeKnowledgeBase, TenantID: 7, KnowledgeBaseID: kb.ID},
		},
	}
	guard, err := service.prepareAgentPromptInputs(viewer, settings)
	require.NoError(
		t,
		err,
		"KB metadata must not require the connector's private manager capability or enumerate personal files",
	)
	held, err := holdAgentPromptInputs(viewer, guard)
	require.NoError(t, err)
	require.True(t, held)
	require.Len(t, guard.kbInfos, 1)
	require.Equal(t, kb.Name, guard.kbInfos[0].Name)
	require.True(t, guard.kbInfos[0].OmitDocumentMetadata)
	require.Empty(t, guard.documents)
	require.NoError(t, CheckMessageMaterialOutputBoundary(viewer))
	require.NoError(t, db.Where("id=?", "agent-source-viewer-grant").Delete(&types.ResourceGroupGrant{}).Error)
	require.Error(
		t,
		CheckMessageMaterialOutputBoundary(viewer),
		"withdrawing only the exact source-KB grant must block buffered HTTP metadata",
	)
}
