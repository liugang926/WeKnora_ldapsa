package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type derivedKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	rows []*types.Knowledge
	err  error
}

func (r derivedKnowledgeRepo) ListKnowledgeByKnowledgeBaseID(
	_ context.Context, _ uint64, _ string,
) ([]*types.Knowledge, error) {
	return r.rows, r.err
}

type derivedDataSourceRepo struct {
	interfaces.DataSourceRepository
	sources []*types.DataSource
	err     error
}

type derivedKBRepo struct {
	interfaces.KnowledgeBaseRepository
	marked bool
}

func (r derivedKBRepo) GetKnowledgeBaseByID(_ context.Context, id string) (*types.KnowledgeBase, error) {
	return &types.KnowledgeBase{ID: id, TenantID: 7, EverHadNextcloudSource: r.marked}, nil
}

func (r derivedKBRepo) GetKnowledgeBaseByIDs(_ context.Context, ids []string) ([]*types.KnowledgeBase, error) {
	result := make([]*types.KnowledgeBase, 0, len(ids))
	for _, id := range ids {
		kb, _ := r.GetKnowledgeBaseByID(context.Background(), id)
		result = append(result, kb)
	}
	return result, nil
}

func (r derivedKBRepo) ListKnowledgeBasesByTenantID(_ context.Context, _ uint64) ([]*types.KnowledgeBase, error) {
	kb, _ := r.GetKnowledgeBaseByID(context.Background(), "kb")
	return []*types.KnowledgeBase{kb}, nil
}

type derivedShareRepo struct {
	interfaces.KBShareRepository
}

type rejectingDerivedWikiKB struct {
	interfaces.KnowledgeBaseService
}

func (rejectingDerivedWikiKB) RejectNextcloudDerivedKB(context.Context, string) error {
	return ErrNextcloudDerivedContent
}

type derivedWikiRepo struct {
	interfaces.WikiPageRepository
}

func (derivedWikiRepo) GetByID(context.Context, string) (*types.WikiPage, error) {
	return &types.WikiPage{KnowledgeBaseID: "kb"}, nil
}

func (derivedShareRepo) ListByKnowledgeBase(_ context.Context, kbID string) ([]*types.KnowledgeBaseShare, error) {
	return []*types.KnowledgeBaseShare{{KnowledgeBaseID: kbID, SourceTenantID: 7}}, nil
}

func (r derivedDataSourceRepo) FindByKnowledgeBase(
	_ context.Context, _ string,
) ([]*types.DataSource, error) {
	return r.sources, r.err
}

func TestRejectNextcloudDerivedKBChecksConfigurationAndDocuments(t *testing.T) {
	ordinary := &types.Knowledge{ID: "ordinary", TenantID: 7, KnowledgeBaseID: "kb", Channel: types.ChannelWeb}
	withNextcloud := &types.Knowledge{
		ID:              "nextcloud",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Channel:         types.ConnectorTypeNextcloud,
	}
	withMarker := &types.Knowledge{ID: "marker", TenantID: 7, KnowledgeBaseID: "kb", Metadata: []byte(
		`{"nextcloud_file_id":"42"}`,
	)}
	for _, tc := range []struct {
		name    string
		rows    []*types.Knowledge
		sources []*types.DataSource
		wantErr bool
	}{
		{"ordinary", []*types.Knowledge{ordinary}, nil, false},
		{"new source without documents", nil, []*types.DataSource{{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			Type:            types.ConnectorTypeNextcloud,
		}}, true},
		{"imported document", []*types.Knowledge{withNextcloud}, nil, true},
		{"metadata marker", []*types.Knowledge{withMarker}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := RejectNextcloudDerivedKB(context.Background(),
				&types.KnowledgeBase{ID: "kb", TenantID: 7},
				derivedKnowledgeRepo{rows: tc.rows}, derivedDataSourceRepo{sources: tc.sources})
			if tc.wantErr {
				require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRejectNextcloudDerivedKBFailsClosedOnLookupFailure(t *testing.T) {
	boom := errors.New("database unavailable")
	err := RejectNextcloudDerivedKB(context.Background(),
		&types.KnowledgeBase{ID: "kb", TenantID: 7},
		derivedKnowledgeRepo{}, derivedDataSourceRepo{err: boom})
	require.ErrorIs(t, err, boom)
	err = RejectNextcloudDerivedKB(context.Background(),
		&types.KnowledgeBase{ID: "kb", TenantID: 7},
		derivedKnowledgeRepo{err: boom}, derivedDataSourceRepo{})
	require.ErrorIs(t, err, boom)
}

func TestNextcloudSourceManagedMutationGuardAllowsOrdinaryRows(t *testing.T) {
	require.NoError(t, rejectNextcloudSourceManagedMutation(&types.Knowledge{
		Channel:  types.ChannelWeb,
		Metadata: types.JSON(`{"datasource_id":"ordinary","external_id":"doc"}`),
	}))
	require.ErrorIs(t, rejectNextcloudSourceManagedMutation(&types.Knowledge{
		Channel:  types.ChannelWeb,
		Metadata: types.JSON(`{"nextcloud_file_id":"42"}`),
	}), ErrNextcloudSourceManagedMutation)
	require.Equal(t, 409, ErrNextcloudSourceManagedMutation.HTTPCode)
}

func TestRejectNextcloudDerivedKBRetainsDenialAfterSourceHardDelete(t *testing.T) {
	err := RejectNextcloudDerivedKB(context.Background(),
		&types.KnowledgeBase{ID: "kb", TenantID: 7, EverHadNextcloudSource: true},
		derivedKnowledgeRepo{}, derivedDataSourceRepo{})
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
}

func TestNextcloudKnowledgeBaseCannotBeSharedOrUseExistingShare(t *testing.T) {
	svc := &kbShareService{
		kbRepo:    derivedKBRepo{marked: true},
		kgRepo:    derivedKnowledgeRepo{},
		dsRepo:    derivedDataSourceRepo{},
		shareRepo: derivedShareRepo{},
	}
	_, err := svc.ShareKnowledgeBase(context.Background(), "kb", "org", "user", 7, types.OrgRoleViewer)
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	_, shared, err := svc.CheckTenantKBPermission(context.Background(), "kb", 8, types.TenantRoleViewer)
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	require.False(t, shared)
}

func TestWikiServiceReadsRejectNextcloudBeforeRepositoryAccess(t *testing.T) {
	svc := &wikiPageService{kbService: rejectingDerivedWikiKB{}, repo: derivedWikiRepo{}}
	ctx := context.Background()
	_, err := svc.GetPageBySlug(ctx, "kb", "page")
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	_, err = svc.GetPageByID(ctx, "page")
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	_, err = svc.GetIndex(ctx, "kb")
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	_, err = svc.SearchPages(ctx, "kb", "query", 10)
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	_, err = svc.ListPages(ctx, &types.WikiPageListRequest{KnowledgeBaseID: "kb"})
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
	_, err = svc.GetGraph(ctx, &types.WikiGraphRequest{KnowledgeBaseID: "kb"})
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
}

func TestAgentShareRejectsPersistentlyMarkedKB(t *testing.T) {
	for _, mode := range []string{"selected", "all"} {
		t.Run(mode, func(t *testing.T) {
			agent := &types.CustomAgent{
				ID: "agent-1", TenantID: 7,
				Config: types.CustomAgentConfig{
					KBSelectionMode: mode, KnowledgeBases: []string{"kb"},
				},
			}
			svc := &agentShareService{
				kbRepo:              derivedKBRepo{marked: true},
				kgRepo:              derivedKnowledgeRepo{},
				dsRepo:              derivedDataSourceRepo{},
				agentRepo:           &builtinShareAgentRepo{agent: agent},
				shareRepo:           builtinShareRepo{},
				enforceSourcePolicy: true,
			}
			_, err := svc.ShareAgent(shareMgmtCtx(
				types.TenantRoleAdmin,
			), "agent-1", "org-1", "user-1", 7, types.OrgRoleViewer)
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			_, err = svc.GetSharedAgentForTenant(context.Background(), 8, types.TenantRoleViewer, "agent-1", 7)
			require.ErrorIs(t, err, ErrAgentSharePermission)
			svc.kbRepo = derivedKBRepo{marked: false}
			require.NoError(t, svc.rejectNextcloudAgentScope(context.Background(), agent),
				"an ordinary knowledge base remains shareable")
		})
	}
}

func TestNextcloudTransferPreflightDoesNotMutateSource(t *testing.T) {
	for _, operation := range []access.KBTransferOperation{access.KBTransferClone, access.KBTransferMove} {
		t.Run(string(operation), func(t *testing.T) {
			f := transferFixture(t, operation)
			require.NoError(t, f.db.Model(&types.Knowledge{}).Where("id = ?", "doc").
				Update("channel", types.ConnectorTypeNextcloud).Error)
			var err error
			if operation == access.KBTransferClone {
				_, err = f.svc.planKnowledgeClone(f.ctx, f.kbs.values["kb"], f.kbs.values["other"])
			} else {
				_, err = f.svc.planKnowledgeMove(f.ctx, f.kbs.values["kb"], f.kbs.values["other"], []string{
					"doc",
				}, "reuse_vectors")
			}
			require.ErrorIs(t, err, ErrNextcloudDerivedContent)
			row, getErr := f.repo.GetKnowledgeByID(f.ctx, 7, "doc")
			require.NoError(t, getErr)
			require.Equal(t, "kb", row.KnowledgeBaseID)
			require.Zero(t, f.chunkRepo.writes)
		})
	}
}

func TestCloneRejectsKBMarkerAfterAllSourceRowsAreGone(t *testing.T) {
	f := transferFixture(t, access.KBTransferClone)
	f.kbs.values["kb"].EverHadNextcloudSource = true
	require.NoError(t, f.db.Unscoped().Where("id = ?", "doc").Delete(&types.Knowledge{}).Error)
	_, err := f.svc.planKnowledgeClone(f.ctx, f.kbs.values["kb"], f.kbs.values["other"])
	require.ErrorIs(t, err, ErrNextcloudDerivedContent)
}
