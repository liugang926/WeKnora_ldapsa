package tools

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/readlease"
	"github.com/Tencent/WeKnora/internal/application/repository"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type leasedAgentKnowledgeService struct {
	*readDocKnowledgeService
	store   *repository.NextcloudContentLeaseStore
	revoked atomic.Bool
}

func TestQueryKnowledgeGraphPreservesPublicSourceFailure(t *testing.T) {
	tool, _, _, _, _ := graphLeaseFixture(t)
	tool.scopeEnforced = false
	tool.scopeKnowledgeService = nil
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
	require.Error(t, err)
	require.False(t, result.Success)
	require.Equal(t, "Nextcloud graph source document is unavailable", result.Error)
	require.Equal(t, result.Error, apperrors.PublicMessage(err))
	require.Equal(t, "nextcloud graph source document is unavailable", err.Error())
	require.NotContains(t, result.Output, "classified graph passage")
}

func (s *leasedAgentKnowledgeService) BeginNextcloudRead(ctx context.Context,
	targets types.SearchTargets,
) (*readlease.NextcloudReadGuard, error) {
	scopes := make([]readlease.NextcloudKBReadScope, 0, len(targets))
	for _, target := range targets {
		scopes = append(scopes, readlease.NextcloudKBReadScope{
			TenantID: target.TenantID, KBID: target.KnowledgeBaseID,
		})
	}
	return readlease.BeginNextcloudKBRead(ctx, s.store, scopes)
}

func (s *leasedAgentKnowledgeService) CheckNextcloudReadTargets(context.Context,
	types.SearchTargets,
) error {
	if s.revoked.Load() {
		return errors.New("group read grant withdrawn")
	}
	return nil
}

func (s *leasedAgentKnowledgeService) CheckKnowledgePublication(context.Context,
	*types.Knowledge,
) error {
	if s.revoked.Load() {
		return errors.New("source publication withdrawn")
	}
	return nil
}

func agentReadTestStore(t *testing.T) (*repository.NextcloudContentLeaseStore, *gorm.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent-read.db")
	db, err := gorm.Open(sqlite.Open("file:"+path+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = sqlDB.Close() })
	script, err := os.ReadFile("../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql")
	require.NoError(t, err)
	require.NoError(t, db.Exec(string(script)).Error)
	return repository.NewNextcloudContentLeaseStore(db), db
}

func TestAgentReadDocumentLeaseAndMidReadRevocation(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		name := "authorized"
		if revoke {
			name = "revoked during chunk load"
		}
		t.Run(name, func(t *testing.T) {
			store, db := agentReadTestStore(t)
			tool, chunkRepo := newReadDocumentFixture(2)
			doc := tool.knowledgeService.(*readDocKnowledgeService).docs["doc-1"]
			doc.Channel = types.ConnectorTypeNextcloud
			metadata, err := json.Marshal(map[string]string{
				"datasource_id": "ds-1", "external_id": "nextcloud:instance:41",
				"nextcloud_file_id": "41",
			})
			require.NoError(t, err)
			doc.Metadata = types.JSON(metadata)
			svc := &leasedAgentKnowledgeService{
				readDocKnowledgeService: tool.knowledgeService.(*readDocKnowledgeService), store: store,
			}
			tool.knowledgeService = svc
			if revoke {
				chunkRepo.onList = func() {
					chunkRepo.onList = nil
					svc.revoked.Store(true)
					scope := repository.NextcloudContentScope{
						TenantID:        7,
						KnowledgeBaseID: "kb-1", KnowledgeID: "doc-1",
						DataSourceID: "ds-1", ExternalID: "nextcloud:instance:41",
					}
					require.NoError(t, store.RetireKnowledge(context.Background(), scope))
				}
			}
			result, err := tool.Execute(context.Background(), json.RawMessage(`{"id":"doc-1"}`))
			if revoke {
				require.Error(t, err)
				require.False(t, result.Success)
				require.NotContains(t, result.Output, "Section 0 body")
			} else {
				require.NoError(t, err)
				require.True(t, result.Success)
				require.Contains(t, result.Output, "Section 0 body")
			}
			var leases, active int64
			require.NoError(t, db.Table("nextcloud_content_leases").Count(&leases).Error)
			require.EqualValues(t, 2, leases)
			require.NoError(t, db.Table("nextcloud_content_leases").
				Where("released_at_ms IS NULL").Count(&active).Error)
			require.Zero(t, active)
		})
	}
}

func TestAgentListDocumentsPinsVisibleSourceRows(t *testing.T) {
	store, db := agentReadTestStore(t)
	tool := newListDocumentsFixture()
	svc := &leasedAgentKnowledgeService{
		readDocKnowledgeService: tool.knowledgeService.(*readDocKnowledgeService), store: store,
	}
	tool.knowledgeService = svc
	doc := svc.docs["doc-a"]
	doc.TenantID = 7
	doc.Channel = types.ConnectorTypeNextcloud
	doc.Metadata = types.JSON(`{"datasource_id":"ds-1","external_id":"nextcloud:instance:41","nextcloud` +
		`_file_id":"41"}`)
	result, err := tool.Execute(context.Background(), json.RawMessage(`{"knowledge_base_id":"kb-1"}`))
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Contains(t, result.Output, "Alpha Guide")
	var leases, active int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&leases).Error)
	require.EqualValues(t, 2, leases)
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}

type graphLeaseKBService struct {
	*stubKnowledgeBaseService
	onSearch func(context.Context) error
}

func (s *graphLeaseKBService) HybridSearch(ctx context.Context, _ string,
	_ types.SearchParams,
) ([]*types.SearchResult, error) {
	if s.onSearch != nil {
		if err := s.onSearch(ctx); err != nil {
			return nil, err
		}
	}
	return s.results, nil
}

type graphLeaseKnowledgeService struct {
	*leasedAgentKnowledgeService
	onGet func(context.Context)
}

func (s *graphLeaseKnowledgeService) GetKnowledgeByIDOnly(ctx context.Context,
	id string,
) (*types.Knowledge, error) {
	if s.onGet != nil {
		s.onGet(ctx)
	}
	return s.readDocKnowledgeService.GetKnowledgeByIDOnly(ctx, id)
}

func graphLeaseFixture(t *testing.T) (*QueryKnowledgeGraphTool,
	*graphLeaseKnowledgeService, *repository.NextcloudContentLeaseStore, *gorm.DB,
	*graphLeaseKBService,
) {
	t.Helper()
	store, db := agentReadTestStore(t)
	readTool, _ := newReadDocumentFixture(1)
	knowledge := readTool.knowledgeService.(*readDocKnowledgeService)
	doc := knowledge.docs["doc-1"]
	doc.Channel = types.ConnectorTypeNextcloud
	doc.Metadata = types.JSON(`{"datasource_id":"ds-1","external_id":"nextcloud:instance:41","nextcloud` +
		`_file_id":"41"}`)
	svc := &graphLeaseKnowledgeService{leasedAgentKnowledgeService: &leasedAgentKnowledgeService{
		readDocKnowledgeService: knowledge, store: store,
	}}
	kb := &graphLeaseKBService{stubKnowledgeBaseService: &stubKnowledgeBaseService{
		kb: &types.KnowledgeBase{
			ID: "kb-1", TenantID: 7,
			ExtractConfig: &types.ExtractConfig{
				Enabled: true,
				Nodes:   []*types.GraphNode{{Name: "engine"}},
			},
		},
		results: []*types.SearchResult{{
			ID: "chunk-1", KnowledgeID: doc.ID,
			KnowledgeBaseID: doc.KnowledgeBaseID, KnowledgeTitle: doc.Title,
			KnowledgeChannel: doc.Channel, Metadata: doc.GetMetadata(),
			Content: "classified graph passage", Score: 0.9,
		}},
	}}
	targets := types.SearchTargets{
		{Type: types.SearchTargetTypeKnowledgeBase, KnowledgeBaseID: "kb-1", TenantID: 7},
		{Type: types.SearchTargetTypeKnowledgeBase, KnowledgeBaseID: "kb-2", TenantID: 7},
	}
	tool := NewQueryKnowledgeGraphTool(kb, targets).WithKnowledgeScope(svc)
	return tool, svc, store, db, kb
}

func TestQueryKnowledgeGraphHoldsLeaseThroughToolOutput(t *testing.T) {
	for _, retire := range []bool{false, true} {
		name := "authorized"
		if retire {
			name = "retired before output"
		}
		t.Run(name, func(t *testing.T) {
			tool, svc, store, db, kb := graphLeaseFixture(t)
			doc := svc.docs["doc-1"]
			scope := repository.NextcloudContentScope{
				TenantID:        7,
				KnowledgeBaseID: "kb-1", KnowledgeID: doc.ID,
				DataSourceID: "ds-1", ExternalID: "nextcloud:instance:41",
			}
			kb.onSearch = func(ctx context.Context) error {
				guard := readlease.NextcloudReadGuardFromContext(ctx)
				if guard == nil {
					return errors.New("graph retrieval has no outer read lease")
				}
				if retire {
					// The broad lease must protect this result even if its exact
					// generation has not yet been pinned when retirement wins.
					return nil
				}
				return guard.PinKnowledge(doc)
			}
			if retire {
				var nowMS int64
				require.NoError(t, db.Raw(`SELECT CAST(strftime('%s','now') AS INTEGER)*1000`).Scan(&nowMS).Error)
				require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
					(tenant_id, knowledge_base_id, activated_at_ms, legacy_drained_at_ms,
					 reader_revision, builder_revision) VALUES (7, 'kb-1', ?, ?, 'test', 'test')`,
					nowMS-1000, nowMS).Error)
				svc.onGet = func(ctx context.Context) {
					require.NoError(t, store.RetireKnowledge(ctx, scope))
					var retiredMS int64
					require.NoError(t, db.Raw(`SELECT retired_at_ms FROM nextcloud_content_fences
						WHERE tenant_id`+
						` = 7 AND knowledge_base_id = 'kb-1' AND knowledge_id = 'doc-1'`).
						Scan(&retiredMS).Error)
					_, err := store.ClaimKnowledgeGC(ctx, scope, time.UnixMilli(retiredMS), time.Minute)
					require.ErrorIs(t, err, repository.ErrNextcloudContentGCBusy,
						"the outer graph lease must block a physical GC claim")
				}
			}
			result, err := tool.Execute(context.Background(), json.RawMessage(
				`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
			if retire {
				require.Error(t, err)
				require.False(t, result.Success)
				require.NotContains(t, result.Output, "classified graph passage")
			} else {
				require.NoError(t, err)
				require.True(t, result.Success)
				require.Contains(t, result.Output, "classified graph passage")
			}
			var active int64
			require.NoError(t, db.Table("nextcloud_content_leases").
				Where("released_at_ms IS NULL").Count(&active).Error)
			require.Zero(t, active)
			var otherKB int64
			require.NoError(t, db.Table("nextcloud_content_leases").
				Where("knowledge_base_id = ?", "kb-2").Count(&otherKB).Error)
			require.Zero(t, otherKB, "the tool may lease only requested KBs")
		})
	}
}

func TestQueryKnowledgeGraphRejectsRevokedScopeBeforeOutput(t *testing.T) {
	tool, svc, _, db, kb := graphLeaseFixture(t)
	graphDoc := svc.docs["doc-1"]
	kb.onSearch = func(ctx context.Context) error {
		guard := readlease.NextcloudReadGuardFromContext(ctx)
		if guard == nil {
			return errors.New("graph retrieval has no outer read lease")
		}
		return guard.PinKnowledge(graphDoc)
	}
	svc.onGet = func(context.Context) { svc.revoked.Store(true) }
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
	require.Error(t, err)
	require.False(t, result.Success)
	require.NotContains(t, result.Output, "classified graph passage")
	var active int64
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}

func TestQueryKnowledgeGraphRejectsChangedGenerationBeforeOutput(t *testing.T) {
	tool, svc, _, db, _ := graphLeaseFixture(t)
	// Retrieval captured the old source tuple. A later successful publication
	// must not authorize the old chunk text by pinning the replacement row.
	svc.onGet = func(context.Context) {
		svc.docs["doc-1"].Metadata = types.JSON(
			`{"datasource_id":"ds-1","external_id":"nextcloud:instance:replacement","` +
				`nextcloud_file_id":"41"}`)
	}
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
	require.Error(t, err)
	require.False(t, result.Success)
	require.NotContains(t, result.Output, "classified graph passage")
	var active int64
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}

func TestQueryKnowledgeGraphRejectsStaleSecondChunkOfSameDocument(t *testing.T) {
	tool, svc, _, db, kb := graphLeaseFixture(t)
	staleMetadata := kb.results[0].Metadata
	svc.docs["doc-1"].Metadata = types.JSON(`{"datasource_id":"ds-1","external_id":"nextcloud:instance:replacement","` +
		`nextcloud_file_id":"41"}`)
	kb.results[0].Metadata = svc.docs["doc-1"].GetMetadata()
	kb.results = append(kb.results, &types.SearchResult{
		ID: "stale-chunk", KnowledgeID: "doc-1", KnowledgeBaseID: "kb-1",
		KnowledgeTitle: "Engine Manual", KnowledgeChannel: types.ConnectorTypeNextcloud,
		Metadata: staleMetadata, Content: "stale graph passage", Score: 0.5,
	})
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
	require.Error(t, err)
	require.False(t, result.Success)
	require.NotContains(t, result.Output, "stale graph passage")
	require.NotContains(t, result.Output, "classified graph passage")
	var active int64
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}

func TestQueryKnowledgeGraphRejectsTagRevokedAfterInitialFilter(t *testing.T) {
	tool, svc, _, db, _ := graphLeaseFixture(t)
	tool.searchTargets = types.SearchTargets{{
		Type:            types.SearchTargetTypeKnowledge,
		KnowledgeBaseID: "kb-1", TenantID: 7, TagIDs: []string{"tag-1"},
	}}
	svc.tags = map[string][]*types.KnowledgeTag{
		"doc-1": {{ID: "tag-1"}},
	}
	// The graph backend's first tag check succeeds. The final document reload
	// runs after that filter and sees the tag removed before model output.
	svc.onGet = func(context.Context) { delete(svc.tags, "doc-1") }
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
	require.Error(t, err)
	require.False(t, result.Success)
	require.NotContains(t, result.Output, "classified graph passage")
	var active int64
	require.NoError(t, db.Table("nextcloud_content_leases").
		Where("released_at_ms IS NULL").Count(&active).Error)
	require.Zero(t, active)
}

func TestQueryKnowledgeGraphRejectsOutOfScopeKBWithoutLease(t *testing.T) {
	tool, _, _, db, _ := graphLeaseFixture(t)
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-outside"],"query":"engine"}`))
	require.Error(t, err)
	require.False(t, result.Success)
	var count int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&count).Error)
	require.Zero(t, count)
}

func TestQueryKnowledgeGraphRejectsNextcloudWithoutLeaseProvider(t *testing.T) {
	tool, _, _, db, kb := graphLeaseFixture(t)
	// A malformed search row may have lost every Nextcloud marker. Scoped
	// production calls must still fail closed when the service is absent.
	kb.results[0].KnowledgeChannel = ""
	kb.results[0].Metadata = nil
	tool.scopeKnowledgeService = nil
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
	require.Error(t, err)
	require.False(t, result.Success)
	require.NotContains(t, result.Output, "classified graph passage")
	var count int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&count).Error)
	require.Zero(t, count)
}

func TestQueryKnowledgeGraphUnscopedOrdinaryResultsRemainReadable(t *testing.T) {
	tool, svc, _, db, kb := graphLeaseFixture(t)
	tool.scopeEnforced = false
	svc.docs["doc-1"].Channel = ""
	svc.docs["doc-1"].Metadata = nil
	kb.results[0].KnowledgeChannel = ""
	kb.results[0].Metadata = nil
	result, err := tool.Execute(context.Background(), json.RawMessage(
		`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Contains(t, result.Output, "classified graph passage")
	var count int64
	require.NoError(t, db.Table("nextcloud_content_leases").Count(&count).Error)
	require.Zero(t, count, "unscoped ordinary results need no source lease")
}

func TestQueryKnowledgeGraphLegacyUnscopedNilService(t *testing.T) {
	for _, empty := range []bool{false, true} {
		name := "ordinary result"
		if empty {
			name = "empty result"
		}
		t.Run(name, func(t *testing.T) {
			tool, _, _, _, kb := graphLeaseFixture(t)
			tool.scopeEnforced = false
			tool.scopeKnowledgeService = nil
			if empty {
				kb.results = nil
			} else {
				kb.results[0].KnowledgeChannel = ""
				kb.results[0].Metadata = nil
			}
			result, err := tool.Execute(context.Background(), json.RawMessage(
				`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
			require.NoError(t, err)
			require.True(t, result.Success)
		})
	}
}

func TestQueryKnowledgeGraphNonProviderServiceRejectsOnlySourceRows(t *testing.T) {
	for _, source := range []bool{false, true} {
		name := "ordinary"
		if source {
			name = "Nextcloud"
		}
		t.Run(name, func(t *testing.T) {
			tool, svc, _, _, kb := graphLeaseFixture(t)
			tool.scopeKnowledgeService = svc.readDocKnowledgeService
			if !source {
				svc.docs["doc-1"].Channel = ""
				svc.docs["doc-1"].Metadata = nil
				kb.results[0].KnowledgeChannel = ""
				kb.results[0].Metadata = nil
			}
			result, err := tool.Execute(context.Background(), json.RawMessage(
				`{"knowledge_base_ids":["kb-1"],"query":"engine"}`))
			if source {
				require.Error(t, err)
				require.False(t, result.Success)
				require.NotContains(t, result.Output, "classified graph passage")
			} else {
				require.NoError(t, err)
				require.True(t, result.Success)
				require.Contains(t, result.Output, "classified graph passage")
			}
		})
	}
}
