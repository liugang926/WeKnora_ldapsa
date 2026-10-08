package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type collectionHTTPKB struct {
	interfaces.KnowledgeBaseService
	db *gorm.DB
}

func (s collectionHTTPKB) GetKnowledgeBaseByID(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	return s.GetKnowledgeBaseByIDOnly(ctx, id)
}
func (s collectionHTTPKB) GetKnowledgeBaseByIDOnly(ctx context.Context, id string) (*types.KnowledgeBase, error) {
	var kb types.KnowledgeBase
	err := s.db.WithContext(ctx).Where("id = ?", id).Take(&kb).Error
	return &kb, err
}
func (s collectionHTTPKB) ListKnowledgeBases(ctx context.Context) ([]*types.KnowledgeBase, error) {
	return s.ListKnowledgeBasesByTenantID(ctx, types.CallerFromContext(ctx).TenantID)
}
func (s collectionHTTPKB) ListKnowledgeBasesByTenantID(ctx context.Context, tenant uint64) ([]*types.KnowledgeBase, error) {
	var rows []*types.KnowledgeBase
	err := s.db.WithContext(ctx).Where("tenant_id = ?", tenant).Find(&rows).Error
	return rows, err
}

type collectionHTTPShare struct {
	interfaces.KBShareService
	kb      *types.KnowledgeBase
	allowed bool
}

func (s *collectionHTTPShare) CheckTenantKBPermission(context.Context, string, uint64, types.TenantRole) (types.OrgMemberRole, bool, error) {
	return types.OrgRoleViewer, s.allowed, nil
}
func (s *collectionHTTPShare) ListSharedKnowledgeBases(context.Context, uint64, types.TenantRole) ([]*types.SharedKnowledgeBaseInfo, error) {
	if !s.allowed {
		return nil, nil
	}
	return []*types.SharedKnowledgeBaseInfo{{KnowledgeBase: s.kb, SourceTenantID: s.kb.TenantID}}, nil
}

type collectionHTTPAgents struct {
	interfaces.AgentShareService
	agent   *types.CustomAgent
	allowed bool
}

func (s *collectionHTTPAgents) GetSharedAgentForTenant(context.Context, uint64, types.TenantRole, string, ...uint64) (*types.CustomAgent, error) {
	if !s.allowed {
		return nil, nil
	}
	return s.agent, nil
}
func (s *collectionHTTPAgents) TenantCanAccessKBViaSomeSharedAgent(context.Context, uint64, types.TenantRole, *types.KnowledgeBase) (bool, error) {
	return false, nil
}

func collectionHTTPFixture(t *testing.T) (*KnowledgeHandler, *gorm.DB, *repository.KnowledgeInputProvenanceStore, *outputHTTPGroups, *collectionHTTPShare, *collectionHTTPAgents) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "collection.db")), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.KnowledgeTag{}, &types.KnowledgeTagRelation{}))
	db.Config.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(t, db.AutoMigrate(&types.Session{}, &types.Message{}))
	require.NoError(t, db.AutoMigrate(&types.User{}, &types.TenantMember{}, &types.TenantAPIKey{}, &types.Directory{}, &types.DirectoryIdentity{}, &types.DirectoryGroup{}, &types.DirectoryGroupMembership{}, &types.TenantGroupRoleGrant{}, &types.ResourceAccessPolicy{}, &types.ResourceGroupGrant{}))
	db.Config.DisableForeignKeyConstraintWhenMigrating = false
	require.NoError(t, db.Exec(`CREATE TABLE nextcloud_source_versions(candidate_knowledge_id TEXT); CREATE TABLE nextcloud_source_revisions(candidate_knowledge_id TEXT)`).Error)
	for _, name := range []string{"000053_knowledge_input_provenance", "000054_knowledge_configuration_history_inputs", "000055_knowledge_actual_config_controls", "000058_knowledge_collection_checkpoints", "000059_knowledge_collection_display_inputs"} {
		path := "../../migrations/sqlite/" + name + ".up.sql"
		script, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(script)).Error)
	}
	store := repository.NewKnowledgeInputProvenanceStore(db)
	require.NoError(t, db.Create(&types.User{ID: "user", Username: "collection-user", Email: "collection@example.test", IsActive: true}).Error)
	for _, tenant := range []uint64{7, 9} {
		require.NoError(t, db.Create(&types.TenantMember{UserID: "user", TenantID: tenant, Role: types.TenantRoleAdmin, Status: types.TenantMemberStatusActive}).Error)
	}
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(actual, service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
		return actual
	}
	repository.ConfigureKnowledgeDisplayInputWriters(store, repository.NewKnowledgeTagRepository(db), repository.NewKnowledgeRepository(db), factory)
	kb := &types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "original human KB", Type: types.KnowledgeBaseTypeDocument}
	sharedKB := &types.KnowledgeBase{ID: "shared-kb", TenantID: 9, Name: "original shared KB", Type: types.KnowledgeBaseTypeDocument}
	for _, base := range []*types.KnowledgeBase{kb, sharedKB} {
		ctx := collectionHumanInputContext(base.TenantID)
		require.NoError(t, store.CreateKnowledgeBaseWithInput(repository.WithIndependentKnowledgeBaseNameInput(repository.WithIndependentKnowledgeInput(ctx), "", base.Name), base))
	}
	for _, k := range []*types.Knowledge{
		{ID: "match", TenantID: 7, KnowledgeBaseID: "kb", Type: "manual", Title: "match original", FileName: "match.md", FileType: "md", FolderPath: "docs/spec", Channel: "web", Source: "manual", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", Metadata: types.JSON(`{"manual":{"content":"original body","version":1}}`), CustomMetadata: types.JSON(`{}`)},
		{ID: "excluded", TenantID: 7, KnowledgeBaseID: "kb", Type: "manual", Title: "excluded original", FileName: "else.pdf", FileType: "pdf", FolderPath: "other", Channel: "web", Source: "manual", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", Metadata: types.JSON(`{"manual":{"content":"excluded body","version":1}}`), CustomMetadata: types.JSON(`{}`)},
		{ID: "shared-match", TenantID: 9, KnowledgeBaseID: "shared-kb", Type: "manual", Title: "match shared original", FileName: "shared.md", FileType: "md", FolderPath: "shared", Channel: "web", Source: "manual", ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled", Metadata: types.JSON(`{"manual":{"content":"shared body","version":1}}`), CustomMetadata: types.JSON(`{}`)},
	} {
		_, err := store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(context.Background()), k, k.Metadata)
		require.NoError(t, err)
	}
	kbs := collectionHTTPKB{db: db}
	shares := &collectionHTTPShare{kb: sharedKB, allowed: true}
	agents := &collectionHTTPAgents{agent: &types.CustomAgent{ID: "agent", TenantID: 9, Config: types.CustomAgentConfig{KBSelectionMode: "selected", KnowledgeBases: []string{"shared-kb"}}}, allowed: true}
	groups := &outputHTTPGroups{allowed: true}
	guard := access.NewNextcloudSourceLineageGuard(nil, kbs, shares, agents, groups, nil)
	kg, err := newCollectionHTTPKnowledgeService(db, kbs, shares)
	require.NoError(t, err)
	h := &KnowledgeHandler{kgService: kg, kbService: kbs, kbShareService: shares, agentShareService: agents}
	ConfigureKnowledgeHandlerCollectionOutput(h, service.NewKnowledgeCollectionOutputAuthorizer(store, guard))
	return h, db, store, groups, shares, agents
}

func collectionHumanInputContext(tenant uint64) context.Context {
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: tenant, UserID: "user", Role: types.TenantRoleAdmin})
	ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
	return types.WithExecutionTenant(ctx, tenant)
}

func serveCollectionHTTP(h *KnowledgeHandler, url string, api bool) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := documentHandlerRouter()
	r.Use(func(c *gin.Context) {
		ctx := c.Request.Context()
		if api {
			ctx = types.WithTenantAPIKeyScope(ctx, types.TenantAPIKeyScope{KeyID: 1, KnowledgeBaseIDs: types.StringArray{"kb"}})
			ctx = context.WithValue(ctx, types.PrincipalContextKey, types.Principal{Type: types.PrincipalAPITenant, ID: "api-key"})
		} else {
			ctx = context.WithValue(ctx, types.PrincipalContextKey, types.Principal{Type: types.PrincipalWebUser, ID: "user"})
		}
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	r.GET("/knowledge-bases/:id/knowledge", h.ListKnowledge)
	r.GET("/knowledge-bases/:id/knowledge/folders", h.ListKnowledgeFolders)
	r.GET("/knowledge/search", h.SearchKnowledge)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
}

func TestKnowledgeCollectionHTTPPositiveListFoldersAndThreeSearchBranches(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		api       bool
		total     int64
	}{
		{"list", "/knowledge-bases/kb/knowledge?keyword=match&file_type=md&folder_path=docs&folder_recursive=true&page_size=1", false, 1},
		{"folders", "/knowledge-bases/kb/knowledge/folders", false, 2},
		{"own_shared_search", "/knowledge/search?keyword=match&limit=1", false, 2},
		{"restricted_api_search", "/knowledge/search?keyword=match&limit=1", true, 1},
		{"shared_agent_search", "/knowledge/search?keyword=match&agent_id=agent&agent_source_tenant_id=9&limit=1", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, _, _, _, _ := collectionHTTPFixture(t)
			w := serveCollectionHTTP(h, tc.url, tc.api)
			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			var body map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			if tc.name == "folders" {
				require.Contains(t, string(body["data"]), `"total_document_count":2`)
				require.Contains(t, string(body["data"]), `"path":"docs/spec"`)
			} else {
				var total int64
				require.NoError(t, json.Unmarshal(body["total"], &total))
				require.Equal(t, tc.total, total)
				require.Contains(t, string(body["data"]), "match")
			}
			require.NotContains(t, w.Body.String(), "admission_id")
			require.NotContains(t, w.Body.String(), "SourceLineage")
		})
	}
}

func TestKnowledgeCollectionHTTPWholeCandidatesAndFinalRemoteBoundary(t *testing.T) {
	for _, change := range []string{"unproved_excluded", "excluded_body", "body_aba", "insert", "delete", "insert_delete_aba", "config_aba", "withdraw_group", "withdraw_share", "withdraw_agent", "final_grant_withdraw"} {
		t.Run(change, func(t *testing.T) {
			h, db, _, groups, shares, agents := collectionHTTPFixture(t)
			url := "/knowledge-bases/kb/knowledge?keyword=match&file_type=md&page_size=1"
			if change == "withdraw_share" {
				url = "/knowledge/search?keyword=match&limit=1"
			}
			if change == "withdraw_agent" {
				shares.allowed = false
				url = "/knowledge/search?keyword=match&agent_id=agent&agent_source_tenant_id=9&limit=1"
			}
			if change == "unproved_excluded" {
				require.NoError(t, db.Create(&types.Knowledge{ID: "legacy-excluded", TenantID: 7, KnowledgeBaseID: "kb", Title: "unaccounted secret", FileName: "hidden.pdf", Channel: "web", CustomMetadata: types.JSON(`{}`)}).Error)
			} else if change == "final_grant_withdraw" {
				queries := 0
				name := "collection_final_grant_withdraw"
				require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
					if tx.Statement.Table == "knowledge_collection_scope_versions" {
						queries++
						if queries == 5 {
							groups.allowed = false
						}
					}
				}))
				t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
			} else {
				fired := false
				groups.hook = func(call int) {
					if fired || call < 5 {
						return
					}
					fired = true
					switch change {
					case "excluded_body":
						require.NoError(t, db.Model(&types.Knowledge{}).Where("id='excluded'").Update("title", "unaccounted mutation").Error)
					case "body_aba":
						require.NoError(t, db.Model(&types.Knowledge{}).Where("id='excluded'").Update("title", "other").Error)
						require.NoError(t, db.Model(&types.Knowledge{}).Where("id='excluded'").Update("title", "excluded original").Error)
					case "insert", "insert_delete_aba":
						require.NoError(t, db.Create(&types.Knowledge{ID: "inserted", TenantID: 7, KnowledgeBaseID: "kb", Title: "hidden insert", Channel: "web", CustomMetadata: types.JSON(`{}`)}).Error)
						if change == "insert_delete_aba" {
							require.NoError(t, db.Unscoped().Where("id='inserted'").Delete(&types.Knowledge{}).Error)
						}
					case "delete":
						require.NoError(t, db.Unscoped().Where("id='excluded'").Delete(&types.Knowledge{}).Error)
					case "config_aba":
						require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='changed' WHERE id='kb'").Error)
						require.NoError(t, db.Exec("UPDATE knowledge_bases SET summary_model_id='' WHERE id='kb'").Error)
					case "withdraw_group":
						groups.allowed = false
					case "withdraw_share":
						shares.allowed = false
					case "withdraw_agent":
						agents.allowed = false
					}
				}
			}
			w := serveCollectionHTTP(h, url, false)
			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "original body")
			require.NotContains(t, w.Body.String(), "match original")
		})
	}
}

func newCollectionHTTPKnowledgeService(db *gorm.DB, kbs interfaces.KnowledgeBaseService, shares interfaces.KBShareService) (interfaces.KnowledgeService, error) {
	return service.NewKnowledgeService(nil, repository.NewKnowledgeRepository(db), nil, kbs, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, shares, nil, nil, nil, nil, nil, nil, nil, nil)
}

func TestKnowledgeCollectionHTTPTenThousandCandidatesRemainUsable(t *testing.T) {
	h, db, _, _, _, _ := collectionHTTPFixture(t)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		store := repository.NewKnowledgeInputProvenanceStore(tx)
		for i := 0; i < 9998; i++ {
			k := &types.Knowledge{ID: fmt.Sprintf("scale-%05d", i), TenantID: 7, KnowledgeBaseID: "kb", Type: "manual", Title: "scale input", FileName: "scale.md", FileType: "md", Channel: "web", Source: "manual", ParseStatus: types.ParseStatusCompleted, CustomMetadata: types.JSON(`{}`)}
			if _, err := store.CreateKnowledgeWithInput(repository.WithIndependentKnowledgeInput(context.Background()), k, "scale original body"); err != nil {
				return err
			}
		}
		return nil
	}))
	start := time.Now()
	w := serveCollectionHTTP(h, "/knowledge-bases/kb/knowledge?page_size=1", false)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"total":10000`)
	require.Contains(t, w.Body.String(), `"page_size":1`)
	t.Logf("real SQLite HTTP selected one row from 10000 originally admitted candidates in %s", time.Since(start))
}

func TestKnowledgeCollectionHTTPOriginalTagFilterAndDisplay(t *testing.T) {
	for _, change := range []string{"positive", "excluded_tag_body_aba", "excluded_relation_aba", "legacy_excluded_tag"} {
		t.Run(change, func(t *testing.T) {
			h, db, store, groups, _, _ := collectionHTTPFixture(t)
			tags := repository.NewKnowledgeTagRepository(db)
			knowledge := repository.NewKnowledgeRepository(db)
			factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
				actual := repository.NewKnowledgeInputProvenanceStore(tx)
				repository.ConfigureKnowledgeInputActorGroupAccess(actual, service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)))
				return actual
			}
			repository.ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
			ctx := collectionHumanInputContext(7)
			for _, tc := range []struct{ id, knowledge string }{{"tag-match", "match"}, {"tag-excluded", "excluded"}} {
				tag := &types.KnowledgeTag{ID: tc.id, TenantID: 7, KnowledgeBaseID: "kb", Name: "original human " + tc.id, Color: "blue", SortOrder: 1}
				createCtx := repository.WithIndependentKnowledgeTagCreateInput(ctx, "kb", tag.Name, tag.Color, tag.SortOrder)
				if change == "legacy_excluded_tag" && tc.id == "tag-excluded" {
					createCtx = repository.WithIndependentKnowledgeInput(ctx)
				}
				require.NoError(t, tags.Create(createCtx, tag))
				require.NoError(t, knowledge.SetKnowledgeTags(repository.WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{tc.knowledge: {tag.ID}}), tc.knowledge, []string{tag.ID}))
			}
			if change == "excluded_tag_body_aba" || change == "excluded_relation_aba" {
				fired := false
				groups.hook = func(call int) {
					if fired || call < 5 {
						return
					}
					fired = true
					if change == "excluded_tag_body_aba" {
						require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id='tag-excluded'").Update("name", "changed").Error)
						require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("id='tag-excluded'").Update("name", "original human tag-excluded").Error)
					} else {
						var relation types.KnowledgeTagRelation
						require.NoError(t, db.Where("knowledge_id='excluded'").Take(&relation).Error)
						require.NoError(t, db.Where("knowledge_id='excluded'").Delete(&types.KnowledgeTagRelation{}).Error)
						require.NoError(t, db.Create(&relation).Error)
					}
				}
			}
			w := serveCollectionHTTP(h, "/knowledge-bases/kb/knowledge?tag_ids=tag-match&keyword=match&page_size=1", false)
			if change == "positive" {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Contains(t, w.Body.String(), "original human tag-match")
				require.Contains(t, w.Body.String(), `"total":1`)
				require.NotContains(t, w.Body.String(), "excluded original")
				search := serveCollectionHTTP(h, "/knowledge/search?keyword=match", false)
				require.Equal(t, http.StatusOK, search.Code, search.Body.String())
				require.Contains(t, search.Body.String(), "original human KB")
				require.Contains(t, search.Body.String(), "original shared KB")
			} else {
				require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
				require.NotContains(t, w.Body.String(), "original human tag-match")
			}
		})
	}
}

func TestKnowledgeCollectionHTTPAuthorizationPrecedesCallerPredicates(t *testing.T) {
	h, db, _, groups, _, _ := collectionHTTPFixture(t)
	groups.allowed = false
	filteredQueries := 0
	name := "collection_predicate_after_authority"
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "LOWER(file_name)") {
			filteredQueries++
		}
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	w := serveCollectionHTTP(h, "/knowledge-bases/kb/knowledge?keyword=match&page_size=1", false)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.Zero(t, filteredQueries, "caller keyword evaluation cannot inspect unauthorised candidate metadata")
}

func TestKnowledgeCollectionHTTPMissingVerifierNeverUsesLegacyReaders(t *testing.T) {
	for _, url := range []string{"/knowledge-bases/kb/knowledge?keyword=match", "/knowledge-bases/kb/knowledge/folders", "/knowledge/search?keyword=match"} {
		h, _, _, _, _, _ := collectionHTTPFixture(t)
		h.collectionOutputAuthorizer = nil
		w := serveCollectionHTTP(h, url, false)
		require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), "original body")
		require.NotContains(t, w.Body.String(), "match original")
	}
}
