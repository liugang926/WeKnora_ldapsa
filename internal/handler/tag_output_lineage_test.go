package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func tagHTTPFixture(
	t *testing.T,
) (*TagHandler, *gorm.DB, *repository.KnowledgeInputProvenanceStore, *outputHTTPGroups) {
	t.Helper()
	_, db, store, groups, shares, agents := collectionHTTPFixture(t)
	b, e := os.ReadFile("../../migrations/sqlite/000060_tag_statistics_checkpoints.up.sql")
	require.NoError(t, e)
	require.NoError(t, db.Exec(string(b)).Error)
	tagRepo := repository.NewKnowledgeTagRepository(db)
	kgRepo := repository.NewKnowledgeRepository(db)
	actualGroups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
	repository.ConfigureKnowledgeInputActorGroupAccess(store, actualGroups)
	factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
		actual := repository.NewKnowledgeInputProvenanceStore(tx)
		repository.ConfigureKnowledgeInputActorGroupAccess(
			actual,
			service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
		)
		return actual
	}
	repository.ConfigureKnowledgeDisplayInputWriters(store, tagRepo, kgRepo, factory)
	kb := collectionHTTPKB{db: db}
	svc, e := service.NewKnowledgeTagService(
		kb,
		tagRepo,
		kgRepo,
		repository.NewChunkRepository(db),
		nil,
		nil,
		nil,
		nil,
		shares,
		nil,
		nil,
	)
	require.NoError(t, e)
	h := NewTagHandler(svc, tagRepo, repository.NewChunkRepository(db))
	guard := access.NewNextcloudSourceLineageGuard(nil, kb, shares, agents, groups, nil, store)
	ConfigureTagHandlerOutputLineage(h, service.NewTagOutputAuthorizer(store, guard))
	return h, db, store, groups
}

func serveTagHTTP(h *TagHandler, ctx context.Context, method, url, body string) *httptest.ResponseRecorder {
	if method != http.MethodGet {
		grant, err := access.ResolveKB(
			ctx,
			access.KBRequest{Caller: types.CallerFromContext(ctx)},
			&types.KnowledgeBase{ID: "kb", TenantID: 7},
			types.OrgRoleEditor,
			nil,
			nil,
		)
		if err == nil {
			ctx = grant.Context(ctx)
		}
	}
	r := documentHandlerRouter()
	r.Use(func(c *gin.Context) { c.Request = c.Request.WithContext(ctx); c.Next() })
	r.GET("/knowledge-bases/:id/tags", h.ListTags)
	r.POST("/knowledge-bases/:id/tags", h.CreateTag)
	r.PUT("/knowledge-bases/:id/tags/:tag_id", h.UpdateTag)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestTagOutputActualHTTPCRUDAndList(t *testing.T) {
	h, db, store, _ := tagHTTPFixture(t)
	ctx := collectionHumanInputContext(7)
	w := serveTagHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/tags",
		`{"name":"actual public name","color":"blue","sort_order":2}`,
	)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "actual public name")
	var tag types.KnowledgeTag
	require.NoError(t, db.Where("name='actual public name'").Take(&tag).Error)
	w = serveTagHTTP(h, ctx, http.MethodPut, "/knowledge-bases/kb/tags/"+tag.ID, `{"color":"green"}`)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "actual public name")
	knowledge := repository.NewKnowledgeRepository(db)
	repository.ConfigureKnowledgeDisplayInputWriters(
		store,
		h.tagRepo,
		knowledge,
		func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
			actual := repository.NewKnowledgeInputProvenanceStore(tx)
			repository.ConfigureKnowledgeInputActorGroupAccess(
				actual,
				service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
			)
			return actual
		},
	)
	require.NoError(
		t,
		knowledge.SetKnowledgeTags(
			repository.WithIndependentKnowledgeTagAssignmentInput(ctx, map[string][]string{"match": {tag.ID}}),
			"match",
			[]string{tag.ID},
		),
	)
	actual, e := store.CaptureKnowledgeOutputInputs(
		ctx,
		types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "match"},
	)
	require.NoError(t, e)
	chunk := &types.Chunk{
		ID:              "tag-faq",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     "match",
		ChunkType:       types.ChunkTypeFAQ,
		Content:         "actual FAQ",
		TagID:           tag.ID,
	}
	require.NoError(
		t,
		repository.NewChunkRepository(db).
			CreateChunks(repository.WithKnowledgeBuildInputProof(ctx, store, actual.Proof), []*types.Chunk{chunk}),
	)
	w = serveTagHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/tags?page_size=1&keyword=public", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), `"knowledge_count":1`)
	require.Contains(t, w.Body.String(), `"chunk_count":1`)
}

func TestTagOutputActualHTTPUnknownRetainedAndMissingVerifier(t *testing.T) {
	h, db, _, _ := tagHTTPFixture(t)
	ctx := collectionHumanInputContext(7)
	tag := &types.KnowledgeTag{
		ID:              "legacy-tag",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Name:            "unknown retained secret",
		Color:           "red",
		SortOrder:       2,
	}
	require.NoError(t, db.Create(tag).Error)
	w := serveTagHTTP(h, ctx, http.MethodPut, "/knowledge-bases/kb/tags/legacy-tag", `{"color":"green"}`)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), tag.Name)
	require.NoError(t, db.Where("id='legacy-tag'").Take(tag).Error)
	require.Equal(
		t,
		"green",
		tag.Color,
		"the actual authorized partial edit committed; response still cannot disclose retained unknown Name",
	)
	w = serveTagHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/tags?keyword=absent", "")
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), tag.Name)
	h.outputAuthorizer = nil
	w = serveTagHTTP(h, ctx, http.MethodPost, "/knowledge-bases/kb/tags", `{"name":"new"}`)
	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
}

func TestTagOutputActualHTTPRevokeBeforeWrite(t *testing.T) {
	for _, mode := range []string{"group", "web", "api", "tag-ABA", "relation-ABA", "chunk-ABA", "missing-actor"} {
		t.Run(mode, func(t *testing.T) {
			h, db, store, groups := tagHTTPFixture(t)
			ctx := collectionHumanInputContext(7)
			tag := &types.KnowledgeTag{
				ID:              "checked-tag",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Name:            "private returned tag",
				Color:           "blue",
				SortOrder:       2,
			}
			require.NoError(
				t,
				h.tagRepo.Create(
					repository.WithIndependentKnowledgeTagCreateInput(ctx, "kb", tag.Name, tag.Color, tag.SortOrder),
					tag,
				),
			)
			var key *types.TenantAPIKey
			if mode == "api" {
				tenant := uint64(7)
				key = &types.TenantAPIKey{
					TenantID:         &tenant,
					ScopeType:        types.APIKeyScopeTenant,
					Name:             "tag read key",
					KeyHash:          "tag-read-key",
					Capabilities:     types.StringArray{string(types.APIKeyCapabilityRetrieve)},
					KnowledgeBaseIDs: types.StringArray{"kb"},
				}
				require.NoError(t, db.Create(key).Error)
				ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
				ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalAPITenant, ID: "7"})
				ctx = types.WithTenantAPIKeyScope(
					ctx,
					types.TenantAPIKeyScope{
						KeyID:            key.ID,
						ScopeType:        key.ScopeType,
						Capabilities:     key.Capabilities,
						KnowledgeBaseIDs: key.KnowledgeBaseIDs,
					},
				)
			}
			if mode == "missing-actor" {
				guard := access.NewNextcloudSourceLineageGuard(nil, collectionHTTPKB{db: db}, nil, nil, groups, nil)
				ConfigureTagHandlerOutputLineage(h, service.NewTagOutputAuthorizer(store, guard))
			}
			fired := false
			groups.hook = func(calls int) {
				if calls != 5 {
					return
				}
				fired = true
				switch mode {
				case "group":
					groups.allowed = false
				case "web":
					require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
				case "api":
					require.NoError(t, db.Model(key).Update("revoked_at", time.Now()).Error)
				case "tag-ABA":
					x := &types.KnowledgeTag{ID: "transient", TenantID: 7, KnowledgeBaseID: "kb", Name: "x"}
					require.NoError(t, db.Create(x).Error)
					require.NoError(t, db.Delete(x).Error)
				case "relation-ABA":
					r := &types.KnowledgeTagRelation{KnowledgeID: "match", TagID: tag.ID}
					require.NoError(t, db.Create(r).Error)
					require.NoError(t, db.Delete(r).Error)
				case "chunk-ABA":
					c := &types.Chunk{
						ID:              "transient",
						TenantID:        7,
						KnowledgeBaseID: "kb",
						KnowledgeID:     "match",
						TagID:           tag.ID,
					}
					require.NoError(t, db.Create(c).Error)
					require.NoError(t, db.Unscoped().Delete(c).Error)
				}
			}
			w := serveTagHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/tags", "")
			status := http.StatusForbidden
			if mode == "missing-actor" {
				status = http.StatusServiceUnavailable
			} else {
				require.True(t, fired)
			}
			require.Equal(t, status, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), tag.Name)
		})
	}
}

// This combines actual SQL original receipts, the signed connector HTTP
// transport, the production current-user resolver and production source guard.
// The isolated directory snapshot and endpoint stand in for unavailable AD/NC.
func TestTagOutputActualHTTPSignedSourceDependencies(t *testing.T) {
	for _, mode := range []string{"allowed", "deny_excluded_before_filter", "personal_revoke_before_write", "source_withdraw_during_final_remote"} {
		t.Run(mode, func(t *testing.T) {
			kh, calls := collectionNextcloudHTTPFixture(t, mode)
			db := kh.kbService.(collectionHTTPKB).db
			migration, e := os.ReadFile("../../migrations/sqlite/000060_tag_statistics_checkpoints.up.sql")
			require.NoError(t, e)
			require.NoError(t, db.Exec(string(migration)).Error)
			store := repository.NewKnowledgeInputProvenanceStore(db)
			actualGroups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
			repository.ConfigureKnowledgeInputActorGroupAccess(store, actualGroups)
			tags := repository.NewKnowledgeTagRepository(db)
			knowledge := repository.NewKnowledgeRepository(db)
			factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
				actual := repository.NewKnowledgeInputProvenanceStore(tx)
				repository.ConfigureKnowledgeInputActorGroupAccess(
					actual,
					service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
				)
				return actual
			}
			repository.ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
			ctx := collectionHumanInputContext(7)
			original, e := store.CaptureKnowledgeOutputInputs(
				ctx,
				types.KnowledgeBuildInputCaptureRequest{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "source"},
			)
			require.NoError(t, e)
			producer, e := repository.WithDerivedKnowledgeDisplayInput(ctx, original.Proof)
			require.NoError(t, e)
			derived := &types.KnowledgeTag{
				ID:              "derived-source-tag",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Name:            "private source-derived tag name",
				Color:           "blue",
				SortOrder:       2,
			}
			require.NoError(t, tags.Create(producer, derived))
			ordinary := &types.KnowledgeTag{
				ID:              "ordinary-tag",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Name:            "ordinary selected tag",
				Color:           "gray",
				SortOrder:       -1,
			}
			require.NoError(
				t,
				tags.Create(
					repository.WithIndependentKnowledgeTagCreateInput(
						ctx,
						"kb",
						ordinary.Name,
						ordinary.Color,
						ordinary.SortOrder,
					),
					ordinary,
				),
			)
			var ds types.DataSource
			require.NoError(t, db.Where("id='ds'").Take(&ds).Error)
			user := "user"
			now := time.Now().UTC()
			dir := collectionDirectory{
				identity: &types.DirectoryIdentity{
					ID:              "identity",
					DirectoryID:     "ad-1",
					ObjectGUID:      "00112233-4455-6677-8899-aabbccddeeff",
					UserID:          &user,
					Status:          types.DirectoryObjectActive,
					SnapshotVersion: 1,
				},
				directory: &types.Directory{
					ID:                   "ad-1",
					Protocol:             types.DirectoryProtocolAD,
					Enabled:              true,
					SnapshotVersion:      1,
					LastSuccessfulSyncAt: &now,
					StaleAfterSeconds:    900,
				},
			}
			publication := access.NewNextcloudPublicationGuard(dir, publicationHandlerSources{source: &ds})
			guard := access.NewNextcloudSourceLineageGuard(
				knowledge.(access.NextcloudLineageSnapshotLookup),
				collectionHTTPKB{db: db},
				kh.kbShareService,
				kh.agentShareService,
				actualGroups,
				publication,
				store,
			)
			svc, e := service.NewKnowledgeTagService(
				collectionHTTPKB{db: db},
				tags,
				knowledge,
				repository.NewChunkRepository(db),
				nil,
				nil,
				nil,
				nil,
				kh.kbShareService,
				nil,
				nil,
			)
			require.NoError(t, e)
			h := NewTagHandler(svc, tags, repository.NewChunkRepository(db))
			ConfigureTagHandlerOutputLineage(h, service.NewTagOutputAuthorizer(store, guard))
			// The response is source-empty ordinary text, but original candidate Name
			// influences its filter/page/total and must still authorize its dependency.
			w := serveTagHTTP(h, ctx, http.MethodGet, "/knowledge-bases/kb/tags?page_size=1&keyword=ordinary", "")
			require.Positive(t, calls.Load())
			if mode == "allowed" {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Contains(t, w.Body.String(), ordinary.Name)
				// A fresh color edit cannot erase the unchanged derived Name/Sort origin.
				w = serveTagHTTP(h, ctx, http.MethodPut, "/knowledge-bases/kb/tags/"+derived.ID, `{"color":"green"}`)
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Contains(t, w.Body.String(), derived.Name)
			} else {
				require.GreaterOrEqual(t, w.Code, 400)
				require.NotContains(t, w.Body.String(), ordinary.Name)
				require.NotContains(t, w.Body.String(), derived.Name)
			}
		})
	}
}

func TestTagOutputActualHTTPAllOriginalNameSources(t *testing.T) {
	for _, mode := range []string{"tag-two-sources-allowed", "tag-deny-source-b", "tag-late-deny-source-b"} {
		t.Run(mode, func(t *testing.T) {
			kh, calls := collectionNextcloudHTTPFixture(t, mode)
			db := kh.kbService.(collectionHTTPKB).db
			if !db.Migrator().HasTable("knowledge_input_actor_selectors") {
				migration, e := os.ReadFile("../../migrations/sqlite/000056_knowledge_original_worker_actor.up.sql")
				require.NoError(t, e)
				require.NoError(t, db.Exec(string(migration)).Error)
			}
			store := repository.NewKnowledgeInputProvenanceStore(db)
			actualGroups := service.NewGroupAccessService(repository.NewGroupAccessRepository(db))
			repository.ConfigureKnowledgeInputActorGroupAccess(store, actualGroups)
			ctx := collectionHumanInputContext(7)
			tags := repository.NewKnowledgeTagRepository(db)
			knowledge := repository.NewKnowledgeRepository(db)
			factory := func(tx *gorm.DB) *repository.KnowledgeInputProvenanceStore {
				actual := repository.NewKnowledgeInputProvenanceStore(tx)
				repository.ConfigureKnowledgeInputActorGroupAccess(
					actual,
					service.NewGroupAccessService(repository.NewGroupAccessRepository(tx)),
				)
				return actual
			}
			repository.ConfigureKnowledgeDisplayInputWriters(store, tags, knowledge, factory)
			var first types.Knowledge
			require.NoError(t, db.Where("id='source'").Take(&first).Error)
			second := first
			second.ID = "source-b"
			second.Title = "second original source"
			second.ParseStatus = types.ParseStatusPending
			second.EnableStatus = "disabled"
			meta := first.GetMetadata()
			meta["external_id"] = "nextcloud:instance:78"
			meta["nextcloud_file_id"] = "78"
			meta["nextcloud_path"] = "second.md"
			meta["nextcloud_etag"] = ""
			meta["nextcloud_target_etag"] = "original-etag"
			body, e := json.Marshal(meta)
			require.NoError(t, e)
			second.Metadata = types.JSON(body)
			require.NoError(t, db.Create(&second).Error)
			versions := knowledge.(interface {
				StageNextcloudVersion(context.Context, uint64, string, string, string, string, string) error
				PublishNextcloudVersion(context.Context, string) (bool, error)
			})
			require.NoError(
				t,
				versions.StageNextcloudVersion(ctx, 7, "kb", "ds", "nextcloud:instance:78", "original-etag", second.ID),
			)
			scope := repository.NextcloudContentScope{
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     second.ID,
				DataSourceID:    "ds",
				ExternalID:      "nextcloud:instance:78",
			}
			leases := repository.NewNextcloudContentLeaseStore(db)
			lease, e := leases.AcquireKnowledge(
				ctx,
				scope,
				repository.NextcloudContentBuildLease,
				"tag-second-source-build",
				time.Minute,
			)
			require.NoError(t, e)
			build := repository.WithNextcloudBuildLease(ctx, leases, lease, scope)
			proof, e := store.AdmitNextcloudBuildInputs(build, []byte("actual second original source bytes"))
			require.NoError(t, e)
			build = repository.WithKnowledgeBuildInputProof(build, store, proof)
			require.NoError(
				t,
				repository.NewChunkRepository(db).
					CreateChunks(build, []*types.Chunk{{ID: "source-b-chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: second.ID, Content: "actual second source parsed text", ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready"}}),
			)
			require.NoError(t, db.Where("id=?", second.ID).Take(&second).Error)
			second.ParseStatus = types.ParseStatusCompleted
			second.EnableStatus = "enabled"
			require.NoError(
				t,
				knowledge.UpdateKnowledge(repository.WithKnowledgeBuildInputProof(build, store, proof), &second),
			)
			published, e := versions.PublishNextcloudVersion(build, second.ID)
			require.NoError(t, e)
			require.True(t, published)
			resolver := knowledge.(access.NextcloudLineageSnapshotLookup)
			_, a, e := resolver.ResolveNextcloudSourceLineage(ctx, 7, "kb", first.ID)
			require.NoError(t, e)
			_, b, e := resolver.ResolveNextcloudSourceLineage(ctx, 7, "kb", second.ID)
			require.NoError(t, e)
			inherited, e := types.NewCompleteSourceLineage(a, b)
			require.NoError(t, e)
			derivedCtx, e := repository.WithDerivedKnowledgeInput(ctx, inherited)
			require.NoError(t, e)
			combined := &types.Knowledge{
				ID:              "combined-tag-input",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Type:            types.KnowledgeTypeManual,
				Channel:         "web",
				CustomMetadata:  types.JSON(`{}`),
			}
			original, e := store.CreateKnowledgeWithInput(
				derivedCtx,
				combined,
				"actual consumed original A and B fields",
			)
			require.NoError(t, e)
			producer, e := repository.WithDerivedKnowledgeDisplayInput(ctx, original)
			require.NoError(t, e)
			tag := &types.KnowledgeTag{
				ID:              "both-source-tag",
				TenantID:        7,
				KnowledgeBaseID: "kb",
				Name:            "private name from both original A and B",
				Color:           "blue",
				SortOrder:       2,
			}
			require.NoError(t, tags.Create(producer, tag))
			captured, e := store.CaptureTagOutputInputs(
				ctx,
				types.TagOutputReadRequest{TenantID: 7, KnowledgeBaseID: "kb", TagID: tag.ID},
			)
			require.NoError(t, e)
			require.Equal(t, inherited, captured.TagProofs[0].SourceLineage)
			var ds types.DataSource
			require.NoError(t, db.Where("id='ds'").Take(&ds).Error)
			user := "user"
			now := time.Now().UTC()
			dir := collectionDirectory{
				identity: &types.DirectoryIdentity{
					ID:              "identity",
					DirectoryID:     "ad-1",
					ObjectGUID:      "00112233-4455-6677-8899-aabbccddeeff",
					UserID:          &user,
					Status:          types.DirectoryObjectActive,
					SnapshotVersion: 1,
				},
				directory: &types.Directory{
					ID:                   "ad-1",
					Protocol:             types.DirectoryProtocolAD,
					Enabled:              true,
					SnapshotVersion:      1,
					LastSuccessfulSyncAt: &now,
					StaleAfterSeconds:    900,
				},
			}
			guard := access.NewNextcloudSourceLineageGuard(
				resolver,
				collectionHTTPKB{db: db},
				kh.kbShareService,
				kh.agentShareService,
				actualGroups,
				access.NewNextcloudPublicationGuard(dir, publicationHandlerSources{source: &ds}),
				store,
			)
			svc, e := service.NewKnowledgeTagService(
				collectionHTTPKB{db: db},
				tags,
				knowledge,
				repository.NewChunkRepository(db),
				nil,
				nil,
				nil,
				nil,
				kh.kbShareService,
				nil,
				nil,
			)
			require.NoError(t, e)
			h := NewTagHandler(svc, tags, repository.NewChunkRepository(db))
			ConfigureTagHandlerOutputLineage(h, service.NewTagOutputAuthorizer(store, guard))
			// Color is actual fresh independent input. Name retains BOTH original
			// sources, so a permission to A alone can never publish the old Name.
			w := serveTagHTTP(h, ctx, http.MethodPut, "/knowledge-bases/kb/tags/"+tag.ID, `{"color":"green"}`)
			if mode == "tag-two-sources-allowed" {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Contains(t, w.Body.String(), tag.Name)
			} else {
				require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
				require.NotContains(t, w.Body.String(), tag.Name)
			}
			require.GreaterOrEqual(t, calls.Load(), int64(2))
			if mode == "tag-late-deny-source-b" {
				require.GreaterOrEqual(t, calls.Load(), int64(6))
			}
		})
	}
}

func TestTagOutputActualHTTPCreatedCommittedThenUserRevoked(t *testing.T) {
	h, db, _, groups := tagHTTPFixture(t)
	ctx := collectionHumanInputContext(7)
	fired := false
	groups.hook = func(calls int) {
		if calls == 5 {
			fired = true
			require.NoError(t, db.Model(&types.User{}).Where("id='user'").Update("is_active", false).Error)
		}
	}
	w := serveTagHTTP(
		h,
		ctx,
		http.MethodPost,
		"/knowledge-bases/kb/tags",
		`{"name":"new committed private tag","color":"blue","sort_order":2}`,
	)
	require.True(t, fired)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), "new committed private tag")
	var count int64
	require.NoError(t, db.Model(&types.KnowledgeTag{}).Where("name='new committed private tag'").Count(&count).Error)
	require.Equal(t, int64(1), count)
}
