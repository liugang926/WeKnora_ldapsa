package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type auxiliaryMetadataService struct {
	interfaces.KnowledgeService
	db   *gorm.DB
	name bool
	last []*types.Knowledge
}

func (s *auxiliaryMetadataService) join(ctx context.Context, rows []*types.Knowledge) error {
	var ids []string
	for _, k := range rows {
		ids = append(ids, k.ID)
	}
	tags, err := repository.NewKnowledgeRepository(s.db).GetKnowledgeTags(ctx, ids)
	if err != nil {
		return err
	}
	for _, k := range rows {
		k.Tags = tags[k.ID]
		if s.name {
			var kb types.KnowledgeBase
			if err := s.db.WithContext(ctx).Where("id = ?", k.KnowledgeBaseID).Take(&kb).Error; err != nil {
				return err
			}
			k.KnowledgeBaseName = kb.Name
		}
	}
	s.last = rows
	return nil
}

func (s *auxiliaryMetadataService) GetKnowledgeByID(ctx context.Context, id string) (*types.Knowledge, error) {
	k, err := s.KnowledgeService.GetKnowledgeByID(ctx, id)
	if err != nil {
		return nil, err
	}
	err = s.join(ctx, []*types.Knowledge{k})
	return k, err
}

func (s *auxiliaryMetadataService) GetKnowledgeBatch(
	ctx context.Context,
	tenant uint64,
	ids []string,
) ([]*types.Knowledge, error) {
	rows, err := s.KnowledgeService.GetKnowledgeBatch(ctx, tenant, ids)
	if err != nil {
		return nil, err
	}
	err = s.join(ctx, rows)
	return rows, err
}

func serveAuxiliaryMetadataHTTP(h *KnowledgeHandler, batch bool) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	r := documentHandlerRouter()
	r.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(
			types.WithPrincipal(c.Request.Context(), types.Principal{Type: types.PrincipalWebUser, ID: "user"}),
		)
		c.Next()
	})
	url := "/knowledge/match"
	if batch {
		r.GET("/knowledge/batch", h.GetKnowledgeBatch)
		url = "/knowledge/batch?kb_id=kb&ids=match"
	} else {
		r.GET("/knowledge/:id", h.GetKnowledge)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
}

func TestKnowledgeOutputAuxiliaryActualHTTPExplicitMetadata(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, change := range []string{
			"original_tags",
			"original_tags_and_kb_name",
			"unconsumed_name_and_unrelated_legacy",
			"legacy_tag",
			"tag_aba_before_output",
			"name_aba_before_output",
			"relation_aba_before_output",
			"wire_alias_before_output",
		} {
			t.Run(map[bool]string{false: "single", true: "batch"}[batch]+"/"+change, func(t *testing.T) {
				h, db, store, groups, shares, agents := collectionHTTPFixture(t)
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
				tag := &types.KnowledgeTag{
					ID:              "tag",
					TenantID:        7,
					KnowledgeBaseID: "kb",
					Name:            "original consumed tag",
					Color:           "blue",
					SortOrder:       2,
				}
				createCtx := repository.WithIndependentKnowledgeTagCreateInput(
					ctx,
					"kb",
					tag.Name,
					tag.Color,
					tag.SortOrder,
				)
				if change == "legacy_tag" {
					createCtx = repository.WithIndependentKnowledgeInput(ctx)
				}
				require.NoError(t, tags.Create(createCtx, tag))
				require.NoError(
					t,
					knowledge.SetKnowledgeTags(
						repository.WithIndependentKnowledgeTagAssignmentInput(
							ctx,
							map[string][]string{"match": {tag.ID}},
						),
						"match",
						[]string{tag.ID},
					),
				)
				joined := &auxiliaryMetadataService{
					KnowledgeService: h.kgService,
					db:               db,
					name:             change == "original_tags_and_kb_name" || change == "name_aba_before_output",
				}
				h.kgService = joined
				guard := access.NewNextcloudSourceLineageGuard(
					nil,
					collectionHTTPKB{db: db},
					shares,
					agents,
					groups,
					nil,
					store,
				)
				ConfigureKnowledgeHandlerOutputLineage(h, service.NewKnowledgeOutputAuthorizer(store, guard))
				if change == "unconsumed_name_and_unrelated_legacy" {
					require.NoError(
						t,
						db.Model(&types.KnowledgeBase{}).
							Where("id='kb'").
							Update("name", "unproved unconsumed name").
							Error,
					)
					require.NoError(
						t,
						db.Create(
							&types.Knowledge{
								ID:              "unrelated-legacy",
								TenantID:        7,
								KnowledgeBaseID: "kb",
								Type:            "manual",
								Title:           "unrelated unconsumed legacy body",
								Channel:         "web",
								CustomMetadata:  types.JSON(`{}`),
							},
						).Error,
					)
				}
				if change == "tag_aba_before_output" || change == "name_aba_before_output" ||
					change == "relation_aba_before_output" ||
					change == "wire_alias_before_output" {
					fired := false
					groups.hook = func(call int) {
						if fired || call < 5 {
							return
						}
						fired = true
						switch change {
						case "tag_aba_before_output":
							require.NoError(
								t,
								db.Model(&types.KnowledgeTag{}).Where("id='tag'").Update("name", "changed").Error,
							)
							require.NoError(
								t,
								db.Model(&types.KnowledgeTag{}).Where("id='tag'").Update("name", tag.Name).Error,
							)
						case "name_aba_before_output":
							require.NoError(
								t,
								db.Model(&types.KnowledgeBase{}).Where("id='kb'").Update("name", "changed").Error,
							)
							require.NoError(
								t,
								db.Model(&types.KnowledgeBase{}).
									Where("id='kb'").
									Update("name", "original human KB").
									Error,
							)
						case "relation_aba_before_output":
							var relation types.KnowledgeTagRelation
							require.NoError(t, db.Where("knowledge_id='match'").Take(&relation).Error)
							require.NoError(
								t,
								db.Where("knowledge_id='match'").Delete(&types.KnowledgeTagRelation{}).Error,
							)
							require.NoError(t, db.Create(&relation).Error)
						case "wire_alias_before_output":
							joined.last[0].Tags[0].Name = "unaccounted wire tag"
						}
					}
				}
				w := serveAuxiliaryMetadataHTTP(h, batch)
				positive := change == "original_tags" || change == "original_tags_and_kb_name" ||
					change == "unconsumed_name_and_unrelated_legacy"
				if positive {
					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					require.Contains(t, w.Body.String(), "original consumed tag")
					if joined.name {
						require.Contains(t, w.Body.String(), "original human KB")
					}
					require.NotContains(t, w.Body.String(), "unrelated unconsumed legacy body")
					require.NotContains(t, w.Body.String(), "unproved unconsumed name")
				} else {
					require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
					require.NotContains(t, w.Body.String(), "original consumed tag")
					require.NotContains(t, w.Body.String(), "unaccounted wire tag")
				}
			})
		}
	}
}
