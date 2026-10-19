package service

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func applyEnrichmentTemplateMigration(t *testing.T, f *enrichmentTestFixture, pg bool) {
	t.Helper()
	require.NoError(t, f.db.Exec("ALTER TABLE knowledge_bases DROP COLUMN enrichment_templates").Error)
	path := "../../../migrations/sqlite/000066_knowledge_enrichment_template_origins.up.sql"
	migration := f.db
	if pg {
		path = "../../../migrations/versioned/000147_knowledge_enrichment_template_origins.up.sql"
		sqlDB, e := f.db.DB()
		require.NoError(t, e)
		migration, e = gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
		require.NoError(t, e)
	}
	data, e := os.ReadFile(path)
	require.NoError(t, e)
	require.NoError(t, migration.Exec(string(data)).Error)
	if !f.db.Migrator().HasTable("nextcloud_content_leases") {
		leasePath := "../../../migrations/sqlite/000046_nextcloud_content_leases.up.sql"
		if pg {
			leasePath = "../../../migrations/versioned/000127_nextcloud_content_leases.up.sql"
		}
		leaseData, e := os.ReadFile(leasePath)
		require.NoError(t, e)
		require.NoError(t, migration.Exec(string(leaseData)).Error)
	}
	// Current Session columns require the actual scoped-source target controls
	// before the template reader can acquire physical parent leases.
	require.NoError(t, f.db.AutoMigrate(&types.DataSource{}))
	installMachineOnboardingSchema(t, f.db, migration)
	if pg {
		sqlDB, e := f.db.DB()
		require.NoError(t, e)
		sqlDB.SetMaxIdleConns(0)
	}
}

func newTemplateArtifact(t *testing.T, f *enrichmentTestFixture, id string) context.Context {
	t.Helper()
	k := &types.Knowledge{
		ID:                   id,
		TenantID:             7,
		KnowledgeBaseID:      "kb",
		Type:                 types.KnowledgeTypeManual,
		Channel:              "web",
		Title:                "actual new artifact",
		Source:               "manual",
		Metadata:             types.JSON(`{"manual":{"content":"actual new human content"}}`),
		CustomMetadata:       types.JSON(`{}`),
		ParseStatus:          types.ParseStatusFinalizing,
		EnableStatus:         "enabled",
		PendingSubtasksCount: 1,
		SummaryStatus:        types.SummaryStatusPending,
	}
	proof, e := f.store.CreateKnowledgeWithInput(
		repository.WithIndependentKnowledgeInput(f.ctx),
		k,
		"actual new human content",
	)
	require.NoError(t, e)
	ctx := repository.WithKnowledgeBuildInputProof(f.ctx, f.store, proof)
	c := &types.Chunk{
		ID:              id + "-chunk",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		KnowledgeID:     id,
		Content:         strings.Repeat("Actual new document content after the trusted template configuration. ", 3),
		ChunkType:       types.ChunkTypeText,
		IsEnabled:       true,
		IndexStatus:     "ready",
		Metadata:        types.JSON(`{}`),
	}
	require.NoError(t, f.s.chunkRepo.CreateChunks(ctx, []*types.Chunk{c}))
	f.k = k
	f.chunks = []*types.Chunk{c}
	f.build = ctx
	return ctx
}

func runKnowledgeTrustedTemplates(t *testing.T, pg bool) {
	for _, mode := range []string{"new actual admin template", "new exact code template", "derived all actual parents", "parent body ABA", "model ABA", "admin revoke", "unknown old custom YAML", "old config no adoption"} {
		t.Run(mode, func(t *testing.T) {
			f := newEnrichmentTestFixture(t, pg)
			applyEnrichmentTemplateMigration(t, f, pg)
			svc := NewKnowledgeEnrichmentTemplateService(
				f.store,
				f.s.repo,
				f.s.modelService,
				f.guard,
				repository.NewNextcloudContentLeaseStore(f.db),
			)
			_, _, e := f.s.repo.FinalizeSubtask(f.build, f.k.ID)
			require.NoError(t, e)
			switch mode {
			case "new actual admin template":
				_, e = svc.SetIndependentTemplate(
					f.ctx,
					7,
					"kb",
					"summary",
					"A newly authored actual summary template in {{language}}.",
				)
			case "new exact code template":
				_, e = svc.InstallCodeTemplate(
					context.Background(),
					7,
					"kb",
					"summary",
					"An exact code-owned summary instruction in {{language}}.",
				)
			case "unknown old custom YAML":
				f.s.config.Conversation.GenerateSummaryPrompt = "unproved legacy operator text"
				require.False(
					t,
					f.post.enqueueSummaryGenerationTask(
						f.build,
						types.KnowledgePostProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID},
						0,
					),
				)
				require.Zero(t, f.model.calls)
				return
			case "old config no adoption":
				require.NoError(
					t,
					f.db.Model(&types.KnowledgeBase{}).
						Where("id='kb'").
						Update("enrichment_templates", types.JSON(`{"summary":"unproved old current row"}`)).
						Error,
				)
				_, e = svc.SetIndependentTemplate(
					f.ctx,
					7,
					"kb",
					"summary",
					"new literal cannot recertify unknown retained controls",
				)
				require.Error(t, e)
				require.Zero(t, f.model.calls)
				return
			default:
				if mode == "parent body ABA" {
					f.model.beforeReturn = func() {
						old := f.chunks[0].Content
						require.NoError(
							t,
							f.db.Model(&types.Chunk{}).
								Where("id = ?", f.chunks[0].ID).
								Update("content", "changed").
								Error,
						)
						require.NoError(
							t,
							f.db.Model(&types.Chunk{}).Where("id = ?", f.chunks[0].ID).Update("content", old).Error,
						)
					}
				}
				if mode == "model ABA" {
					f.model.beforeReturn = func() {
						require.NoError(
							t,
							f.db.Model(&types.Model{}).Where("id='summary'").Update("name", "changed").Error,
						)
						require.NoError(
							t,
							f.db.Model(&types.Model{}).Where("id='summary'").Update("name", "local summary").Error,
						)
					}
				}
				if mode == "admin revoke" {
					f.model.beforeReturn = func() {
						require.NoError(
							t,
							f.db.Model(&types.TenantMember{}).
								Where("user_id='enrich-human'").
								Update("role", types.TenantRoleViewer).
								Error,
						)
					}
				}
				_, e = svc.GenerateDerivedTemplate(f.ctx, 7, "kb", "summary", []string{f.k.ID})
				require.Equal(t, 1, f.model.calls, "derived original parent reads must reach the model: %v", e)
			}
			if mode == "parent body ABA" || mode == "model ABA" || mode == "admin revoke" {
				require.Error(t, e)
				var kb types.KnowledgeBase
				require.NoError(t, f.db.Where("id='kb'").Take(&kb).Error)
				require.Nil(t, kb.EnrichmentTemplates)
				return
			}
			require.NoError(t, e)
			old := f.k
			require.Error(
				t,
				f.store.RecheckKnowledgeBuildInputs(f.build, repository.KnowledgeBuildInputProofFromContext(f.build)),
				"old original config is never re-stamped",
			)
			ctx := newTemplateArtifact(t, f, "template-new-artifact")
			f.s.config.Conversation.GenerateSummaryPrompt = "unknown global YAML is not the proved override"
			require.True(
				t,
				f.post.enqueueSummaryGenerationTask(
					ctx,
					types.KnowledgePostProcessPayload{
						TenantID:        7,
						KnowledgeBaseID: "kb",
						KnowledgeID:     f.k.ID,
						Language:        "en-US",
					},
					0,
				),
			)
			require.NoError(t, f.s.ProcessSummaryGeneration(context.Background(), f.queue.tasks[len(f.queue.tasks)-1]))
			assertEnrichmentReadable(t, f, []string{f.chunks[0].ID})
			require.NotEqual(t, old.ID, f.k.ID)
		})
	}
}

func TestKnowledgeTrustedEnrichmentTemplatesSQLite(t *testing.T) {
	runKnowledgeTrustedTemplates(t, false)
}

func TestPostgresKnowledgeTrustedEnrichmentTemplates(t *testing.T) {
	runKnowledgeTrustedTemplates(t, true)
}

func runKnowledgeDerivedTemplateSourceUnion(t *testing.T, pg bool) {
	f, _, _, allow, _ := sourceBuildFixture(t, pg)
	applyEnrichmentTemplateMigration(t, f, pg)
	var source types.Knowledge
	require.NoError(t, f.db.Where("id = ?", f.k.ID).Take(&source).Error)
	source.ParseStatus = types.ParseStatusCompleted
	source.EnableStatus = "enabled"
	require.NoError(t, f.s.repo.UpdateKnowledge(f.build, &source))
	require.NoError(t, f.db.Where("id = ?", source.ID).Take(&source).Error)
	svc := NewKnowledgeEnrichmentTemplateService(
		f.store,
		f.s.repo,
		f.s.modelService,
		f.guard,
		repository.NewNextcloudContentLeaseStore(f.db),
	)
	f.model.beforeReturn = func() {
		var count int64
		require.NoError(
			t,
			f.db.Table("nextcloud_content_leases").
				Where("knowledge_id = ? AND kind = 'read' AND released_at_ms IS NULL", source.ID).
				Count(&count).
				Error,
		)
		require.Positive(t, count, "actual parent bytes stay pinned through model and commit")
	}
	_, e := svc.GenerateDerivedTemplate(f.ctx, 7, "kb", "summary", []string{source.ID})
	require.NoError(t, e)
	f.model.beforeReturn = nil
	read, e := svc.ReadTemplate(f.ctx, 7, "kb", "summary")
	require.NoError(t, e)
	defer func() { require.NoError(t, read.Close()) }()
	require.NoError(t, read.Check(f.ctx))
	ctx := newTemplateArtifact(t, f, "derived-template-artifact")
	original := repository.KnowledgeBuildInputProofFromContext(ctx)
	require.Len(t, original.SourceLineage.Sources, 1)
	require.Equal(t, source.ID, original.SourceLineage.Sources[0].KnowledgeID)
	require.True(
		t,
		f.post.enqueueSummaryGenerationTask(
			ctx,
			types.KnowledgePostProcessPayload{TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: f.k.ID},
			0,
		),
	)
	require.NoError(t, f.s.ProcessSummaryGeneration(context.Background(), f.queue.tasks[len(f.queue.tasks)-1]))
	assertEnrichmentReadable(t, f, []string{f.chunks[0].ID})
	allow.Store(false)
	require.Error(t, read.Check(f.ctx), "withdrawn template source cannot pass the final response check")
	require.Error(t, f.store.RecheckKnowledgeBuildInputs(f.ctx, original))
	var current types.Knowledge
	require.NoError(t, f.db.Where("id = ?", f.k.ID).Take(&current).Error)
	_, e = NewKnowledgeOutputAuthorizer(f.store, f.guard).Begin(f.ctx, []*types.Knowledge{&current})
	require.Error(t, e)
}

func TestKnowledgeDerivedTemplateAllSourceUnionSQLite(t *testing.T) {
	runKnowledgeDerivedTemplateSourceUnion(t, false)
}

func TestPostgresKnowledgeDerivedTemplateAllSourceUnion(t *testing.T) {
	runKnowledgeDerivedTemplateSourceUnion(t, true)
}

func runKnowledgeTrustedQuestionTemplate(t *testing.T, pg bool) {
	f := newEnrichmentTestFixture(t, pg)
	applyEnrichmentTemplateMigration(t, f, pg)
	svc := NewKnowledgeEnrichmentTemplateService(
		f.store,
		f.s.repo,
		f.s.modelService,
		f.guard,
		repository.NewNextcloudContentLeaseStore(f.db),
	)
	_, err := svc.SetIndependentTemplate(
		f.ctx,
		7,
		"kb",
		"question",
		"Write {{question_count}} questions for {{doc_name}}: {{content}} {{context}} in {{language}}",
	)
	require.NoError(t, err)
	ctx := newTemplateArtifact(t, f, "question-template-artifact")
	f.s.config.Conversation.GenerateQuestionsPrompt = "unproved legacy global question template"
	f.model.questions = true
	require.Equal(
		t,
		1,
		f.post.enqueueQuestionGenerationTasks(
			ctx,
			types.KnowledgePostProcessPayload{
				TenantID:        7,
				KnowledgeBaseID: "kb",
				KnowledgeID:     f.k.ID,
				Language:        "en-US",
			},
			types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2},
			0,
			f.chunks,
		),
	)
	require.NoError(t, f.s.ProcessQuestionGeneration(context.Background(), f.queue.tasks[len(f.queue.tasks)-1]))
	assertEnrichmentReadable(t, f, []string{f.chunks[0].ID})
	_, err = f.s.RegenerateChunkQuestions(f.ctx, f.chunks[0].ID)
	require.NoError(t, err)
	assertEnrichmentReadable(t, f, []string{f.chunks[0].ID})
}

func TestKnowledgeTrustedQuestionTemplateSQLite(t *testing.T) {
	runKnowledgeTrustedQuestionTemplate(t, false)
}

func TestPostgresKnowledgeTrustedQuestionTemplate(t *testing.T) {
	runKnowledgeTrustedQuestionTemplate(t, true)
}
