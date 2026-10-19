//go:build sqlite_fts5

package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const configurationGCActualTemplate = "ACTUAL_CONTROLLED_TEMPLATE_FROM_PROJECT_CEDAR_SOURCE"

type configurationGCActualModels struct {
	interfaces.ModelService
	t     *testing.T
	calls int
}

func (m *configurationGCActualModels) GetChatModel(context.Context, string) (chat.Chat, error) {
	return m, nil
}
func (m *configurationGCActualModels) GetModelID() string { return "summary" }
func (m *configurationGCActualModels) GetModelName() string {
	return "controlled configuration GC provider"
}

func (m *configurationGCActualModels) Chat(
	_ context.Context,
	input []chat.Message,
	_ *chat.ChatOptions,
) (*types.ChatResponse, error) {
	var prompt strings.Builder
	for _, message := range input {
		prompt.WriteString(message.Content)
	}
	require.Contains(
		m.t,
		prompt.String(),
		"Alice knows Bob",
		"the production template producer must actually consume this source",
	)
	m.calls++
	return &types.ChatResponse{Content: configurationGCActualTemplate}, nil
}

func (m *configurationGCActualModels) ChatStream(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	panic("unexpected streaming template generation")
}

type configurationGCActualFixture struct {
	history                  *historyVectorActualFixture
	admin                    context.Context
	host, clone, independent *types.KnowledgeBase
	lineage                  *types.SourceLineage
	templateInputID          string
	clonePlan                *repository.KnowledgeConfigurationOriginalBodyReadPlan
	cloneLeaseIDs            []string
}

func newConfigurationGCActualFixture(t *testing.T) *configurationGCActualFixture {
	t.Helper()
	h := newHistoryVectorActualFixture(t)
	db, store := h.m.f.db, h.m.f.store
	require.True(t, db.Migrator().HasTable("knowledge_configuration_original_body_manifests"))
	// Install actual template input/result DDL without replacing an existing
	// AutoMigrate column or its original-body controls.
	ddl, err := os.ReadFile("../../../migrations/sqlite/000066_knowledge_enrichment_template_origins.up.sql")
	require.NoError(t, err)
	add := "ALTER TABLE knowledge_bases ADD COLUMN enrichment_templates TEXT;"
	require.Equal(t, 1, strings.Count(string(ddl), add))
	require.NoError(t, db.Exec(strings.Replace(string(ddl), add, "", 1)).Error)
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(db))
	require.NoError(
		t,
		db.Model(&types.TenantMember{}).
			Where("tenant_id=? AND user_id=?", 7, "enrich-human").
			Update("role", types.TenantRoleAdmin).
			Error,
	)
	admin := historyVectorActualCaller(t, h.m, "enrich-human", types.TenantRoleAdmin)
	create := func(id, name string, templates *types.KnowledgeEnrichmentTemplates) *types.KnowledgeBase {
		kb := &types.KnowledgeBase{
			ID:                  id,
			TenantID:            7,
			CreatorID:           "enrich-human",
			Name:                name,
			Type:                types.KnowledgeBaseTypeDocument,
			EmbeddingModelID:    "embed",
			SummaryModelID:      "summary",
			ChunkingConfig:      types.ChunkingConfig{ChunkSize: 512},
			IndexingStrategy:    types.IndexingStrategy{VectorEnabled: true},
			EnrichmentTemplates: templates,
		}
		ctx := repository.WithIndependentKnowledgeBaseNameInput(
			repository.WithIndependentKnowledgeInput(admin),
			id,
			name,
		)
		require.NoError(t, store.CreateKnowledgeBaseWithInput(ctx, kb))
		return kb
	}
	host := create("actual-config-template-host", "Actual independent template host", nil)
	independent := create(
		"actual-config-independent",
		"New independent configuration remains retained",
		&types.KnowledgeEnrichmentTemplates{Summary: "A newly authored independent literal, with no connector source."},
	)
	models := &configurationGCActualModels{ModelService: h.models, t: t}
	svc := NewKnowledgeEnrichmentTemplateService(store, h.m.f.s.repo, models, h.m.f.guard, store.ContentLeaseStore())
	host, err = svc.GenerateDerivedTemplate(admin, 7, host.ID, "summary", []string{h.source.ID})
	require.NoError(t, err)
	require.Equal(t, 1, models.calls)
	require.Equal(t, configurationGCActualTemplate, host.EnrichmentTemplates.Content("summary"))
	var template struct{ ID string }
	require.NoError(
		t,
		db.Table("knowledge_enrichment_template_inputs").
			Select("id").
			Where("knowledge_base_id=?", host.ID).
			Take(&template).
			Error,
	)
	// This is metadata from the actual immutable producer. Do not make an
	// admission/proof/lineage from the present configuration body.
	plan, err := store.KnowledgeConfigurationBodyReadPlan(admin, 7, host.ID)
	require.NoError(t, err)
	var origin struct{ SourceLineage string }
	require.NoError(
		t,
		db.Table("knowledge_config_input_origins").
			Select("source_lineage").
			Where("origin_id=?", plan.OriginID).
			Take(&origin).
			Error,
	)
	var lineage types.SourceLineage
	require.NoError(t, json.Unmarshal([]byte(origin.SourceLineage), &lineage))
	require.NoError(t, lineage.RequireComplete())
	require.Len(t, lineage.Sources, 1)
	require.Equal(t, h.source.ID, lineage.Sources[0].KnowledgeID)
	require.NoError(
		t,
		h.m.f.guard.Check(
			admin,
			&lineage,
			access.LineageReadScope{
				Targets: types.SearchTargets{
					&types.SearchTarget{
						Type:            types.SearchTargetTypeKnowledgeBase,
						TenantID:        7,
						KnowledgeBaseID: host.ID,
					},
				},
			},
		),
	)
	ids, err := store.AcquireKnowledgeConfigurationBodyReadLeases(
		admin,
		plan,
		"actual-source-configuration-copy",
		2*time.Minute,
	)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	require.NoError(t, store.RecheckKnowledgeConfigurationBodyReadLeases(admin, plan, ids, 2*time.Minute))
	actual, currentLineage, err := store.ReadKnowledgeTemplateConfiguration(admin, 7, host.ID)
	require.NoError(t, err)
	require.Equal(t, &lineage, currentLineage)
	clone := *actual
	clone.ID = "actual-config-cloned-target"
	clone.Name = "Actual derived configuration copy"
	clone.CreatorID = "enrich-human"
	copyContext, err := repository.WithDerivedKnowledgeInput(admin, currentLineage)
	require.NoError(t, err)
	copyContext = repository.WithIndependentKnowledgeBaseNameInput(copyContext, clone.ID, clone.Name)
	require.NoError(t, store.CreateKnowledgeBaseWithInput(copyContext, &clone))
	var virgin bool
	require.NoError(
		t,
		db.Raw("SELECT virgin FROM nextcloud_kb_blank_witnesses WHERE tenant_id=? AND knowledge_base_id=?", 7, clone.ID).
			Scan(&virgin).
			Error,
	)
	require.True(t, virgin, "approved processing configuration is not imported document business content")
	for _, table := range []string{"knowledges", "chunks", "data_sources", "wiki_pages", "nextcloud_onboarding_operations"} {
		var n int64
		require.NoError(t, db.Table(table).Where("tenant_id=? AND knowledge_base_id=?", 7, clone.ID).Count(&n).Error)
		require.Zero(t, n, "config heads/manifests must not manufacture business rows in "+table)
	}
	require.NoError(t, store.RecheckKnowledgeConfigurationBodyReadLeases(admin, plan, ids, 2*time.Minute))
	bodies := repository.NewOriginalBodyRetentionStore(db)
	for _, id := range ids {
		require.NoError(t, bodies.ReleaseLease(admin, id))
	}
	clonePlan, err := store.KnowledgeConfigurationBodyReadPlan(admin, 7, clone.ID)
	require.NoError(t, err)
	cloneIDs, err := store.AcquireKnowledgeConfigurationBodyReadLeases(
		admin,
		clonePlan,
		"actual-cloned-configuration-reader",
		2*time.Minute,
	)
	require.NoError(t, err)
	f := &configurationGCActualFixture{h, admin, host, &clone, independent, &lineage, template.ID, clonePlan, cloneIDs}
	t.Cleanup(func() {
		for _, id := range cloneIDs {
			_ = bodies.ReleaseLease(context.Background(), id)
		}
	})
	return f
}

var configurationGCActualColumns = []string{
	"chunking_config",
	"image_processing_config",
	"vlm_config",
	"asr_config",
	"extract_config",
	"faq_config",
	"question_generation_config",
	"auto_tag_config",
	"profile_config",
	"wiki_config",
	"indexing_strategy",
	"enrichment_templates",
}

func configurationGCActualRaw(t *testing.T, db *gorm.DB, id string) map[string]any {
	t.Helper()
	var rows []map[string]any
	require.NoError(
		t,
		db.Table("knowledge_bases").
			Select(configurationGCActualColumns).
			Where("id=? AND tenant_id=?", id, 7).
			Find(&rows).
			Error,
	)
	require.Len(t, rows, 1)
	return rows[0]
}

func configurationGCActualString(v any) string {
	if v != nil {
		value := reflect.ValueOf(v)
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return ""
			}
			return configurationGCActualString(value.Elem().Interface())
		}
	}
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case sql.RawBytes:
		return string(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func (f *configurationGCActualFixture) retireSource(t *testing.T) string {
	t.Helper()
	h := f.history
	h.m.allow.Store(false)
	require.Error(
		t,
		h.m.f.guard.Check(
			f.admin,
			f.lineage,
			access.LineageReadScope{
				Targets: types.SearchTargets{
					&types.SearchTarget{
						Type:            types.SearchTargetTypeKnowledgeBase,
						TenantID:        7,
						KnowledgeBaseID: f.host.ID,
					},
				},
			},
		),
		"actual controlled NC READ is revoked",
	)
	// An authorization failure is not a deletion fact. This real repository
	// source-deletion lifecycle creates the tombstone and retires actual fences.
	versions, ok := h.m.f.s.repo.(interface {
		TombstoneNextcloudVersion(context.Context, uint64, string, string, string) error
	})
	require.True(t, ok)
	require.NoError(
		t,
		versions.TombstoneNextcloudVersion(f.admin, 7, h.source.KnowledgeBaseID, h.m.ds.ID, h.m.externalID),
	)
	gc := repository.NewNextcloudGCStore(h.m.f.db)
	n, err := gc.InventoryRetired(f.admin, time.Now().UTC(), 100)
	require.NoError(t, err)
	require.Positive(t, n)
	var job struct {
		ID        string
		NotBefore time.Time
	}
	require.NoError(
		t,
		h.m.f.db.Table("nextcloud_gc_jobs").
			Select("id,not_before").
			Where("tenant_id=? AND knowledge_id=?", 7, h.source.ID).
			Take(&job).
			Error,
	)
	require.False(
		t,
		time.Now().UTC().Before(job.NotBefore),
		"real deletion is immediately eligible; do not forge the deadline",
	)
	return job.ID
}

func TestKnowledgeConfigurationActualSQLSourceTombstoneClearsCloneBodiesAfterReadLeases(t *testing.T) {
	f := newConfigurationGCActualFixture(t)
	db, store := f.history.m.f.db, f.history.m.f.store
	before := configurationGCActualRaw(t, db, f.clone.ID)
	require.Contains(t, configurationGCActualString(before["enrichment_templates"]), configurationGCActualTemplate)
	independentBefore := configurationGCActualRaw(t, db, f.independent.ID)
	var templateHead struct{ BodyID string }
	require.NoError(
		t,
		db.Table("original_body_field_heads").
			Select("body_id").
			Where("source_table=? AND row_key=? AND column_name=?", "knowledge_enrichment_template_inputs", f.templateInputID, "parents").
			Take(&templateHead).
			Error,
	)
	var parentPayload struct {
		Payload *string
		State   string
	}
	require.NoError(
		t,
		db.Table("original_body_payloads").
			Select("payload,state").
			Where("id=?", templateHead.BodyID).
			Take(&parentPayload).
			Error,
	)
	require.Equal(t, "live", parentPayload.State)
	require.NotNil(t, parentPayload.Payload)
	require.Contains(t, *parentPayload.Payload, "Alice knows Bob")
	job := f.retireSource(t)
	bodies := repository.NewOriginalBodyRetentionStore(db)
	_, err := bodies.PurgeSelectedJob(f.admin, 7, job, 100)
	require.ErrorIs(t, err, repository.ErrNextcloudContentGCUncovered)
	require.Equal(t, before, configurationGCActualRaw(t, db, f.clone.ID))
	// This isolated SQL test now models an explicitly covered deployment for
	// this single source KB. It does not establish production reader coverage.
	// Production activation remains gated on the separate full reader audit.
	var nowMS int64
	require.NoError(t, db.Raw("SELECT CAST((julianday('now') - 2440587.5)*86400000 AS INTEGER)").Scan(&nowMS).Error)
	require.NoError(t, db.Exec(`INSERT INTO nextcloud_content_lease_coverage
 (tenant_id,knowledge_base_id,activated_at_ms,legacy_drained_at_ms,reader_revision,builder_revision)
 VALUES (?,?,?,?,?,?)`, 7, f.history.source.KnowledgeBaseID, nowMS-1000, nowMS, "isolated-config-body-test", "isolated-config-body-test").Error)
	_, err = bodies.PurgeSelectedJob(f.admin, 7, job, 100)
	require.NoError(t, err)
	require.Equal(
		t,
		before,
		configurationGCActualRaw(t, db, f.clone.ID),
		"actual target-body leases must protect all shared fields in the cloned generation",
	)
	for _, id := range f.cloneLeaseIDs {
		require.NoError(t, bodies.ReleaseLease(f.admin, id))
	}
	require.ErrorIs(
		t,
		store.RecheckKnowledgeConfigurationBodyReadLeases(f.admin, f.clonePlan, f.cloneLeaseIDs, 2*time.Minute),
		repository.ErrOriginalBodyUnavailable,
	)
	for i := 0; i < 8; i++ {
		n, e := bodies.PurgeSelectedJob(f.admin, 7, job, 100)
		require.NoError(t, e)
		raw := configurationGCActualRaw(t, db, f.clone.ID)
		if body := configurationGCActualString(raw["enrichment_templates"]); body == "" || body == "null" {
			break
		}
		require.Positive(t, n, "a retained blocker cannot be claimed as completed physical collection")
	}
	after := configurationGCActualRaw(t, db, f.clone.ID)
	for _, column := range configurationGCActualColumns {
		original := configurationGCActualString(before[column])
		if original != "" && original != "null" {
			require.Contains(
				t,
				[]string{"", "null"},
				configurationGCActualString(after[column]),
				column+" must actually contain no original JSON body in SQL",
			)
		}
	}
	hostAfter := configurationGCActualRaw(t, db, f.host.ID)
	require.Contains(
		t,
		[]string{"", "null"},
		configurationGCActualString(hostAfter["enrichment_templates"]),
		"reverse source inventory must include both the template host and the clone",
	)
	require.Equal(
		t,
		independentBefore,
		configurationGCActualRaw(t, db, f.independent.ID),
		"the independent KB remains an actual current incoming owner",
	)
	parentPayload = struct {
		Payload *string
		State   string
	}{}
	require.NoError(
		t,
		db.Table("original_body_payloads").
			Select("payload,state").
			Where("id=?", templateHead.BodyID).
			Take(&parentPayload).
			Error,
	)
	require.Equal(t, "purged", parentPayload.State)
	require.Nil(t, parentPayload.Payload, "captured parent vault bytes must actually be removed")
	var cleared int64
	require.NoError(
		t,
		db.Raw("SELECT COALESCE(SUM(r.cleared_fields),0) FROM original_body_purge_receipts r JOIN original_body_field_heads h ON h.body_id=r.body_id WHERE h.source_table=? AND h.row_key=? AND h.column_name=?", "knowledge_bases", f.clone.ID, "enrichment_templates").
			Scan(&cleared).
			Error,
	)
	require.Positive(t, cleared)
	_, err = store.KnowledgeConfigurationBodyReadPlan(f.admin, 7, f.clone.ID)
	require.ErrorIs(
		t,
		err,
		repository.ErrOriginalBodyUnavailable,
		"a cleared generation cannot issue a new original read plan",
	)
	require.NoError(t, bodies.VerifyJournalReady())
	var leases int64
	require.NoError(t, db.Table("original_body_leases").Where("released_at_ms IS NULL").Count(&leases).Error)
	require.Zero(t, leases)
	// The source's file/index lifecycle is separate. This test proves SQL config
	// and parent-vault removal, not overall job completion or disk-space release.
}

func TestKnowledgeConfigurationActualSQLMissingManifestHeadCannotBeReadmittedByGC(t *testing.T) {
	f := newConfigurationGCActualFixture(t)
	db, store := f.history.m.f.db, f.history.m.f.store
	before := configurationGCActualRaw(t, db, f.clone.ID)
	var head struct {
		SlotID string
		BodyID string
	}
	require.NoError(
		t,
		db.Table("original_body_field_heads").
			Select("slot_id,body_id").
			Where("source_table=? AND row_key=? AND column_name=?", "knowledge_bases", f.clone.ID, "enrichment_templates").
			Take(&head).
			Error,
	)
	require.NoError(t, db.Exec("DELETE FROM original_body_field_heads WHERE slot_id=?", head.SlotID).Error)
	_, err := store.KnowledgeConfigurationBodyReadPlan(f.admin, 7, f.clone.ID)
	require.ErrorIs(t, err, repository.ErrOriginalBodyUnavailable)
	job := f.retireSource(t)
	for _, id := range f.cloneLeaseIDs {
		require.NoError(t, repository.NewOriginalBodyRetentionStore(db).ReleaseLease(f.admin, id))
	}
	_, err = repository.NewOriginalBodyRetentionStore(db).PurgeSelectedJob(f.admin, 7, job, 100)
	require.ErrorIs(t, err, repository.ErrOriginalBodyUnavailable)
	require.Equal(t, before, configurationGCActualRaw(t, db, f.clone.ID))
	var count int64
	require.NoError(t, db.Table("original_body_field_heads").Where("slot_id=?", head.SlotID).Count(&count).Error)
	require.Zero(t, count, "GC must not make a new original head from current template text")
}

func TestKnowledgeConfigurationActualSQLReadPlanAndLeaseCheckpointsAreMetadataOnly(t *testing.T) {
	f := newConfigurationGCActualFixture(t)
	db, store := f.history.m.f.db, f.history.m.f.store
	selects := []string{}
	name := "actual-config-metadata-only"
	observe := func(tx *gorm.DB) {
		query := strings.ToUpper(strings.NewReplacer("`", "", "\"", "").Replace(tx.Statement.SQL.String()))
		if !strings.HasPrefix(strings.TrimSpace(query), "SELECT") {
			return
		}
		for _, table := range []string{"KNOWLEDGE_BASES", "KNOWLEDGE_ENRICHMENT_TEMPLATE_INPUTS"} {
			if strings.Contains(query, "FROM "+table+" ") || strings.Contains(query, "JOIN "+table+" ") {
				selects = append(selects, tx.Statement.SQL.String())
				break
			}
		}
	}
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, observe))
	require.NoError(t, db.Callback().Row().After("gorm:row").Register(name, observe))
	defer func() { _ = db.Callback().Query().Remove(name); _ = db.Callback().Row().Remove(name) }()
	plan, err := store.KnowledgeConfigurationBodyReadPlan(f.admin, 7, f.clone.ID)
	require.NoError(t, err)
	ids, err := store.AcquireKnowledgeConfigurationBodyReadLeases(
		f.admin,
		plan,
		"actual-metadata-only-reader",
		2*time.Minute,
	)
	require.NoError(t, err)
	defer func() {
		for _, id := range ids {
			_ = repository.NewOriginalBodyRetentionStore(db).ReleaseLease(context.Background(), id)
		}
	}()
	require.NoError(t, store.RecheckKnowledgeConfigurationBodyReadLeases(f.admin, plan, ids, 2*time.Minute))
	// A pool of one makes accidental base-DB renewal inside this writer fail to
	// finish. The existing transaction helper must validate only its own rows.
	require.NoError(t, db.WithContext(f.admin).Transaction(func(tx *gorm.DB) error {
		return store.RecheckKnowledgeConfigurationBodyReadLeasesInTx(f.admin, tx, plan, ids)
	}))
	require.Empty(
		t,
		selects,
		"metadata plans/checkpoints must never consume KB config or captured parent bodies before the caller's fresh source READ",
	)
}

func TestKnowledgeConfigurationActualSQLMissingOriginalParentsRefusesMetadataCheckpoints(t *testing.T) {
	f := newConfigurationGCActualFixture(t)
	db, store := f.history.m.f.db, f.history.m.f.store
	before := configurationGCActualRaw(t, db, f.clone.ID)
	var body struct{ BodyID string }
	require.NoError(
		t,
		db.Table("original_body_field_heads").
			Select("body_id").
			Where("source_table=? AND row_key=? AND column_name=?", "knowledge_bases", f.clone.ID, "enrichment_templates").
			Take(&body).
			Error,
	)
	var count int64
	require.NoError(t, db.Table("original_body_parent_refs").Where("body_id=?", body.BodyID).Count(&count).Error)
	require.Positive(t, count, "the real admitted clone must retain exact actual source parents")
	require.NoError(t, db.Exec("DELETE FROM original_body_parent_refs WHERE body_id=?", body.BodyID).Error)
	require.ErrorIs(
		t,
		store.RecheckKnowledgeConfigurationBodyReadLeases(f.admin, f.clonePlan, f.cloneLeaseIDs, 2*time.Minute),
		repository.ErrOriginalBodyUnavailable,
	)
	_, err := store.KnowledgeConfigurationBodyReadPlan(f.admin, 7, f.clone.ID)
	require.ErrorIs(t, err, repository.ErrOriginalBodyUnavailable)
	job := f.retireSource(t)
	for _, id := range f.cloneLeaseIDs {
		require.NoError(t, repository.NewOriginalBodyRetentionStore(db).ReleaseLease(f.admin, id))
	}
	_, err = repository.NewOriginalBodyRetentionStore(db).PurgeSelectedJob(f.admin, 7, job, 100)
	require.ErrorIs(t, err, repository.ErrOriginalBodyUnavailable)
	require.Equal(t, before, configurationGCActualRaw(t, db, f.clone.ID))
	require.NoError(t, db.Table("original_body_parent_refs").Where("body_id=?", body.BodyID).Count(&count).Error)
	require.Zero(t, count, "GC must not fabricate replacement parent refs from current configuration JSON")
}
