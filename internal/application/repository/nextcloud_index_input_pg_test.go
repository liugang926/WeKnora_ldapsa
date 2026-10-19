package repository

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func nextcloudIndexProducerPostgresFixture(t *testing.T) (*gorm.DB, context.Context,
	*types.KnowledgeBuildInputProof,
) {
	t.Helper()
	store, db, actor, _, _, _ := displayTestFixture(t, true)
	ConfigureKnowledgeInputActorGroupAccess(store, displayTestGroups{db: db})
	require.NoError(t, db.AutoMigrate(&types.Model{}))
	for _, id := range []string{"summary", "embed"} {
		require.NoError(t, db.Create(&types.Model{
			ID: id, TenantID: 7,
			Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive,
		}).Error)
	}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	migration, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}), &gorm.Config{})
	require.NoError(t, err)
	ddl, err := os.ReadFile("../../../migrations/versioned/000143_knowledge_enrichment_inputs.up.sql")
	require.NoError(t, err)
	require.NoError(t, migration.Exec(string(ddl)).Error)
	var kb types.KnowledgeBase
	require.NoError(t, db.Where("id = ? AND tenant_id = ?", "kb", 7).Take(&kb).Error)
	kb.IndexingStrategy = types.DefaultIndexingStrategy()
	require.NoError(t, store.ReplaceKnowledgeBaseConfigurationWithInput(WithIndependentKnowledgeInput(actor), &kb))
	k := &types.Knowledge{
		ID: "actual-index-producer", TenantID: 7, KnowledgeBaseID: kb.ID,
		Type: types.KnowledgeTypeManual, Source: "manual", Channel: "web", Title: "actual producer input",
		Metadata:       types.JSON(`{"manual":{"content":"actual admitted body","version":1}}`),
		CustomMetadata: types.JSON(`{}`), ParseStatus: types.ParseStatusPending, EnableStatus: "disabled",
	}
	original, err := store.CreateKnowledgeWithInput(WithIndependentKnowledgeInput(actor), k, "actual admitted body")
	require.NoError(t, err)
	ctx := WithKnowledgeBuildInputProof(actor, store, original)
	snapshot, err := store.CaptureKnowledgeEnrichmentInputs(ctx, types.KnowledgeBuildInputCaptureRequest{
		TenantID: 7, KnowledgeBaseID: kb.ID, KnowledgeID: k.ID, Expected: original,
	})
	require.NoError(t, err)
	ctx = WithKnowledgeBuildInputProof(ctx, store, snapshot.Proof)
	controls := struct {
		Kind, SummaryModel, EmbeddingModel string
		SampleRows                         int
	}{
		types.TypeDataTableSummary, kb.SummaryModelID, kb.EmbeddingModelID, 10,
	}
	producer, err := store.AdmitKnowledgeProducerInput(ctx, snapshot.Proof, "summary", controls)
	require.NoError(t, err)
	ctx = WithKnowledgeProducerInput(ctx, producer, func() any { return controls })
	require.NoError(t, CheckKnowledgeProducerInput(ctx))
	// Use the actual native vector row shape; the producer guard and INSERT
	// share one SQL transaction, as they do in the PG retriever.
	require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public").Error)
	require.NoError(t, db.Exec(`CREATE TABLE embeddings (
		id BIGSERIAL PRIMARY KEY, source_id TEXT NOT NULL, source_type INTEGER NOT NULL,
		chunk_id TEXT, knowledge_id TEXT, knowledge_base_id TEXT, content TEXT,
		dimension INTEGER NOT NULL, embedding public.halfvec,
		is_enabled BOOLEAN NOT NULL DEFAULT TRUE);
		CREATE UNIQUE INDEX embeddings_unique_source ON embeddings(source_id,source_type)`).Error)
	return db, ctx, snapshot.Proof
}

func indexProducerTransactionCheck(ctx context.Context, tx *gorm.DB, proof *types.KnowledgeBuildInputProof) error {
	if err := LockNextcloudIndexKnowledgeBaseInTx(ctx, tx, proof.TenantID, proof.KnowledgeBaseID,
		proof.KnowledgeID); err != nil {
		return err
	}
	return ValidateNextcloudIndexInputWriteInTx(ctx, tx, proof.TenantID, proof.KnowledgeBaseID, proof.KnowledgeID)
}

func indexProducerInsert(tx *gorm.DB, proof *types.KnowledgeBuildInputProof) error {
	return tx.Exec(`INSERT INTO embeddings(source_id,source_type,chunk_id,knowledge_id,knowledge_base_id,
		content,dimension,embedding) VALUES ('actual-table-output',1,'actual-table-output',?,?,
		'actual model output',2,'[1,0]')`, proof.KnowledgeID, proof.KnowledgeBaseID).Error
}

func indexProducerMutation(db *gorm.DB, kind string) error {
	switch kind {
	case "summary", "embed":
		return db.Model(&types.Model{}).Where("id = ?", kind).Update("name", "actual changed model").Error
	case "routing":
		// This field is deliberately create-only in GORM. Use an actual SQL
		// control mutation to exercise its real monotonic routing trigger;
		// a Model.Update would silently leave the original route unchanged.
		return db.Exec("UPDATE knowledge_bases SET vector_store_id = ? WHERE id = ? AND tenant_id = ?",
			"changed-vector-store", "kb", 7).Error
	default:
		return db.Model(&types.Tenant{}).Where("id = ?", 7).Update("status", "disabled").Error
	}
}

// These races cover the actual selected model/routing/tenant revision commit
// boundary. Existing source/body/claim fences cover normal source writes; this
// is not a claim that arbitrary raw SQL source mutations are all serialized.
func TestPostgresNextcloudIndexProducerLocksSerializeModelRoutingAndTenantMutation(t *testing.T) {
	for _, kind := range []string{"summary", "embed", "routing", "tenant"} {
		t.Run(kind, func(t *testing.T) {
			db, ctx, proof := nextcloudIndexProducerPostgresFixture(t)
			write := db.WithContext(ctx).Begin()
			require.NoError(t, write.Error)
			t.Cleanup(func() { _ = write.Rollback().Error })
			require.NoError(t, indexProducerTransactionCheck(ctx, write, proof))
			require.NoError(t, indexProducerInsert(write, proof))
			started, completed := make(chan struct{}), make(chan error, 1)
			go func() { close(started); completed <- indexProducerMutation(db, kind) }()
			<-started
			select {
			case err := <-completed:
				t.Fatalf("actual %s UPDATE passed the vector transaction revision lock: %v", kind, err)
			case <-time.After(100 * time.Millisecond):
			}
			require.NoError(t, write.Commit().Error)
			var committed int64
			require.NoError(
				t,
				db.Table("embeddings").Where("knowledge_id=?", proof.KnowledgeID).Count(&committed).Error,
			)
			require.EqualValues(t, 1, committed)
			select {
			case err := <-completed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("actual producer mutation failed to resume after write commit")
			}
			if kind == "routing" {
				var actual types.KnowledgeBase
				require.NoError(t, db.Where("id = ? AND tenant_id = ?", proof.KnowledgeBaseID, proof.TenantID).
					Take(&actual).Error)
				require.NotNil(t, actual.VectorStoreID)
				require.Equal(t, "changed-vector-store", *actual.VectorStoreID,
					"the test must mutate the real route, not only its updated_at")
			}
			require.Error(t, db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				return indexProducerTransactionCheck(ctx, tx, proof)
			}), "old producer proof must fail after the serialized mutation commits")
		})
	}
}

func TestPostgresNextcloudIndexProducerRechecksMutationWinningRevisionLock(t *testing.T) {
	db, ctx, proof := nextcloudIndexProducerPostgresFixture(t)
	mutation := db.Begin()
	require.NoError(t, mutation.Error)
	t.Cleanup(func() { _ = mutation.Rollback().Error })
	require.NoError(t, indexProducerMutation(mutation, "summary"))
	started, completed := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		completed <- db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := indexProducerTransactionCheck(ctx, tx, proof); err != nil {
				return err
			}
			return indexProducerInsert(tx, proof)
		})
	}()
	<-started
	select {
	case err := <-completed:
		t.Fatalf("vector guard did not wait for the actual uncommitted model revision: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, mutation.Commit().Error)
	select {
	case err := <-completed:
		require.ErrorIs(t, err, ErrKnowledgeInputChanged,
			"the initial old model MVCC read cannot authorize output after its revision lock changes")
	case <-time.After(5 * time.Second):
		t.Fatal("vector guard did not resume after actual mutation commit")
	}
	var committed int64
	require.NoError(t, db.Table("embeddings").Where("knowledge_id=?", proof.KnowledgeID).Count(&committed).Error)
	require.Zero(t, committed, "guarded transaction cannot insert a vector from the old producer/model snapshot")
}

func TestPostgresNextcloudIndexLegacyLeaseContractDoesNotRequireProvenanceSchema(t *testing.T) {
	db := leaseTestPostgres(t)
	ctx := context.Background()
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		if err := LockNextcloudIndexKnowledgeBaseInTx(ctx, tx, 7, "legacy-kb", "legacy-knowledge"); err != nil {
			return err
		}
		return ValidateNextcloudIndexInputWriteInTx(ctx, tx, 7, "legacy-kb", "legacy-knowledge")
	}), "no original/producer input preserves the existing native lease-only schema contract")
}
