package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func bodyActualFixture(t *testing.T, pg bool) (*enrichmentTestFixture, *repository.OriginalBodyRetentionStore) {
	t.Helper()
	f := newEnrichmentTestFixture(t, pg)
	path := "../../../migrations/sqlite/000071_original_body_retention.up.sql"
	if pg {
		path = "../../../migrations/versioned/000152_original_body_retention.up.sql"
	}
	sql, e := os.ReadFile(path)
	require.NoError(t, e)
	require.NoError(t, f.db.Exec(string(sql)).Error)
	installConfigurationOriginalBodyFixtureSchema(t, f.db, pg)
	require.NoError(t, repository.InstallOriginalBodyRetentionTriggers(f.db))
	require.NoError(t, repository.InstallOriginalBodyRetentionCallbacks(f.db))
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	key := filepath.Join(dir, "key")
	pin := filepath.Join(dir, "pin")
	require.NoError(t, os.WriteFile(key, []byte(strings.Repeat("owned-retention-key-", 3)), 0o600))
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY", dir)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY", key)
	t.Setenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN", pin)
	store := repository.NewOriginalBodyRetentionStore(f.db)
	require.NoError(t, store.InitializeJournal())
	inventoryKnownConfigurationOriginalBodyFixture(f.ctx, t, f.db, f.store)
	return f, store
}

func runOriginalBodyActualTagProducer(t *testing.T, pg bool) {
	f, store := bodyActualFixture(t, pg)
	require.True(t, f.db.Migrator().HasTable("original_body_payloads"))
	proof := repository.KnowledgeBuildInputProofFromContext(f.build)
	require.NotNil(t, proof)
	snapshot, e := f.store.CaptureKnowledgeBuildInputs(
		f.build,
		types.KnowledgeBuildInputCaptureRequest{
			TenantID:        7,
			KnowledgeBaseID: "kb",
			KnowledgeID:     f.k.ID,
			ChunkIDs:        []string{f.chunks[0].ID},
			Expected:        proof,
		},
	)
	require.NoError(t, e)
	tag, e := f.store.CaptureKnowledgeAutoTagInputs(f.ctx, snapshot.Proof)
	require.NoError(t, e)
	var raw string
	require.NoError(
		t,
		f.db.Raw("SELECT actual_inputs FROM knowledge_auto_tag_input_manifests WHERE manifest_id=?", tag.ManifestID).
			Scan(&raw).
			Error,
	)
	var headCount, payloadCount int64
	require.NoError(t, f.db.Table("original_body_field_heads").Count(&headCount).Error)
	require.NoError(t, f.db.Table("original_body_payloads").Count(&payloadCount).Error)
	t.Logf("actual auto-tag heads=%d payloads=%d", headCount, payloadCount)
	require.Contains(t, raw, "_original_body_version")
	require.NotContains(t, raw, f.chunks[0].Content)
	require.NoError(t, f.store.RecheckKnowledgeAutoTagInputs(f.ctx, tag))
	var body struct {
		ID        string
		BodyBytes int64
		Payload   *string
		State     string
	}
	require.NoError(t, f.db.Table("original_body_payloads").Where("storage_kind='vault'").Take(&body).Error)
	require.NotNil(t, body.Payload)
	require.Contains(t, *body.Payload, f.chunks[0].Content)
	require.Positive(t, body.BodyBytes)
	n, e := store.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n, "actual current owner cannot expire")
	// A real SQL lifecycle closure, using DB clock. The scheduler receives no
	// future date or ready boolean. This isolated fixture advances the boundary.
	expr := "datetime('now','-2 hours')"
	if pg {
		expr = "CURRENT_TIMESTAMP-INTERVAL '2 hours'"
	}
	require.NoError(t, f.db.Exec("UPDATE knowledges SET deleted_at="+expr+" WHERE id=?", f.k.ID).Error)
	lease, e := store.AcquireLease(context.Background(), body.ID, "read", "actual-original-consumer", time.Minute)
	require.NoError(t, e)
	n, e = store.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Zero(t, n)
	require.NoError(t, store.ReleaseLease(context.Background(), lease))
	n, e = store.PurgeDue(context.Background(), 100)
	require.NoError(t, e)
	require.Positive(t, n)
	require.ErrorIs(t, f.store.RecheckKnowledgeAutoTagInputs(f.ctx, tag), repository.ErrOriginalBodyUnavailable)
	var bytes int64
	query := "SELECT COALESCE(length(CAST(payload AS BLOB)),0) FROM original_body_payloads WHERE id=?"
	if pg {
		query = "SELECT COALESCE(octet_length(payload),0) FROM original_body_payloads WHERE id=?"
	}
	require.NoError(t, f.db.Raw(query, body.ID).Scan(&bytes).Error)
	require.Zero(t, bytes)
	var receipts int64
	require.NoError(
		t,
		f.db.Table("original_body_purge_receipts").Where("body_id=? AND after_bytes=0", body.ID).Count(&receipts).Error,
	)
	require.Equal(t, int64(1), receipts)
	fresh := &types.Knowledge{
		ID:              "new-current-original",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Type:            "manual",
		Title:           "new human original",
		Description:     "new independent current body",
		Metadata:        types.JSON(`{}`),
		CustomMetadata:  types.JSON(`{}`),
	}
	_, e = f.store.CreateKnowledgeWithInput(
		repository.WithIndependentKnowledgeInput(f.ctx),
		fresh,
		map[string]string{"title": fresh.Title, "description": fresh.Description},
	)
	require.NoError(t, e)
	var read types.Knowledge
	require.NoError(t, f.db.Where("id=?", fresh.ID).Take(&read).Error)
	require.Equal(t, fresh.Description, read.Description)
}

func TestOriginalBodyActualSQLiteTagProducer(t *testing.T) {
	runOriginalBodyActualTagProducer(t, false)
}

func TestPostgresOriginalBodyActualTagProducer(t *testing.T) {
	runOriginalBodyActualTagProducer(t, true)
}
