package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestPostgresNextcloudProfileWriteWaitsForSourceMarker(t *testing.T) {
	// leaseTestPostgres gives this test its own schema in disposable PostgreSQL.
	db := leaseTestPostgres(t)
	require.NoError(t, db.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY, generated_profile JSONB,
		ever_had_nextcloud_source BOOLEAN NOT NULL DEFAULT FALSE,
		updated_at TIMESTAMPTZ,
		deleted_at TIMESTAMPTZ
	)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO knowledge_bases (id) VALUES ('racing'), ('ordinary')`).Error)
	profiles := NewKnowledgeBaseRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Registration holds the KB row while it marks source provenance. The
	// generated-profile write must wait and then re-evaluate its WHERE clause.
	mark := db.WithContext(ctx).Begin()
	require.NoError(t, mark.Error)
	defer mark.Rollback()
	require.NoError(t, mark.Exec(`UPDATE knowledge_bases
		SET ever_had_nextcloud_source = TRUE WHERE id = 'racing'`).Error)
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		close(started)
		finished <- profiles.UpdateKnowledgeBaseGeneratedProfile(ctx, "racing",
			&types.KnowledgeBaseProfile{Gist: "stale generated profile"})
	}()
	<-started
	select {
	case err := <-finished:
		t.Fatalf("profile write crossed uncommitted source marker: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, mark.Commit().Error)
	select {
	case err := <-finished:
		require.ErrorIs(t, err, types.ErrKnowledgeBaseProfileUnsupported)
	case <-ctx.Done():
		t.Fatal("profile write did not resume after source marker committed")
	}
	var stored string
	require.NoError(t, db.Raw(`SELECT COALESCE(generated_profile::TEXT, '')
		FROM knowledge_bases WHERE id = 'racing'`).Scan(&stored).Error)
	require.Empty(t, stored)

	require.NoError(t, profiles.UpdateKnowledgeBaseGeneratedProfile(ctx, "ordinary",
		&types.KnowledgeBaseProfile{Gist: "ordinary profile"}))
	require.NoError(t, db.Raw(`SELECT generated_profile::TEXT
		FROM knowledge_bases WHERE id = 'ordinary'`).Scan(&stored).Error)
	require.Contains(t, stored, "ordinary profile")
}
