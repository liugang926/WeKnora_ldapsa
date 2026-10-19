package repository_test

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudScopedDocumentRequiresCurrentPublishedSource(t *testing.T) {
	db, store, _ := onboardingDB(t)
	require.NoError(
		t,
		db.Exec(
			("CREATE TABLE nextcloud_source_versions(tenant_id INTEGER,knowledge_base_id" +
				" TEXT,candidate_knowledge_id TEXT,state TEXT)"),
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.Knowledge{
				ID:              "source-current",
				TenantID:        7,
				KnowledgeBaseID: "source-kb",
				Channel:         types.ConnectorTypeNextcloud,
			},
		).Error,
	)
	require.NoError(
		t,
		db.Create(
			&types.Knowledge{ID: "ordinary-doc", TenantID: 7, KnowledgeBaseID: "source-kb", Channel: "manual"},
		).Error,
	)
	require.NoError(
		t,
		db.Exec("INSERT INTO nextcloud_source_versions VALUES(7,'source-kb','source-current','staging')").Error,
	)
	_, err := store.ScopedDocument(context.Background(), 7, "source-current")
	require.Error(t, err)
	require.NoError(t, db.Exec("UPDATE nextcloud_source_versions SET state='published'").Error)
	kb, err := store.ScopedDocument(context.Background(), 7, "source-current")
	require.NoError(t, err)
	require.Equal(t, "source-kb", kb)
	_, err = store.ScopedDocument(context.Background(), 9, "source-current")
	require.Error(t, err)
	_, err = store.ScopedDocument(context.Background(), 7, "ordinary-doc")
	require.Error(t, err)
	require.NoError(t, db.Exec("UPDATE nextcloud_source_versions SET state='withdrawn'").Error)
	_, err = store.ScopedDocument(context.Background(), 7, "source-current")
	require.Error(t, err)
}
