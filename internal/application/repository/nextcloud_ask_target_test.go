package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func installAskTarget(t *testing.T, db *gorm.DB) {
	t.Helper()
	metadata, err := json.Marshal(map[string]string{
		"datasource_id": "ds", "external_id": "nextcloud:instance:77",
		"source_resource_id": "binding", "nextcloud_instance_id": "instance",
		"nextcloud_binding_id": "binding", "nextcloud_file_id": "77",
		"nextcloud_path": "file.md", "nextcloud_target_etag": "etag-1",
		"nextcloud_etag": "etag-1",
	})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.Knowledge{
		ID: "candidate", TenantID: 7, KnowledgeBaseID: "kb", Title: "file.md",
		Channel: types.ConnectorTypeNextcloud, ParseStatus: types.ParseStatusCompleted,
		EnableStatus: "enabled", Metadata: types.JSON(metadata),
	}).Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb", DataSourceID: "ds",
		ExternalID: "nextcloud:instance:77", DesiredETag: "etag-1",
		CandidateKnowledgeID: "candidate", State: "published",
	}).Error)
}

func TestNextcloudAskTargetRequiresExactCurrentPublication(t *testing.T) {
	_, db := newNextcloudVersionTestRepo(t)
	installAskTarget(t, db)
	lookup := NewNextcloudAskTargetRepository(db)
	find := func() (*types.Knowledge, error) {
		return lookup.FindPublished(context.Background(), "instance", "binding", 77, "etag-1")
	}
	target, err := find()
	require.NoError(t, err)
	require.Equal(t, "candidate", target.ID)
	_, err = lookup.FindPublished(context.Background(), "instance", "binding", 77, "etag-old")
	require.ErrorIs(t, err, ErrNextcloudAskTargetNotFound)
	_, err = lookup.FindPublished(context.Background(), "instance", "other-binding", 77, "etag-1")
	require.ErrorIs(t, err, ErrNextcloudAskTargetNotFound)
	_, err = lookup.FindPublished(context.Background(), "instance", "binding", 78, "etag-1")
	require.ErrorIs(t, err, ErrNextcloudAskTargetNotFound)

	require.NoError(t, db.Model(&nextcloudSourceVersion{}).Where("external_id = ?", "nextcloud:instance:77").
		Update("state", "staging").Error)
	_, err = find()
	require.ErrorIs(t, err, ErrNextcloudAskTargetNotFound)
	require.NoError(t, db.Model(&nextcloudSourceVersion{}).Where("external_id = ?", "nextcloud:instance:77").
		Update("state", "published").Error)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "candidate").
		Update("parse_status", types.ParseStatusFailed).Error)
	_, err = find()
	require.ErrorIs(t, err, ErrNextcloudAskTargetNotFound)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "candidate").
		Update("parse_status", types.ParseStatusCompleted).Error)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).Where("binding_id = ?", "binding").
		Update("state", "decommissioned").Error)
	_, err = find()
	require.True(t, errors.Is(err, ErrNextcloudAskTargetNotFound))
}

func TestNextcloudAskTargetRejectsPublishedTextWithNoRetrievableChunk(t *testing.T) {
	_, db := newNextcloudVersionTestRepo(t)
	installAskTarget(t, db)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "candidate").
		Update("file_type", "md").Error)
	lookup := NewNextcloudAskTargetRepository(db)
	find := func() error {
		_, err := lookup.FindPublished(context.Background(), "instance", "binding", 77, "etag-1")
		return err
	}
	require.ErrorIs(t, find(), ErrNextcloudAskTargetNotFound)
	chunk := &types.Chunk{
		ID: "chunk-77", TenantID: 7, KnowledgeBaseID: "kb",
		KnowledgeID: "candidate", ChunkType: types.ChunkTypeText,
		Content: "current text", IsEnabled: true, IndexStatus: "ready",
	}
	require.NoError(t, db.Create(chunk).Error)
	require.NoError(t, find())
	require.NoError(t, db.Model(chunk).Update("index_status", "failed").Error)
	require.ErrorIs(t, find(), ErrNextcloudAskTargetNotFound)
}
