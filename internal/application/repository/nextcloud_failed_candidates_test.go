package repository

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNextcloudFailedCandidateListScopedKeyset(t *testing.T) {
	ctx := context.Background()
	knowledge, db := newNextcloudVersionTestRepo(t)
	for _, fileID := range []string{"77", "88", "99"} {
		candidate := insertNextcloudVersionTestKnowledge(t, db, "candidate-"+fileID, "",
			types.ParseStatusPending, "disabled")
		metadata := candidate.GetMetadata()
		metadata["external_id"] = "nextcloud:instance:" + fileID
		metadata["nextcloud_file_id"] = fileID
		encoded, err := json.Marshal(metadata)
		require.NoError(t, err)
		require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", candidate.ID).
			Update("metadata", types.JSON(encoded)).Error)
		require.NoError(t, knowledge.StageNextcloudVersion(ctx, 7, "kb", "ds",
			"nextcloud:instance:"+fileID, "etag-"+fileID, candidate.ID))
		require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", candidate.ID).
			Update("parse_status", types.ParseStatusFailed).Error)
	}
	// This failed candidate shares the instance and KB string but belongs to a
	// different tenant and source. Its ID must never appear on tenant 7's page.
	var original types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&original).Error)
	alien := &types.DataSource{
		ID: "alien-ds", TenantID: 8, KnowledgeBaseID: "kb",
		Type: types.ConnectorTypeNextcloud, Status: types.DataSourceStatusActive, Config: original.Config,
	}
	require.NoError(t, db.Create(alien).Error)
	var originalPair NextcloudSourcePairing
	require.NoError(t, db.Where("datasource_id = ?", "ds").Take(&originalPair).Error)
	require.NoError(t, db.Create(&NextcloudSourcePairing{
		OperationID: uuid.NewString(), TenantID: 8,
		KnowledgeBaseID: "kb", DataSourceID: "alien-ds", InstanceID: "instance",
		BindingID: originalPair.BindingID, BaseURL: originalPair.BaseURL,
		ConfigSHA: originalPair.ConfigSHA, State: "active",
	}).Error)
	require.NoError(t, db.Create(&types.Knowledge{
		ID: "alien-candidate", TenantID: 8,
		KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud,
		ParseStatus: types.ParseStatusFailed,
	}).Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 8, KnowledgeBaseID: "kb",
		DataSourceID: "alien-ds", ExternalID: "nextcloud:instance:55", DesiredETag: "alien",
		CandidateKnowledgeID: "alien-candidate", State: "staging",
	}).Error)
	// A malformed row may be the final raw row in a page. Its opaque cursor
	// must still reach later valid files without exposing the malformed ID.
	require.NoError(t, db.Create(&types.Knowledge{
		ID: "malformed-candidate", TenantID: 7,
		KnowledgeBaseID: "kb", Channel: types.ConnectorTypeNextcloud,
		ParseStatus: types.ParseStatusFailed,
	}).Error)
	require.NoError(t, db.Create(&nextcloudSourceVersion{
		TenantID: 7, KnowledgeBaseID: "kb",
		DataSourceID: "ds", ExternalID: "nextcloud:instance:77x", DesiredETag: "bad",
		CandidateKnowledgeID: "malformed-candidate", State: "staging",
	}).Error)

	repo := NewNextcloudSourcePairingRepository(db)
	pair, _, err := repo.ActiveSourcePairingForDataSource(ctx, 7, "ds")
	require.NoError(t, err)
	_, _, err = repo.ActiveSourcePairingForDataSource(ctx, 8, "ds")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingMissing)
	var files []string
	cursor := ""
	sawEmptyPageWithCursor := false
	for {
		page, next, err := repo.ListFailedCandidates(ctx, pair, 1, cursor)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), 1)
		if len(page) == 0 && next != "" {
			sawEmptyPageWithCursor = true
		}
		for _, item := range page {
			files = append(files, item.FileID)
			require.Equal(t, "parse_failed", item.LastErrorCode)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	require.Equal(t, []string{"77", "88", "99"}, files)
	require.True(t, sawEmptyPageWithCursor)
	_, _, err = repo.ListFailedCandidates(ctx, pair, 51, "")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	_, _, err = repo.ListFailedCandidates(ctx, pair, 1, "not-a-cursor")
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	require.NoError(t, db.Model(&NextcloudSourcePairing{}).
		Where("operation_id = ?", pair.OperationID).Update("state", "decommissioned").Error)
	page, _, err := repo.ListFailedCandidates(ctx, pair, 50, "")
	require.NoError(t, err)
	require.Empty(t, page, "closed pairing must no longer reveal a failure list")
}

func TestNextcloudFailedCandidateStatusRequiresCurrentActiveSourceIdentity(t *testing.T) {
	ctx := context.Background()
	knowledge, db := newNextcloudVersionTestRepo(t)
	var originalSource types.DataSource
	require.NoError(t, db.Where("id = ?", "ds").Take(&originalSource).Error)
	candidate := insertNextcloudVersionTestKnowledge(t, db, "candidate-77", "",
		types.ParseStatusPending, "disabled")
	require.NoError(t, knowledge.StageNextcloudVersion(ctx, 7, "kb", "ds",
		"nextcloud:instance:77", "etag-77", candidate.ID))
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", candidate.ID).
		Update("parse_status", types.ParseStatusFailed).Error)
	var pair NextcloudSourcePairing
	require.NoError(t, db.Where("datasource_id = ?", "ds").Take(&pair).Error)
	repo := NewNextcloudSourcePairingRepository(db)
	_, err := repo.FailedCandidateRetryStatus(ctx, pair, 77)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("status", types.DataSourceStatusPaused).Error)
	_, err = repo.FailedCandidateRetryStatus(ctx, pair, 77)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("status", types.DataSourceStatusActive).Error)
	newConfig, err := (&types.DataSourceConfig{
		Type:        types.ConnectorTypeNextcloud,
		ResourceIDs: []string{"binding"}, Settings: map[string]interface{}{"base_url": "https://nextcloud.example"},
		Credentials: map[string]interface{}{"token": "different-token", "key_id": "pair_test"},
	}).ToJSON()
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("config", newConfig).Error)
	_, err = repo.FailedCandidateRetryStatus(ctx, pair, 77)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("config", originalSource.Config).Error)
	var current types.Knowledge
	require.NoError(t, db.Where("id = ?", candidate.ID).Take(&current).Error)
	metadata := current.GetMetadata()
	metadata["nextcloud_file_id"] = "88"
	encoded, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", candidate.ID).
		Update("metadata", types.JSON(encoded)).Error)
	_, err = repo.FailedCandidateRetryStatus(ctx, pair, 77)
	require.ErrorIs(t, err, ErrNextcloudSourcePairingConflict)
}
