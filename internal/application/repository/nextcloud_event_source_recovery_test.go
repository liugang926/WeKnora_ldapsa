package repository

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestNextcloudDispatchRecoversErrorSourceThroughActivePairing(t *testing.T) {
	ctx := context.Background()
	db, inbox, now, _ := newNextcloudQueuedPublicationTest(t)
	require.NoError(t, db.Model(&types.DataSource{}).Where("id = ?", "ds").
		Update("status", types.DataSourceStatusError).Error)
	require.NoError(t, db.Table("nextcloud_event_dispatch").
		Where("connection_id = ?", "event-connection").
		Updates(map[string]any{
			"state": "idle", "dispatched_id": 0, "target_event_id": 0,
			"next_attempt_at": now,
		}).Error)

	claim, err := inbox.ClaimDispatch(ctx, "event-connection", now)
	require.NoError(t, err)
	require.NotNil(t, claim, "an error source with the exact active pairing must remain recoverable")
	require.Equal(t, int64(1), claim.TargetEventID)
}
