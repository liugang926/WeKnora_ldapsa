package im

import (
	"context"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestIMLegacySessionOwnerAdoption(t *testing.T) {
	db := newLifecycleTestDB(t)
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.Exec(`CREATE TABLE sessions(id TEXT PRIMARY KEY,tenant_id INTEGER,user_id TEXT,updated_at DATETIME,deleted_at DATETIME)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE im_channel_sessions(id TEXT PRIMARY KEY,tenant_id INTEGER,im_channel_id TEXT,platform TEXT,user_id TEXT,chat_id TEXT,thread_id TEXT,session_id TEXT,agent_id TEXT,deleted_at DATETIME)`).Error)
	ch := &IMChannel{ID: "channel", TenantID: 42, AgentID: "agent", Platform: "dingtalk", Enabled: true, SessionMode: "thread"}
	require.NoError(t, db.Create(ch).Error)
	mapping := &ChannelSession{ID: "map", TenantID: 42, IMChannelID: "channel", Platform: "dingtalk", UserID: "first", ChatID: "chat", ThreadID: "thread", SessionID: "session", AgentID: "agent"}
	require.NoError(t, db.Exec(`INSERT INTO sessions(id,tenant_id,user_id) VALUES('session',42,'')`).Error)
	require.NoError(t, db.Exec(`INSERT INTO im_channel_sessions(id,tenant_id,im_channel_id,platform,user_id,chat_id,thread_id,session_id,agent_id) VALUES('map',42,'channel','dingtalk','first','chat','thread','session','agent')`).Error)
	msg := &IncomingMessage{Platform: PlatformDingtalk, UserID: "second", ChatID: "chat", ThreadID: "thread"}
	ctx := withIMIdentity(context.Background(), 42, "channel", msg)
	owner := imChannelSessionOwner(ch, msg)
	ctx = types.WithIMChannelScope(ctx, types.IMChannelScope{TenantID: 42, AgentID: "agent", ChannelID: "channel", OwnerID: owner, Verify: func(context.Context, uint64, types.ResourceType, string, types.ResourceAction) error { return nil }})
	svc := &Service{db: db}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- svc.ensureChannelSessionOwner(ctx, mapping) }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	var row struct{ UserID string }
	require.NoError(t, db.Table("sessions").Select("user_id").Where("id='session'").Take(&row).Error)
	require.Equal(t, owner, row.UserID)
	wrong := *mapping
	wrong.IMChannelID = "other"
	require.Error(t, svc.ensureChannelSessionOwner(ctx, &wrong))
	require.NoError(t, db.Exec(`UPDATE sessions SET user_id='different-owner' WHERE id='session'`).Error)
	require.Error(t, svc.ensureChannelSessionOwner(ctx, mapping))
	require.NoError(t, db.Exec(`UPDATE sessions SET user_id='' WHERE id='session'`).Error)
	require.NoError(t, db.Exec(`INSERT INTO im_channel_sessions(id,tenant_id,im_channel_id,platform,user_id,chat_id,thread_id,session_id,agent_id) VALUES('other-map',42,'other','dingtalk','first','chat','thread','session','agent')`).Error)
	require.Error(t, svc.ensureChannelSessionOwner(ctx, mapping))
}
