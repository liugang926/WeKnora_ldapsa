package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	glog "gorm.io/gorm/logger"
)

type authReadCounts struct {
	glog.Interface
	mu              sync.Mutex
	total, retained int
}

func (l *authReadCounts) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total++
	if strings.Contains(sql,
		"FROM `original_body_") ||
		strings.Contains(sql,
			"FROM \"original_body_") ||
		strings.Contains(sql,
			"FROM original_body_") {

		l.retained++
	}
}
func (l *authReadCounts) reset() { l.mu.Lock(); defer l.mu.Unlock(); l.total = 0; l.retained = 0 }
func TestIMAuthorizationMetadataLiveAndCost(t *testing.T) {
	f := NewNormalReferencesHTTPFixtureForTest(t, false, &ragEntryModel{})
	ctx := types.WithCaller(f.Ctx, types.Caller{TenantID: 7, UserID: "system-7", Role: types.TenantRoleViewer})
	svc := &sessionService{knowledgeBaseService: f.KBs}
	reader := f.KBs.(interfaces.KnowledgeBaseAuthorizationReader)
	require.NotNil(t, f.DB.Callback().Query().Get("original_body:resolve"))
	metadata, err := reader.GetKnowledgeBaseAuthorizationMetadata(ctx, "kb")
	require.NoError(t, err)
	require.Equal(t, uint64(7), metadata.TenantID)
	require.NoError(t, svc.CheckIMChannelKnowledgeBase(ctx, 7, "kb"))
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 8, "kb"))
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 7, "missing"))
	require.Error(t, (&sessionService{}).CheckIMChannelKnowledgeBase(ctx, 7, "kb"))
	counts := &authReadCounts{Interface: glog.Default.LogMode(glog.Silent)}
	f.DB.Logger = counts
	// This is the exact pre-change check's full-read and permission sequence.
	baseline := func() error {
		base := context.WithValue(ctx, types.IMChannelScopeContextKey, nil)
		kb, e := f.KBs.GetKnowledgeBaseByIDOnly(base, "kb")
		if e != nil || kb == nil || kb.DeletedAt.Valid || kb.TenantID != 7 {
			return fmt.Errorf("unavailable: %v", e)
		}
		allowed, e := kbReadPermissions(base, svc.kbShareService).Check("kb", 7, types.OrgRoleViewer)
		if e != nil || !allowed {
			return fmt.Errorf("share unavailable")
		}
		return nil
	}
	candidate := func() error { return svc.CheckIMChannelKnowledgeBase(ctx, 7, "kb") }
	var oldTotal, newTotal time.Duration
	for run := 0; run < 5; run++ {
		for _, c := range []struct {
			name string
			call func() error
		}{{"baseline", baseline}, {"candidate", candidate}} {
			counts.reset()
			start := time.Now()
			for chunk := 0; chunk < 100; chunk++ {
				require.NoError(t, c.call())
			}
			elapsed := time.Since(start)
			t.Logf("AUTH_COST run=%d path=%s boundaries=100 elapsed_ns=%d queries=%d retained_queries=%d",
				run,
				c.name,
				elapsed.Nanoseconds(),
				counts.total,
				counts.retained)
			if c.name == "baseline" {
				oldTotal += elapsed
				require.Positive(t, counts.retained)
			} else {
				newTotal += elapsed
				require.Zero(t, counts.retained)
			}
		}
	}
	require.Less(t, newTotal, oldTotal, "controlled real authorization cost must decrease")
}

func TestIMAuthorizationMetadataDoesNotGrantBodyAccess(t *testing.T) {
	f := NewNormalReferencesHTTPFixtureForTest(t, false, &ragEntryModel{})
	reader := f.KBs.(interfaces.KnowledgeBaseAuthorizationReader)
	// Deny retained body reads to demonstrate the DTO never consumes them;
	// ordinary full-object reads must still fail rather than silently bypass it.
	require.NoError(t,
		f.DB.Callback().Query().Before("gorm:query").Register(
			"test:deny-retained-read",
			func(tx *gorm.DB) {
				if tx.Statement.Table == "original_body_field_heads" {
					_ = tx.AddError(fmt.Errorf("retained read denied"))
				}
			}))
	defer func() { require.NoError(t, f.DB.Callback().Query().Remove("test:deny-retained-read")) }()
	_, err := reader.GetKnowledgeBaseAuthorizationMetadata(f.Ctx, "kb")
	require.NoError(t, err)
	_, err = f.KBs.GetKnowledgeBaseByIDOnly(f.Ctx, "kb")
	require.Error(t, err)
	// Real repository uses the same selected-field contract independently.
	_,
		err = repository.NewKnowledgeBaseRepository(
		f.DB).(interfaces.KnowledgeBaseAuthorizationReader).GetKnowledgeBaseAuthorizationMetadata(
		f.Ctx,
		"kb")
	require.NoError(t, err)
}

func TestIMAuthorizationMetadataStructuralRevocation(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	pool, err := db.DB()
	require.NoError(t, err)
	pool.SetMaxOpenConns(1)
	defer func() { require.NoError(t, pool.Close()) }()
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7, Name: "fixture"}).Error)
	ctx := types.WithCaller(context.Background(),
		types.Caller{
			TenantID: 7,
			UserID:   "system-7",
			Role:     types.TenantRoleViewer,
		})
	shares := &fakeKBShareService{allowedKBs: map[string]bool{"kb": true}}
	svc := &sessionService{
		knowledgeBaseService: &knowledgeBaseService{
			repo: repository.NewKnowledgeBaseRepository(
				db),
		},
		kbShareService: shares,
	}
	require.NoError(t, svc.CheckIMChannelKnowledgeBase(ctx, 7, "kb"))
	require.NoError(t, db.Exec("UPDATE knowledge_bases SET tenant_id=8 WHERE id='kb'").Error)
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 7, "kb"))
	require.NoError(t, svc.CheckIMChannelKnowledgeBase(ctx, 8, "kb"))
	shares.allowedKBs["kb"] = false
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 8, "kb"))
	shares.allowedKBs["kb"] = true
	require.NoError(t, svc.CheckIMChannelKnowledgeBase(ctx, 8, "kb"))
	require.NoError(t, db.Exec("UPDATE knowledge_bases SET deleted_at=? WHERE id='kb'", time.Now()).Error)
	require.Error(t, svc.CheckIMChannelKnowledgeBase(ctx, 8, "kb"))
}
