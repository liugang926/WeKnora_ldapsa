package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/access"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/application/service"
	sessionhandler "github.com/Tencent/WeKnora/internal/handler/session"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/stream"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const periodicDirectoryGroupGUID = "11223344-5566-7788-99aa-bbccddeeff00"

type directoryPeriodicHTTPModel struct {
	t                  *testing.T
	f                  *service.NormalReferencesHTTPFixtureForTest
	stream             *normalReferencesObservedStreamForTest
	apply              func(context.Context, bool, bool) error
	unlink             func(context.Context) error
	mode               string
	abandon            chan struct{}
	transportReturned  chan struct{}
	calls              atomic.Int64
	inputFound         atomic.Bool
	stopped            atomic.Bool
	syncedEpochs       atomic.Int64
	faultApplied       atomic.Bool
	sourceCallsAtFault atomic.Int64
	mu                 sync.Mutex
	operationErr       error
}

func (m *directoryPeriodicHTTPModel) GetModelID() string   { return "normal-reference-model" }
func (m *directoryPeriodicHTTPModel) GetModelName() string { return "actual periodic directory stream" }

func (m *directoryPeriodicHTTPModel) Chat(
	context.Context,
	[]chat.Message,
	*chat.ChatOptions,
) (*types.ChatResponse, error) {
	return nil, fmt.Errorf("periodic directory test must enter the real final streaming producer")
}

func (m *directoryPeriodicHTTPModel) recordError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.operationErr = err
}

func (m *directoryPeriodicHTTPModel) ChatStream(
	ctx context.Context,
	input []chat.Message,
	_ *chat.ChatOptions,
) (<-chan types.StreamResponse, error) {
	m.calls.Add(1)
	if err := types.CheckHistoryMaterialLifetime(ctx); err != nil {
		return nil, err
	}
	for _, message := range input {
		require.Equal(
			m.t,
			m.f.Lineage,
			message.SourceLineage,
			"the actual collector still uses the captured complete original source union",
		)
		if strings.Contains(message.Content, "Current employee policy") {
			m.inputFound.Store(true)
		}
	}
	if !m.inputFound.Load() {
		return nil, fmt.Errorf("actual search/summary/merge did not consume the original signed source")
	}
	for _, table := range []string{"original_body_leases", "nextcloud_content_leases"} {
		var held int64
		if err := m.f.DB.Table(table).Where("kind='read' AND released_at_ms IS NULL").Count(&held).Error; err != nil {
			return nil, err
		}
		if held == 0 {
			return nil, fmt.Errorf("original physical %s were not held at model entry", table)
		}
	}
	out := make(chan types.StreamResponse)
	go func() {
		defer close(out)
		defer close(m.transportReturned)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		// Observe actual native Redis polls after the pre-answer producer's
		// queued references/progress drain. No timer creates EOF or Done.
		for m.stream.consecutiveEmptyPolls.Load() < 3 {
			select {
			case <-ctx.Done():
				m.stopped.Store(true)
				return
			case <-m.abandon:
				return // failed-test transport cleanup only
			case <-ticker.C:
			}
		}
		for epoch := 1; epoch <= 2; epoch++ {
			if err := m.apply(ctx, true, true); err != nil {
				m.recordError(err)
				return
			}
			m.syncedEpochs.Add(1)
			if m.mode == "unchanged_complete_sync" {
				select {
				case out <- types.StreamResponse{
					ResponseType: types.ResponseTypeAnswer,
					Content:      fmt.Sprintf("CURRENT PERIODIC DIRECTORY EPOCH %d; ", epoch),
				}:
				case <-ctx.Done():
					m.stopped.Store(true)
					return
				case <-m.abandon:
					return
				}
			}
			// Keep the provider open while real SSE polls continue after each
			// committed complete epoch. Negative modes emit no answer bytes.
			before := m.stream.emptyPolls.Load()
			for m.stream.emptyPolls.Load()-before < 3 {
				select {
				case <-ctx.Done():
					m.stopped.Store(true)
					return
				case <-m.abandon:
					return
				case <-ticker.C:
				}
			}
		}
		if m.mode == "unchanged_complete_sync" {
			// Done occurs only after the two real open-provider sync windows;
			// it is an actual authorized model result, not a fake completion.
			select {
			case out <- types.StreamResponse{
				ResponseType: types.ResponseTypeAnswer,
				Content:      "CURRENT PERIODIC DIRECTORY FINAL",
				Done:         true,
			}:
			case <-ctx.Done():
				m.stopped.Store(true)
			case <-m.abandon:
			}
			return
		}
		var err error
		switch m.mode {
		case "disabled":
			err = m.apply(ctx, false, true)
		case "out_of_scope":
			err = m.apply(ctx, true, false)
		case "unlinked":
			err = m.unlink(ctx)
		default:
			err = fmt.Errorf("unknown periodic directory fault")
		}
		if err != nil {
			m.recordError(err)
			return
		}
		m.sourceCallsAtFault.Store(m.f.SourceAuthorizeCalls())
		m.faultApplied.Store(true)
		// The real current-source/material refusal must stop this indefinitely
		// open provider. No model Done/EOF/timer/client-cancel causes success.
		select {
		case <-ctx.Done():
			m.stopped.Store(true)
		case <-m.abandon:
		}
	}()
	return out, nil
}

func runDirectoryPeriodicActualHTTP(t *testing.T, pg bool) {
	for _, mode := range []string{"unchanged_complete_sync", "disabled", "out_of_scope", "unlinked"} {
		t.Run(mode, func(t *testing.T) {
			model := &directoryPeriodicHTTPModel{
				t:                 t,
				mode:              mode,
				abandon:           make(chan struct{}),
				transportReturned: make(chan struct{}),
			}
			f := service.NewNormalReferencesHTTPFixtureForTest(t, pg, model, "directory-periodic")
			model.f = f
			// This mode installs the real 28/109 directory schema and auxiliary
			// auth table before creating any ordinary message. It never creates
			// or resigns message/source originals through setup SQL.
			directories := repository.NewDirectoryRepository(f.DB)
			directoryService := service.NewDirectoryService(directories)
			links, err := directories.GetIdentityByUserID(f.Ctx, f.Session.UserID)
			require.NoError(t, err)
			require.Len(t, links, 1)
			identity := *links[0]
			capturedCaller := types.CallerFromContext(f.Ctx)
			capturedPrincipal, ok := types.PrincipalFromContext(f.Ctx)
			require.True(t, ok)
			require.Equal(t, f.Session.UserID, capturedCaller.UserID)
			apply := func(ctx context.Context, enabled, present bool) error {
				snapshot := &types.DirectorySnapshot{
					DirectoryID:        identity.DirectoryID,
					Complete:           true,
					PaginationComplete: true,
					StartedAt:          time.Now().UTC(),
					Trigger:            types.DirectorySyncTriggerScheduled,
					Groups: []types.DirectoryGroupSnapshot{
						{
							ObjectGUID:     periodicDirectoryGroupGUID,
							ObjectSID:      "S-1-5-21-1000-2000",
							DN:             "cn=Periodic Readers,dc=example,dc=test",
							SAMAccountName: "periodic-readers",
						},
					},
				}
				if present {
					snapshot.Identities = []types.DirectoryIdentitySnapshot{
						{
							ObjectGUID:     identity.ObjectGUID,
							ObjectSID:      "S-1-5-21-1000-1000",
							DN:             "cn=Alice,dc=example,dc=test",
							SAMAccountName: "alice",
							Enabled:        enabled,
						},
					}
					snapshot.Memberships = []types.DirectoryMembershipSnapshot{
						{
							GroupObjectGUID: periodicDirectoryGroupGUID,
							UserObjectGUID:  identity.ObjectGUID,
							Direct:          true,
							Source:          types.DirectoryMembershipDirect,
						},
					}
				}
				_, err := directoryService.ApplySnapshot(ctx, snapshot)
				return err
			}
			require.NoError(
				t,
				apply(f.Ctx, true, true),
				"baseline complete directory input is applied through the production service",
			)
			_, baseline, err := directories.GetUserLoginSnapshot(f.Ctx, f.Session.UserID)
			require.NoError(t, err)
			require.NotNil(t, baseline)
			require.NotNil(t, baseline.Identity)
			require.Equal(t, identity.ID, baseline.Identity.ID)
			require.Equal(t, []string{periodicDirectoryGroupGUID}, baseline.EffectiveGroupObjectGUIDs)
			groups, err := directories.ListGroups(f.Ctx, identity.DirectoryID, "", 0, 10)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			groupRepo := repository.NewGroupAccessRepository(f.DB)
			grant := &types.TenantGroupRoleGrant{
				TenantID:         7,
				DirectoryGroupID: groups[0].ID,
				Role:             types.TenantRoleViewer,
				Origin:           types.GrantOriginDirectory,
				CreatedBy:        f.Session.UserID,
			}
			_, err = groupRepo.UpsertTenantGroupRoleGrant(f.Ctx, grant)
			require.NoError(t, err)
			model.apply = apply
			model.unlink = func(ctx context.Context) error { return directoryService.UnlinkIdentity(ctx, identity.ID) }
			mini := miniredis.RunT(t)
			redis, err := stream.NewRedisStreamManager(mini.Addr(), "", "", 0, "periodic-directory-actual", time.Minute)
			require.NoError(t, err)
			observed := &normalReferencesObservedStreamForTest{
				RedisStreamManager: redis,
				watcher:            make(chan context.Context, 1),
			}
			model.stream = observed
			h := sessionhandler.NewHandler(
				f.Sessions,
				f.Messages,
				nil,
				observed,
				f.Config,
				f.KBs,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				f.Models,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.Use(middleware.ErrorHandler())
			router.Use(func(c *gin.Context) {
				c.Request = c.Request.WithContext(f.Ctx)
				c.Set(types.TenantIDContextKey.String(), uint64(7))
				c.Set(types.RequestIDContextKey.String(), "periodic-directory-actual-request")
				c.Next()
			})
			router.POST("/knowledge-chat/:session_id", h.KnowledgeQA)
			httpReturned := make(chan time.Time, 1)
			server := httptest.NewServer(
				http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						defer func() { httpReturned <- time.Now() }()
						router.ServeHTTP(w, r)
					},
				),
			)
			t.Cleanup(func() {
				// Failed-test transport cleanup runs before httptest.Close, and
				// is never counted as production cancellation/lease termination.
				close(model.abandon)
				f.Cancel()
				server.CloseClientConnections()
				server.Close()
				_ = redis.Close()
			})
			// Keep a bounded handler deadline with enough headroom for shared
			// CI runners, while still failing a hung SSE response.
			client := &http.Client{Timeout: 30 * time.Second}
			started := time.Now()
			response, err := client.Post(
				server.URL+"/knowledge-chat/"+f.Session.ID,
				"application/json",
				strings.NewReader(
					("{\"query\":\"current employee policy\",\"knowledge_base_ids\":[\"kb\"],\"s" +
						"ummary_model_id\":\"normal-reference-model\",\"disable_title\":true}"),
				),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, response.Body.Close()) }()
			var body bytes.Buffer
			_, err = body.ReadFrom(response.Body)
			assert.NoError(t, err, "the real original SSE response must terminate within the bounded HTTP send")
			assert.Equal(t, http.StatusOK, response.StatusCode)
			select {
			case returnedAt := <-httpReturned:
				assert.Less(t, returnedAt.Sub(started), 25*time.Second)
			case <-time.After(5 * time.Second):
				assert.Fail(t, "actual HTTP handler did not actually return before final oracle")
			}
			wait := func(ch <-chan struct{}, name string) {
				select {
				case <-ch:
				case <-time.After(3 * time.Second):
					assert.Fail(t, name+" did not actually return before final oracle")
				}
			}
			wait(f.ProducerReturned, "actual KnowledgeQA producer")
			wait(model.transportReturned, "actual provider transport")
			assert.Eventually(
				t,
				func() bool {
					var source, bodies int64
					return f.DB.Table("nextcloud_content_leases").
						Where("kind='read' AND released_at_ms IS NULL").
						Count(&source).
						Error == nil &&
						f.DB.Table("original_body_leases").
							Where("kind='read' AND released_at_ms IS NULL").
							Count(&bodies).
							Error == nil &&
						source == 0 &&
						bodies == 0
				},
				3*time.Second,
				10*time.Millisecond,
				"actual producer/sender release all original pins before fixture cleanup",
			)
			model.mu.Lock()
			operationErr := model.operationErr
			model.mu.Unlock()
			assert.NoError(t, operationErr, "genuine ApplySnapshot/unlink completed, not a setup fault")
			assert.EqualValues(t, 2, model.syncedEpochs.Load())
			assert.EqualValues(t, 1, model.calls.Load())
			assert.True(t, model.inputFound.Load())
			assert.Positive(t, observed.emptyPolls.Load())
			assert.Positive(t, f.SourceAuthorizeCalls(), "actual paired signed source HTTP was used")
			assert.Equal(t, capturedCaller, types.CallerFromContext(f.Ctx))
			principal, present := types.PrincipalFromContext(f.Ctx)
			assert.True(t, present)
			assert.Equal(t, capturedPrincipal, principal)
			var completed, manifests int64
			require.NoError(
				t,
				f.DB.Model(&types.Message{}).
					Where("session_id=? AND role='assistant' AND is_completed=true", f.Session.ID).
					Count(&completed).
					Error,
			)
			require.NoError(
				t,
				f.DB.Table("message_completion_input_manifests c").
					Joins("JOIN message_material_receipts r ON r.admission_id=c.assistant_admission_id").
					Joins("JOIN messages m ON m.id=r.message_id").
					Where("m.session_id=? AND m.role='assistant'", f.Session.ID).
					Count(&manifests).
					Error,
			)
			text := body.String()
			if mode == "unchanged_complete_sync" {
				assert.EqualValues(t, 1, completed)
				assert.EqualValues(t, 1, manifests)
				assert.Contains(t, text, "CURRENT PERIODIC DIRECTORY EPOCH 1")
				assert.Contains(t, text, "CURRENT PERIODIC DIRECTORY EPOCH 2")
				assert.Contains(t, text, "CURRENT PERIODIC DIRECTORY FINAL")
				assert.Contains(t, text, "\"response_type\":\"complete\"")
				assert.NotContains(t, text, "\"response_type\":\"error\"")
				_, after, err := directories.GetUserLoginSnapshot(f.Ctx, f.Session.UserID)
				require.NoError(t, err)
				require.NotNil(t, after)
				require.NotNil(t, after.Identity)
				assert.Equal(t, baseline.Directory.SnapshotVersion+2, after.Directory.SnapshotVersion)
				assert.Equal(t, baseline.Identity.ID, after.Identity.ID)
				assert.Equal(t, baseline.Identity.ObjectGUID, after.Identity.ObjectGUID)
				assert.Equal(t, baseline.Identity.UserID, after.Identity.UserID)
				assert.Equal(t, baseline.EffectiveGroupObjectGUIDs, after.EffectiveGroupObjectGUIDs)
				currentGrants, err := groupRepo.ListTenantGroupRoleGrants(f.Ctx, 7)
				require.NoError(t, err)
				require.Len(t, currentGrants, 1)
				assert.Equal(t, grant.ID, currentGrants[0].ID)
				assert.Equal(t, grant.Role, currentGrants[0].Role)
				var assistant types.Message
				require.NoError(t, f.DB.Where("session_id=? AND role='assistant'", f.Session.ID).Take(&assistant).Error)
				assert.Equal(t, f.Lineage, assistant.SourceLineage)
			} else {
				assert.True(t, model.faultApplied.Load())
				assert.True(t, model.stopped.Load(), "only actual model ctx cancellation counts as stop")
				assert.Zero(t, completed)
				assert.Zero(t, manifests)
				assert.NotContains(t, text, "CURRENT PERIODIC DIRECTORY")
				assert.NotContains(t, text, "\"response_type\":\"answer\"")
				assert.NotContains(t, text, "\"response_type\":\"complete\"")
				assert.Contains(t, text, "\"response_type\":\"error\"")
				for _, completionErr := range f.CompletionErrors() {
					assert.ErrorIs(t, completionErr, access.ErrNextcloudPublicationDenied)
				}
				// Calls racing an already-started check are recorded, not called
				// evidence of post-withdrawal exposure. Final failed production
				// leaves no source-bearing frame/completed body/manifest.
				t.Logf(
					("safe periodic metadata: epochs=%d source_calls_at_fault=%d source_calls_te" +
						"rminal=%d empty_polls=%d"),
					model.syncedEpochs.Load(),
					model.sourceCallsAtFault.Load(),
					f.SourceAuthorizeCalls(),
					observed.emptyPolls.Load(),
				)
			}
		})
	}
}

func TestDirectoryPeriodicSyncActualHTTPSQLite(t *testing.T) {
	runDirectoryPeriodicActualHTTP(t, false)
}

func TestPostgresDirectoryPeriodicSyncActualHTTP(t *testing.T) {
	runDirectoryPeriodicActualHTTP(t, true)
}
