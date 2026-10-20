package repository

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	sqlite_vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

var historyCandidateRegisterSQLiteVec sync.Once

type historyCandidateActualFixture struct {
	store *KnowledgeInputProvenanceStore
	db    *gorm.DB
	ctx   context.Context
	repo  *messageRepository
}

type historyCandidateActualChild struct {
	session *types.Session
	user    *types.Message
	answer  *types.Message
	child   *types.CommittedHistoryKnowledgeInput
}

func newHistoryCandidateActualFixture(t *testing.T) *historyCandidateActualFixture {
	t.Helper()
	historyCandidateRegisterSQLiteVec.Do(sqlite_vec.Auto)
	store, db, ctx, _, _, _ := displayTestFixture(t, false)
	ctx = context.WithValue(ctx, types.UserIDContextKey, types.CallerFromContext(ctx).UserID)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, db.AutoMigrate(&types.MessageArtifactRecord{}))
	for _, name := range []string{
		"000070_message_original_materials.up.sql",
		"000073_steer_original_materials.up.sql",
	} {
		script, err := os.ReadFile("../../../migrations/sqlite/" + name)
		require.NoError(t, err)
		require.NoError(t, db.Exec(string(script)).Error)
	}
	repo := NewMessageRepository(db).(*messageRepository)
	ConfigureMessageMaterialWriters(repo, store)
	require.NoError(
		t,
		db.Exec(`CREATE VIRTUAL TABLE history_candidate_test_vec USING vec0(embedding float[2] distance_metric=cosine);
		CREATE TABLE history_candidate_test_index(rowid INTEGER PRIMARY KEY,knowledge_id TEXT NOT NULL UNIQUE)`).Error,
	)
	return &historyCandidateActualFixture{store: store, db: db, ctx: ctx, repo: repo}
}

func (f *historyCandidateActualFixture) produce(t *testing.T, suffix, owner string) *historyCandidateActualChild {
	t.Helper()
	ctx := f.ctx
	if owner != types.CallerFromContext(ctx).UserID {
		require.NoError(
			t,
			f.db.Create(
				&types.User{ID: owner, Username: owner, Email: owner + "@history-candidate.test", IsActive: true},
			).Error,
		)
		require.NoError(
			t,
			f.db.Create(
				&types.TenantMember{
					UserID:   owner,
					TenantID: 7,
					Role:     types.TenantRoleAdmin,
					Status:   types.TenantMemberStatusActive,
				},
			).Error,
		)
		ctx = types.WithCaller(ctx, types.Caller{TenantID: 7, UserID: owner, Role: types.TenantRoleAdmin})
		ctx = types.WithPrincipal(ctx, types.Principal{Type: types.PrincipalWebUser, ID: owner})
		ctx = context.WithValue(ctx, types.UserIDContextKey, owner)
	}
	session := &types.Session{TenantID: 7, UserID: owner}
	require.NoError(t, f.db.Create(session).Error)
	lineage, err := types.NewCompleteSourceLineage()
	require.NoError(t, err)
	user := &types.Message{
		SessionID:     session.ID,
		RequestID:     "candidate-request-" + suffix,
		Role:          "user",
		Content:       "PRIVATE original authored question " + suffix,
		IsCompleted:   true,
		SourceLineage: lineage,
	}
	producer, err := f.store.WithMessageProducerInput(ctx, user, true)
	require.NoError(t, err)
	user, err = f.repo.CreateMessage(producer, user)
	require.NoError(t, err)
	// The actual service initializes an assistant placeholder with the valid
	// unknown envelope. A nil lineage would persist JSON null in its original
	// admission, which the strict decoder correctly rejects on completion.
	answer := &types.Message{
		SessionID:     session.ID,
		RequestID:     user.RequestID,
		Role:          "assistant",
		SourceLineage: types.UnknownSourceLineage(),
	}
	producer, err = f.store.WithMessageProducerInput(ctx, answer, true)
	require.NoError(t, err)
	answer, err = f.repo.CreateMessage(producer, answer)
	require.NoError(t, err)
	_, generation, err := f.store.MessageLiveControl(ctx, session.ID, answer.ID)
	require.NoError(t, err)
	initialRequest := types.MessageMaterialReadRequest{
		TenantID:   7,
		SessionID:  session.ID,
		MessageIDs: []string{user.ID},
	}
	initialPlan, err := f.store.PeekMessageMaterialReadPlan(ctx, initialRequest)
	require.NoError(t, err)
	initialRead, err := WithMessageMaterialReadPlan(ctx, initialPlan)
	require.NoError(t, err)
	initialSnapshot, err := f.store.CaptureMessageMaterialInputs(initialRead, initialRequest)
	require.NoError(t, err)
	require.Len(t, initialSnapshot.Proofs, 1)
	require.Len(t, initialSnapshot.Messages, 1)
	initial := initialSnapshot.Proofs[0]
	require.Equal(t, user.ID, initial.MessageID)
	require.NoError(t, initial.SourceLineage.RequireComplete())
	answer.Content = "PRIVATE original generated answer " + suffix
	answer.IsCompleted = true
	answer.SourceLineage, err = types.UnionSourceLineage(lineage, initial.SourceLineage)
	require.NoError(t, err)
	producer, err = f.store.WithMessageProducerInput(initialRead, answer, false)
	require.NoError(t, err)
	producer, err = f.store.WithMessageProducerAdvance(producer, answer)
	require.NoError(t, err)
	metadataParent := *initialSnapshot
	metadataParent.Messages = nil
	// Mirror the typed production completion boundary using the reader's
	// actual proof and live generation. No proof, receipt or admission is
	// constructed from guessed current row headers or inserted by the test.
	manifest := &types.MessageCompletionInputManifest{
		Version:               1,
		Recipe:                "history-initial-and-consumed-steer-v1",
		AssistantMessageID:    answer.ID,
		AssistantGenerationID: generation,
		SessionID:             initial.SessionID,
		RequestID:             answer.RequestID,
		OwnerID:               initial.OwnerID,
		TenantID:              initial.TenantID,
		OwnerRevision:         initial.OwnerRevision,
		Initial:               initial,
		Steers:                []*types.MessageCompletionSteerParent{},
		Inputs:                []*types.MessageMaterialSnapshot{&metadataParent},
		SourceLineage:         answer.SourceLineage,
	}
	producer, err = f.store.WithActualMessageCompletionInputs(producer, manifest)
	require.NoError(t, err)
	require.NoError(t, f.repo.UpdateMessage(producer, answer))
	var committed struct{ ParentManifest string }
	require.NoError(
		t,
		f.db.Table("message_completion_input_manifests c").
			Select("c.parent_manifest").
			Joins("JOIN message_material_receipts r ON r.admission_id=c.assistant_admission_id").
			Where("r.message_id=?", answer.ID).
			Take(&committed).
			Error,
	)
	var actualManifest types.MessageCompletionInputManifest
	require.NoError(t, json.Unmarshal([]byte(committed.ParentManifest), &actualManifest))
	require.Equal(t, initial.AdmissionID, actualManifest.Initial.AdmissionID)
	require.Equal(t, generation, actualManifest.AssistantGenerationID)
	require.Len(t, actualManifest.Inputs, 1)
	require.Nil(t, actualManifest.Inputs[0].Messages)

	// Actual repository producers mint all original receipts. The child is
	// created by the real committed-parent producer, never inserted or signed
	// from fabricated current-row headers or a test-made material proof.
	request, err := f.store.CommittedMessageParentRequest(ctx, 7, session.ID, answer.ID)
	require.NoError(t, err)
	plan, err := f.store.PeekMessageMaterialReadPlan(ctx, request)
	require.NoError(t, err)
	read, err := WithMessageMaterialReadPlan(ctx, plan)
	require.NoError(t, err)
	snapshot, err := f.store.CaptureMessageMaterialInputs(read, request)
	require.NoError(t, err)
	require.Len(t, snapshot.Messages, 2)
	child, err := f.store.CreateCommittedHistoryKnowledgeInput(
		read,
		types.CommittedHistoryKnowledgeInputRequest{
			TenantID:           7,
			KnowledgeBaseID:    "kb",
			SessionID:          session.ID,
			AssistantMessageID: answer.ID,
		},
	)
	require.NoError(t, err)
	// Only processing status is changed for the native candidate fixture;
	// this test does not claim DocumentProcess/model/indexing acceptance.
	require.NoError(
		t,
		f.db.Model(&types.Knowledge{}).
			Where("id=?", child.Knowledge.ID).
			Updates(map[string]any{"parse_status": types.ParseStatusCompleted, "enable_status": "enabled"}).
			Error,
	)
	return &historyCandidateActualChild{session: session, user: user, answer: child.Assistant, child: child}
}

func (f *historyCandidateActualFixture) index(t *testing.T, rowID int, childID string, vector []float32) {
	t.Helper()
	blob, err := sqlite_vec.SerializeFloat32(vector)
	require.NoError(t, err)
	require.NoError(
		t,
		f.db.Exec("INSERT INTO history_candidate_test_vec(rowid,embedding) VALUES(?,?)", rowID, blob).Error,
	)
	require.NoError(
		t,
		f.db.Exec("INSERT INTO history_candidate_test_index(rowid,knowledge_id) VALUES(?,?)", rowID, childID).Error,
	)
}

func (f *historyCandidateActualFixture) nativeTopOne(t *testing.T, ids []string) string {
	t.Helper()
	if len(ids) == 0 {
		return ""
	}
	blob, err := sqlite_vec.SerializeFloat32([]float32{1, 0})
	require.NoError(t, err)
	var hits []struct{ KnowledgeID string }
	// Execute the real vec0 prefilter, rather than a fake retriever or sorting
	// a supplied hit list in Go. Separate native adapter tests exercise its
	// full tenant/KB/status SQL; here the production owned whitelist is the
	// changed boundary, and the nearer rejected child must not consume k=1.
	require.NoError(t, f.db.Raw(`SELECT i.knowledge_id FROM history_candidate_test_vec v
		JOIN history_candidate_test_index i ON i.rowid=v.rowid
		WHERE v.embedding MATCH ? AND k=1 AND v.rowid IN
		(SELECT rowid FROM history_candidate_test_index WHERE knowledge_id IN ?)
		ORDER BY v.distance ASC`, blob, ids).Scan(&hits).Error)
	require.Len(t, hits, 1)
	return hits[0].KnowledgeID
}

type historyCandidateSQLRecorder struct {
	gormlogger.Interface
	statements []string
}

func (r *historyCandidateSQLRecorder) Trace(_ context.Context, _ time.Time, query func() (string, int64), _ error) {
	sql, _ := query()
	r.statements = append(r.statements, sql)
}

func (f *historyCandidateActualFixture) owned(t *testing.T, target, blocker *historyCandidateActualChild) []string {
	t.Helper()
	recorder := &historyCandidateSQLRecorder{Interface: f.db.Logger}
	f.db.Logger = recorder
	defer func() { f.db.Logger = recorder.Interface }()
	ids, err := f.store.OwnedHistoryKnowledgeIDs(
		f.ctx,
		7,
		"kb",
		types.SessionOwnerIDFromContext(f.ctx),
		[]string{target.session.ID, blocker.session.ID},
	)
	require.NoError(t, err)
	selected := false
	for _, sql := range recorder.statements {
		if strings.Contains(sql, "SELECT a.knowledge_id") {
			selected = true
			require.NotContains(t, sql, "PRIVATE")
			for _, bodyColumn := range []string{
				".content",
				"initial_input",
				"rendered_content",
				"context_checkpoint",
				"parent_materials",
				".title",
			} {
				require.NotContains(t, strings.ToLower(sql), bodyColumn)
			}
		}
	}
	require.True(t, selected, "execute the production metadata whitelist query")
	return ids
}

func TestHistoryKnowledgeCandidateActualSQLiteOriginalControlsBeforeNativeTopK(t *testing.T) {
	f := newHistoryCandidateActualFixture(t)
	owner := types.SessionOwnerIDFromContext(f.ctx)
	target := f.produce(t, "valid-target", owner)
	f.index(t, 1, target.child.Knowledge.ID, []float32{0.8, 0.6})
	mutations := []string{
		"owner-replaced",
		"owner-ABA",
		"assistant-receipt-other",
		"user-receipt-other",
		"assistant-receipt-missing",
		"user-receipt-missing",
		"assistant-old-origin-admission",
		"assistant-content-ABA",
		"user-content-ABA",
		"assistant-request-ABA",
		"user-request-ABA",
		"assistant-child-pointer-ABA",
	}
	for i, mutation := range mutations {
		t.Run(mutation, func(t *testing.T) {
			originalOwner := owner
			if mutation == "owner-replaced" {
				originalOwner = "other-original-owner"
			}
			blocker := f.produce(t, mutation, originalOwner)
			f.index(t, i+2, blocker.child.Knowledge.ID, []float32{1, 0})
			require.Equal(
				t,
				blocker.child.Knowledge.ID,
				f.nativeTopOne(t, []string{target.child.Knowledge.ID, blocker.child.Knowledge.ID}),
				"the rejected original is genuinely nearer in the real native index",
			)
			switch mutation {
			case "owner-replaced":
				require.NoError(t, f.db.Model(blocker.session).Update("user_id", owner).Error)
			case "owner-ABA":
				require.NoError(t, f.db.Model(blocker.session).Update("user_id", "transient-other-owner").Error)
				require.NoError(t, f.db.Model(blocker.session).Update("user_id", owner).Error)
			case "assistant-receipt-other", "user-receipt-other":
				message, other := blocker.answer, target.answer
				if mutation == "user-receipt-other" {
					message, other = blocker.user, target.user
				}
				var receipt messageMaterialReceipt
				require.NoError(t, f.db.Where("message_id=?", other.ID).Take(&receipt).Error)
				require.NoError(
					t,
					f.db.Model(&messageMaterialReceipt{}).
						Where("message_id=?", message.ID).
						Update("admission_id", receipt.AdmissionID).
						Error,
				)
			case "assistant-receipt-missing", "user-receipt-missing":
				message := blocker.answer
				if mutation == "user-receipt-missing" {
					message = blocker.user
				}
				require.NoError(t, f.db.Where("message_id=?", message.ID).Delete(&messageMaterialReceipt{}).Error)
			case "assistant-old-origin-admission":
				var admission messageMaterialAdmission
				require.NoError(
					t,
					f.db.Where("message_id=?", blocker.answer.ID).Order("revision ASC").Take(&admission).Error,
				)
				require.NoError(
					t,
					f.db.Model(&messageMaterialReceipt{}).
						Where("message_id=?", blocker.answer.ID).
						Update("admission_id", admission.ID).
						Error,
				)
			case "assistant-content-ABA", "user-content-ABA", "assistant-request-ABA", "user-request-ABA":
				message, field, original := blocker.answer, "content", blocker.answer.Content
				if strings.HasPrefix(mutation, "user-") {
					message, original = blocker.user, blocker.user.Content
				}
				if strings.Contains(mutation, "request") {
					field, original = "request_id", message.RequestID
				}
				require.NoError(
					t,
					f.db.Model(&types.Message{}).
						Where("id=?", message.ID).
						Update(field, "transient foreign original").
						Error,
				)
				require.NoError(t, f.db.Model(&types.Message{}).Where("id=?", message.ID).Update(field, original).Error)
			case "assistant-child-pointer-ABA":
				require.NoError(
					t,
					f.db.Model(&types.Message{}).
						Where("id=?", blocker.answer.ID).
						Update("knowledge_id", target.child.Knowledge.ID).
						Error,
				)
				require.NoError(
					t,
					f.db.Model(&types.Message{}).
						Where("id=?", blocker.answer.ID).
						Update("knowledge_id", blocker.child.Knowledge.ID).
						Error,
				)
			}
			ids := f.owned(t, target, blocker)
			require.Equal(t, []string{target.child.Knowledge.ID}, ids)
			require.Equal(
				t,
				target.child.Knowledge.ID,
				f.nativeTopOne(t, ids),
				"invalid immutable parents must be excluded before native TopK",
			)
			plans, err := f.store.PeekOwnedHistoryKnowledgeParentPlans(
				f.ctx,
				7,
				"kb",
				owner,
				[]string{target.child.Knowledge.ID, blocker.child.Knowledge.ID},
				[]string{target.session.ID, blocker.session.ID},
			)
			require.NoError(t, err)
			require.Len(t, plans, 1)
			require.Equal(t, target.child.Knowledge.ID, plans[0].KnowledgeID)
		})
	}
}

func TestHistoryKnowledgeCandidateActualSQLiteOwnChildPointerAndRawUserFieldsRemainUsable(t *testing.T) {
	f := newHistoryCandidateActualFixture(t)
	owner := types.SessionOwnerIDFromContext(f.ctx)
	target := f.produce(t, "own-child-pointer", owner)
	other := f.produce(t, "other-valid-session", owner)
	ids := f.owned(t, target, other)
	require.ElementsMatch(
		t,
		[]string{target.child.Knowledge.ID, other.child.Knowledge.ID},
		ids,
		"the real producer's child-pointer write must remain admitted",
	)
	var rawBefore, rawAfter uint64
	require.NoError(
		t,
		f.db.Table("message_material_versions").
			Select("revision").
			Where("message_id=?", target.user.ID).
			Scan(&rawBefore).
			Error,
	)
	// These generated fields are not consumed by a raw-user parent read.
	// Their separate versions may advance; the original authored admission
	// and the immutable history producer's raw revision must remain intact.
	require.NoError(
		t,
		f.db.Model(&types.Message{}).
			Where("id=?", target.user.ID).
			Updates(map[string]any{"rendered_content": "unconsumed generated expansion", "images": types.MessageImages{{
				Caption: "unconsumed generated caption",
			}}}).
			Error,
	)
	require.NoError(
		t,
		f.db.Table("message_material_versions").
			Select("revision").
			Where("message_id=?", target.user.ID).
			Scan(&rawAfter).
			Error,
	)
	require.Equal(t, rawBefore, rawAfter)
	require.ElementsMatch(t, ids, f.owned(t, target, other))
	plans, err := f.store.PeekOwnedHistoryKnowledgeParentPlans(
		f.ctx,
		7,
		"kb",
		owner,
		ids,
		[]string{target.session.ID, other.session.ID},
	)
	require.NoError(t, err)
	require.Len(t, plans, 2)
	for _, plan := range plans {
		read, err := WithMessageMaterialReadPlan(f.ctx, plan.MessagePlan)
		require.NoError(t, err)
		snapshot, err := f.store.CaptureMessageMaterialInputs(read, plan.MessagePlan.Request)
		require.NoError(t, err)
		for _, message := range snapshot.Messages {
			if message.Role == "user" {
				require.Empty(t, message.RenderedContent)
				require.Empty(t, message.Images)
			}
		}
	}
}
